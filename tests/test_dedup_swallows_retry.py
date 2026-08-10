"""Regression: the one-line dedup that eats your retries.

This is the failure that motivated the whole two-phase design. It is written
twice on purpose — once against the naive implementation to show the bug is real,
once against ``Idempotency`` to show it is gone.
"""

from __future__ import annotations

import asyncio

import pytest

from burstq.dedup import ClaimResult, Idempotency


class NaiveDedup:
    """What almost everyone writes first: one SET NX, no notion of 'finished'."""

    def __init__(self, redis, keys, ttl: int = 30) -> None:
        self._redis = redis
        self._keys = keys
        self._ttl = ttl

    async def seen_before(self, event_id: str) -> bool:
        acquired = await self._redis.set(
            self._keys.idempotency(event_id), "1", nx=True, ex=self._ttl
        )
        return acquired is None


async def test_naive_dedup_loses_a_message_when_the_worker_dies(redis, keys):
    """Demonstrates the bug: a crash mid-handler turns the retry into a no-op."""
    naive = NaiveDedup(redis, keys)
    event_id = "evt-1"
    handled: list[str] = []

    # First delivery: claims the key, then the process dies before handling.
    assert await naive.seen_before(event_id) is False
    # ... crash here. Nothing was handled, nothing was acked.

    # Redis Streams correctly re-delivers the entry. The retry asks the same
    # question and gets the wrong answer.
    if not await naive.seen_before(event_id):
        handled.append(event_id)

    assert handled == [], "expected the naive version to swallow the retry"
    # The message is now lost: the retry believed it was a duplicate and would ack.


async def test_two_phase_claim_survives_a_worker_death(redis, keys, settings):
    """The fix: an uncommitted claim expires, so the retry gets a clean shot."""
    idem = Idempotency(redis, keys, settings)
    event_id = "evt-1"
    handled: list[str] = []

    assert await idem.claim(event_id) is ClaimResult.ACQUIRED
    # ... crash before commit(). processing_ttl is 2s in the test settings.
    await asyncio.sleep(settings.processing_ttl + 0.3)

    result = await idem.claim(event_id)
    assert result is ClaimResult.ACQUIRED, "retry must be allowed to reprocess"
    handled.append(event_id)
    await idem.commit(event_id)

    assert handled == [event_id]


async def test_release_lets_the_retry_run_immediately(redis, keys, settings):
    """A handled failure should not cost a whole processing_ttl of silence."""
    idem = Idempotency(redis, keys, settings)

    assert await idem.claim("evt-2") is ClaimResult.ACQUIRED
    await idem.release("evt-2")

    assert await idem.claim("evt-2") is ClaimResult.ACQUIRED


async def test_committed_work_is_a_real_duplicate(redis, keys, settings):
    idem = Idempotency(redis, keys, settings)

    assert await idem.claim("evt-3") is ClaimResult.ACQUIRED
    await idem.commit("evt-3")

    assert await idem.claim("evt-3") is ClaimResult.ALREADY_DONE
    assert await idem.state("evt-3") == "done"


async def test_concurrent_delivery_is_reported_as_in_flight(redis, keys, settings):
    idem = Idempotency(redis, keys, settings)

    assert await idem.claim("evt-4") is ClaimResult.ACQUIRED
    assert await idem.claim("evt-4") is ClaimResult.IN_FLIGHT


async def test_missing_event_id_is_never_collapsed(redis, keys, settings):
    """No identifier means no idempotency guarantee — treat each as distinct
    rather than merging unrelated events under an empty key."""
    idem = Idempotency(redis, keys, settings)

    assert await idem.claim("") is ClaimResult.ACQUIRED
    assert await idem.claim("") is ClaimResult.ACQUIRED


@pytest.mark.parametrize("bad_ttls", [(60.0, 30.0), (60.0, 60.0)])
def test_config_rejects_a_dedup_window_shorter_than_the_claim(bad_ttls):
    from burstq import ConfigError, Settings

    processing, dedup = bad_ttls
    with pytest.raises(ConfigError, match="dedup_ttl"):
        Settings(processing_ttl=processing, dedup_ttl=dedup)

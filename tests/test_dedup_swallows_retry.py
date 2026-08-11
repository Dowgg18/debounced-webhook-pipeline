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


async def test_a_claim_that_outlives_redelivery_loses_the_message(redis, keys, settings):
    """Why processing_ttl must be shorter than claim_idle_seconds.

    Two phases fixed the *mechanism* of the swallowed retry. This is the same
    loss reached through timing instead: if the dead worker's claim is still
    alive when recovery redelivers the entry, the retry reads IN_FLIGHT, assumes
    a healthy peer is on it, acks, and the message is gone.

    Built on a stub rather than ``Settings`` because Settings now refuses this
    combination outright — which is the point of the test below it. The stub is
    how the configuration could look before the guard existed.
    """
    from types import SimpleNamespace

    # A claim that lasts far longer than the redelivery window.
    long_claim = SimpleNamespace(processing_ttl=999.0, dedup_ttl=10_000.0)
    idem = Idempotency(redis, keys, long_claim)

    assert await idem.claim("evt-timing") is ClaimResult.ACQUIRED
    # ... worker dies here, having committed nothing.

    # Recovery redelivers. The retry asks the same question.
    verdict = await idem.claim("evt-timing")

    assert verdict is ClaimResult.IN_FLIGHT, (
        "the dead worker's claim is still standing, so the retry defers to a peer "
        "that no longer exists — and the consumer acks on IN_FLIGHT"
    )


def test_config_refuses_a_claim_that_outlives_redelivery():
    from burstq import ConfigError, Settings

    with pytest.raises(ConfigError, match="processing_ttl"):
        Settings(processing_ttl=300.0, claim_idle_seconds=120.0, dedup_ttl=3600.0)

    with pytest.raises(ConfigError, match="processing_ttl"):
        Settings(processing_ttl=120.0, claim_idle_seconds=120.0, dedup_ttl=3600.0)

    # The defaults were already safe; now they are guarded rather than lucky.
    ok = Settings()
    assert ok.processing_ttl < ok.claim_idle_seconds

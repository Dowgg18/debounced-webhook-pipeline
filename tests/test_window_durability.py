"""Regression: a debounce window must survive the process that opened it.

The sleeping-coroutine version of a debouncer keeps its schedule in one process's
memory. Deploy during the window and the buffered messages remain in Redis with
nothing left to drain them. Here the deadline is data, so a fresh process picks
the window up and finishes the job.

The second half covers the other half of that bug: while a coroutine sleeps, the
stream entry it came from stays *pending*, and crash recovery eventually steals
work that is still running. The fix is structural — the consumer acks in
milliseconds — plus a config invariant that refuses the dangerous combination.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import RecordingHandler, delivery

from burstq import ConfigError, Pipeline, Settings


async def test_a_restart_does_not_lose_an_open_window(redis, settings, adapter):
    lost_handler = RecordingHandler()
    first = Pipeline(redis, adapter, lost_handler, settings)

    # Process #1 buffers a burst and dies before the window closes.
    await first.start()
    try:
        await first.accept(delivery("c1", "half a sentence"))
        await asyncio.sleep(0.1)  # < window_seconds
    finally:
        await first.stop()

    assert lost_handler.calls == [], "window closed too early for this test to mean anything"

    # Process #2 starts fresh. It has no memory of process #1.
    recovered_handler = RecordingHandler()
    second = Pipeline(redis, adapter, recovered_handler, settings)
    await second.start()
    try:
        await asyncio.sleep(1.2)
    finally:
        await second.stop()

    assert len(recovered_handler.calls) == 1
    assert recovered_handler.calls[0][1] == "half a sentence"


async def test_the_stream_entry_is_acked_before_the_window_closes(redis, settings, adapter):
    """The consumer's work is milliseconds; nothing stays pending for the window.

    If this regresses, ``pending`` stays > 0 for the whole window and crash
    recovery starts competing with live handlers.
    """
    handler = RecordingHandler()
    pipeline = Pipeline(redis, adapter, handler, settings)

    await pipeline.start()
    try:
        await pipeline.accept(delivery("c2", "buffered"))
        await asyncio.sleep(0.15)  # inside the window, before the flush

        health = await pipeline.health()
        assert health["pending"] == 0, (
            "the stream entry is still pending while the window is open — "
            "the debounce sleep moved back into the consumer"
        )
        assert health["open_windows"] == 1

        await asyncio.sleep(1.0)
    finally:
        await pipeline.stop()

    assert len(handler.calls) == 1


async def test_a_failing_handler_leaves_the_burst_for_a_retry(redis, settings, adapter):
    attempts: list[int] = []

    async def flaky(burst) -> None:
        attempts.append(burst.size)
        if len(attempts) == 1:
            raise RuntimeError("downstream is having a moment")

    pipeline = Pipeline(redis, adapter, flaky, settings)
    # Lease short enough that the retry lands inside the test.
    pipeline.flusher._settings = settings  # noqa: SLF001 - explicit in-test wiring

    await pipeline.start()
    try:
        await pipeline.accept(delivery("c3", "important"))
        await asyncio.sleep(0.6)
        assert len(attempts) == 1

        # The window was NOT discarded, so a manual re-flush finds it intact.
        assert await pipeline.flusher.flush_one("c3") is True
    finally:
        await pipeline.stop()

    assert len(attempts) == 2, "the burst was dropped instead of retried"


async def test_a_handled_burst_is_removed_exactly_once(redis, settings, adapter):
    handler = RecordingHandler()
    pipeline = Pipeline(redis, adapter, handler, settings)

    await pipeline.accept(delivery("c4", "done"))
    await pipeline.start()
    try:
        await asyncio.sleep(1.2)
    finally:
        await pipeline.stop()

    assert len(handler.calls) == 1
    assert await pipeline.window.open_windows() == 0
    assert await redis.exists(pipeline.keys.window("c4")) == 0


def test_config_refuses_a_recovery_window_that_fights_live_work():
    """max_window + handler_timeout must fit inside claim_idle_seconds.

    In the original system this relationship existed only as a comment, in a
    different file from the two constants it constrained.
    """
    with pytest.raises(ConfigError, match="claim_idle_seconds"):
        Settings(
            window_seconds=5,
            max_window_seconds=30,
            handler_timeout=20,
            claim_idle_seconds=30,  # 30 <= 30 + 20
        )

    # The same numbers with honest recovery timing are accepted.
    Settings(
        window_seconds=5,
        max_window_seconds=30,
        handler_timeout=20,
        claim_idle_seconds=120,
    )


def test_config_refuses_a_lock_that_expires_under_its_handler():
    with pytest.raises(ConfigError, match="lock_timeout"):
        Settings(handler_timeout=60, lock_timeout=30)

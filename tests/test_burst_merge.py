"""The behaviour the whole pipeline exists for: N messages, one handler call.

``test_burst_merge_across_reads`` is the guard against "simplifying" the consumer
loop into an ``await asyncio.gather(...)``. With an awaited loop, the second
message is not even *read* until the first window has closed, so the two never
merge and the handler fires twice. Everything else in this file would still pass.
"""

from __future__ import annotations

import asyncio

from conftest import RecordingHandler, delivery

from burstq import Pipeline


async def _run(pipeline: Pipeline, seconds: float) -> None:
    await pipeline.start()
    try:
        await asyncio.sleep(seconds)
    finally:
        await pipeline.stop()


async def test_three_messages_become_one_burst(redis, settings, adapter):
    handler = RecordingHandler()
    pipeline = Pipeline(redis, adapter, handler, settings)

    await pipeline.accept(delivery("c1", "hi"))
    await pipeline.accept(delivery("c1", "is it still available"))
    await pipeline.accept(delivery("c1", "?"))

    await _run(pipeline, 1.5)

    assert len(handler.calls) == 1
    conversation, text, size = handler.calls[0]
    assert conversation == "c1"
    assert size == 3
    assert text == "hi is it still available ?"


async def test_burst_merge_across_reads(redis, settings, adapter):
    """Messages that arrive while the first window is open must still merge.

    This is the regression test for the awaited-consumer-loop trap. The gap is
    deliberately shorter than window_seconds and longer than one poll cycle.
    """
    handler = RecordingHandler()
    pipeline = Pipeline(redis, adapter, handler, settings)

    await pipeline.start()
    try:
        await pipeline.accept(delivery("c2", "first"))
        await asyncio.sleep(0.12)  # < window_seconds (0.3)
        await pipeline.accept(delivery("c2", "second"))
        await asyncio.sleep(1.2)
    finally:
        await pipeline.stop()

    assert len(handler.calls) == 1, (
        "the two messages opened separate windows — the consumer loop is "
        "blocking on the work it dispatches"
    )
    assert handler.calls[0][2] == 2


async def test_separate_conversations_do_not_serialise(redis, settings, adapter):
    handler = RecordingHandler()
    pipeline = Pipeline(redis, adapter, handler, settings)

    await pipeline.accept(delivery("a", "one"))
    await pipeline.accept(delivery("b", "two"))

    await _run(pipeline, 1.5)

    assert {c for c, _, _ in handler.calls} == {"a", "b"}
    assert all(size == 1 for _, _, size in handler.calls)


async def test_max_window_caps_an_endless_typist(redis, settings, adapter):
    """A conversation that keeps receiving must still be answered."""
    handler = RecordingHandler()
    pipeline = Pipeline(redis, adapter, handler, settings)

    await pipeline.start()
    try:
        # Keep pushing the soft deadline for longer than max_window_seconds (1.0).
        for i in range(8):
            await pipeline.accept(delivery("c3", f"m{i}"))
            await asyncio.sleep(0.2)
        await asyncio.sleep(1.0)
    finally:
        await pipeline.stop()

    assert handler.calls, "max_window_seconds did not force a flush"
    assert handler.calls[0][0] == "c3"


async def test_a_duplicate_delivery_does_not_duplicate_the_text(redis, settings, adapter):
    handler = RecordingHandler()
    pipeline = Pipeline(redis, adapter, handler, settings)

    body = delivery("c4", "only once")
    await pipeline.accept(body)
    await pipeline.accept(body)  # provider retries the same event_id

    await _run(pipeline, 1.5)

    assert len(handler.calls) == 1
    assert handler.calls[0][1] == "only once"
    assert handler.calls[0][2] == 1


async def test_non_message_deliveries_never_reach_the_stream(redis, settings, adapter):
    handler = RecordingHandler()
    pipeline = Pipeline(redis, adapter, handler, settings)

    assert await pipeline.accept({"type": "message.delivered", "id": "x"}) is None
    assert await pipeline.accept({"type": "presence.update"}) is None

    assert await redis.xlen(pipeline.keys.stream) == 0

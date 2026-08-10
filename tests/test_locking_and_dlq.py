"""Two smaller guarantees: mutual exclusion that survives a second replica, and
a dead-letter queue somebody can actually read.
"""

from __future__ import annotations

import asyncio

from conftest import delivery

from burstq import InProcessLock, Pipeline, RedisLock, Settings


async def test_in_process_lock_serialises_one_conversation():
    lock = InProcessLock()
    order: list[str] = []

    async def worker(tag: str, hold: float) -> None:
        async with lock.acquire("same-conversation") as held:
            assert held
            order.append(f"{tag}:in")
            await asyncio.sleep(hold)
            order.append(f"{tag}:out")

    await asyncio.gather(worker("a", 0.05), worker("b", 0.0))

    # Whichever ran first must have finished before the other started.
    assert order[1].endswith(":out")
    assert order[0].split(":")[0] == order[1].split(":")[0]


async def test_in_process_lock_does_not_serialise_different_conversations():
    lock = InProcessLock()
    started: list[str] = []

    async def worker(conversation: str) -> None:
        async with lock.acquire(conversation) as held:
            assert held
            started.append(conversation)
            await asyncio.sleep(0.05)

    await asyncio.wait_for(asyncio.gather(worker("a"), worker("b")), timeout=0.2)
    assert set(started) == {"a", "b"}


async def test_in_process_lock_declares_that_it_is_not_replica_safe():
    """The property exists so the limitation is discoverable in code, not only
    in a paragraph of the README."""
    assert InProcessLock.replica_safe is False
    assert RedisLock.replica_safe is True


async def test_redis_lock_excludes_a_second_process(redis, keys):
    """Two RedisLock instances stand in for two replicas."""
    replica_a = RedisLock(redis, keys, timeout=5, max_wait=0.3)
    replica_b = RedisLock(redis, keys, timeout=5, max_wait=0.3)

    async with replica_a.acquire("c1") as held_a:
        assert held_a
        async with replica_b.acquire("c1") as held_b:
            assert held_b is False, "two replicas both entered the same conversation"

    # Released: the second replica can now take it.
    async with replica_b.acquire("c1") as held_again:
        assert held_again


async def test_redis_lock_release_is_ownership_checked(redis, keys):
    """An overrunning handler must not delete a lock someone else now holds."""
    short = RedisLock(redis, keys, timeout=1, max_wait=0.2)
    other = RedisLock(redis, keys, timeout=5, max_wait=0.2)

    async with short.acquire("c2") as held:
        assert held
        await asyncio.sleep(1.2)  # our lock expires while we are still "working"
        async with other.acquire("c2") as taken:
            assert taken  # someone else legitimately took it
        # exiting `short` here must not delete `other`'s lock â€” token mismatch

    assert await redis.exists(keys.lock("c2")) == 0


async def test_a_permanently_failing_entry_lands_in_the_dlq(redis, adapter):
    """Retry budget exhausted -> DLQ, and the entry is acked so it stops cycling.

    The delivery is driven straight through the consumer rather than waiting for
    ``claim_idle_seconds`` to elapse between attempts: this asserts the retry
    accounting, not the wall clock. Recovery timing has its own coverage in
    ``test_window_durability``.
    """
    settings = Settings(
        namespace="dlq-test",
        window_seconds=0.2,
        max_window_seconds=1.0,
        processing_ttl=2.0,
        dedup_ttl=30.0,
        handler_timeout=1.0,
        claim_idle_seconds=10.0,
        lock_timeout=5.0,
        block_ms=0,
        max_retries=2,
    )

    async def never_called(burst) -> None:  # pragma: no cover
        raise AssertionError("should not reach the handler")

    pipeline = Pipeline(redis, adapter, never_called, settings)

    # Poison the buffering step so every attempt fails the same way.
    async def explode(*_args, **_kwargs):
        raise RuntimeError("redis is sulking")

    pipeline.window.append = explode  # type: ignore[method-assign]

    await pipeline.accept(delivery("c1", "doomed"))
    await pipeline.consumer.ensure_group()

    result = await redis.xreadgroup(
        groupname=settings.group,
        consumername=settings.consumer_name,
        streams={pipeline.keys.stream: ">"},
        count=10,
    )
    (_stream, entries), = result
    stream_id, fields = entries[0]

    for _ in range(settings.max_retries):
        await pipeline.consumer._handle_entry(stream_id, fields)  # noqa: SLF001

    dead = await pipeline.dlq()
    assert dead, "a permanently failing entry never reached the dead-letter queue"
    assert "redis is sulking" in dead[0]["error"]
    assert int(dead[0]["attempts"]) >= settings.max_retries
    assert dead[0]["event"]["text"] == "doomed"

    # And it was acked, so it does not cycle forever.
    health = await pipeline.health()
    assert health["pending"] == 0


async def test_a_failing_entry_releases_its_idempotency_claim(redis, adapter, settings):
    """A failed attempt must not leave a claim that makes the retry a no-op."""
    from burstq.dedup import ClaimResult

    async def never_called(burst) -> None:  # pragma: no cover
        raise AssertionError("should not reach the handler")

    pipeline = Pipeline(redis, adapter, never_called, settings)

    async def explode(*_args, **_kwargs):
        raise RuntimeError("nope")

    pipeline.window.append = explode  # type: ignore[method-assign]

    body = delivery("c9", "will fail")
    await pipeline.accept(body)
    await pipeline.consumer.ensure_group()

    result = await redis.xreadgroup(
        groupname=settings.group,
        consumername=settings.consumer_name,
        streams={pipeline.keys.stream: ">"},
        count=10,
    )
    (_stream, entries), = result
    stream_id, fields = entries[0]

    await pipeline.consumer._handle_entry(stream_id, fields)  # noqa: SLF001

    assert await pipeline.idempotency.state(body["id"]) is None
    assert await pipeline.idempotency.claim(body["id"]) is ClaimResult.ACQUIRED


async def test_health_reports_the_lock_mode(redis, settings, adapter):
    async def noop(burst) -> None: ...

    pipeline = Pipeline(redis, adapter, noop, settings)
    health = await pipeline.health()

    assert health["ok"] is True
    assert health["replica_safe_lock"] is False


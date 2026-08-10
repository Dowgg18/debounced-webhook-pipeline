"""Per-conversation mutual exclusion, in two flavours — and why the choice matters.

The pipeline is parallel across conversations and serial within one. Two messages
from the same person must not be handled concurrently; two messages from two
different people should never wait on each other.

An ``asyncio.Lock`` per conversation delivers that beautifully — inside one
process. The system this came from ran exactly that, and it was correct, because
it ran one replica.

The trap is that nothing in the code says so. The stream consumer group is
designed for N workers; the consumer name is derived from the hostname
specifically so that several can coexist. Scale to two replicas — for uptime
during a deploy, say — and the in-process lock silently provides no mutual
exclusion at all. Nothing errors. Two workers just start handling the same
conversation at the same time, and you get duplicated replies for a subset of
users nobody can reproduce.

So the choice is explicit in ``Settings``, and the in-process implementation
names its own limitation.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from .redis_keys import Keys

# Releases only if we still own the lock. Without the token check, a handler that
# overran its timeout would delete a lock a *different* worker had since taken.
_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class ConversationLock(Protocol):
    def acquire(self, conversation_id: str) -> AsyncIterator[bool]: ...


class InProcessLock:
    """Single-replica mutual exclusion. Cheap, and honest about its scope.

    Stale entries are swept so a long-lived process does not accumulate one lock
    per conversation it has ever seen.
    """

    replica_safe = False

    def __init__(self, idle_ttl: float = 3600.0) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._last_used: dict[str, float] = {}
        self._idle_ttl = idle_ttl

    @contextlib.asynccontextmanager
    async def acquire(self, conversation_id: str) -> AsyncIterator[bool]:
        lock = self._get(conversation_id)
        async with lock:
            yield True

    def _get(self, conversation_id: str) -> asyncio.Lock:
        now = time.monotonic()
        self._last_used[conversation_id] = now
        lock = self._locks.get(conversation_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[conversation_id] = lock
            if len(self._locks) % 1000 == 0:
                self._sweep(now)
        return lock

    def _sweep(self, now: float) -> None:
        stale = [k for k, t in self._last_used.items() if now - t > self._idle_ttl]
        for key in stale:
            lock = self._locks.get(key)
            if lock is not None and not lock.locked():
                del self._locks[key]
                del self._last_used[key]

    def stats(self) -> dict[str, int]:
        return {
            "tracked": len(self._locks),
            "held": sum(1 for lk in self._locks.values() if lk.locked()),
        }


class RedisLock:
    """Multi-replica mutual exclusion via SET NX PX with an ownership token.

    Deliberately not a full Redlock: that buys correctness across independent
    Redis masters, and this pipeline already treats a single Redis as its source
    of truth. Adding Redlock here would be ceremony, not safety.
    """

    replica_safe = True

    def __init__(
        self,
        redis: Redis,
        keys: Keys,
        timeout: float = 60.0,
        poll_interval: float = 0.05,
        max_wait: float = 30.0,
    ) -> None:
        self._redis = redis
        self._keys = keys
        self._timeout = timeout
        self._poll = poll_interval
        self._max_wait = max_wait
        self._release = redis.register_script(_RELEASE)

    @contextlib.asynccontextmanager
    async def acquire(self, conversation_id: str) -> AsyncIterator[bool]:
        key = self._keys.lock(conversation_id)
        token = uuid.uuid4().hex
        deadline = time.monotonic() + self._max_wait
        acquired = False

        while time.monotonic() < deadline:
            if await self._redis.set(key, token, nx=True, px=int(self._timeout * 1000)):
                acquired = True
                break
            await asyncio.sleep(self._poll)

        if not acquired:
            # Yielding False rather than raising: the caller decides whether a
            # contended conversation is an error or something to retry later.
            yield False
            return

        try:
            yield True
        finally:
            await self._release(keys=[key], args=[token])

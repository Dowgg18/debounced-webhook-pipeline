"""The burst window: durable, leased, and not a sleeping coroutine.

THE BUG
-------
The natural way to debounce is to sleep::

    await buffer.push(conversation_id, text)
    await asyncio.sleep(WINDOW)
    if not await buffer.is_last(conversation_id, event_id):
        return
    burst = await buffer.drain(conversation_id)
    await handler(burst)

This works, and it has two defects that only appear in production.

**It holds the stream entry hostage.** The sleep happens inside the processing of
a stream message, so the entry stays *pending* for the whole window plus the
whole handler. Redis Streams' crash recovery steals entries that have been
pending too long — that is its entire job. Set the window to 5s, the handler
timeout to 20s with two retries, and the idle threshold to 30s, and recovery
starts re-delivering work that is still running. The two mechanisms fight, and
the symptom is a handler that occasionally runs twice for no visible reason.

**It does not survive a restart.** A pending sleep is a coroutine in one
process's memory. Deploy during the window and the buffered messages are still
in Redis, but nothing is scheduled to ever drain them. They sit there until the
list TTL quietly deletes them.

THE FIX
-------
Move the schedule into Redis. A sorted set holds one entry per open window,
scored by its flush deadline. The consumer appends and acks immediately — its
job is done in milliseconds. A separate flusher asks "what is due?" and works on
that. A restart loses nothing, because the deadline is data, not a stack frame.

Claiming a due window takes a lease (deadline pushed forward) rather than
removing it, so a flusher that dies mid-handler does not take the burst with it.
The window is only removed after the handler returns.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from .types import Burst

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from .config import Settings
    from .redis_keys import Keys

# Appends a message and (re)schedules the flush deadline atomically.
#
# KEYS[1] window list   KEYS[2] window meta   KEYS[3] due set
# ARGV[1] text  ARGV[2] event_id  ARGV[3] now  ARGV[4] window  ARGV[5] max_window
# ARGV[6] list ttl
_APPEND = """
local first = redis.call('HGET', KEYS[2], 'first_at')
if not first then
  first = ARGV[3]
  redis.call('HSET', KEYS[2], 'first_at', first)
end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('RPUSH', KEYS[1] .. ':ids', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('EXPIRE', KEYS[1] .. ':ids', ARGV[6])
redis.call('EXPIRE', KEYS[2], ARGV[6])

local soft = tonumber(ARGV[3]) + tonumber(ARGV[4])
local hard = tonumber(first) + tonumber(ARGV[5])
local deadline = math.min(soft, hard)
redis.call('ZADD', KEYS[3], deadline, ARGV[7])
return deadline
"""

# Claims due conversations by leasing them forward instead of removing them.
#
# KEYS[1] due set   ARGV[1] now   ARGV[2] lease seconds   ARGV[3] limit
_CLAIM_DUE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
if #due == 0 then return {} end
local lease = tonumber(ARGV[1]) + tonumber(ARGV[2])
for i = 1, #due do
  redis.call('ZADD', KEYS[1], lease, due[i])
end
return due
"""


class BurstWindow:
    def __init__(self, redis: Redis, keys: Keys, settings: Settings) -> None:
        self._redis = redis
        self._keys = keys
        self._settings = settings
        self._append = redis.register_script(_APPEND)
        self._claim_due = redis.register_script(_CLAIM_DUE)

    async def append(self, conversation_id: str, text: str, event_id: str) -> float:
        """Buffer one message and return the deadline its conversation now has."""
        list_key = self._keys.window(conversation_id)
        ttl = int(self._settings.max_window_seconds + self._settings.handler_timeout + 60)
        deadline = await self._append(
            keys=[list_key, self._keys.window_meta(conversation_id), self._keys.due_windows],
            args=[
                text,
                event_id,
                time.time(),
                self._settings.window_seconds,
                self._settings.max_window_seconds,
                ttl,
                conversation_id,
            ],
        )
        return float(deadline)

    async def claim_due(self, limit: int = 100, lease_seconds: float | None = None) -> list[str]:
        """Conversation ids whose window has closed, leased to this caller."""
        lease = lease_seconds or (self._settings.handler_timeout + 30)
        raw = await self._claim_due(
            keys=[self._keys.due_windows],
            args=[time.time(), lease, limit],
        )
        return [c.decode() if isinstance(c, bytes) else str(c) for c in (raw or [])]

    async def read(self, conversation_id: str) -> Burst | None:
        """Read the buffered burst without consuming it.

        Non-destructive on purpose: if the handler fails, the lease expires and
        the same burst is retried. Removing on read would make every handler
        failure a silent data loss.
        """
        list_key = self._keys.window(conversation_id)
        pipe = self._redis.pipeline()
        pipe.lrange(list_key, 0, -1)
        pipe.lrange(f"{list_key}:ids", 0, -1)
        parts, ids = await pipe.execute()
        if not parts:
            return None
        texts = [p.decode() if isinstance(p, bytes) else str(p) for p in parts]
        event_ids = tuple(i.decode() if isinstance(i, bytes) else str(i) for i in (ids or []))
        return Burst(
            conversation_id=conversation_id,
            text=" ".join(t for t in texts if t),
            event_ids=event_ids,
        )

    async def discard(self, conversation_id: str) -> None:
        """Remove a window after its burst was handled successfully."""
        list_key = self._keys.window(conversation_id)
        pipe = self._redis.pipeline()
        pipe.delete(list_key)
        pipe.delete(f"{list_key}:ids")
        pipe.delete(self._keys.window_meta(conversation_id))
        pipe.zrem(self._keys.due_windows, conversation_id)
        await pipe.execute()

    async def open_windows(self) -> int:
        return int(await self._redis.zcard(self._keys.due_windows))

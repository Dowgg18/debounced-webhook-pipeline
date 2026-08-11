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
# Each entry carries the stream id it came from. The consumer dispatches entries
# concurrently on purpose, so the order they reach this script is *not* the order
# they arrived in — two messages 300ms apart can land here in either order. Redis
# already assigned a monotonic id when the webhook was ingested, so that is the
# ordering authority; the list is sorted by it on read rather than trusted to be
# in push order.
#
# KEYS[1] window list   KEYS[2] window meta   KEYS[3] due set
# ARGV[1] entry (stream_id \0 event_id \0 text)   ARGV[2] now
# ARGV[3] window   ARGV[4] max_window   ARGV[5] ttl   ARGV[6] conversation_id
_APPEND = """
local first = redis.call('HGET', KEYS[2], 'first_at')
if not first then
  first = ARGV[2]
  redis.call('HSET', KEYS[2], 'first_at', first)
end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[5])

local soft = tonumber(ARGV[2]) + tonumber(ARGV[3])
local hard = tonumber(first) + tonumber(ARGV[4])
local deadline = math.min(soft, hard)
redis.call('ZADD', KEYS[3], deadline, ARGV[6])
return deadline
"""

_SEP = "\x00"


def _stream_order(stream_id: str) -> tuple[int, int]:
    """Redis stream ids are ``<milliseconds>-<sequence>``.

    Sorting them as strings is wrong the moment a sequence number reaches two
    digits: ``…-10`` sorts before ``…-9``. Parse both halves.
    """
    ms, _, seq = stream_id.partition("-")
    try:
        return int(ms), int(seq or 0)
    except ValueError:
        return 0, 0

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

    async def append(
        self, conversation_id: str, text: str, event_id: str, stream_id: str = ""
    ) -> float:
        """Buffer one message and return the deadline its conversation now has.

        ``stream_id`` is what makes the merge deterministic. Without it the burst
        reads back in whatever order concurrent tasks happened to push.
        """
        list_key = self._keys.window(conversation_id)
        ttl = int(self._settings.max_window_seconds + self._settings.handler_timeout + 60)
        entry = _SEP.join((stream_id, event_id, text))
        deadline = await self._append(
            keys=[list_key, self._keys.window_meta(conversation_id), self._keys.due_windows],
            args=[
                entry,
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
        raw = await self._redis.lrange(self._keys.window(conversation_id), 0, -1)
        if not raw:
            return None

        entries = []
        for item in raw:
            value = item.decode() if isinstance(item, bytes) else str(item)
            stream_id, _, rest = value.partition(_SEP)
            event_id, _, text = rest.partition(_SEP)
            entries.append((_stream_order(stream_id), event_id, text))

        # Sorted by the id Redis assigned at ingest, not by push order.
        entries.sort(key=lambda e: e[0])

        return Burst(
            conversation_id=conversation_id,
            text=" ".join(text for _, _, text in entries if text),
            event_ids=tuple(event_id for _, event_id, _ in entries),
        )

    async def discard(self, conversation_id: str) -> None:
        """Remove a window after its burst was handled successfully."""
        pipe = self._redis.pipeline()
        pipe.delete(self._keys.window(conversation_id))
        pipe.delete(self._keys.window_meta(conversation_id))
        pipe.zrem(self._keys.due_windows, conversation_id)
        await pipe.execute()

    async def open_windows(self) -> int:
        return int(await self._redis.zcard(self._keys.due_windows))

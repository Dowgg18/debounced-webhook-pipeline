"""The stream consumer: read, buffer, ack. Nothing slow lives here.

THE TRAP
--------
This loop looks like it should await the work it dispatches::

    entries = await redis.xreadgroup(...)
    await asyncio.gather(*[process(e) for e in entries])   # wrong

It is the natural shape, it passes every unit test, and it makes the burst
window useless in production.

Follow two messages from one person, 300ms apart, with a 5 second window:

- Message A is read and dispatched. Awaiting it means the loop stops here.
- Message B arrives in Redis 300ms later. Nobody reads it: the loop is blocked.
- A's window closes 5s later. Only then does the loop go back for more.
- B is finally read — and opens a *fresh* window, alone.

The two messages never meet. The debouncer answers twice, which is the exact
behaviour it exists to prevent. And it only misbehaves under the condition it was
built for: two messages close together. Test it with one message and it is
perfect.

So the loop dispatches and immediately goes back to reading. Concurrency is
bounded by a semaphore rather than by blocking the reader.

In this design the point is nearly moot — the consumer's own work is now
milliseconds, because the sleeping moved to the flusher — but the shape is kept
deliberately, and the regression test with it. The next person to "simplify" this
into a gather will be caught by ``test_burst_merge``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import TYPE_CHECKING

from . import metrics
from .dedup import ClaimResult
from .ingest import decode_event

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from .config import Settings
    from .dedup import Idempotency
    from .redis_keys import Keys
    from .window import BurstWindow

logger = logging.getLogger("burstq.consumer")


class StreamConsumer:
    def __init__(
        self,
        redis: Redis,
        keys: Keys,
        settings: Settings,
        window: BurstWindow,
        idempotency: Idempotency,
    ) -> None:
        self._redis = redis
        self._keys = keys
        self._settings = settings
        self._window = window
        self._idem = idempotency
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)
        self._tasks: set[asyncio.Task] = set()
        self._stopping = asyncio.Event()

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._keys.stream, self._settings.group, id="0", mkstream=True
            )
            logger.info("consumer group created: %s", self._settings.group)
        except Exception as exc:  # noqa: BLE001
            if "BUSYGROUP" not in str(exc):
                raise

    async def run(self) -> None:
        await self.ensure_group()
        logger.info("consumer started: %s", self._settings.consumer_name)
        while not self._stopping.is_set():
            try:
                await self._recover()
                await self._read_new()
                await self._report_backlog()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("consumer loop error")
                await asyncio.sleep(2)

    async def stop(self) -> None:
        self._stopping.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    # -- reading ------------------------------------------------------------

    async def _recover(self) -> None:
        """Reclaim entries a dead consumer never acked."""
        try:
            result = await self._redis.xautoclaim(
                self._keys.stream,
                self._settings.group,
                self._settings.consumer_name,
                min_idle_time=self._settings.claim_idle_ms,
                start_id="0-0",
                count=self._settings.batch_size,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("xautoclaim unavailable: %s", exc)
            return

        entries = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else []
        if not entries:
            return
        logger.info("reclaiming %d stranded entr(ies)", len(entries))
        metrics.retries.inc(len(entries))
        for stream_id, fields in entries:
            self._dispatch(stream_id, fields)

    async def _read_new(self) -> None:
        kwargs: dict = {
            "groupname": self._settings.group,
            "consumername": self._settings.consumer_name,
            "streams": {self._keys.stream: ">"},
            "count": self._settings.batch_size,
        }
        # block_ms=0 selects polling instead of a blocking read. Blocking is the
        # better default against a real server — no idle round trips — but some
        # Redis-compatible backends (and every in-process fake) implement BLOCK
        # poorly or not at all, and a consumer that never returns is worse than
        # one that wakes up ten times a second.
        if self._settings.block_ms > 0:
            kwargs["block"] = self._settings.block_ms

        try:
            result = await self._redis.xreadgroup(**kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.warning("xreadgroup failed: %s", exc)
            await asyncio.sleep(1)
            return

        if not result:
            if self._settings.block_ms <= 0:
                await asyncio.sleep(self._settings.idle_poll_seconds)
            return

        for _stream, entries in result:
            for stream_id, fields in entries:
                self._dispatch(stream_id, fields)

    def _dispatch(self, stream_id: bytes | str, fields: dict) -> None:
        """Fire and forget — see the module docstring for why this is not a gather."""
        task = asyncio.create_task(self._bounded(stream_id, fields))
        self._tasks.add(task)
        task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.exception("entry task crashed", exc_info=exc)

    async def _bounded(self, stream_id: bytes | str, fields: dict) -> None:
        async with self._semaphore:
            await self._handle_entry(stream_id, fields)

    # -- one entry ----------------------------------------------------------

    async def _handle_entry(self, stream_id: bytes | str, fields: dict) -> None:
        sid = stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)
        event_id = ""
        try:
            event = decode_event(fields)
            event_id = event.event_id

            claim = await self._idem.claim(event.event_id)
            if claim is ClaimResult.ALREADY_DONE:
                metrics.dropped.labels(reason="duplicate").inc()
                await self._ack(sid)
                return
            if claim is ClaimResult.IN_FLIGHT:
                # Another worker holds this exact delivery. Ack ours; theirs will
                # finish the job. Not acking would just re-deliver a no-op.
                metrics.dropped.labels(reason="in_flight").inc()
                await self._ack(sid)
                return

            await self._window.append(event.conversation_id, event.text, event.event_id)

            # The delivery is now durably inside a window with its own retry
            # semantics, so its idempotency record can be finalised here. Handler
            # failures are retried at window level, not delivery level.
            await self._idem.commit(event.event_id)

            metrics.consumed.labels(outcome="buffered").inc()
            await self._ack(sid)

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if event_id:
                # Let the retry have a clean shot instead of waiting out the TTL.
                with contextlib.suppress(Exception):
                    await self._idem.release(event_id)
            await self._fail(sid, fields, exc)

    async def _ack(self, stream_id: str) -> None:
        pipe = self._redis.pipeline()
        pipe.xack(self._keys.stream, self._settings.group, stream_id)
        pipe.delete(self._keys.retries(stream_id))
        await pipe.execute()

    async def _fail(self, stream_id: str, fields: dict, exc: Exception) -> None:
        retry_key = self._keys.retries(stream_id)
        attempts = int(await self._redis.incr(retry_key))
        await self._redis.expire(retry_key, 86400)
        metrics.consumed.labels(outcome="error").inc()
        logger.warning("entry %s failed (attempt %d): %s", stream_id, attempts, exc)

        if attempts >= self._settings.max_retries:
            await self._dead_letter(stream_id, fields, exc, attempts)
            await self._ack(stream_id)
            metrics.dead_lettered.inc()
        else:
            metrics.retries.inc()

    async def _dead_letter(
        self, stream_id: str, fields: dict, exc: Exception, attempts: int
    ) -> None:
        raw = fields.get(b"event") or fields.get("event") or b"{}"
        await self._redis.xadd(
            self._keys.dlq,
            {
                "original_id": stream_id,
                "event": raw if isinstance(raw, str) else raw.decode(),
                "error": f"{type(exc).__name__}: {exc}"[:500],
                "attempts": str(attempts),
            },
            maxlen=self._settings.dlq_maxlen,
            approximate=True,
        )
        logger.error("dead-lettered %s after %d attempts: %s", stream_id, attempts, exc)

    async def _report_backlog(self) -> None:
        try:
            groups = await self._redis.xinfo_groups(self._keys.stream)
        except Exception:  # noqa: BLE001
            return
        for group in groups:
            name = group.get(b"name") or group.get("name")
            name_str = name.decode() if isinstance(name, bytes) else str(name)
            if name_str == self._settings.group:
                pending = group.get(b"pending") or group.get("pending") or 0
                metrics.stream_backlog.set(int(pending))
                return


async def read_dlq(redis: Redis, keys: Keys, limit: int = 50) -> list[dict]:
    """The dead-letter queue is only useful if somebody can read it."""
    try:
        raw = await redis.xrevrange(keys.dlq, count=limit)
    except Exception:  # noqa: BLE001
        return []

    out: list[dict] = []
    for stream_id, fields in raw or []:
        entry: dict = {
            "id": stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)
        }
        for key, value in fields.items():
            k = key.decode() if isinstance(key, bytes) else str(key)
            v = value.decode() if isinstance(value, bytes) else str(value)
            if k == "event":
                with contextlib.suppress(json.JSONDecodeError):
                    v = json.loads(v)
            entry[k] = v
        out.append(entry)
    return out

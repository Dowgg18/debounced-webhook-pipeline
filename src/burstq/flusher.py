"""The flusher: closes windows and calls the handler exactly once per burst.

This is where the slow work happens, deliberately separated from stream
consumption. The consumer's job is measured in milliseconds; this one is measured
in whatever the handler costs.

Delivery semantics, stated plainly because a queue that is vague about this is
worse than no queue:

  * A burst is handled **at least once**. If the process dies after the handler's
    side effects but before ``discard``, the lease expires and the same burst is
    handled again. Handlers must tolerate that.
  * A burst is **never silently dropped**. The window is only removed after the
    handler returns, so every failure path ends in a retry or the DLQ.
  * Ordering is guaranteed **within a conversation** and nowhere else.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from . import metrics

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from .config import Settings
    from .locking import InProcessLock, RedisLock
    from .redis_keys import Keys
    from .types import Handler
    from .window import BurstWindow

logger = logging.getLogger("burstq.flusher")


class WindowFlusher:
    def __init__(
        self,
        redis: Redis,
        keys: Keys,
        settings: Settings,
        window: BurstWindow,
        lock: InProcessLock | RedisLock,
        handler: Handler,
        poll_interval: float = 0.25,
    ) -> None:
        self._redis = redis
        self._keys = keys
        self._settings = settings
        self._window = window
        self._lock = lock
        self._handler = handler
        self._poll = poll_interval
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)
        self._tasks: set[asyncio.Task] = set()
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        logger.info("flusher started (window=%.1fs)", self._settings.window_seconds)
        while not self._stopping.is_set():
            try:
                due = await self._window.claim_due(limit=self._settings.batch_size)
                for conversation_id in due:
                    self._dispatch(conversation_id)
                metrics.open_windows.set(await self._window.open_windows())
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("flusher loop error")
                await asyncio.sleep(2)
            await asyncio.sleep(self._poll)

    async def stop(self) -> None:
        self._stopping.set()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def _dispatch(self, conversation_id: str) -> None:
        task = asyncio.create_task(self._bounded(conversation_id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _bounded(self, conversation_id: str) -> None:
        async with self._semaphore:
            await self.flush_one(conversation_id)

    async def flush_one(self, conversation_id: str) -> bool:
        """Close one window. Returns True when the burst was handled."""
        burst = await self._window.read(conversation_id)
        if burst is None or not burst.text:
            # Nothing buffered — a leftover scheduler entry. Clear it.
            await self._window.discard(conversation_id)
            return False

        async with self._lock.acquire(conversation_id) as held:
            if not held:
                # Someone else holds this conversation. Leave the window in place;
                # the lease will bring it back around.
                logger.debug("conversation %s busy, deferring", conversation_id)
                return False

            started = time.monotonic()
            try:
                await asyncio.wait_for(
                    self._handler(burst), timeout=self._settings.handler_timeout
                )
            except TimeoutError:
                metrics.bursts_handled.labels(outcome="timeout").inc()
                logger.warning(
                    "handler timed out after %.1fs for %s",
                    self._settings.handler_timeout,
                    conversation_id,
                )
                return False
            except asyncio.CancelledError:
                raise
            except Exception:
                metrics.bursts_handled.labels(outcome="error").inc()
                logger.exception("handler failed for %s", conversation_id)
                return False

            metrics.handler_seconds.observe(time.monotonic() - started)
            metrics.burst_size.observe(burst.size)
            metrics.bursts_handled.labels(outcome="ok").inc()

            await self._window.discard(conversation_id)
            return True

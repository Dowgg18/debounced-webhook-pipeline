"""Assembly. The only module most users need to read.

    from redis.asyncio import Redis
    from burstq import Pipeline, Settings

    pipeline = Pipeline(
        redis=Redis.from_url("redis://localhost:6379/0"),
        settings=Settings(window_seconds=5),
        adapter=MyProviderAdapter(),
        handler=my_handler,
    )

    await pipeline.start()                 # consumer + flusher
    await pipeline.accept(webhook_body)    # from your HTTP route
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from .config import Settings
from .consumer import StreamConsumer, read_dlq
from .dedup import Idempotency
from .flusher import WindowFlusher
from .ingest import Ingestor
from .locking import InProcessLock, RedisLock
from .redis_keys import Keys
from .window import BurstWindow

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from .types import Handler, ProviderAdapter

logger = logging.getLogger("burstq")


class Pipeline:
    def __init__(
        self,
        redis: Redis,
        adapter: ProviderAdapter,
        handler: Handler,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or Settings()
        self.redis = redis
        self.keys = Keys(self.settings.namespace)

        self.idempotency = Idempotency(redis, self.keys, self.settings)
        self.window = BurstWindow(redis, self.keys, self.settings)
        self.lock = (
            RedisLock(redis, self.keys, timeout=self.settings.lock_timeout)
            if self.settings.distributed_lock
            else InProcessLock()
        )
        self.ingestor = Ingestor(redis, self.keys, adapter)
        self.consumer = StreamConsumer(
            redis, self.keys, self.settings, self.window, self.idempotency
        )
        self.flusher = WindowFlusher(
            redis, self.keys, self.settings, self.window, self.lock, handler
        )

        self._tasks: list[asyncio.Task] = []

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if self._tasks:
            raise RuntimeError("pipeline already started")
        if not self.settings.distributed_lock:
            logger.warning(
                "using the in-process lock: correct for a single replica only. "
                "Set Settings(distributed_lock=True) before scaling out."
            )
        self._tasks = [
            asyncio.create_task(self.consumer.run(), name="burstq-consumer"),
            asyncio.create_task(self.flusher.run(), name="burstq-flusher"),
        ]

    async def stop(self) -> None:
        await self.consumer.stop()
        await self.flusher.stop()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    # -- ingress ------------------------------------------------------------

    async def accept(self, body: dict[str, Any]) -> str | None:
        return await self.ingestor.accept(body)

    # -- introspection ------------------------------------------------------

    async def dlq(self, limit: int = 50) -> list[dict]:
        return await read_dlq(self.redis, self.keys, limit)

    async def health(self) -> dict[str, Any]:
        """A health check that actually touches its dependency.

        A ``/health`` that returns ``{"ok": true}`` without a round trip to the
        thing it depends on is a status page for the web framework, not for the
        service.
        """
        info: dict[str, Any] = {
            "redis": "unknown",
            "stream": self.keys.stream,
            "group": self.settings.group,
            "replica_safe_lock": getattr(self.lock, "replica_safe", False),
        }
        # Reachability is the health question. Everything after it is detail, and
        # detail that does not exist yet is not a fault: on a fresh deployment the
        # stream has no entries and no consumer group, and XINFO GROUPS raises
        # rather than returning empty. Reporting that as "error" would make a
        # correctly-installed service look broken until its first message.
        try:
            await self.redis.ping()
            info["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001
            info["redis"] = "error"
            info["error"] = str(exc)
            info["ok"] = False
            return info

        info["ok"] = True
        info["stream_length"] = await self._safe_len(self.keys.stream)
        info["dlq_length"] = await self._safe_len(self.keys.dlq)
        info["pending"] = 0
        info["consumers"] = 0

        try:
            info["open_windows"] = await self.window.open_windows()
        except Exception:  # noqa: BLE001
            info["open_windows"] = 0

        try:
            groups = await self.redis.xinfo_groups(self.keys.stream)
        except Exception:  # noqa: BLE001
            groups = []  # stream not created yet

        for group in groups:
            name = group.get(b"name") or group.get("name")
            name_str = name.decode() if isinstance(name, bytes) else str(name)
            if name_str == self.settings.group:
                info["pending"] = int(group.get(b"pending") or group.get("pending") or 0)
                info["consumers"] = int(group.get(b"consumers") or group.get("consumers") or 0)
                break

        return info

    async def _safe_len(self, key: str) -> int:
        try:
            return int(await self.redis.xlen(key))
        except Exception:  # noqa: BLE001
            return 0

"""A runnable demo: FastAPI in front, burstq behind, a fake provider to poke it.

    docker compose up -d redis
    pip install -e ".[demo,metrics]"
    uvicorn demo.app:app --reload

    python demo/burst.py          # fire a burst and watch it merge
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from typing import Any

from fastapi import FastAPI, Request
from redis.asyncio import Redis

from burstq import Burst, InboundEvent, Pipeline, Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("demo")


class DemoAdapter:
    """Adapter for a made-up provider. Swap this for yours; nothing else changes."""

    def parse(self, body: dict[str, Any]) -> InboundEvent | None:
        if body.get("type") != "message.received":
            return None
        message = body.get("message") or {}
        if message.get("from_me"):
            # Providers echo your own outbound messages back at you. Left in
            # because forgetting it is how a bot ends up replying to itself.
            return None
        return InboundEvent(
            event_id=str(message.get("id") or ""),
            conversation_id=str(message.get("chat") or ""),
            text=str(message.get("text") or ""),
            received_at=float(body.get("timestamp") or time.time()),
            raw=body,
        )


async def handle(burst: Burst) -> None:
    """The terminal step. In a real system this is where the slow work lives."""
    log.info(
        "handling conversation=%s messages=%d text=%r",
        burst.conversation_id,
        burst.size,
        burst.text,
    )
    await asyncio.sleep(0.5)  # stand-in for a model call / API round trip


settings = Settings(
    redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    namespace="demo",
    window_seconds=float(os.environ.get("WINDOW_SECONDS", "3")),
    consumer_name=f"demo-{os.getpid()}",
)
redis = Redis.from_url(settings.redis_url)
pipeline = Pipeline(redis=redis, adapter=DemoAdapter(), handler=handle, settings=settings)


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI):
    await pipeline.start()
    yield
    await pipeline.stop()
    await redis.aclose()


app = FastAPI(title="burstq demo", lifespan=lifespan)


@app.post("/webhook")
async def webhook(request: Request) -> dict[str, Any]:
    """Accept and return. Everything expensive happens behind the stream."""
    body = await request.json()
    stream_id = await pipeline.accept(body)
    return {"accepted": stream_id is not None, "id": stream_id}


@app.get("/health")
async def health() -> dict[str, Any]:
    return await pipeline.health()


@app.get("/dlq")
async def dlq(limit: int = 50) -> dict[str, Any]:
    return {"entries": await pipeline.dlq(limit)}

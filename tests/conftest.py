from __future__ import annotations

import os
import time
import uuid

import pytest
import pytest_asyncio

from burstq import Keys, Settings
from burstq.types import InboundEvent

REDIS_URL = os.environ.get("BURSTQ_TEST_REDIS", "redis://localhost:6379/15")

# Whether REDIS_URL answered, decided once per session. Without a server, a
# failed ping costs about 4 s (redis-py retries the connect with backoff), and
# paying that in every test made the no-Docker run take two minutes.
_real_redis: bool | None = None


async def _real_redis_reachable() -> bool:
    global _real_redis
    if _real_redis is None:
        from redis.asyncio import Redis

        probe = Redis.from_url(REDIS_URL, socket_connect_timeout=1.0)
        try:
            await probe.ping()
            _real_redis = True
        except Exception:
            _real_redis = False
        finally:
            await probe.aclose()
    return _real_redis


@pytest_asyncio.fixture
async def redis():
    """A real Redis when one is reachable, an in-process one otherwise.

    Stream semantics and Lua are the substance of this library, so the suite
    never mocks them — it runs the same scripts either way. CI always uses a real
    server (see ``.github/workflows/ci.yml``); the fallback exists so a
    contributor without Docker can still run ``pytest`` and get a signal.
    """
    from redis.asyncio import Redis

    if await _real_redis_reachable():
        client = Redis.from_url(REDIS_URL)
    else:
        try:
            import fakeredis.aioredis as fakeredis
        except ImportError:  # pragma: no cover
            pytest.skip(
                f"no Redis at {REDIS_URL} and fakeredis is not installed "
                "(docker compose up -d redis, or pip install 'fakeredis[lua]')"
            )
        client = fakeredis.FakeRedis()

    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture
def settings() -> Settings:
    return Settings(
        namespace=f"test-{uuid.uuid4().hex[:8]}",
        window_seconds=0.3,
        max_window_seconds=1.0,
        processing_ttl=2.0,
        dedup_ttl=30.0,
        handler_timeout=2.0,
        claim_idle_seconds=10.0,
        lock_timeout=5.0,
        # Poll rather than block: the suite must behave identically on a real
        # server and on the in-process fallback.
        block_ms=0,
        idle_poll_seconds=0.02,
    )


@pytest.fixture
def keys(settings: Settings) -> Keys:
    return Keys(settings.namespace)


class EchoAdapter:
    """Minimal provider adapter: the demo's payload shape, nothing domain-specific."""

    def parse(self, body: dict) -> InboundEvent | None:
        if body.get("type") != "message.received":
            return None
        return InboundEvent(
            event_id=body["id"],
            conversation_id=body["conversation"],
            text=body.get("text", ""),
            received_at=body.get("at", time.time()),
        )


@pytest.fixture
def adapter() -> EchoAdapter:
    return EchoAdapter()


def delivery(conversation: str, text: str, event_id: str | None = None) -> dict:
    return {
        "type": "message.received",
        "id": event_id or uuid.uuid4().hex,
        "conversation": conversation,
        "text": text,
        "at": time.time(),
    }


class RecordingHandler:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    async def __call__(self, burst) -> None:
        self.calls.append((burst.conversation_id, burst.text, burst.size))

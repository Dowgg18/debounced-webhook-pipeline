"""The webhook side: accept, persist, return.

The single most important property of this half is what it does *not* do. A
provider that does not get a 200 within a few seconds retries, and a provider
that retries while you are still working on the first delivery turns one message
into three. So the HTTP handler does exactly two things — parse enough to know
this is a real message, and XADD — then returns. Everything expensive happens on
the other side of the stream.

Measured against the system this came from: ingest is sub-5ms; the work it
defers routinely takes 3-20 seconds.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from . import metrics

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from .redis_keys import Keys
    from .types import InboundEvent, ProviderAdapter


class Ingestor:
    def __init__(self, redis: Redis, keys: Keys, adapter: ProviderAdapter) -> None:
        self._redis = redis
        self._keys = keys
        self._adapter = adapter

    async def accept(self, body: dict[str, Any]) -> str | None:
        """Persist one delivery. Returns the stream id, or None if it was dropped.

        Dropping is a normal outcome, not an error: providers send delivery
        receipts, typing indicators and echoes of our own outbound messages down
        the same webhook.
        """
        try:
            event = self._adapter.parse(body)
        except Exception:
            metrics.dropped.labels(reason="unparseable").inc()
            raise

        if event is None:
            metrics.dropped.labels(reason="not_a_message").inc()
            return None

        if not event.conversation_id:
            metrics.dropped.labels(reason="no_conversation").inc()
            return None

        stream_id = await self._redis.xadd(
            self._keys.stream,
            {
                "event": json.dumps(
                    {
                        "event_id": event.event_id,
                        "conversation_id": event.conversation_id,
                        "text": event.text,
                        "received_at": event.received_at,
                    },
                    ensure_ascii=False,
                ),
                "queued_at": str(time.time()),
            },
        )
        metrics.ingested.inc()
        return stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)


def decode_event(fields: dict) -> InboundEvent:
    """Rebuild an InboundEvent from a stream entry's fields."""
    from .types import InboundEvent  # local import keeps types.py dependency-free

    raw = fields.get(b"event") or fields.get("event") or b"{}"
    payload = json.loads(raw if isinstance(raw, str) else raw.decode())
    return InboundEvent(
        event_id=payload.get("event_id", ""),
        conversation_id=payload.get("conversation_id", ""),
        text=payload.get("text", ""),
        received_at=float(payload.get("received_at") or 0.0),
    )

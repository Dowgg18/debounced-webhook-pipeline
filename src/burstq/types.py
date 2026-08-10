"""Core value types and the two ports the pipeline talks through."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class InboundEvent:
    """One webhook delivery, already normalised by a provider adapter.

    ``event_id`` is the provider's own identifier for the message. It is the
    idempotency key: providers retry deliveries, and two deliveries carrying the
    same ``event_id`` describe the same message, not two messages.

    ``conversation_id`` is the ordering domain. Work is serialised per
    conversation and parallel across conversations.
    """

    event_id: str
    conversation_id: str
    text: str
    received_at: float
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Burst:
    """What the handler is actually called with: a merged window of messages."""

    conversation_id: str
    text: str
    event_ids: tuple[str, ...]

    @property
    def size(self) -> int:
        return len(self.event_ids)


@runtime_checkable
class Handler(Protocol):
    """The terminal step. Everything upstream exists to call this once per burst.

    Implementations must be idempotent-tolerant: at-least-once delivery is the
    contract, so a handler can be invoked twice for the same burst if the
    process dies after the handler's side effects but before the commit.
    """

    async def __call__(self, burst: Burst) -> None: ...


@runtime_checkable
class ProviderAdapter(Protocol):
    """Turns a provider-specific webhook body into zero or one InboundEvent.

    Returning ``None`` means "this delivery is not a user message" — a status
    callback, a receipt, an echo of our own outbound message. The pipeline drops
    those before they cost a Redis round trip.
    """

    def parse(self, body: dict[str, Any]) -> InboundEvent | None: ...

"""Configuration, and the timing invariant that a comment alone failed to enforce.

The pipeline has four durations that interact. In the system this library was
extracted from, three of them were tuned independently, in different files, by
different people, and their relationship lived in nobody's head:

    window_seconds        how long we wait for a burst to finish
    handler_timeout       how long the terminal step may take
    claim_idle_seconds    how long before an unacked entry is stolen back
    processing_ttl        how long an in-flight idempotency claim survives

If ``claim_idle_seconds`` is not comfortably larger than the time an entry can
legitimately stay unacked, the recovery mechanism starts fighting the workers it
is supposed to rescue: a message still being handled gets re-delivered to a
second consumer, and the same burst runs twice.

We do not document that relationship. We refuse to start without it.
"""

from __future__ import annotations

from dataclasses import dataclass


class ConfigError(ValueError):
    """Raised at construction time, never at 3 a.m."""


@dataclass(frozen=True, slots=True)
class Settings:
    # --- Redis -------------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"
    namespace: str = "burstq"

    # --- Burst window ------------------------------------------------------
    window_seconds: float = 5.0
    """How long to keep collecting before handing a conversation to the handler.

    Chat users send "hi" / "i saw the ad" / "is it still available" as three
    messages in four seconds. Answering each one separately reads as a machine.
    """

    max_window_seconds: float = 30.0
    """Hard ceiling. A conversation that keeps receiving messages must still be
    answered eventually, so the window extends but never past this."""

    # --- Idempotency -------------------------------------------------------
    processing_ttl: float = 60.0
    """Lifetime of an *in-flight* idempotency claim.

    This is the fix for the failure mode described in ``dedup.py``: a claim taken
    before the work must expire on its own if the worker dies, otherwise the
    retry sees "already processed" and acks a message nobody ever handled."""

    dedup_ttl: float = 3600.0
    """Lifetime of a *completed* idempotency record. Must outlive the provider's
    own retry schedule, or a late duplicate reopens work already finished."""

    # --- Stream consumption ------------------------------------------------
    group: str = "workers"
    consumer_name: str = "worker-1"
    batch_size: int = 50

    block_ms: int = 1000
    """How long XREADGROUP parks waiting for new entries.

    Set to 0 to poll instead — required for Redis-compatible backends whose BLOCK
    implementation does not return (in-process fakes, some proxies)."""

    idle_poll_seconds: float = 0.05
    """Sleep between empty polls when ``block_ms`` is 0. Ignored otherwise."""

    claim_idle_seconds: float = 120.0
    max_retries: int = 3
    dlq_maxlen: int = 1000
    max_concurrency: int = 100

    # --- Handler -----------------------------------------------------------
    handler_timeout: float = 30.0

    # --- Locking -----------------------------------------------------------
    distributed_lock: bool = False
    """Leave False for a single replica; the in-process lock is cheaper and has
    no failure mode. Set True the moment a second replica exists — see
    ``locking.py`` for why this is not a detail you can defer."""

    lock_timeout: float = 60.0

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ConfigError("window_seconds must be > 0")
        if self.max_window_seconds < self.window_seconds:
            raise ConfigError("max_window_seconds must be >= window_seconds")
        if self.dedup_ttl <= self.processing_ttl:
            raise ConfigError(
                "dedup_ttl must exceed processing_ttl: a completed record has to "
                "outlive an in-flight claim, or a duplicate can slip in between"
            )
        if self.lock_timeout < self.handler_timeout:
            raise ConfigError(
                "lock_timeout must be >= handler_timeout, otherwise the lock can "
                "expire under a handler that is still running"
            )

        # The invariant that the original system only had as a comment.
        unacked_worst_case = self.max_window_seconds + self.handler_timeout
        if self.claim_idle_seconds <= unacked_worst_case:
            raise ConfigError(
                f"claim_idle_seconds ({self.claim_idle_seconds}s) must exceed "
                f"max_window_seconds + handler_timeout ({unacked_worst_case}s). "
                "Below that threshold, crash recovery re-delivers entries that are "
                "still being processed and the same burst is handled twice."
            )

        # The two-phase claim fixes the mechanism; this fixes its timing. If a
        # claim can outlive the redelivery it was meant to survive, the crash it
        # protects against reappears through the other door: the dead worker's
        # claim is still `pending` when the retry arrives, the retry reads that as
        # "someone else has it", acks, and the message is gone.
        if self.processing_ttl >= self.claim_idle_seconds:
            raise ConfigError(
                f"processing_ttl ({self.processing_ttl}s) must be shorter than "
                f"claim_idle_seconds ({self.claim_idle_seconds}s). A claim that "
                "outlives redelivery makes the retry see IN_FLIGHT, ack, and drop "
                "the message — the exact loss the two-phase claim exists to prevent."
            )

    # --- Derived -----------------------------------------------------------
    @property
    def claim_idle_ms(self) -> int:
        return int(self.claim_idle_seconds * 1000)

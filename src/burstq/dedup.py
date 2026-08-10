"""Two-phase idempotency, and the bug that motivated it.

THE BUG
-------
The obvious way to deduplicate a webhook is one line::

    if await redis.set(key, "1", nx=True, ex=TTL) is None:
        return  # already seen

It is correct for the case everyone tests — the provider delivers the same
message twice, the second one is dropped — and it quietly destroys the guarantee
you added a durable queue to get.

Walk a crash through it:

1. Delivery arrives. ``SET NX`` succeeds. We now claim this message is handled.
2. The handler starts. It is halfway through when the process is OOM-killed.
3. The stream entry was never acked, so recovery re-delivers it — correctly.
4. The retry runs the same line. ``SET NX`` fails: the key from step 1 is still
   there, TTL untouched.
5. The retry concludes "duplicate", returns early, and **acks**.

The message is gone. Nobody handled it. The queue reports a clean run. This is
strictly worse than having no queue at all, because the queue's report is now a
lie, and the failure only shows up as a customer saying "I texted you and nobody
answered" days later.

The root cause is a category error: ``SET NX`` records *"someone started"*, and
the code reads it as *"someone finished"*. Those are different facts and they
need different records.

THE FIX
-------
Split the claim from the commit, and make the claim self-healing:

- ``claim()``   writes ``pending`` with a short TTL. If the worker dies, the
                claim evaporates and the retry gets a clean shot.
- ``commit()``  overwrites with ``done`` and the long TTL, once the side effects
                are durable.
- ``release()`` deletes the claim on a handled failure, so the retry does not
                have to wait out ``processing_ttl``.

The states are then unambiguous:

    (no key)  -> nobody has this. Take it.
    pending   -> someone is on it right now. Skip; they will ack or die trying.
    done      -> genuinely finished. Skip forever (until dedup_ttl).
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from .config import Settings
    from .redis_keys import Keys

_PENDING = b"pending"
_DONE = b"done"


class ClaimResult(enum.Enum):
    ACQUIRED = "acquired"
    """We own this event. Proceed, then commit or release."""

    IN_FLIGHT = "in_flight"
    """Another worker holds it. Not an error — drop this delivery silently."""

    ALREADY_DONE = "already_done"
    """A real duplicate of completed work."""


class Idempotency:
    def __init__(self, redis: Redis, keys: Keys, settings: Settings) -> None:
        self._redis = redis
        self._keys = keys
        self._settings = settings

    async def claim(self, event_id: str) -> ClaimResult:
        if not event_id:
            # No identifier means the provider cannot promise us anything.
            # Treat it as unique rather than silently collapsing distinct events.
            return ClaimResult.ACQUIRED

        key = self._keys.idempotency(event_id)
        acquired = await self._redis.set(
            key, _PENDING, nx=True, ex=int(self._settings.processing_ttl)
        )
        if acquired:
            return ClaimResult.ACQUIRED

        current = await self._redis.get(key)
        if current == _DONE:
            return ClaimResult.ALREADY_DONE
        if current is None:
            # The claim expired between our SET and our GET. Race it once more;
            # if we lose again someone else is genuinely working on it.
            retry = await self._redis.set(
                key, _PENDING, nx=True, ex=int(self._settings.processing_ttl)
            )
            return ClaimResult.ACQUIRED if retry else ClaimResult.IN_FLIGHT
        return ClaimResult.IN_FLIGHT

    async def commit(self, event_id: str) -> None:
        """Promote a claim to a completed record. Safe to call more than once."""
        if not event_id:
            return
        await self._redis.set(
            self._keys.idempotency(event_id), _DONE, ex=int(self._settings.dedup_ttl)
        )

    async def commit_many(self, event_ids: tuple[str, ...] | list[str]) -> None:
        ids = [e for e in event_ids if e]
        if not ids:
            return
        pipe = self._redis.pipeline()
        for event_id in ids:
            pipe.set(
                self._keys.idempotency(event_id), _DONE, ex=int(self._settings.dedup_ttl)
            )
        await pipe.execute()

    async def release(self, event_id: str) -> None:
        """Drop a claim after a failure we intend to retry.

        Without this the retry has to outwait ``processing_ttl``, which turns a
        one-second blip into a minute of silence for that conversation.
        """
        if not event_id:
            return
        await self._redis.delete(self._keys.idempotency(event_id))

    async def state(self, event_id: str) -> str | None:
        """Introspection for tests and the debug endpoint."""
        raw = await self._redis.get(self._keys.idempotency(event_id))
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)

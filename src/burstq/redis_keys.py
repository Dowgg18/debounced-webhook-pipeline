"""Every Redis key the pipeline touches, in one file.

Keys scattered across modules as f-strings is how you end up with a production
namespace you cannot enumerate, migrate, or safely FLUSH in staging.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Keys:
    namespace: str

    @property
    def stream(self) -> str:
        return f"{self.namespace}:events"

    @property
    def dlq(self) -> str:
        return f"{self.namespace}:dlq"

    @property
    def due_windows(self) -> str:
        """Sorted set of conversation ids scored by their flush deadline.

        This is the scheduler. No cron, no per-message sleeping task: a single
        loop asks "which conversations are due?" and Redis answers with a range
        query. It also survives a restart, which a sleeping coroutine does not.
        """
        return f"{self.namespace}:due"

    def window(self, conversation_id: str) -> str:
        return f"{self.namespace}:win:{conversation_id}"

    def window_meta(self, conversation_id: str) -> str:
        return f"{self.namespace}:win:{conversation_id}:meta"

    def idempotency(self, event_id: str) -> str:
        return f"{self.namespace}:seen:{event_id}"

    def retries(self, stream_id: str) -> str:
        return f"{self.namespace}:retry:{stream_id}"

    def lock(self, conversation_id: str) -> str:
        return f"{self.namespace}:lock:{conversation_id}"

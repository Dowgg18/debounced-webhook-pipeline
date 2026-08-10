"""burstq — one ordered unit of work per conversation, out of a storm of webhooks."""

from .config import ConfigError, Settings
from .dedup import ClaimResult, Idempotency
from .flusher import WindowFlusher
from .locking import InProcessLock, RedisLock
from .pipeline import Pipeline
from .redis_keys import Keys
from .types import Burst, Handler, InboundEvent, ProviderAdapter
from .window import BurstWindow

__version__ = "0.1.0"

__all__ = [
    "Burst",
    "BurstWindow",
    "ClaimResult",
    "ConfigError",
    "Handler",
    "Idempotency",
    "InProcessLock",
    "InboundEvent",
    "Keys",
    "Pipeline",
    "ProviderAdapter",
    "RedisLock",
    "Settings",
    "WindowFlusher",
    "__version__",
]

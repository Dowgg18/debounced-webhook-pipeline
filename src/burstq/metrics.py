"""Metrics with a no-op fallback.

``prometheus-client`` is an optional dependency. A library that hard-fails on an
observability import is a library people vendor around.
"""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - exercised by import, not by tests
    from prometheus_client import Counter, Gauge, Histogram

    _ENABLED = True
except ImportError:  # pragma: no cover
    _ENABLED = False

    class _Noop:
        def __init__(self, *_: Any, **__: Any) -> None: ...
        def labels(self, *_: Any, **__: Any) -> _Noop:
            return self
        def inc(self, *_: Any, **__: Any) -> None: ...
        def set(self, *_: Any, **__: Any) -> None: ...
        def observe(self, *_: Any, **__: Any) -> None: ...

    Counter = Gauge = Histogram = _Noop  # type: ignore[assignment,misc]


ingested = Counter("burstq_ingested_total", "Webhook deliveries accepted into the stream")
dropped = Counter(
    "burstq_dropped_total", "Deliveries dropped before the stream", ["reason"]
)
consumed = Counter("burstq_consumed_total", "Stream entries consumed", ["outcome"])
bursts_handled = Counter("burstq_bursts_total", "Bursts passed to the handler", ["outcome"])
burst_size = Histogram(
    "burstq_burst_size", "Messages merged into one burst", buckets=(1, 2, 3, 5, 8, 13, 21)
)
handler_seconds = Histogram("burstq_handler_seconds", "Handler wall time")
retries = Counter("burstq_retries_total", "Stream entries re-attempted")
dead_lettered = Counter("burstq_dlq_total", "Entries moved to the dead-letter queue")
stream_backlog = Gauge("burstq_stream_pending", "Entries delivered but not acked")
open_windows = Gauge("burstq_open_windows", "Conversations with a window in flight")

enabled = _ENABLED

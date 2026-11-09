# Changelog

Notable changes to this project. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-08-11

First public version.

### Added
- Core types, key namespace and the timing invariant.
- Two-phase idempotency.
- Durable burst window with a sorted-set scheduler.
- Per-conversation locking, in-process and distributed.
- Ingest and stream consumer with retry and a dead-letter queue.
- Window flusher and pipeline assembly.
- Optional Prometheus metrics (`metrics` extra).
- Runnable demo.

### Fixed
- A burst is ordered by stream id, not by push order.
- A fresh Redis is not reported as unhealthy.
- The timing that could undo the two-phase claim is guarded.

[Unreleased]: https://github.com/Dowgg18/debounced-webhook-pipeline/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Dowgg18/debounced-webhook-pipeline/releases/tag/v0.1.0

# burstq

**One message is not one message.**

People type the way they talk. A single question arrives as four webhook
deliveries in three seconds:

```
14:02:11.284   "hi"
14:02:11.702   "saw the listing"
14:02:12.190   "is it still available"
14:02:12.480   "?"
14:02:14.900   "saw the listing"        ← the provider retrying, not a new message
```

Handle each delivery as it lands and you answer four times, out of order, to
someone who asked one thing. Handle them behind a naive debouncer and you find
out — in production, weeks later — that the debouncer never merged anything.

`burstq` is a Redis Streams pipeline that turns that storm into **exactly one
ordered unit of work per conversation**, and survives a crash in the middle of
it.

```python
from redis.asyncio import Redis
from burstq import Pipeline, Settings

pipeline = Pipeline(
    redis=Redis.from_url("redis://localhost:6379/0"),
    adapter=MyProviderAdapter(),   # provider payload -> InboundEvent
    handler=my_handler,            # called once per burst
    settings=Settings(window_seconds=5),
)

await pipeline.start()             # consumer + flusher
await pipeline.accept(body)        # from your HTTP route, returns in ~2ms
```

---

## The shape

```
  POST /webhook          Redis Stream            due-set (ZSET)
       │                       │                        │
       ▼                       ▼                        ▼
  ┌─────────┐  XADD   ┌────────────────┐  claim  ┌──────────────┐
  │ ingest  │────────▶│    consumer    │────────▶│   flusher    │
  │  ~2ms   │         │  claim → buffer│         │ lock → handle│
  └─────────┘         │  → commit → ACK│         │  → discard   │
       │              └────────────────┘         └──────────────┘
       │                       │                        │
   returns 200            entry acked in            handler sees
   immediately            milliseconds              ONE merged burst
```

Two loops, deliberately split. The consumer never does slow work; the flusher
never touches the stream. Everything that can take seconds happens on the far
side of an ack.

---

## Three failure modes this was built around

Each one has a regression test that fails without the fix. They are the reason
this exists as a library rather than fifty lines in a request handler.

### 1. The deduplicator that eats your retries

The obvious way to deduplicate a webhook is one line:

```python
if await redis.set(key, "1", nx=True, ex=TTL) is None:
    return  # already seen
```

It passes the test everyone writes — send the same delivery twice, the second is
dropped — and it destroys the guarantee you added a durable queue to get.

Walk a crash through it:

1. Delivery arrives, `SET NX` succeeds, work begins.
2. The process is OOM-killed halfway through the handler.
3. The stream entry was never acked, so recovery re-delivers it. Correct so far.
4. The retry runs the same line. `SET NX` fails — the key from step 1 is still
   there, TTL untouched.
5. The retry concludes "duplicate", returns early, and **acks**.

The message is gone. Nobody handled it. The queue reports a clean run. That is
strictly worse than having no queue, because now the report is a lie and the
symptom surfaces days later as *"I texted you and nobody answered."*

The root cause is a category error: `SET NX` records **someone started**, and the
code reads it as **someone finished**. Different facts, different records.

`burstq` splits them. `claim()` writes `pending` with a short TTL; `commit()`
promotes it to `done` with the long one; `release()` drops it on a handled
failure so the retry does not have to outwait the TTL. A claim that nobody
commits expires on its own, and the retry gets a clean shot.

> `tests/test_dedup_swallows_retry.py` implements the naive version alongside the
> real one, so the bug is demonstrated rather than described.

### 2. The debounce window that lives in one process's memory

The natural debouncer sleeps:

```python
await buffer.push(conversation_id, text)
await asyncio.sleep(WINDOW)
if not await buffer.is_last(conversation_id, event_id):
    return
await handler(await buffer.drain(conversation_id))
```

Two defects, both invisible until you are in production.

**It holds the stream entry hostage.** The sleep runs inside the processing of a
stream message, so the entry stays *pending* for the window plus the handler.
Redis Streams' crash recovery steals entries that have been pending too long —
that is its whole job. Window 5s, handler 20s with two retries, idle threshold
30s, and recovery starts re-delivering work that is still running. The two
mechanisms fight, and the symptom is a handler that occasionally runs twice for
no reason anyone can reproduce.

**It does not survive a restart.** A pending sleep is a coroutine in one
process's memory. Deploy mid-window and the buffered messages are still in Redis
with nothing scheduled to ever drain them. They sit until a TTL quietly deletes
them.

`burstq` moves the schedule into Redis. A sorted set holds one entry per open
window, scored by its flush deadline — no cron, no sleeping task, and a restart
loses nothing because the deadline is *data*, not a stack frame. Claiming a due
window takes a lease rather than removing it, so a flusher that dies mid-handler
does not take the burst with it.

### 3. The lock that silently stops locking at two replicas

Per-conversation `asyncio.Lock` is exactly right — inside one process. The trap
is that nothing says so. The consumer group is built for N workers; the consumer
name is derived from the hostname precisely so several can coexist. Add a second
replica for zero-downtime deploys and the in-process lock provides no mutual
exclusion at all. Nothing errors. Two workers just start handling the same
conversation simultaneously, and a subset of users get duplicated replies that
nobody can reproduce locally.

Here the choice is explicit — `Settings(distributed_lock=True)` swaps in a Redis
lock with an ownership token — the in-process implementation carries
`replica_safe = False` as a class attribute, and `start()` logs a warning when it
is in use.

---

## The invariant that a comment could not enforce

Four durations interact, and in the system this came from they were tuned
independently, in different files, with their relationship living in nobody's
head:

| setting | meaning |
|---|---|
| `window_seconds` | how long to wait for a burst to finish |
| `handler_timeout` | how long the terminal step may take |
| `claim_idle_seconds` | how long before an unacked entry is stolen back |
| `processing_ttl` | how long an in-flight idempotency claim survives |

Two relationships have to hold, and both were found the hard way.

`claim_idle_seconds` must exceed the worst case a burst can legitimately take, or
recovery fights live workers. And `processing_ttl` must be *shorter* than
`claim_idle_seconds`, or the two-phase claim is undone by its own timing: a claim
that outlives the redelivery it was meant to survive is still `pending` when the
retry arrives, the retry reads that as "a healthy peer has this", acks, and the
message is gone. Same loss as failure mode 1, reached through the other door.

So `Settings` refuses to construct:

```python
>>> Settings(max_window_seconds=30, handler_timeout=20, claim_idle_seconds=30)
ConfigError: claim_idle_seconds (30s) must exceed max_window_seconds +
handler_timeout (50s). Below that threshold, crash recovery re-delivers entries
that are still being processed and the same burst is handled twice.
```

A misconfiguration that used to be a rare, unreproducible double-reply is now an
error at startup.

---

## Delivery semantics

Stated plainly, because a queue that is vague about this is worse than no queue:

- A burst is handled **at least once**. If the process dies after the handler's
  side effects but before the window is discarded, the lease expires and the same
  burst runs again. **Handlers must tolerate that.**
- A burst is **never silently dropped**. The window is removed only after the
  handler returns, so every failure path ends in a retry or the dead-letter
  queue.
- Ordering is guaranteed **within a conversation** and nowhere else. Two
  conversations never wait on each other; that is the point.

---

## Running it

```bash
docker compose up -d redis
pip install -e ".[demo,metrics,dev]"

uvicorn demo.app:app --reload      # terminal 1
python demo/burst.py               # terminal 2 — fires 5 deliveries
```

Five deliveries go in (one of them a duplicate). One handler call comes out, with
four messages merged:

```
handling conversation=chat-9f2c1a messages=4 text='hi saw the listing is it still available ?'
```

```bash
pytest
curl localhost:8000/health
curl localhost:8000/dlq
```

The suite prefers a real server (`BURSTQ_TEST_REDIS`, default
`redis://localhost:6379/15`) and falls back to an in-process one when none is
reachable, so `pytest` works without Docker. It never mocks the library's own
behaviour — the same Lua scripts and stream commands run either way. CI always
uses a real Redis, across Python 3.11–3.13.

---

## Observability

Prometheus metrics are optional (`pip install burstq[metrics]`) and degrade to
no-ops rather than failing the import.

`burstq_stream_pending` is the one to alert on: if it grows, the consumer is
behind. `burstq_open_windows` next to it separates "we are slow" from "we are
waiting on purpose".

`/health` performs an actual round trip to Redis and reports stream length,
pending count, consumer count, DLQ depth and whether the lock in use is replica
safe. A health endpoint that returns `{"ok": true}` without touching its
dependency is a status page for the web framework, not for the service.

---

## Known limits

- **One Redis.** The distributed lock is `SET NX PX` with an ownership token, not
  Redlock. Redlock buys correctness across independent masters; this pipeline
  already treats a single Redis as its source of truth, so adding it would be
  ceremony, not safety.
- **The flusher polls.** A 250ms tick, not a notification. Simple, survives
  restarts, and costs one `ZRANGEBYSCORE` per tick. If your window is measured in
  milliseconds this is the wrong library.
- **No priority.** Bursts are flushed in deadline order, full stop.
- **`max_concurrency` is per replica**, not global.

## What I would build next

- Per-conversation backpressure: a single very chatty conversation can currently
  occupy a flusher slot repeatedly.
- A DLQ replay endpoint. Reading the dead-letter queue is supported; re-injecting
  from it is still a manual `XADD`.
- An adapter contract test kit, so a new provider adapter can be verified against
  the same suite the demo adapter passes.

---

## Where this came from

A production chat integration handling a few thousand messages a day. The
pipeline shape is the one that ran there; the three fixes above are bugs that
were found in it and are corrected here rather than shipped as-is.

MIT licensed.

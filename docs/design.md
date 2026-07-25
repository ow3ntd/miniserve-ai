# miniserve-ai — Design

> Skeleton (Day 1). Each section is filled in on the day its subject is built,
> not before. Sections marked TBD contain no invented content.

## 1. Overview and positioning

miniserve-ai is a concurrent request scheduler with a pluggable model backend.
The scheduling core — a bounded, thread-safe queue with batch-formation
semantics — is implemented in C++ and exposed to Python via pybind11. Python
(FastAPI + asyncio) owns orchestration; C++ owns the hot path.

## 2. Request lifecycle

*(Written Day 5, before scheduler implementation.)*

Every request is represented by exactly one per-request future, created at
admission and **resolved exactly once** — with a result or with an error.
That single-resolution rule is the central invariant of the design; every
state transition below names which party resolves the future.

States and transitions:

                ┌────────────────────────────────────────────────┐
                │              (queue full) ──► REJECTED 503     │
    arrival ──► ADMITTED ──► QUEUED ──► FORMING ──► EXECUTING ──► COMPLETED 200
                │              │            │            │
                │              │ (deadline) │ (deadline) │ (deadline passed
                │              ▼            ▼            ▼  during inference)
                │           EXPIRED 504  EXPIRED 504  EXPIRED 504 (result discarded)
                │
                └── (shutting down) ──► REJECTED 503

- **ADMITTED** — request passed schema validation; deadline computed as
  `arrival + request_timeout` on the monotonic clock. A future is created.
- **QUEUED** — pushed into the bounded queue. If the queue is full, the
  request is instead rejected immediately (503; see §6.1) and its future
  resolved with the rejection.
- **FORMING** — the worker has popped it into a batch under formation.
  Requests whose deadline has already expired are filtered out here,
  resolved with 504, and **never enter execution** (see §6.2).
- **EXECUTING** — part of a batch inside `ModelRunner.predict()`.
- **COMPLETED** — row *i* of the batch output is routed back to request
  *i*'s future (the ordering contract from §4) and the future resolves 200.
- A request whose deadline passes **during** inference still completes the
  forward pass (a running batch cannot be aborted mid-matmul), but its
  caller receives 504 and the computed result is discarded — deadline
  semantics are honored over sunk cost.

## 3. C++ bounded queue and Python boundary

*(Implemented after Day 5 once the macOS toolchain blocker cleared.)*

The first native primitive is `miniserve::BoundedQueue`, a bounded FIFO of
opaque `uint64_t` request IDs. `try_push()` performs the capacity check and
insertion under one mutex, so admission is linearizable: the caller either
owns a queue slot or receives `false` immediately. `try_pop()` and
`pop_batch(max_items)` remove in FIFO order under the same lock. Queue depth
can therefore never exceed the constructor-provided capacity.

**Why IDs instead of Python objects.** Python owns request payloads and
`asyncio.Future` instances in a registry keyed by request ID. The C++ queue
never stores `py::object`. Copying or destroying a Python object changes its
reference count and requires the GIL; allowing those operations inside a
producer/consumer queue would create unsafe lifetime edges and make the native
core's concurrency claims misleading. Integer IDs keep ownership explicit and
let the scheduler resolve futures on the Python event loop.

**Why C++ at all.** The point is not that a mutex-protected deque is impossible
in Python. The point is to isolate a small, testable admission primitive whose
capacity invariant and FIFO behavior are independent of the event loop and
whose operations can run with the GIL released. The pybind11 methods use
`gil_scoped_release`, and the native test exercises multiple producers and
consumers for loss, duplication, full drain, and bounded completion. This continues the
`lob-latency-lab` pattern: keep the hot concurrent state transition small and
native; keep orchestration and policy in Python.

**Deliberate scope boundary.** The queue does not yet own deadlines, shutdown
state, blocking waits, payloads, or futures. Those are scheduler policies and
will be added only after the basic request registry and exactly-once resolution
path exist.

### 3.1 Python request registry and ownership

The native queue stores only opaque `uint64_t` request IDs. Python owns the
state associated with each ID through `RequestRegistry`:

- an immutable tuple of input token IDs
- an `asyncio.Future` belonging to the active event loop
- the monotonic arrival timestamp
- the absolute monotonic deadline

This boundary prevents the concurrent C++ core from owning Python objects or
performing Python reference-count operations. The native layer is responsible
only for bounded FIFO admission and batch formation; Python remains responsible
for payload lifetime, future completion, cancellation, and error propagation.

Registry mutation is confined to one asyncio event-loop thread. The registry is
therefore intentionally not protected by locks and must not be mutated directly
from worker threads.

Each lifecycle operation removes the record before resolving, failing, or
cancelling its future. This ordering makes completion re-entrant-safe: callbacks
triggered by future completion cannot find and complete the same request again.

`resolve()`, `fail()`, and `cancel()` return `True` only for the first valid
completion. Repeated or late lifecycle events return `False` and become safe
no-ops. This provides the exactly-once ownership contract required for future
races among inference completion, deadlines, cancellation, and shutdown.

Deadlines are recorded at registration using a monotonic clock. This milestone
stores deadline metadata only; expiration enforcement is added later in the
scheduler and batch-formation layers.

### 3.2 Admission transaction

`AdmissionController` connects the Python-owned request registry to the
bounded native queue. Admission is performed synchronously on the asyncio
event-loop thread as a small transaction:

1. Register the payload, future, arrival time, and deadline in Python.
2. Call the native queue's non-blocking `try_push()` with the opaque ID.
3. On success, retain both the registry record and queued request ID.
4. On rejection or enqueue failure, remove the registry record and cancel
   its internal future before propagating the admission error.

The externally visible queue-full outcome is `QueueFullError`, which the API
layer will later translate to HTTP 503. Cancelling the internal future during
rollback is cleanup only: `admit()` does not return the rejected
`PendingRequest`, so cancellation is not presented as the client-visible
result.

Rejected request IDs are not reused. ID allocation remains monotonic so logs,
metrics, and future lifecycle investigations cannot confuse a later request
with an earlier rejected one.

The admission invariants are:

- If `admit()` returns, the request ID is queued and its Python registry record
  remains live.
- If `admit()` raises, no registry record or unresolved internal future remains.
- Queue admission never blocks.
- Queue-full rejection does not disturb previously accepted requests.
- FIFO ordering is preserved by the native queue.

## 4. Model runner and batch shape assembly

*(Written Day 2, when this was built.)*

The model runner (`app/model_runner.py`) owns the model lifecycle and the
batch tensor shape work. The model itself is a deliberately tiny, in-repo
network — embedding → mask-aware mean pooling → linear head — because the
model is incidental to this project; keeping it in-repo makes startup
instant, tests deterministic (seeded weights), and the repo free of
checkpoint downloads.

**The shape problem.** Requests arrive as variable-length token-id
sequences. Tensors are rectangular, so a batch of lengths {3, 12, 1, 4}
cannot be stacked directly. Assembly (`_assemble`) pads every request to
the longest length *in that batch* (not a global maximum — short batches
shouldn't pay for a worst case that isn't present) and builds a parallel
{1.0, 0.0} mask marking which positions are real.

**Why padding can't leak.** Two mechanisms, both in the forward pass:
padded embeddings are multiplied by the 0.0 mask before the sum, and the
mean divides by the count of *real* tokens per row rather than the padded
length. A consequence worth stating explicitly: the pad token's id is
mathematically irrelevant (its embedding is zeroed), so `PAD_ID = 0` can
also legitimately appear as a real token.

**Disassembly and ordering.** The model returns `[B, n_outputs]`; row *i*
belongs to request *i*. That ordering is the contract the scheduler relies
on to route each result back to the correct caller's future.
`_disassemble` converts to plain Python floats so nothing above the runner
ever handles tensors.

**The property that matters.** Batching must never change a request's
result — a padding leak wouldn't crash anything, it would just silently
corrupt outputs, which is the worst failure mode a batching server can
have. This is enforced by test
(`test_batch_invariance_padding_does_not_leak`): each request's solo
output must match its output when batched with unrelated, differently
sized requests. Tolerance is `1e-5`, not exact equality, because batched
and single-row matrix multiplies may take different BLAS kernel paths with
different floating-point summation orders.

Everything runs on CPU under `torch.inference_mode()`.

## 5. Batching design

*(Written Day 5, before scheduler implementation.)*

Two parameters govern batch formation:

- **`max_batch_size`** — hard cap on requests per batch.
- **`max_wait_ms`** — how long a partially filled batch may wait for more
  requests before executing anyway.

Formation rule: the worker pops requests into a forming batch. The batch
window opens when the first request enters the empty batch; the batch is
dispatched when **either** it reaches `max_batch_size` **or** the window
has been open `max_wait_ms` — whichever comes first. Deadline-expired
requests are filtered at formation (§2) and do not count toward batch
size.

The tradeoff, stated against measured numbers (Day 4 baseline,
`results/benchmark_summary.md`): unbatched, this system saturates at
~1,700 req/s with p99 exploding to ~23 ms at concurrency 8 — every request
pays full per-invocation overhead. Batching amortizes that overhead across
the batch at the cost of up to `max_wait_ms` of added latency for the
first request in a window. `max_wait_ms` therefore buys throughput with
p50 latency; the batching benchmarks (Day 13 in the plan) measure that
exchange rate explicitly rather than asserting it.

The race between the `max_wait_ms` timer and concurrent producers (a batch
dispatching while a request is mid-push) is a known hazard; it is a
primary target of the deliberate bug hunt (Day 10) and its resolution will
be documented in §8.

## 6. Backpressure, timeouts, and graceful shutdown

*(Written Day 5. Decisions 6.1–6.3 were made before implementation;
rationale recorded at decision time, not retrofitted.)*

### 6.1 Backpressure: reject instantly on full queue (503)

When a request arrives and the queue holds `max_queue_size` entries, the
server rejects it **immediately** with HTTP 503 — no waiting for a slot.

Rationale: this is explicit backpressure. An instant 503 is the fastest
possible signal for a client to back off or retry elsewhere; a brief
blocking wait would absorb micro-bursts but muddies latency accounting
(time-to-503 becomes load-dependent) and hides the overload signal the
metric exists to expose. Bounded queue + instant rejection also caps
memory at a known constant. Every rejection increments
`queue_full_count`.

### 6.2 Timeout: deadline starts at arrival (504)

The request deadline is `arrival_time + request_timeout` on the monotonic
clock. Queue wait, batching delay, and inference execution all count
against it. A request whose deadline expires while queued is **never
executed**: it is filtered at batch formation and resolved with HTTP 504.

Rationale: the client experiences one number — time since it sent the
request — so the server's deadline must be measured on the same clock, or
"timeout" stops meaning anything under load. Starting the clock at dequeue
would allow a request to sit in queue indefinitely and still "not time
out," which is precisely the pathological regime Day 4 measured (queueing
delay, not compute, dominating tail latency). Never-execute-expired also
protects a saturated server from spending compute on responses no one is
waiting for — under overload, that wasted work is what turns saturation
into collapse.

### 6.3 Shutdown: bounded drain (503 for the queue)

On SIGTERM the server: (1) stops admitting new requests (503), (2) allows
the currently executing batch to finish, (3) resolves every remaining
queued request's future with HTTP 503, then exits.

Rationale: shutdown time is bounded by one batch execution — predictable
for process supervisors — while draining the whole queue would make
shutdown time unbounded under load. Queued-but-unstarted work is safe to
reject (the client gets an honest retryable signal); in-flight work is
finished because aborting mid-batch would discard compute already spent.
**No request future may remain unresolved at exit** — an orphaned future
is a hung client, and the shutdown-drain test exists to prove this
invariant.

### 6.4 HTTP status behavior

| Status | Meaning | Producer |
|---|---|---|
| 200 | prediction returned | scheduler → future |
| 422 | invalid request (schema or vocab) | Pydantic / endpoint |
| 503 `queue full` | backpressure rejection at admission | scheduler admission |
| 503 `shutting down` | rejected at admission, or queued at shutdown | scheduler admission / drain |
| 503 `model not loaded` | readiness failure | endpoint |
| 504 | deadline expired (queued, at formation, or during inference) | scheduler |
| 500 | unexpected execution failure | scheduler → future |

### 6.5 Metrics affected

`queue_full_count`, `timeout_count` (504s), `shutdown_rejected_count`,
current queue depth, queue-wait time and inference time (decomposed —
§6.2 makes queue wait a first-class component of user-visible latency),
batch size history. Full metrics design: §7 (Day 13).

### 6.6 Invariants

1. Every future is resolved exactly once (200, 503, 504, or 500; 422
   never reaches the scheduler).
2. No deadline-expired request enters model execution.
3. Queue depth never exceeds `max_queue_size`.
4. All deadlines use the monotonic clock; wall-clock jumps cannot expire
   or resurrect requests.
5. Shutdown completes in bounded time: ≤ one batch execution plus queue
   drain (rejections are O(1) each).
6. After shutdown begins, no new request is admitted.

### 6.7 Edge cases

- **Deadline expires between admission and enqueue** (pathologically small
  timeout): filtered at formation like any expired request; 504.
- **Entire forming batch expired**: skip execution entirely; resolve all
  with 504; worker proceeds to next batch.
- **Shutdown during an open batch window**: the partial batch executes
  (it is "currently executing" the moment the drain begins); the queue
  behind it is rejected.
- **Repeated SIGTERM**: idempotent; the drain runs once.
- **Queue-full race with a just-freed slot**: a request may see "full"
  while a slot frees concurrently. Accepted: the 503 is still honest at
  the instant it was issued. The reverse race (accept then find no slot)
  must be impossible — admission and enqueue must be atomic in the C++
  core. Treated fully in the Day 10 bug hunt.
- **Client disconnects while queued**: v0.1 does not detect this; the
  request executes and the result is discarded at the response layer.
  Recorded as a limitation (§10).

## 7. Metrics and tracing

TBD (Day 13). Will cover: counter/histogram choices, latency decomposition
(queue wait vs. inference time), and correlation-ID threading.

## 8. Concurrency bugs found and fixed

TBD (Day 10). Summary here; full root-cause writeups in
[`concurrency_bugs.md`](concurrency_bugs.md).

## 9. Benchmark methodology

*(Day 4.)* Methodology — closed-loop generator, 5+ trials with mean ±
stddev, warmup-excluded steady state, cold-start reported separately,
environment logging, AC power, no access logging — is stated in the
preamble of [`results/benchmark_summary.md`](../results/benchmark_summary.md)
and the environment in [`results/environment.md`](../results/environment.md).

## 10. Limitations

TBD. Known now: CPU-only, single-process, local; no distributed
infrastructure by design in v0.1. Client disconnects while queued are not
detected (§6.7).

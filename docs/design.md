# miniserve-ai — Design

This document describes the implemented Day 7 system. Planned policies are
marked explicitly.

## 1. Overview

miniserve-ai is a CPU-only, single-process inference service. FastAPI and
asyncio own orchestration, a small C++ bounded FIFO owns queued request IDs,
and `ModelRunner` owns batch tensor construction and inference.

The current request path is:

    POST /predict
        -> structural and model-specific validation
        -> AdmissionController
        -> absolute deadline in RequestRegistry + C++ BoundedQueue ID
        -> AsyncScheduler
        -> dynamic batch formation and size-or-time dispatch
        -> ModelRunner.predict() in a worker thread
        -> per-request deadline supervision and future completion
        -> HTTP response

## 2. Request ownership and lifecycle

Python owns each request's immutable token tuple, event-loop-bound future,
monotonic arrival time, and stored deadline in `RequestRegistry`. The native
queue stores only an opaque `uint64_t` ID.

Registry mutation is confined to the asyncio event-loop thread. Each lifecycle
operation removes the entry before resolving, failing, or cancelling its
future. Late and repeated operations therefore become safe no-ops, preserving
exactly-once completion without registry locks.

Current transitions are:

    arrival -> VALIDATED -> ADMITTED -> QUEUED -> EXECUTING -> COMPLETED 200
                    |           |          |             |
                    |           |          +-------------+-- EXPIRED 504
                    |           +-- queue full -> REJECTED 503
                    +-- invalid input -> REJECTED 422

`RequestDeadlineExceeded` is the domain exception for an admitted request that
exceeds its end-to-end deadline. Caller cancellation remains a separate
lifecycle outcome. Terminal scheduler stop cancels all pending entries.

`PendingRequest.deadline` is created once during registration as arrival plus
`request_timeout_s`; it remains authoritative across queueing, batch formation,
and inference. `RequestRegistry` exposes the same monotonic clock used to create
and compare deadlines. Durations passed to asyncio waits are derived from that
absolute clock domain rather than starting a fresh phase-local timeout.

The result/deadline/cancellation race is remove-first: the first successful
registry lifecycle operation owns completion. Result first means 200; deadline
first means `RequestDeadlineExceeded` and HTTP 504; caller cancellation first
means cancellation. Subsequent attempts return `False` and are safe no-ops.

## 3. C++ bounded queue and Python boundary

`miniserve::BoundedQueue` is a fixed-capacity FIFO of opaque request IDs.
`try_push()` performs its capacity check and insertion under one mutex and
returns `false` immediately when full. `try_pop()` and
`pop_batch(max_items)` remove IDs in FIFO order under the same lock. Queue
depth cannot exceed the constructor-provided capacity.

The pybind11 boundary releases the GIL around queue operations. Keeping Python
objects out of the queue avoids reference-count and lifetime hazards while the
GIL is released. Native tests cover capacity, FIFO behavior, full drain, and
multi-producer/multi-consumer loss and duplication; Python tests cover the
binding contract.

The queue deliberately does not own payloads, futures, deadlines, blocking
waits, or shutdown policy.

### 3.1 Admission transaction

`AdmissionController` performs one synchronous, non-blocking transaction on
the event-loop thread:

1. Register the request payload, future, arrival time, and deadline in Python.
2. Attempt one native `try_push()` with its opaque ID.
3. Retain both pieces of state on success.
4. On rejection or enqueue failure, remove the registry entry and cancel its
   internal future before propagating the error.

Queue saturation is represented by `QueueFullError`; `/predict` translates it
to HTTP 503. Rejected IDs are not reused. If admission returns, both the queued
ID and registry entry exist; if it raises, no registry entry or unresolved
future remains.

## 4. Model runner and validation

`ModelRunner` owns the load-once model lifecycle and batch tensor work. The
in-repo model is embedding → mask-aware mean pooling → linear head, running on
CPU under `torch.inference_mode()`.

Variable-length requests are padded to the longest request in the current
batch. A parallel mask zeros padded embeddings and makes the mean divide by
the number of real tokens, so padding cannot change another request's output.
The runner returns plain Python rows in input order; output row *i* belongs to
input request *i*.

Pydantic rejects structural errors before endpoint code. The endpoint then
calls `ModelRunner.validate_request()`, which reuses the runner's model-specific
validation rules, including vocabulary bounds. This occurs before admission,
so invalid input returns 422 without entering and poisoning a shared batch.
Validation remains centralized in the runner rather than duplicated in the
endpoint.

## 5. Fixed-size batch worker

`FixedBatchWorker` separates one iteration into phases shared by its original
synchronous `process_once()` API and the asynchronous scheduler:

1. **Prepare.** `prepare_batch(max_items)` pops only the remaining number of
   FIFO IDs a forming batch can accept (defaulting to `max_batch_size`),
   retrieves live requests, and returns a `PreparedBatch`. Unknown IDs are
   stale and safely skipped; an empty or all-stale batch does not invoke the
   model.
2. **Predict.** `predict()` receives only immutable token tuples and invokes
   the blocking model. It enforces the output-cardinality invariant: exactly
   one output per live request.
3. **Complete or fail.** `complete_batch()` routes output row *i* to request
   *i*. `fail_batch()` propagates genuine model/inference errors or an output-
   count violation to every live request in that batch.

Preparation, registry lookup, and completion/failure remain on the event-loop
thread. Only the registry-free immutable inputs and model result cross the
`asyncio.to_thread()` boundary. FIFO correspondence plus registry removal on
completion preserves exactly-once routing.

The optional `max_items` bound is the only Day 6 worker extension. The original
zero-argument preparation and synchronous `process_once()` behavior are
preserved.

### 5.1 Dynamic batch formation

Two configuration values govern the implemented forming-batch lifecycle:

- `max_batch_size` is the maximum number of **live** requests in a batch.
- `max_wait_ms` is the maximum batching delay measured from the first live
  request entering an empty forming batch. It is finite and non-negative;
  zero preserves Day 5 immediate dispatch.

The scheduler uses the asyncio event loop's monotonic clock. Recovering the
first live request establishes one absolute batch deadline. Later live
arrivals may fill remaining capacity, but the stored deadline is never
recomputed, so they cannot extend the window. The batch dispatches immediately
when it becomes full or as a partial batch when the remaining time reaches
zero, whichever happens first.

Stale and cancelled IDs are removed from the native queue but do not count
toward live capacity. The worker continues popping within the remaining
capacity so live requests behind stale IDs can join the same forming batch.
An empty or all-stale batch never invokes the model. Live requests preserve
native FIFO order, and model output row *i* completes live request *i*.

`max_wait_ms` is a batching latency policy, not a client timeout.
`request_timeout_s` is the end-to-end client deadline beginning at registry
admission.

A forming batch waits until the earlier of its fixed first-live-request
`max_wait_ms` deadline and the earliest live request deadline. Expiring a
request does not reset or redefine the batching window. The registry removes
the request exactly once and completes its future with
`RequestDeadlineExceeded`; its opaque ID may remain in the native queue until
dequeued as stale. Stale IDs do not consume capacity, all-stale batches never
invoke the model, and unrelated live requests continue normally. Deadlines are
rechecked immediately before dispatch.

## 6. Asynchronous scheduler

`AsyncScheduler` owns one background asyncio task. Successful admission sets
an `asyncio.Event`; the task waits on that event when idle rather than polling
the queue, so an empty queue consumes no busy-spin CPU.

The task clears the event before consuming signaled work. When a partial batch
is forming, it waits on that same event with the absolute batch deadline as its
timeout—there is no polling loop or short sleep. Admission sets the event and
lets the former inspect new FIFO IDs without changing the deadline.

If admission races timeout expiration, either the timed wait observes the
event or the timeout dispatches the current partial batch. After every dispatch
the scheduler checks the queue again before returning to idle waiting, and a
concurrently set event remains available to the next wait. The same ordering
protects the empty-queue drain-to-wait transition, preventing lost wake-ups
without busy-spinning.

The single scheduler task launches each model call through
`asyncio.to_thread()` and keeps model invocations serialized. While inference
runs, one supervision loop watches all registry-owned deadlines, including
requests queued behind the active invocation. Expired requests can therefore
receive 504 before inference finishes. Python does not attempt to terminate
the underlying executor thread.

Output correspondence remains positional: model row *i* always belongs to
original dispatched request *i*. Completion does not filter or shift output
rows. If request *i* timed out or was cancelled while inference ran, resolving
that ID is a safe no-op; other rows still reach their original requests.

### 6.1 Backpressure

Admission calls the native queue's non-blocking `try_push()` exactly once.
When capacity is exhausted, `QueueFullError` is raised, registration is rolled
back, and `/predict` immediately returns HTTP 503. Previously accepted requests
are unaffected.

### 6.2 Caller cancellation

`AsyncScheduler.wait()` uses `asyncio.shield()` so cancellation of the caller
does not implicitly cancel an internally owned future behind the registry's
back. On caller cancellation, the scheduler explicitly removes and cancels
the request through `RequestRegistry`. Its queued native ID later appears as a
stale ID and is skipped without an `InvalidStateError`.

The wait uses the request's remaining absolute deadline. Because
`RequestDeadlineExceeded` derives from `TimeoutError` and
`asyncio.TimeoutError` aliases that built-in type, a caught wait timeout is
disambiguated by inspecting the owned future: an already-completed future
propagates its domain exception or winning result directly; only a still-
pending future triggers deadline expiration. No per-request background timeout
task or phase-local deadline is created.

### 6.3 Terminal stop

Stop is terminal cancellation, not graceful drain. `stop()` cancels the
background task and all pending registry entries, and the scheduler instance
cannot restart.

This rule is required because cancelling the asyncio wrapper around
`asyncio.to_thread()` does not terminate the underlying inference thread. The
scheduler explicitly cancels and awaits/consumes that wrapper so a later model
exception cannot leak as an un-retrieved asyncio task exception. The executor
thread may still finish after stop. By forbidding restart, the same scheduler
cannot launch a new inference overlapping that old call. Queued IDs are not
gracefully drained.

A graceful policy that handles in-flight and queued work is explicitly
deferred.

## 7. Current HTTP behavior

| Status | Meaning | Producer |
|---|---|---|
| 200 | prediction returned | scheduler → individual future |
| 422 | invalid schema or model-specific input | Pydantic / endpoint |
| 503 `request queue is full` | bounded-queue backpressure | admission / endpoint |
| 503 `model not loaded` | readiness failure | endpoint |
| 504 | admitted request exceeded its absolute deadline | scheduler / endpoint |
| 500 | unexpected model/inference failure | scheduler → future |

Graceful-shutdown-specific responses are planned.

## 8. Metrics and tracing

Not implemented. Planned work includes queue depth and rejection counts, batch
size, queue wait, inference time, timeout counts, and structured correlation.

## 9. Benchmark methodology

The recorded benchmark uses a closed-loop generator, 5+ trials with mean ±
standard deviation, warmup-excluded steady state, separately reported cold
start, environment logging, AC power, and no access logging. Methodology is in
[`results/benchmark_summary.md`](../results/benchmark_summary.md) and the
environment in [`results/environment.md`](../results/environment.md).

Those measurements cover the older synchronous, unbatched baseline. No Day 7
scheduler benchmark has been recorded.

## 10. Limitations

- CPU-only and single-process.
- Shutdown is terminal cancellation rather than graceful drain.
- A running inference thread may finish after stop.
- Queued native IDs are not drained during shutdown.
- Metrics, tracing, and scheduler/open-loop benchmarks remain planned.
- Multiple scheduler workers and GPU serving are not implemented.
- Existing measured results cover the older synchronous baseline.

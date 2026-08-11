# miniserve-ai — Design

This document describes the implemented Day 5 system. Planned policies are
marked explicitly.

## 1. Overview

miniserve-ai is a CPU-only, single-process inference service. FastAPI and
asyncio own orchestration, a small C++ bounded FIFO owns queued request IDs,
and `ModelRunner` owns batch tensor construction and inference.

The current request path is:

    POST /predict
        -> structural and model-specific validation
        -> AdmissionController
        -> RequestRegistry + C++ BoundedQueue
        -> AsyncScheduler
        -> fixed-size batch preparation
        -> ModelRunner.predict() in a worker thread
        -> event-loop-thread future completion
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
                    |           |
                    |           +-- queue full -> REJECTED 503
                    +-- invalid input -> REJECTED 422

Caller cancellation removes and cancels the registry-owned future. Terminal
scheduler stop cancels all pending registry entries. Deadlines are recorded on
the monotonic clock but are not enforced; there is no HTTP 504 path yet.

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

1. **Prepare.** `prepare_batch()` pops up to `max_batch_size` FIFO IDs,
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

Current batching is **fixed-size and immediate**. After waking, the scheduler
repeatedly processes up to `max_batch_size` requests already available; it does
not delay a partial batch to collect more work.

### 5.1 Planned dynamic batching

Dynamic formation using `max_wait_ms` is planned, not implemented. The intended
policy is to dispatch when either `max_batch_size` is reached or the wait
window opened by the first request expires. Its latency/throughput tradeoff and
timer/producer races require separate implementation and measurement.

## 6. Asynchronous scheduler

`AsyncScheduler` owns one background asyncio task. Successful admission sets
an `asyncio.Event`; the task waits on that event when idle rather than polling
the queue, so an empty queue consumes no busy-spin CPU.

The task clears the event before draining all immediately available batches.
Admission during the drain sets it again, so after the queue becomes empty the
next wait observes the set event. Admission after the task begins waiting wakes
it normally. This ordering prevents a request from being stranded around the
drain-to-wait transition.

The single scheduler task awaits each `asyncio.to_thread()` model call before
preparing another batch. Consequently it launches at most one model invocation
at a time, while slow inference does not block the event loop.

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

### 6.3 Terminal stop

Stop is terminal cancellation, not graceful drain. `stop()` cancels the
background task and all pending registry entries, and the scheduler instance
cannot restart.

This rule is required because cancelling an asyncio task awaiting
`asyncio.to_thread()` does not terminate the underlying inference thread. An
already-running model call may therefore finish after scheduler stop. By
forbidding restart, the same scheduler cannot launch a new inference that
overlaps that old call. Queued native IDs are not drained.

A graceful policy that handles in-flight and queued work is explicitly
deferred.

## 7. Current HTTP behavior

| Status | Meaning | Producer |
|---|---|---|
| 200 | prediction returned | scheduler → individual future |
| 422 | invalid schema or model-specific input | Pydantic / endpoint |
| 503 `request queue is full` | bounded-queue backpressure | admission / endpoint |
| 503 `model not loaded` | readiness failure | endpoint |
| 500 | unexpected model/inference failure | scheduler → future |

Deadline 504 and graceful-shutdown-specific responses are planned.

## 8. Metrics and tracing

Not implemented. Planned work includes queue depth and rejection counts, batch
size, queue wait, inference time, timeout counts, and structured correlation.

## 9. Benchmark methodology

The recorded benchmark uses a closed-loop generator, 5+ trials with mean ±
standard deviation, warmup-excluded steady state, separately reported cold
start, environment logging, AC power, and no access logging. Methodology is in
[`results/benchmark_summary.md`](../results/benchmark_summary.md) and the
environment in [`results/environment.md`](../results/environment.md).

Those measurements cover the older synchronous, unbatched baseline. No Day 5
scheduler benchmark has been recorded.

## 10. Limitations

- CPU-only and single-process.
- Dynamic `max_wait_ms` batch formation is not implemented.
- Deadlines are stored but not enforced.
- Shutdown is terminal cancellation rather than graceful drain.
- A running inference thread may finish after stop.
- Queued native IDs are not drained during shutdown.
- Metrics and scheduler benchmarks remain planned.
- Existing measured results cover the older synchronous baseline.

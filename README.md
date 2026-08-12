# miniserve-ai

[![CI](https://github.com/ow3ntd/miniserve-ai/actions/workflows/ci.yml/badge.svg)](https://github.com/ow3ntd/miniserve-ai/actions/workflows/ci.yml)

**A concurrent request scheduler with a pluggable model backend** — a hand-built C++ bounded-queue core exposed to Python via pybind11, with an asynchronous dynamic-batching scheduler and FastAPI integration on top.

The model is incidental; the scheduler is the point.

> **Status: v0.1 in progress — Day 7 end-to-end request deadlines and HTTP 504 are implemented.** Graceful draining, metrics, tracing, and scheduler benchmarks remain planned.

## Why this project exists

Serving systems live or die on their concurrency core: how requests queue, how batches form, what happens when the queue is full, and what happens to in-flight work at shutdown. This repo builds that core deliberately — including intentionally inducing and fixing race conditions — rather than assembling framework defaults. It is the serving-side companion to [`lob-latency-lab`](https://github.com/ow3ntd/lob-latency-lab), which applied the same approach (C++ hot path, stress testing, benchmark methodology) to a limit order book.

## Architecture

```text
POST /predict
    -> structural + model-specific validation
    -> AdmissionController
    -> absolute request deadline recorded in RequestRegistry
    -> native C++ BoundedQueue (opaque IDs)
    -> AsyncScheduler (one asyncio task, Event wake-up)
    -> dynamic batch formation
    -> dispatch on max_batch_size OR max_wait_ms
    -> ModelRunner.predict() in an asyncio.to_thread() worker thread
    -> per-request deadline supervision
    -> exactly-once result / timeout / cancellation completion
    -> HTTP response
```

Python owns request payloads, lifecycle state, and per-request futures. The C++ queue stores only opaque integer IDs, avoiding Python-object lifetime and GIL hazards inside the concurrent core. Registry access and future completion remain confined to the asyncio event-loop thread; only blocking model inference crosses into a worker thread.

A forming batch dispatches when it collects `max_batch_size` **live** requests or when `max_wait_ms` elapses from the first live request, whichever occurs first. Later arrivals may join but cannot reset that deadline. Stale or cancelled IDs do not consume capacity; they are skipped while the scheduler continues looking for live work. `max_wait_ms=0` preserves immediate dispatch. FIFO ordering among live requests is retained, model calls remain serialized, and `asyncio.Event` wake-ups avoid polling and busy-spinning.

`request_timeout_s` is an end-to-end deadline beginning at registration. One absolute deadline is recorded and compared in the registry's monotonic clock domain. `max_wait_ms` is separate: it limits batching latency from the first live request entering a forming batch and is not a client request timeout. Batch waits wake at the earlier of those two deadlines.

Expired requests are removed individually, so they do not poison batch-mates. Their queued native IDs become harmless stale entries, and deadlines are rechecked before dispatch. Requests continue aging behind active inference; an individual caller may receive 504 while its already-running model invocation continues, with late output ignored through exactly-once registry ownership. Other members retain their positional outputs and may complete normally. Caller cancellation remains distinct from deadline expiration.

## Features

- [x] Health endpoint (`GET /health`)
- [x] C++ bounded, thread-safe FIFO with immediate full-queue rejection
- [x] FIFO batch pop and multi-producer/multi-consumer native stress test
- [x] pybind11 boundary that releases the GIL around queue operations
- [x] Model runner abstraction with explicit batch assembly/splitting and a padding-leak invariance test
- [x] Python `RequestRegistry` with exactly-once lifecycle operations
- [x] Transactional, non-blocking `AdmissionController`
- [x] Fixed-size batch worker with FIFO output routing
- [x] Dynamic batch formation using `max_batch_size` and `max_wait_ms`
- [x] End-to-end `request_timeout_s` enforcement and HTTP 504
- [x] One-task asynchronous scheduler with event-driven wake-up
- [x] Async `POST /predict` integration and queue-full HTTP 503
- [x] Per-request future routing and request-cancellation cleanup
- [x] Model inference offloaded from the asyncio event loop and serialized
- [x] Pre-admission model-specific validation
- [x] Reproducible closed-loop baseline benchmark
- [x] CI with native/Python tests plus sanitizer builds
- [ ] Deliberate concurrency bug hunt, documented in `docs/concurrency_bugs.md`
- [ ] Graceful bounded shutdown and drain
- [ ] Prometheus `/metrics` + structured tracing with correlation IDs
- [ ] Open-loop overload tests and recorded trace replay

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Configure and build the C++ core + pybind11 module.
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# Run native and Python tests.
ctest --test-dir build --output-on-failure
PYTHONPATH="build/python:." pytest

# Run the current asynchronous dynamic-batch server.
uvicorn app.main:app --reload
```

Example queue use after building:

```python
from miniserve_core import BoundedQueue

queue = BoundedQueue(2)
assert queue.try_push(101)
assert queue.try_push(102)
assert not queue.try_push(103)  # explicit backpressure signal
assert queue.pop_batch(2) == [101, 102]
```

## Benchmarks

The current measured baseline is synchronous and unbatched. On the recorded Apple M4 environment, c=1 measured 1583.8 ± 59.2 req/s with 0.61 ms p50; c=8 measured 1712.2 ± 47.2 req/s with 3.09 ms p50 and 23.25 ms p99. Full methodology and caveats are in [`results/benchmark_summary.md`](results/benchmark_summary.md).

These are the older synchronous, unbatched baseline results. The C++ queue and scheduler are now integrated into `/predict`, but no newer measured scheduler benchmark has been recorded, so these numbers are **not** queue or batching results.

## HTTP outcomes

- **200** — inference completed before the admitted request's deadline.
- **422** — structural or model-specific input validation failed.
- **503** — bounded native queue admission was rejected because the queue was full.
- **504** — an admitted request exceeded its end-to-end deadline.

## Limitations

- CPU-only, single process, local machine. No GPU, distributed serving, Kubernetes, Redis, or Ray in v0.1.
- Shutdown is terminal cancellation, not graceful drain. A scheduler cannot restart after `stop()`.
- Cancelling the owned asyncio inference wrapper does not terminate an executor thread already running model inference. The wrapper is explicitly cancelled and consumed so it cannot leak an unhandled task exception, but the thread may finish after scheduler stop.
- Queued and in-flight work is not gracefully drained during shutdown.
- Metrics, tracing, scheduler/open-loop benchmarks, multiple workers, and GPU serving remain to be implemented. Existing benchmark results measure the older synchronous baseline.
- Not production-ready and not claimed to be; this is a concurrency-first serving lab.

## Next milestone

Implement graceful bounded shutdown and drain behavior for queued and in-flight work.

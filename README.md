# miniserve-ai

[![CI](https://github.com/ow3ntd/miniserve-ai/actions/workflows/ci.yml/badge.svg)](https://github.com/ow3ntd/miniserve-ai/actions/workflows/ci.yml)

**A concurrent request scheduler with a pluggable model backend** — a hand-built C++ bounded-queue core exposed to Python via pybind11, with an asynchronous fixed-batch scheduler and FastAPI integration on top.

The model is incidental; the scheduler is the point.

> **Status: v0.1 in progress — the Day 5 asynchronous scheduler path is implemented.** Dynamic `max_wait_ms` batch formation, deadline enforcement, graceful draining, metrics, and scheduler benchmarks remain planned.

## Why this project exists

Serving systems live or die on their concurrency core: how requests queue, how batches form, what happens when the queue is full, and what happens to in-flight work at shutdown. This repo builds that core deliberately — including intentionally inducing and fixing race conditions — rather than assembling framework defaults. It is the serving-side companion to [`lob-latency-lab`](https://github.com/ow3ntd/lob-latency-lab), which applied the same approach (C++ hot path, stress testing, benchmark methodology) to a limit order book.

## Architecture

```text
POST /predict
    -> structural + model-specific validation
    -> AdmissionController
    -> RequestRegistry (payloads and futures) + C++ BoundedQueue (IDs)
    -> AsyncScheduler (one asyncio task, Event wake-up)
    -> fixed-size immediate batch preparation
    -> ModelRunner.predict() in an asyncio.to_thread() worker thread
    -> event-loop-thread future completion
    -> HTTP response
```

Python owns request payloads, lifecycle state, and per-request futures. The C++ queue stores only opaque integer IDs, avoiding Python-object lifetime and GIL hazards inside the concurrent core. Registry access and future completion remain confined to the asyncio event-loop thread; only blocking model inference crosses into a worker thread.

## Features

- [x] Health endpoint (`GET /health`)
- [x] C++ bounded, thread-safe FIFO with immediate full-queue rejection
- [x] FIFO batch pop and multi-producer/multi-consumer native stress test
- [x] pybind11 boundary that releases the GIL around queue operations
- [x] Model runner abstraction with explicit batch assembly/splitting and a padding-leak invariance test
- [x] Python `RequestRegistry` with exactly-once lifecycle operations
- [x] Transactional, non-blocking `AdmissionController`
- [x] Fixed-size batch worker with FIFO output routing
- [x] One-task asynchronous scheduler with event-driven wake-up
- [x] Async `POST /predict` integration and queue-full HTTP 503
- [x] Per-request future routing and request-cancellation cleanup
- [x] Model inference offloaded from the asyncio event loop and serialized
- [x] Pre-admission model-specific validation
- [x] Reproducible closed-loop baseline benchmark
- [x] CI with native/Python tests plus sanitizer builds
- [ ] Dynamic batch formation with `max_wait_ms`
- [ ] Deliberate concurrency bug hunt, documented in `docs/concurrency_bugs.md`
- [ ] Deadline enforcement and graceful bounded shutdown
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

# Run the current asynchronous fixed-batch server.
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

## Limitations

- CPU-only, single process, local machine. No GPU, distributed serving, Kubernetes, Redis, or Ray in v0.1.
- Batching is fixed-size and immediate: the scheduler executes up to `max_batch_size` currently available requests. Dynamic `max_wait_ms` batch formation is not implemented.
- Deadlines are stored in `RequestRegistry` but are not enforced.
- Shutdown is terminal cancellation, not graceful drain. A scheduler cannot restart after `stop()`.
- An already-running `asyncio.to_thread()` inference may finish after scheduler stop, and queued native IDs are not drained during shutdown.
- Metrics and scheduler benchmarks remain to be implemented. Existing benchmark results measure the older synchronous baseline.
- Not production-ready and not claimed to be; this is a concurrency-first serving lab.

## Next milestone

Add dynamic batch formation using `max_wait_ms`, building on the current immediate fixed-size scheduler. It is planned, not implemented.

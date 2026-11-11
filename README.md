# miniserve-ai

[![CI](https://github.com/ow3ntd/miniserve-ai/actions/workflows/ci.yml/badge.svg)](https://github.com/ow3ntd/miniserve-ai/actions/workflows/ci.yml)

**A concurrent request scheduler with a pluggable model backend** — a hand-built C++ bounded-queue core exposed to Python via pybind11, with an asynchronous dynamic-batching scheduler and FastAPI integration on top.

The model is incidental; the scheduler is the point.

> **Status: v0.1 in progress — Day 8: scheduler benchmarked with a calibrated load generator.** Graceful draining, metrics, and tracing remain planned.

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
- [x] Calibrated multi-process load generator and measured scheduler sweep (see Benchmarks)
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

Apple M4 (10 cores), macOS 26.2, Python 3.12, torch 2.13; closed-loop load, 5 trials after a discarded cold trial, mean ± sample stddev. Full grid and methodology: [`results/scheduler_sweep.md`](results/scheduler_sweep.md); environment and the load generator's measured ceiling: [`results/environment.md`](results/environment.md).

Headline at c=32 closed-loop clients, 2026-10-15:

| Configuration | req/s | p50 (ms) | p99 (ms) | realized batch |
|---|---:|---:|---:|---:|
| max_batch=1 (no batching) | 4,297 ± 133 | 7.19 | 9.01 | 1.00 |
| max_batch=8, max_wait=0 | 11,164 ± 96 | 2.75 | 4.47 | 7.87 |

Dynamic batching raises throughput 2.6× and cuts p50 2.6× at the same offered load.

- Batches fill from queue depth, not from waiting: at c=32, max_wait=0 already realizes 7.87 of 8; a nonzero max_wait adds tail latency and no throughput.
- max_wait_ms is a latency floor whenever offered concurrency is below max_batch_size (c=1, batch=8, wait=5: 130 req/s, p50 7.7 ms); choose it against the expected arrival rate.
- Little throughput beyond batch ≈ 8 on this model: batch=32 fills to 31.6 at c=32 but gains at most ~10% over batch=8; the ~0.7 ms per-batch cycle is scheduler/HTTP overhead, not the ~0.2–0.5 ms model call. A heavier model shifts this.
- Little's law closes non-circularly: 32 / 11,164 req/s = 2.87 ms against a measured p50 of 2.75 ms, with the generator's ceiling independently measured at ~35k req/s.

The Day 4 figure of ~1,700 req/s was the previous single-process generator's ceiling, not server saturation — corrected 2026-10-15 in [`results/benchmark_summary.md`](results/benchmark_summary.md).

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
- Metrics, tracing, open-loop benchmarks, multiple workers, and GPU serving remain to be implemented. Benchmarks use a tiny in-repo model; per-batch overhead, not compute, bounds throughput.
- Not production-ready and not claimed to be; this is a concurrency-first serving lab.

## Next milestone

Implement graceful bounded shutdown and drain behavior for queued and in-flight work.

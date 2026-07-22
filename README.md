# miniserve-ai

**A concurrent request scheduler with a pluggable model backend** — a hand-built C++ bounded-queue core exposed to Python via pybind11, an async dynamic-batching scheduler on top of it, and failure handling (backpressure, timeouts, graceful drain) exercised under real load and measured with statistical rigor.

The model is incidental; the scheduler is the point.

> **Status: v0.1 in progress — synchronous baseline and C++ bounded-queue milestone complete.** The async scheduler, dynamic batching, deadlines, shutdown, and metrics remain planned. This README distinguishes built behavior from planned behavior.

## Why this project exists

Serving systems live or die on their concurrency core: how requests queue, how batches form, what happens when the queue is full, and what happens to in-flight work at shutdown. This repo builds that core deliberately — including intentionally inducing and fixing race conditions — rather than assembling framework defaults. It is the serving-side companion to [`lob-latency-lab`](https://github.com/ow3ntd/lob-latency-lab), which applied the same approach (C++ hot path, stress testing, benchmark methodology) to a limit order book.

## Architecture

```text
                        ┌────────────────────────────────────────┐
  HTTP requests ──────► │  FastAPI (Python)                      │
                        │    └─ async scheduler (planned)        │
                        │         │            ▲                 │
                        │   ┌─────▼────────────┴─────┐           │
                        │   │ C++ bounded queue of   │  pybind11 │
                        │   │ opaque request IDs     │  boundary │
                        │   └─────┬──────────────────┘           │
                        │         ▼                              │
                        │   model runner (batched inference)     │
                        └────────────────────────────────────────┘
```

Python will own request payloads and per-request futures. The C++ queue stores only opaque integer IDs, avoiding Python-object lifetime and GIL hazards inside the concurrent core.

## Features

- [x] Health endpoint (`GET /health`)
- [x] C++ bounded, thread-safe FIFO with immediate full-queue rejection
- [x] FIFO batch pop and multi-producer/multi-consumer native stress test
- [x] pybind11 boundary that releases the GIL around queue operations
- [x] Model runner abstraction with explicit batch assembly/splitting and a padding-leak invariance test
- [x] Synchronous `POST /predict` baseline
- [x] Reproducible closed-loop baseline benchmark
- [ ] Async scheduler with dynamic batching (`max_batch_size`, `max_wait_ms`)
- [ ] Deliberate concurrency bug hunt, documented in `docs/concurrency_bugs.md`
- [ ] Backpressure wiring (503), request timeouts, graceful bounded shutdown
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

# Run the current synchronous baseline server.
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

The C++ queue is not yet integrated into `/predict`, so these numbers are **not** queue or batching results.

## Limitations

- CPU-only, single process, local machine. No GPU, distributed serving, Kubernetes, Redis, or Ray in v0.1.
- The C++ queue currently stores request IDs only; Python scheduler integration is the next milestone.
- No blocking queue operations, cancellation, deadlines, shutdown state, or metrics in the core yet.
- Not production-ready and not claimed to be; this is a concurrency-first serving lab.

## Next milestone

Add the Python async scheduler around the proven queue primitive: a request registry keyed by ID, exactly-once future resolution, one worker, and fixed-size batch execution before adding timers or shutdown behavior.

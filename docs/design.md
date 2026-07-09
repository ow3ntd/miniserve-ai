# miniserve-ai — Design

> Skeleton (Day 1). Each section is filled in on the day its subject is built,
> not before. Sections marked TBD contain no invented content.

## 1. Overview and positioning

miniserve-ai is a concurrent request scheduler with a pluggable model backend.
The scheduling core — a bounded, thread-safe queue with batch-formation
semantics — is implemented in C++ and exposed to Python via pybind11. Python
(FastAPI + asyncio) owns orchestration; C++ owns the hot path.

## 2. Request lifecycle

TBD (Day 5, scheduler design doc). Will cover: request admission → queue →
batch formation → inference → result routing to per-request futures →
response, including every failure exit (rejection, timeout, shutdown).

## 3. Why the queue core lives in C++

TBD (Day 1.5). Will cover: GIL constraints on true multi-producer
concurrency, lock granularity, and the throughline from the
`lob-latency-lab` matching engine.

## 4. Batching design

TBD (Days 5, 8–9). Will cover: `max_batch_size`, `max_wait_ms`, the
latency/throughput tradeoff with measured numbers, and the race between
batch-timeout timers and producers.

## 5. Backpressure, timeouts, and graceful shutdown

TBD (Days 5, 11–12). Will cover: bounded queue rejection (503) semantics,
per-request timeout without orphaned futures, and SIGTERM drain behavior.

## 6. Metrics and tracing

TBD (Day 13). Will cover: counter/histogram choices, latency decomposition
(queue wait vs. inference time), and correlation-ID threading.

## 7. Concurrency bugs found and fixed

TBD (Day 10). Summary here; full root-cause writeups in
[`concurrency_bugs.md`](concurrency_bugs.md).

## 8. Benchmark methodology

TBD (Day 4). Will restate the rigor requirements (trial counts, warm/cold
separation, load shape, environment logging) and link `results/`.

## 9. Limitations

TBD. Known now: CPU-only, single-process, local; no distributed
infrastructure by design in v0.1.

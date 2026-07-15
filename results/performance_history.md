# Performance history

Chronological record of measured performance as the system evolves.
Same rules as lob-latency-lab: real numbers only, environment and
methodology logged, no retroactive edits to old entries.

---

## 2026-07-14 — v0.1 baseline (pre-scheduler, pre-batching)

- Synchronous /predict; every request a batch of one through FastAPI's
  threadpool. No queue, no batching, no backpressure.
- c=1: 1583.8 ± 59.2 req/s, p50 0.61 ms. c=8: 1712.2 ± 47.2 req/s,
  p50 3.09 ms, p99 23.25 ms. Saturation at ~1,700 req/s; concurrency
  adds queueing delay, not throughput.
- Purpose: the reference point every scheduler/batching change gets
  compared against.

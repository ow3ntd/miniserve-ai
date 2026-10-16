# Benchmark summary

Methodology: closed-loop load generator (`load_tests/load_test.py`),
5 trials per scenario, 50-request warmup before timing. Cold-start =
first request against a freshly started server. Values are mean ± stddev
across trials. Environment and the generator's measured ceiling: see
`environment.md`. Server run without access logging or auto-reload.

## v0.1 baseline — synchronous /predict, no batching (Day 4, historical)

Measured 2026-07-14 with the original single-process asyncio/httpx
generator (500-800 requests per trial). Kept as the historical reference
point; see the correction below before reading the throughput figures as
server limits.

| Scenario | req/s | p50 (ms) | p95 (ms) | p99 (ms) | error rate |
|---|---|---|---|---|---|
| c=1, no batching | 1583.8 ± 59.2 | 0.61 ± 0.00 | 0.69 ± 0.04 | 0.96 ± 0.47 | 0.0000% |
| c=8, no batching | 1712.2 ± 47.2 | 3.09 ± 0.05 | 6.25 ± 1.93 | 23.25 ± 8.40 | 0.0000% |

Cold-start (first request, fresh server): 25.55 ms — ~40x the c=1
steady-state p50, which is what the warmup phase exists to exclude.

### Correction (2026-10-15)

The original interpretation of this table said the server saturates
around 1,700 req/s and that added concurrency buys queueing delay rather
than throughput. That reading was wrong. The ~1,700 req/s figure was the
ceiling of the load generator itself: a single asyncio process using
httpx, whose per-request client-side cost caps it near 1,800 req/s on
this machine regardless of what the server does. Evidence, gathered
while diagnosing the Day 8 scheduler sweep (`docs/devlog.md`):

- Per-request timestamps shared across both processes showed that at
  c=32 a request spent ~11.6 ms between the client's send and the
  server's handler entry and ~5 ms between handler return and client
  receive, with under 0.6 ms inside the server. Little's Law at 32
  closed-loop workers and ~1,800 req/s gives ~17.8 ms in system, which
  is almost entirely that client-side time.
- The same server, driven by a generator built from OS threads with
  persistent connections, served 10,227 ± 112 req/s at c=32 with
  max_batch_size=8 and 3,467 ± 749 req/s with max_batch_size=1.
- The threaded generator's calibration against `GET /health` is recorded
  in `environment.md` ("Load generator ceiling").

What the Day 4 table does still show: with every request executed as a
batch of one, c=8 raised p50 ~5x over c=1 and p99 to ~23 ms. The
latency shape is real; the throughput ceiling was not the server's.
The Little's Law check in the original text was consistent precisely
because the in-system time it used was dominated by the generator.

The Day 4 numbers are not re-measured here because the Day 4 code
(synchronous `/predict`, no scheduler) no longer exists on `main`. The
scheduler sweep in `scheduler_sweep.md`, with max_batch_size=1 and
max_wait_ms=0, is the closest current analogue of "no batching".

## Scheduler sweep (Day 8)

See `scheduler_sweep.md` for the max_batch_size × max_wait_ms ×
concurrency grid measured with the threaded generator, and
`superseded/scheduler_sweep_httpx_2026-10-15.md` for the earlier run
whose c≥8 rows measured the httpx generator rather than the scheduler.
Interpretation of the sweep is deliberately not written here yet.

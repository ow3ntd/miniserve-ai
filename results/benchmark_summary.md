# Benchmark summary

Methodology: closed-loop load generator (`load_tests/load_test.py`),
5 trials per scenario, 500-800 requests per trial, 50-request warmup
before timing. Cold-start = first request against a freshly started
server. Values are mean ± stddev across trials. Environment: see
`environment.md`. Server run without access logging or auto-reload.

## v0.1 baseline — synchronous /predict, no batching (Day 4)

| Scenario | req/s | p50 (ms) | p95 (ms) | p99 (ms) | error rate |
|---|---|---|---|---|---|
| c=1, no batching | 1583.8 ± 59.2 | 0.61 ± 0.00 | 0.69 ± 0.04 | 0.96 ± 0.47 | 0.0000% |
| c=8, no batching | 1712.2 ± 47.2 | 3.09 ± 0.05 | 6.25 ± 1.93 | 23.25 ± 8.40 | 0.0000% |

Cold-start (first request, fresh server): 25.55 ms — ~40x the c=1
steady-state p50, which is what the warmup phase exists to exclude.

## Interpretation

Going from 1 to 8 concurrent clients raised throughput only ~8%
(1584 -> 1712 req/s) while p50 rose ~5x (0.61 -> 3.09 ms) and p99 grew
from under 1 ms to ~23 ms. The server saturates around ~1,700 req/s on
this machine; beyond that, added concurrency buys queueing delay, not
throughput. A Little's Law check is consistent: 1712 req/s x ~4.7 ms
mean in-system time ~= 8 requests in flight, matching the 8 closed-loop
workers.

Each request runs the model as a batch of one, so per-request overhead
(HTTP handling, validation, a full model invocation) is paid 8 times
over for work one batched forward pass could amortize. This is the
baseline the scheduler and dynamic batching (Days 6-9) exist to beat,
and the p99 blowup under concurrency is the specific tail behavior
batching + bounded queueing should tame.

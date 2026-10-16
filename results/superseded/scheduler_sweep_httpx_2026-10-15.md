> Superseded 2026-10-15: these numbers were produced with the single-process asyncio/httpx load generator and measure that generator's ceiling and its collapse under bursty responses, not the scheduler (see docs/devlog.md, Day 8).
> Kept unchanged as evidence; the valid re-run with the threaded generator is ../scheduler_sweep.md.

# Scheduler sweep: max_batch_size x max_wait_ms x concurrency

Measured 2026-10-15 17:02 PDT at commit `0e20563`. Total wall time 2.5 min. Environment: see `environment.md`. Raw per-trial rows (including discarded cold trials): `scheduler_sweep_raw.csv`.

## Methodology

- Grid: max_batch_size ∈ {1, 8, 32} × max_wait_ms ∈ {0, 1, 5} × closed-loop concurrency c ∈ {1, 8, 32}.
- One fresh `uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level warning` process per (max_batch_size, max_wait_ms), configured through `MINISERVE_*` env vars with `MINISERVE_QUEUE_CAPACITY=256` and the default request timeout. All concurrency levels run against that process in ascending order; it is then killed.
- Load: `load_tests/load_test.py`'s closed-loop `run_trial`, 50 warmup requests per cell, then 6 trials of 500 requests split evenly across workers (c=1 sends 500, c=8 sends 496, c=32 sends 480).
- Cold-trial policy: the first trial of every cell is discarded, so 6 run and 5 are reported. This covers both process cold start (first cell of each server) and the regime change when concurrency steps up (later cells).
- Realized batch size: `GET /stats` is read immediately before and after each trial; the row's value is Δrequests_dispatched / Δbatches_dispatched. Counters only count model calls actually issued.
- Values are mean ± sample stddev across the reported trials. Latencies are client-observed end-to-end, in ms. p50/p99 are nearest-rank percentiles per trial, then averaged across trials.

## Results

### c = 1

| max_batch | max_wait_ms | req/s | p50 (ms) | p99 (ms) | realized batch | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 1438.6 ± 6.9 | 0.69 ± 0.00 | 0.78 ± 0.01 | 1.00 ± 0.00 | 0/2500 |
| 1 | 1 | 1382.4 ± 59.7 | 0.69 ± 0.01 | 1.12 ± 0.41 | 1.00 ± 0.00 | 0/2500 |
| 1 | 5 | 1127.8 ± 185.3 | 0.72 ± 0.01 | 3.75 ± 2.48 | 1.00 ± 0.00 | 0/2500 |
| 8 | 0 | 1395.2 ± 50.6 | 0.69 ± 0.01 | 1.09 ± 0.53 | 1.00 ± 0.00 | 0/2500 |
| 8 | 1 | 354.9 ± 7.6 | 2.63 ± 0.20 | 5.41 ± 1.21 | 1.00 ± 0.00 | 0/2500 |
| 8 | 5 | 109.9 ± 0.8 | 8.97 ± 0.13 | 13.09 ± 2.97 | 1.00 ± 0.00 | 0/2500 |
| 32 | 0 | 1430.4 ± 45.0 | 0.68 ± 0.02 | 0.89 ± 0.13 | 1.00 ± 0.00 | 0/2500 |
| 32 | 1 | 347.7 ± 8.4 | 2.71 ± 0.09 | 5.29 ± 1.04 | 1.00 ± 0.00 | 0/2500 |
| 32 | 5 | 111.0 ± 3.0 | 8.95 ± 0.28 | 12.38 ± 2.98 | 1.00 ± 0.00 | 0/2500 |

### c = 8

| max_batch | max_wait_ms | req/s | p50 (ms) | p99 (ms) | realized batch | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 1777.6 ± 162.5 | 3.38 ± 0.09 | 24.29 ± 4.66 | 1.00 ± 0.00 | 0/2480 |
| 1 | 1 | 1675.8 ± 118.7 | 3.42 ± 0.05 | 24.05 ± 10.83 | 1.00 ± 0.00 | 0/2480 |
| 1 | 5 | 1708.0 ± 113.8 | 3.38 ± 0.07 | 26.90 ± 3.07 | 1.00 ± 0.00 | 0/2480 |
| 8 | 0 | 1724.4 ± 138.7 | 3.43 ± 0.11 | 26.85 ± 6.54 | 1.10 ± 0.06 | 0/2480 |
| 8 | 1 | 1536.0 ± 17.4 | 4.32 ± 0.10 | 13.60 ± 1.20 | 2.74 ± 0.06 | 0/2480 |
| 8 | 5 | 1141.2 ± 147.0 | 6.65 ± 0.61 | 17.22 ± 5.54 | 7.70 ± 0.40 | 0/2480 |
| 32 | 0 | 1628.2 ± 62.7 | 3.52 ± 0.12 | 29.83 ± 3.19 | 1.10 ± 0.02 | 0/2480 |
| 32 | 1 | 1574.5 ± 96.4 | 4.48 ± 0.14 | 10.83 ± 2.67 | 4.82 ± 2.79 | 0/2480 |
| 32 | 5 | 825.3 ± 23.3 | 9.56 ± 0.30 | 14.66 ± 2.43 | 7.95 ± 0.07 | 0/2480 |

### c = 32

| max_batch | max_wait_ms | req/s | p50 (ms) | p99 (ms) | realized batch | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 1799.7 ± 36.6 | 17.26 ± 0.12 | 21.60 ± 3.76 | 1.00 ± 0.00 | 0/2400 |
| 1 | 1 | 1706.2 ± 137.1 | 17.44 ± 0.50 | 28.44 ± 9.76 | 1.00 ± 0.00 | 0/2400 |
| 1 | 5 | 1422.2 ± 292.7 | 19.91 ± 1.73 | 51.57 ± 47.44 | 1.00 ± 0.00 | 0/2400 |
| 8 | 0 | 609.7 ± 40.9 | 25.30 ± 3.59 | 229.01 ± 32.35 | 1.08 ± 0.01 | 0/2400 |
| 8 | 1 | 886.9 ± 591.1 | 21.01 ± 4.15 | 168.76 ± 83.89 | 2.55 ± 1.85 | 0/2400 |
| 8 | 5 | 660.4 ± 81.6 | 19.71 ± 3.42 | 218.18 ± 28.27 | 4.42 ± 0.33 | 0/2400 |
| 32 | 0 | 878.6 ± 518.6 | 21.20 ± 3.36 | 172.14 ± 84.08 | 1.24 ± 0.31 | 0/2400 |
| 32 | 1 | 562.4 ± 47.2 | 25.17 ± 2.37 | 260.94 ± 29.67 | 1.73 ± 0.12 | 0/2400 |
| 32 | 5 | 608.6 ± 58.5 | 25.24 ± 4.44 | 228.68 ± 18.28 | 4.18 ± 0.41 | 0/2400 |

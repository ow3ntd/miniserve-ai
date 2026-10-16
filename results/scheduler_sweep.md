# Scheduler sweep: max_batch_size x max_wait_ms x concurrency

Measured 2026-10-15 18:13 PDT at commit `0e20563-dirty`. Total wall time 1.7 min. Environment: see `environment.md`. Raw per-trial rows (including discarded cold trials): `scheduler_sweep_raw.csv`.

## Methodology

- Grid: max_batch_size ∈ {1, 8, 32} × max_wait_ms ∈ {0, 1, 5} × closed-loop concurrency c ∈ {1, 8, 32}.
- One fresh `uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level warning` process per (max_batch_size, max_wait_ms), configured through `MINISERVE_*` env vars with `MINISERVE_QUEUE_CAPACITY=256` and the default request timeout. All concurrency levels run against that process in ascending order; it is then killed.
- Load: `load_tests/load_test.py`'s closed-loop `run_trial` (one OS thread with a persistent HTTP/1.1 connection per worker, threads spread across up to 4 generator processes), 50 warmup requests per cell, then 6 trials of 500 requests split evenly across workers (c=1 sends 500, c=8 sends 496, c=32 sends 480).
- Cold-trial policy: the first trial of every cell is discarded, so 6 run and 5 are reported. This covers both process cold start (first cell of each server) and the regime change when concurrency steps up (later cells).
- Realized batch size: `GET /stats` is read immediately before and after each trial; the row's value is Δrequests_dispatched / Δbatches_dispatched. Counters only count model calls actually issued.
- Values are mean ± sample stddev across the reported trials. Latencies are client-observed end-to-end, in ms. p50/p99 are nearest-rank percentiles per trial, then averaged across trials.

## Results

### c = 1

| max_batch | max_wait_ms | req/s | p50 (ms) | p99 (ms) | realized batch | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 3077.4 ± 88.1 | 0.33 ± 0.00 | 0.39 ± 0.08 | 1.00 ± 0.00 | 0/2500 |
| 1 | 1 | 3111.8 ± 311.6 | 0.31 ± 0.01 | 0.61 ± 0.39 | 1.00 ± 0.00 | 0/2500 |
| 1 | 5 | 3057.6 ± 43.3 | 0.32 ± 0.00 | 0.42 ± 0.04 | 1.00 ± 0.00 | 0/2500 |
| 8 | 0 | 3185.0 ± 78.3 | 0.31 ± 0.01 | 0.39 ± 0.04 | 1.00 ± 0.00 | 0/2500 |
| 8 | 1 | 429.8 ± 12.6 | 2.20 ± 0.07 | 5.76 ± 1.94 | 1.00 ± 0.00 | 0/2500 |
| 8 | 5 | 129.4 ± 1.4 | 7.73 ± 0.06 | 9.50 ± 0.92 | 1.00 ± 0.00 | 0/2500 |
| 32 | 0 | 3054.4 ± 20.9 | 0.33 ± 0.00 | 0.35 ± 0.00 | 1.00 ± 0.00 | 0/2500 |
| 32 | 1 | 411.0 ± 8.9 | 2.48 ± 0.01 | 3.74 ± 0.79 | 1.00 ± 0.00 | 0/2500 |
| 32 | 5 | 135.8 ± 2.8 | 7.32 ± 0.16 | 8.88 ± 0.79 | 1.00 ± 0.00 | 0/2500 |

### c = 8

| max_batch | max_wait_ms | req/s | p50 (ms) | p99 (ms) | realized batch | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 4481.1 ± 22.8 | 1.75 ± 0.01 | 2.13 ± 0.06 | 1.00 ± 0.00 | 0/2480 |
| 1 | 1 | 4345.8 ± 199.0 | 1.78 ± 0.09 | 2.66 ± 0.18 | 1.00 ± 0.00 | 0/2480 |
| 1 | 5 | 4078.8 ± 510.0 | 1.79 ± 0.03 | 6.21 ± 7.28 | 1.00 ± 0.00 | 0/2480 |
| 8 | 0 | 8296.8 ± 2017.2 | 0.89 ± 0.15 | 2.33 ± 1.92 | 3.84 ± 0.15 | 0/2480 |
| 8 | 1 | 9134.8 ± 596.1 | 0.85 ± 0.05 | 1.35 ± 0.16 | 8.00 ± 0.00 | 0/2480 |
| 8 | 5 | 10595.2 ± 81.0 | 0.73 ± 0.00 | 1.13 ± 0.04 | 8.00 ± 0.00 | 0/2480 |
| 32 | 0 | 9614.7 ± 198.6 | 0.80 ± 0.02 | 1.37 ± 0.14 | 3.95 ± 0.02 | 0/2480 |
| 32 | 1 | 4033.5 ± 4.4 | 1.96 ± 0.01 | 2.37 ± 0.04 | 8.00 ± 0.00 | 0/2480 |
| 32 | 5 | 1026.4 ± 36.6 | 7.56 ± 0.30 | 12.11 ± 3.13 | 8.00 ± 0.00 | 0/2480 |

### c = 32

| max_batch | max_wait_ms | req/s | p50 (ms) | p99 (ms) | realized batch | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 4297.4 ± 133.2 | 7.19 ± 0.19 | 9.01 ± 0.69 | 1.00 ± 0.00 | 0/2400 |
| 1 | 1 | 4110.7 ± 421.9 | 7.60 ± 0.74 | 9.41 ± 1.68 | 1.00 ± 0.00 | 0/2400 |
| 1 | 5 | 3609.8 ± 1061.2 | 7.40 ± 0.32 | 25.96 ± 28.57 | 1.00 ± 0.00 | 0/2400 |
| 8 | 0 | 11163.8 ± 95.9 | 2.75 ± 0.04 | 4.47 ± 0.18 | 7.87 ± 0.00 | 0/2400 |
| 8 | 1 | 10348.3 ± 1633.9 | 2.78 ± 0.05 | 8.64 ± 9.74 | 8.00 ± 0.00 | 0/2400 |
| 8 | 5 | 10257.0 ± 2723.5 | 2.68 ± 0.07 | 13.62 ± 20.85 | 8.00 ± 0.00 | 0/2400 |
| 32 | 0 | 11358.8 ± 2114.3 | 2.45 ± 0.07 | 8.03 ± 10.16 | 13.13 ± 0.53 | 0/2400 |
| 32 | 1 | 10512.8 ± 1700.8 | 2.55 ± 0.12 | 8.07 ± 9.18 | 21.47 ± 1.08 | 0/2400 |
| 32 | 5 | 9452.8 ± 2635.2 | 2.95 ± 0.65 | 9.86 ± 9.17 | 31.60 ± 0.89 | 0/2400 |
## Interpretation

1. **Batches fill from queue depth, not from waiting.** At c=32 with
   max_batch=8, max_wait_ms=0 already realizes 7.87 of 8 per batch at
   11,164 ± 96 req/s (p50 2.75 ms, p99 4.47 ms). Adding a wait window
   buys no throughput at this load (10,348 at 1 ms, 10,257 at 5 ms) and
   lengthens the tail (p99 8.64 and 13.62 ms, each inflated by one slow
   trial; see Caveats). With 32 closed-loop clients the queue is deep
   enough that a batch is full the moment the scheduler looks.
2. **max_wait_ms is a latency floor whenever offered concurrency is below
   max_batch_size.** With one client, max_batch=8 and max_wait_ms=5 the
   batch can never fill, so every request waits out the whole window:
   129.4 ± 1.4 req/s at p50 7.73 ms, against 3,185 req/s and 0.31 ms for
   the same client with max_wait_ms=0. The same holds at c=8 with
   max_batch=32 (1,026 req/s at 5 ms). Choose max_wait_ms against the
   expected arrival rate, not against the batch size.
3. **Little throughput beyond batch ≈ 8 on this model.** max_batch=32 at
   c=32 fills to 13.1 (wait 0), 21.5 (wait 1) and 31.6 (wait 5) per batch,
   yet those cells average 11,359, 10,513 and 9,453 req/s against 11,164
   for max_batch=8; their unperturbed trials run 12.2–12.7k req/s, at
   most ~10% more for 4× the batch. The in-repo model costs ~0.2 ms per
   call at batch 1 and under 0.5 ms at batch 8 (Day 8 trace), while the
   per-batch cycle at batch 8 is ~0.7 ms (11,164 req/s ÷ 7.87 per batch
   = 1,418 batches/s). Scheduler and HTTP overhead per batch, not model
   compute, bounds throughput here. A heavier model shifts the balance
   toward larger batches.
4. **Little's law closes, and non-circularly this time.** 32 in-flight
   requests ÷ 11,164 req/s = 2.87 ms mean in-system time against a
   measured p50 of 2.75 ms. Unlike the Day 4 check, the generator's own
   ceiling was measured independently (≈ 35k req/s against `GET /health`,
   `environment.md`), so the latency is the server's, not the client's.

## Caveats

Single uvicorn process, CPU only, tiny in-repo model: the numbers describe
scheduler and HTTP overhead on this machine, not model-bound serving. The
generator shares the 10-core machine with the server. Several c=32 cells
contain a recurring slow trial at ≈ 7,400–7,700 req/s with p99 ≈ 25 ms
(one trial of five in max_batch=8/wait=1, 8/5, 32/0 and 32/1; three of
five in 32/5, which makes that cell bimodal; two of five in 1/5). Those
trials are kept, not dropped, and are visible in `scheduler_sweep_raw.csv`;
they inflate the affected means' stddevs and p99s. Their cause has not
been investigated. Trials are short (480–500 requests, ≈ 45 ms at 11k
req/s), which makes single scheduling hiccups visible.

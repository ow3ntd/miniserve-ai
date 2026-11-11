# Benchmark environment

All benchmark results in this directory were measured on this machine
unless an entry states otherwise. Last refreshed 2026-10-15 for the
scheduler sweep.

## Machine

| Item | Value |
|---|---|
| CPU | Apple M4 (`sysctl -n machdep.cpu.brand_string`) |
| Cores | 10 |
| RAM | 24 GB |
| OS | macOS 26.2 (build 25C56) |
| Python | Python 3.12.0 (project `.venv`) |
| Key packages | fastapi==0.139.0 httpx==0.28.1 torch==2.13.0 uvicorn==0.51.0 pybind11==3.0.4 (uvicorn auto-selects uvloop 0.22.1 and httptools 0.8.0) |
| Native core | `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release`; extension loaded from `build/python` |

## Load generator ceiling

Measured 2026-10-15 so that server numbers can be read as server numbers.
`load_tests/load_test.py` (closed-loop; one OS thread with a persistent
HTTP/1.1 connection per worker, threads spread across `--processes`
processes) driving `GET /health`, which does near-zero application work.
5 trials of 20,000 requests after a 50-request warmup. "Cores" is
CPU time divided by wall time over the whole command, including process
spawn and warmup, so it slightly understates in-trial utilization.

Exact commands (one uvicorn on :8000; for the two-server rows a second
identical uvicorn on :8001):

```
python load_tests/load_test.py --url http://127.0.0.1:8000/health --method GET --concurrency 32 --processes 4 --requests 20000 --trials 5 --warmup 50
python load_tests/load_test.py --url http://127.0.0.1:8000/health --url http://127.0.0.1:8001/health --method GET --concurrency 32 --processes 4 --requests 20000 --trials 5 --warmup 50
```

| servers | c | processes | req/s | p50 (ms) | p99 (ms) | client cores | server cores |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 4 (capped to 1) | 11748.1 ± 90.8 | 0.08 ± 0.00 | 0.11 ± 0.00 | 0.49 | 0.50 |
| 1 | 8 | 4 | 32865.2 ± 1809.5 | 0.23 ± 0.00 | 0.47 ± 0.19 | 1.80 | 0.85 |
| 1 | 32 | 4 | 35068.0 ± 2062.4 | 0.87 ± 0.03 | 1.43 ± 0.25 | 2.40 | 0.85 |
| 1 | 64 | 4 | 33935.4 ± 1847.7 | 1.78 ± 0.06 | 2.92 ± 0.44 | 2.63 | 0.82 |
| 2 | 32 | 4 | 42112.7 ± 1611.9 | 0.71 ± 0.06 | 1.74 ± 0.16 | 3.50 | 0.75 + 0.74 |
| 2 | 64 | 4 | 40781.7 ± 2610.9 | 1.19 ± 0.17 | 4.72 ± 0.64 | 3.51 | 0.70 + 0.66 |

How to read it:

- **Against one server the plateau (~35k req/s at c≥8) is uvicorn's,
  not the generator's.** The server process is near one full core
  (its single event loop) while the generator has headroom, and the same
  generator pushes 42k req/s when a second server is added. ~35k req/s
  is therefore the ceiling of the HTTP layer every `/predict` measurement
  also passes through; no `/predict` number can exceed it.
- **The generator's own ceiling with 4 processes is ≥ 42k req/s**
  (3.5 cores; each CPython process is GIL-bound near one core). A
  single-process, threads-only run of the same generator plateaued at
  24–27k req/s, which is why `--processes 4` is the default in the sweep.
- **Acceptance rule used for the sweep:** the single-server ceiling at
  c=32 must be at least ~3× the best scheduler cell. 35,068 req/s vs.
  the sweep's best cell (11,359 req/s, max_batch=32, w=0, c=32) gives
  3.1×; vs. the ~10,200 req/s cell seen during diagnosis, 3.4×.
- **Historical:** the asyncio/httpx generator used through 2026-10-15
  capped near 1,800 req/s and collapsed to ~600 req/s under bursty
  responses (see `docs/devlog.md`, Day 8). Its numbers are kept under
  `superseded/`.

## Runs

| Benchmark | Date | Commit | Server invocation | Load generator |
|---|---|---|---|---|
| v0.1 baseline (`benchmark_summary.md`, Day 4) | 2026-07-14 | not recorded | `uvicorn app.main:app --host 127.0.0.1 --port 8000 --no-access-log` | asyncio/httpx `load_tests/load_test.py` (since replaced), closed-loop |
| Scheduler sweep, superseded (`superseded/scheduler_sweep_httpx_2026-10-15.md`) | 2026-10-15 | `0e20563` | `uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level warning`, one process per config via `MINISERVE_*` env vars, `MINISERVE_QUEUE_CAPACITY=256` | asyncio/httpx generator; measured the generator, not the scheduler |
| Scheduler sweep (`scheduler_sweep.md`, Day 8) | 2026-10-15 | `0e20563` plus the uncommitted generator/sweep changes that became the next commit on `bench/scheduler-sweep` | same as above | `load_tests/sweep_scheduler.py` driving the threaded `load_test.py` (`--processes 4`), reading `GET /stats` around every trial |

Conditions for the 2026-10-15 runs: AC power, 1-minute load average
≈ 2.4 before start, no other CPU-heavy user processes (a stray
single-threaded Python process found pinning one core was terminated
before the first run). Server and load generator ran on the same
10-core machine, so client-side HTTP cost is included in every latency
and the two compete for cores; the calibration above bounds that effect.

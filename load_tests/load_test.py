"""Closed-loop load generator for miniserve-ai (Day 4 baseline).

Closed-loop: each worker waits for its response before sending the next
request, so offered load adapts to server speed. (An open-loop generator,
which fires at a fixed rate regardless of responses, comes later with the
scheduler experiments -- the distinction matters and is recorded in the
benchmark methodology.)

Runs W concurrent workers x R requests per trial, for T trials, after a
warmup phase. Reports per-trial throughput and latency percentiles, plus
mean/stddev across trials. The first warmup request's latency is reported
separately as the cold-start observation.
"""

import argparse
import asyncio
import statistics
import time

import httpx

PAYLOAD = {"tokens": [1, 5, 9, 200]}


def percentile(sorted_vals: list[float], p: float) -> float:
    """Nearest-rank percentile; sorted_vals must be non-empty and sorted."""
    k = max(0, min(len(sorted_vals) - 1, round(p / 100 * len(sorted_vals)) - 1))
    return sorted_vals[k]


async def run_requests(
    client: httpx.AsyncClient, url: str, n: int
) -> tuple[list[float], int]:
    """Send n requests back-to-back; return (latencies_ms, error_count)."""
    latencies: list[float] = []
    errors = 0
    for _ in range(n):
        t0 = time.perf_counter()
        try:
            resp = await client.post(url, json=PAYLOAD)
            ok = resp.status_code == 200
        except httpx.HTTPError:
            ok = False
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if ok:
            latencies.append(elapsed_ms)
        else:
            errors += 1
    return latencies, errors


async def run_trial(
    url: str, concurrency: int, total_requests: int
) -> dict:
    per_worker = total_requests // concurrency
    async with httpx.AsyncClient(timeout=30.0) as client:
        t0 = time.perf_counter()
        results = await asyncio.gather(
            *(run_requests(client, url, per_worker) for _ in range(concurrency))
        )
        wall_s = time.perf_counter() - t0

    latencies = sorted(l for lats, _ in results for l in lats)
    errors = sum(e for _, e in results)
    sent = per_worker * concurrency
    return {
        "sent": sent,
        "ok": len(latencies),
        "errors": errors,
        "error_rate": errors / sent,
        "wall_s": wall_s,
        "rps": len(latencies) / wall_s,
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "mean_ms": statistics.fmean(latencies),
    }


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default="http://127.0.0.1:8000/predict")
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--requests", type=int, default=500,
                    help="requests per trial (split across workers)")
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=50)
    args = ap.parse_args()

    # Warmup -- also captures the cold-start observation (first request
    # against a freshly started server, if the server IS freshly started).
    async with httpx.AsyncClient(timeout=30.0) as client:
        t0 = time.perf_counter()
        resp = await client.post(args.url, json=PAYLOAD)
        cold_ms = (time.perf_counter() - t0) * 1000.0
        if resp.status_code != 200:
            raise SystemExit(f"warmup request failed: {resp.status_code} {resp.text}")
        for _ in range(args.warmup - 1):
            await client.post(args.url, json=PAYLOAD)

    print(f"cold-start (first request): {cold_ms:.2f} ms")
    print(f"config: concurrency={args.concurrency} "
          f"requests/trial={args.requests} trials={args.trials}\n")

    trials = []
    for i in range(args.trials):
        r = await run_trial(args.url, args.concurrency, args.requests)
        trials.append(r)
        print(f"trial {i + 1}: {r['rps']:8.1f} req/s | "
              f"p50 {r['p50_ms']:6.2f} ms | p95 {r['p95_ms']:6.2f} ms | "
              f"p99 {r['p99_ms']:6.2f} ms | errors {r['errors']}")

    rps_vals = [t["rps"] for t in trials]
    p50s = [t["p50_ms"] for t in trials]
    p95s = [t["p95_ms"] for t in trials]
    p99s = [t["p99_ms"] for t in trials]
    err = sum(t["errors"] for t in trials) / sum(t["sent"] for t in trials)

    def ms(vals: list[float]) -> str:
        return f"{statistics.fmean(vals):.2f} ± {statistics.stdev(vals):.2f}"

    print("\nacross trials (mean ± stddev):")
    print(f"  throughput: {statistics.fmean(rps_vals):.1f} ± "
          f"{statistics.stdev(rps_vals):.1f} req/s")
    print(f"  p50: {ms(p50s)} ms | p95: {ms(p95s)} ms | p99: {ms(p99s)} ms")
    print(f"  aggregate error rate: {err:.4%}")

    print("\nmarkdown row (paste into results/benchmark_summary.md):")
    print(f"| c={args.concurrency}, no batching | "
          f"{statistics.fmean(rps_vals):.1f} ± {statistics.stdev(rps_vals):.1f} | "
          f"{ms(p50s)} | {ms(p95s)} | {ms(p99s)} | {err:.4%} |")


if __name__ == "__main__":
    asyncio.run(main())

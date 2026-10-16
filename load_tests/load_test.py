"""Closed-loop load generator for miniserve-ai.

W workers, each owning one persistent HTTP/1.1 connection, send requests
back-to-back: every worker waits for its response before sending the next
request, so offered load adapts to server speed (closed-loop). Workers are
OS threads, optionally spread across P processes (--processes) because one
CPython process tops out near 24-27k req/s on this machine (GIL-bound);
see results/environment.md ("Load generator ceiling").

Runs W workers x R requests per trial, for T trials, after a warmup phase.
Reports per-trial throughput and latency percentiles, plus mean/stddev
across trials. The first warmup request's latency is reported separately
as the cold-start observation.

--url may be given more than once; workers are assigned to URLs round-robin.
This exists to calibrate the generator against several server processes at
once, so its own ceiling can be measured independently of any one server.

History: until 2026-10-15 this generator was a single asyncio process
using httpx. That design capped at ~1,800 req/s on the benchmark machine
and, when the server returned responses in bursts (dynamic batching),
collapsed to ~600 req/s with a 200 ms tail -- numbers that were initially
read as a scheduler defect. Threads/processes with persistent connections
do not share one event loop or one connection pool, so a slow worker
cannot stall the others. See docs/devlog.md (Day 8) for the measurements
that justified the change.
"""

from __future__ import annotations

import argparse
import http.client
import json
import multiprocessing
import statistics
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit

PAYLOAD = {"tokens": [1, 5, 9, 200]}
TIMEOUT_S = 30.0


def percentile(sorted_vals: list[float], p: float) -> float:
    """Nearest-rank percentile; sorted_vals must be non-empty and sorted."""
    k = max(0, min(len(sorted_vals) - 1, round(p / 100 * len(sorted_vals)) - 1))
    return sorted_vals[k]


@dataclass(frozen=True)
class Target:
    """One HTTP request shape, parsed once and shared by all workers."""

    host: str
    port: int
    path: str
    method: str
    body: bytes | None
    headers: dict[str, str]

    @classmethod
    def from_url(cls, url: str, method: str = "POST") -> "Target":
        parts = urlsplit(url)
        if parts.scheme not in ("http", ""):
            raise ValueError(f"only http:// URLs are supported, got {url!r}")
        path = parts.path or "/"
        if parts.query:
            path += "?" + parts.query
        method = method.upper()
        if method == "POST":
            body = json.dumps(PAYLOAD).encode()
            headers = {"Content-Type": "application/json"}
        else:
            body, headers = None, {}
        return cls(
            host=parts.hostname or "127.0.0.1",
            port=parts.port or 80,
            path=path,
            method=method,
            body=body,
            headers=headers,
        )

    def connect(self) -> http.client.HTTPConnection:
        conn = http.client.HTTPConnection(self.host, self.port, timeout=TIMEOUT_S)
        conn.connect()
        return conn


def _send_one(
    conn: http.client.HTTPConnection, target: Target
) -> tuple[bool, float, http.client.HTTPConnection]:
    """Send one request on a persistent connection.

    Returns (ok, elapsed_ms, conn). On a transport error the connection is
    replaced so the worker can continue; the failed request counts as an
    error and is not timed.
    """
    t0 = time.perf_counter()
    try:
        conn.request(target.method, target.path, body=target.body, headers=target.headers)
        resp = conn.getresponse()
        resp.read()
        ok = resp.status == 200
    except (OSError, http.client.HTTPException):
        ok = False
        conn.close()
        try:
            conn = target.connect()
        except OSError:
            pass
    return ok, (time.perf_counter() - t0) * 1000.0, conn


def run_requests(
    target: Target,
    n: int,
    start: threading.Barrier | None = None,
    ready: threading.Semaphore | None = None,
) -> tuple[list[float], int]:
    """Worker body: open one connection, send n requests back-to-back.

    Returns (latencies_ms, error_count). If `start` is given, the worker
    opens its connection first, signals `ready` (if given), and then waits
    on the barrier, so connect time is excluded from the timed window.
    """
    conn = target.connect()
    if ready is not None:
        ready.release()
    if start is not None:
        start.wait()
    latencies: list[float] = []
    errors = 0
    for _ in range(n):
        ok, elapsed_ms, conn = _send_one(conn, target)
        if ok:
            latencies.append(elapsed_ms)
        else:
            errors += 1
    conn.close()
    return latencies, errors


def _run_workers_in_threads(
    targets: Sequence[Target],
    per_worker: int,
    global_start=None,
) -> list[tuple[list[float], int]]:
    """Run one thread per target in this process.

    All threads open their connections, then are released together. If
    `global_start` (a multiprocessing barrier) is given, this process also
    waits on it after its own connections are open and before releasing
    its threads, so every process starts sending at the same instant.
    """
    n = len(targets)
    results: list[tuple[list[float], int]] = [([], 0)] * n
    local_start = threading.Barrier(n + 1)
    ready = threading.Semaphore(0)

    def worker(index: int) -> None:
        results[index] = run_requests(targets[index], per_worker, local_start, ready)

    threads = [
        threading.Thread(target=worker, args=(i,), name=f"load-{i}", daemon=True)
        for i in range(n)
    ]
    for t in threads:
        t.start()
    for _ in range(n):
        ready.acquire()  # every connection in this process is open
    if global_start is not None:
        global_start.wait()
    local_start.wait()  # release every worker together
    for t in threads:
        t.join()
    return results


def _process_main(targets, per_worker, global_start, out_queue) -> None:
    """Child-process entry point for multi-process trials."""
    out_queue.put(_run_workers_in_threads(targets, per_worker, global_start))


def run_trial(
    url: str | Sequence[str],
    concurrency: int,
    total_requests: int,
    method: str = "POST",
    processes: int = 1,
) -> dict:
    """One closed-loop trial: `concurrency` workers, total_requests split evenly.

    total_requests // concurrency per worker, so the number actually sent
    (reported as "sent") is a multiple of concurrency. Workers are threads;
    with processes > 1 they are spread as evenly as possible across that
    many child processes (never more processes than workers). `url` may be
    one URL or several; workers are assigned to URLs round-robin.
    """
    urls = [url] if isinstance(url, str) else list(url)
    targets = [Target.from_url(urls[i % len(urls)], method) for i in range(concurrency)]
    per_worker = total_requests // concurrency
    if per_worker <= 0:
        raise ValueError("total_requests must be at least concurrency")
    processes = max(1, min(processes, concurrency))

    if processes == 1:
        # Connections open before the clock starts (see _run_workers_in_threads).
        start = threading.Barrier(2)
        holder: list = []

        def run() -> None:
            holder.append(_run_workers_in_threads(targets, per_worker, start))

        runner = threading.Thread(target=run, daemon=True)
        runner.start()
        start.wait()
        t0 = time.perf_counter()
        runner.join()
        wall_s = time.perf_counter() - t0
        results = holder[0]
    else:
        ctx = multiprocessing.get_context("spawn")
        global_start = ctx.Barrier(processes + 1)
        out_queue = ctx.Queue()
        chunks = [targets[i::processes] for i in range(processes)]
        procs = [
            ctx.Process(
                target=_process_main,
                args=(chunk, per_worker, global_start, out_queue),
                daemon=True,
            )
            for chunk in chunks
        ]
        for proc in procs:
            proc.start()
        global_start.wait()  # every process has all its connections open
        t0 = time.perf_counter()
        results = []
        for _ in procs:
            results.extend(out_queue.get())
        wall_s = time.perf_counter() - t0
        for proc in procs:
            proc.join()

    latencies = sorted(l for lats, _ in results for l in lats)
    errors = sum(e for _, e in results)
    sent = per_worker * concurrency
    if not latencies:
        raise RuntimeError(f"all {sent} requests failed")
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


def warmup(url: str, n: int, method: str = "POST") -> float:
    """Send n sequential requests on one connection; return the first's latency in ms.

    The first request against a freshly started server is the cold-start
    observation. Raises if the first request does not return 200.
    """
    target = Target.from_url(url, method)
    conn = target.connect()
    t0 = time.perf_counter()
    conn.request(target.method, target.path, body=target.body, headers=target.headers)
    resp = conn.getresponse()
    payload = resp.read()
    cold_ms = (time.perf_counter() - t0) * 1000.0
    if resp.status != 200:
        raise SystemExit(f"warmup request failed: {resp.status} {payload[:200]!r}")
    for _ in range(max(0, n - 1)):
        _, _, conn = _send_one(conn, target)
    conn.close()
    return cold_ms


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", action="append",
                    help="target URL; repeatable, workers are assigned round-robin "
                         "(default http://127.0.0.1:8000/predict)")
    ap.add_argument("--method", default="POST", choices=["POST", "GET"],
                    help="POST sends the fixed /predict payload; GET sends no body "
                         "(use with --url .../health to calibrate the generator)")
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--requests", type=int, default=500,
                    help="requests per trial (split across workers)")
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=50)
    ap.add_argument("--processes", type=int, default=1,
                    help="spread workers across this many processes (default 1)")
    args = ap.parse_args()
    urls = args.url or ["http://127.0.0.1:8000/predict"]

    cold_ms = None
    for u in urls:
        first = warmup(u, args.warmup, args.method)
        cold_ms = first if cold_ms is None else cold_ms
    print(f"cold-start (first request): {cold_ms:.2f} ms")
    print(f"config: {args.method} {' '.join(urls)} concurrency={args.concurrency} "
          f"processes={args.processes} requests/trial={args.requests} "
          f"trials={args.trials}\n")

    trials = []
    for i in range(args.trials):
        r = run_trial(urls, args.concurrency, args.requests, args.method, args.processes)
        trials.append(r)
        print(f"trial {i + 1}: {r['rps']:8.1f} req/s | "
              f"p50 {r['p50_ms']:6.2f} ms | p95 {r['p95_ms']:6.2f} ms | "
              f"p99 {r['p99_ms']:6.2f} ms | errors {r['errors']}")

    rps_vals = [t["rps"] for t in trials]
    p50s = [t["p50_ms"] for t in trials]
    p95s = [t["p95_ms"] for t in trials]
    p99s = [t["p99_ms"] for t in trials]
    err = sum(t["errors"] for t in trials) / sum(t["sent"] for t in trials)

    def sd(vals: list[float]) -> float:
        return statistics.stdev(vals) if len(vals) > 1 else 0.0

    def ms(vals: list[float]) -> str:
        return f"{statistics.fmean(vals):.2f} ± {sd(vals):.2f}"

    print("\nacross trials (mean ± stddev):")
    print(f"  throughput: {statistics.fmean(rps_vals):.1f} ± "
          f"{sd(rps_vals):.1f} req/s")
    print(f"  p50: {ms(p50s)} ms | p95: {ms(p95s)} ms | p99: {ms(p99s)} ms")
    print(f"  aggregate error rate: {err:.4%}")

    print("\nmarkdown row (fill in the scenario label):")
    print(f"| c={args.concurrency}, p={args.processes} | "
          f"{statistics.fmean(rps_vals):.1f} ± {sd(rps_vals):.1f} | "
          f"{ms(p50s)} | {ms(p95s)} | {ms(p99s)} | {err:.4%} |")


if __name__ == "__main__":
    main()

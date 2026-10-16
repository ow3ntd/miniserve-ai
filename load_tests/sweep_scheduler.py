"""Scheduler parameter sweep for miniserve-ai (Day 8 benchmark).

Measures the async dynamic-batching scheduler across a grid of
max_batch_size x max_wait_ms, each under several closed-loop concurrency
levels. For every grid point one fresh server process is started with the
configuration passed through MINISERVE_* environment variables; the
process is killed after all concurrency levels have run against it.

Load generation reuses the closed-loop generator in load_test.py
(run_trial / warmup) unchanged: one OS thread with a persistent HTTP/1.1
connection per worker. Around every trial the harness reads GET /stats and
differences the monotonic dispatch counters, so each row records the batch
size the scheduler actually realized in that trial, not merely the maximum
it was permitted.

Cold-trial policy: within every cell (one server config x one concurrency
level) the harness runs trials + 1 trials and discards the first. The first
cell of a server process absorbs process cold start (allocator, torch
kernels, uvicorn worker warm-up); later cells absorb the regime change when
concurrency jumps. Discarding the first trial of every cell treats both the
same way and keeps the reported sample size uniform. Discarded trials are
still written to the raw CSV with kept=False.

Request counts per trial follow run_trial(): total_requests is split evenly
across workers, so concurrency levels that do not divide it send slightly
fewer requests (500 -> 500 at c=1, 496 at c=8, 480 at c=32). The raw CSV
records the exact count sent.

Usage:
    PYTHONPATH="build/python:." python load_tests/sweep_scheduler.py
    PYTHONPATH="build/python:." python load_tests/sweep_scheduler.py --smoke
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from load_tests.load_test import run_trial, warmup  # noqa: E402

DEFAULT_BATCH_SIZES = (1, 8, 32)
DEFAULT_WAITS_MS = (0, 1, 5)
DEFAULT_CONCURRENCY = (1, 8, 32)
SERVER_QUEUE_CAPACITY = 256
HOST = "127.0.0.1"


@dataclass(frozen=True)
class TrialRow:
    commit: str
    max_batch_size: int
    max_wait_ms: float
    concurrency: int
    trial: int
    kept: bool
    sent: int
    ok: int
    errors: int
    wall_s: float
    rps: float
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    batches_dispatched: int
    requests_dispatched: int
    realized_mean_batch_size: float


# --------------------------------------------------------------------- #
# Server lifecycle                                                       #
# --------------------------------------------------------------------- #


class ServerProcess:
    """One uvicorn process configured through MINISERVE_* env vars."""

    def __init__(
        self,
        *,
        port: int,
        max_batch_size: int,
        max_wait_ms: float,
        queue_capacity: int,
    ) -> None:
        self.port = port
        self.max_batch_size = max_batch_size
        self.max_wait_ms = max_wait_ms
        self.queue_capacity = queue_capacity
        self.base_url = f"http://{HOST}:{port}"
        self._proc: subprocess.Popen | None = None
        self._log = tempfile.TemporaryFile(mode="w+")

    def __enter__(self) -> "ServerProcess":
        env = dict(os.environ)
        env["MINISERVE_QUEUE_CAPACITY"] = str(self.queue_capacity)
        env["MINISERVE_MAX_BATCH_SIZE"] = str(self.max_batch_size)
        env["MINISERVE_MAX_WAIT_MS"] = str(self.max_wait_ms)
        # The native extension lives in build/python; make sure the child
        # sees it even if the caller's PYTHONPATH did not include it.
        extra = [str(REPO_ROOT / "build" / "python"), str(REPO_ROOT)]
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = os.pathsep.join(
            extra + ([existing] if existing else [])
        )
        self._proc = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "app.main:app",
                "--host", HOST,
                "--port", str(self.port),
                "--log-level", "warning",
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        try:
            self._wait_until_healthy(timeout_s=30.0)
            self._verify_config()
        except Exception:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *_exc) -> None:
        proc = self._proc
        self._proc = None
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        self._log.close()

    def _server_log(self) -> str:
        self._log.flush()
        self._log.seek(0)
        return self._log.read()

    def _wait_until_healthy(self, *, timeout_s: float) -> None:
        assert self._proc is not None
        deadline = time.monotonic() + timeout_s
        with httpx.Client(timeout=1.0) as client:
            while time.monotonic() < deadline:
                if self._proc.poll() is not None:
                    raise RuntimeError(
                        "server exited during startup with code "
                        f"{self._proc.returncode}:\n{self._server_log()}"
                    )
                try:
                    if client.get(f"{self.base_url}/health").status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                time.sleep(0.05)
        raise RuntimeError(
            f"server did not become healthy within {timeout_s}s:\n"
            f"{self._server_log()}"
        )

    def _verify_config(self) -> None:
        """Fail loudly if the server did not pick up the env-var config."""
        stats = read_stats(self.base_url)
        expected = {
            "queue_capacity": self.queue_capacity,
            "max_batch_size": self.max_batch_size,
            "max_wait_ms": float(self.max_wait_ms),
        }
        actual = {key: stats[key] for key in expected}
        if actual != expected:
            raise RuntimeError(
                f"server config mismatch: expected {expected}, got {actual}"
            )


def read_stats(base_url: str) -> dict:
    with httpx.Client(timeout=5.0) as client:
        resp = client.get(f"{base_url}/stats")
        resp.raise_for_status()
        return resp.json()


# --------------------------------------------------------------------- #
# Load                                                                   #
# --------------------------------------------------------------------- #


def run_cell(
    server: ServerProcess,
    *,
    commit: str,
    concurrency: int,
    requests: int,
    trials: int,
    warmup_requests: int,
    discard_first: bool,
    processes: int = 1,
) -> list[TrialRow]:
    predict_url = f"{server.base_url}/predict"
    warmup(predict_url, warmup_requests)

    total = trials + 1 if discard_first else trials
    rows: list[TrialRow] = []
    for index in range(1, total + 1):
        before = read_stats(server.base_url)
        result = run_trial(
            predict_url, concurrency, requests, processes=processes
        )
        after = read_stats(server.base_url)

        batches = after["batches_dispatched"] - before["batches_dispatched"]
        dispatched = (
            after["requests_dispatched"] - before["requests_dispatched"]
        )
        row = TrialRow(
            commit=commit,
            max_batch_size=server.max_batch_size,
            max_wait_ms=server.max_wait_ms,
            concurrency=concurrency,
            trial=index,
            kept=not (discard_first and index == 1),
            sent=result["sent"],
            ok=result["ok"],
            errors=result["errors"],
            wall_s=result["wall_s"],
            rps=result["rps"],
            mean_ms=result["mean_ms"],
            p50_ms=result["p50_ms"],
            p95_ms=result["p95_ms"],
            p99_ms=result["p99_ms"],
            batches_dispatched=batches,
            requests_dispatched=dispatched,
            realized_mean_batch_size=(dispatched / batches) if batches else 0.0,
        )
        rows.append(row)
        tag = "kept" if row.kept else "cold"
        print(
            f"  b={row.max_batch_size:<2} w={row.max_wait_ms:<3g} "
            f"c={row.concurrency:<2} trial {index}/{total} [{tag}]: "
            f"{row.rps:8.1f} req/s | p50 {row.p50_ms:6.2f} ms | "
            f"p99 {row.p99_ms:6.2f} ms | batch {row.realized_mean_batch_size:5.2f} "
            f"({row.requests_dispatched}/{row.batches_dispatched}) | "
            f"errors {row.errors}",
            flush=True,
        )
    return rows


# --------------------------------------------------------------------- #
# Reporting                                                              #
# --------------------------------------------------------------------- #


def mean_sd(values: list[float]) -> tuple[float, float]:
    if len(values) < 2:
        return (values[0] if values else float("nan")), 0.0
    return statistics.fmean(values), statistics.stdev(values)


def fmt(values: list[float], digits: int) -> str:
    mean, sd = mean_sd(values)
    return f"{mean:.{digits}f} ± {sd:.{digits}f}"


def write_raw_csv(path: Path, rows: list[TrialRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_summary_md(
    path: Path,
    rows: list[TrialRow],
    *,
    args: argparse.Namespace,
    commit: str,
    started_at: str,
    elapsed_s: float,
) -> None:
    kept = [r for r in rows if r.kept]
    concurrencies = sorted({r.concurrency for r in kept})
    batch_sizes = sorted({r.max_batch_size for r in kept})
    waits = sorted({r.max_wait_ms for r in kept})
    per_cell = len(kept) // max(1, len(concurrencies) * len(batch_sizes) * len(waits))

    lines: list[str] = []
    lines.append("# Scheduler sweep: max_batch_size x max_wait_ms x concurrency")
    lines.append("")
    lines.append(f"Measured {started_at} at commit `{commit}`. "
                 f"Total wall time {elapsed_s / 60:.1f} min. "
                 "Environment: see `environment.md`. Raw per-trial rows "
                 "(including discarded cold trials): `scheduler_sweep_raw.csv`.")
    lines.append("")
    lines.append("## Methodology")
    lines.append("")
    lines.append(
        f"- Grid: max_batch_size ∈ {{{', '.join(map(str, batch_sizes))}}} × "
        f"max_wait_ms ∈ {{{', '.join(f'{w:g}' for w in waits)}}} × "
        f"closed-loop concurrency c ∈ {{{', '.join(map(str, concurrencies))}}}."
    )
    lines.append(
        f"- One fresh `uvicorn app.main:app --host {HOST} --port {args.port} "
        f"--log-level warning` process per (max_batch_size, max_wait_ms), "
        f"configured through `MINISERVE_*` env vars with "
        f"`MINISERVE_QUEUE_CAPACITY={SERVER_QUEUE_CAPACITY}` and the default "
        f"request timeout. All concurrency levels run against that process "
        f"in ascending order; it is then killed."
    )
    sent_by_c = ", ".join(
        f"c={c} sends {args.requests // c * c}" for c in concurrencies
    )
    lines.append(
        f"- Load: `load_tests/load_test.py`'s closed-loop `run_trial` (one OS "
        f"thread with a persistent HTTP/1.1 connection per worker, threads "
        f"spread across up to {args.processes} generator processes), "
        f"{args.warmup} warmup requests per cell, then {per_cell + 1} trials of "
        f"{args.requests} requests split evenly across workers ({sent_by_c})."
    )
    lines.append(
        f"- Cold-trial policy: the first trial of every cell is discarded, "
        f"so {per_cell + 1} run and {per_cell} are reported. This covers both "
        f"process cold start (first cell of each server) and the regime "
        f"change when concurrency steps up (later cells)."
    )
    lines.append(
        "- Realized batch size: `GET /stats` is read immediately before and "
        "after each trial; the row's value is "
        "Δrequests_dispatched / Δbatches_dispatched. Counters only count "
        "model calls actually issued."
    )
    lines.append(
        "- Values are mean ± sample stddev across the reported trials. "
        "Latencies are client-observed end-to-end, in ms. p50/p99 are "
        "nearest-rank percentiles per trial, then averaged across trials."
    )
    lines.append("")
    lines.append("## Results")
    lines.append("")

    for c in concurrencies:
        lines.append(f"### c = {c}")
        lines.append("")
        lines.append("| max_batch | max_wait_ms | req/s | p50 (ms) | p99 (ms) | realized batch | errors |")
        lines.append("|---:|---:|---:|---:|---:|---:|---:|")
        for b in batch_sizes:
            for w in waits:
                cell = [
                    r for r in kept
                    if r.concurrency == c and r.max_batch_size == b
                    and r.max_wait_ms == w
                ]
                if not cell:
                    continue
                errors = sum(r.errors for r in cell)
                sent = sum(r.sent for r in cell)
                lines.append(
                    f"| {b} | {w:g} "
                    f"| {fmt([r.rps for r in cell], 1)} "
                    f"| {fmt([r.p50_ms for r in cell], 2)} "
                    f"| {fmt([r.p99_ms for r in cell], 2)} "
                    f"| {fmt([r.realized_mean_batch_size for r in cell], 2)} "
                    f"| {errors}/{sent} |"
                )
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


# --------------------------------------------------------------------- #
# Main                                                                   #
# --------------------------------------------------------------------- #


def git_commit() -> str:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, text=True
        ).strip()
        dirty = subprocess.run(
            ["git", "diff", "--quiet", "HEAD"], cwd=REPO_ROOT
        ).returncode != 0
        return sha + ("-dirty" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--batch-sizes", type=int, nargs="+",
                    default=list(DEFAULT_BATCH_SIZES))
    ap.add_argument("--wait-ms", type=float, nargs="+",
                    default=list(DEFAULT_WAITS_MS))
    ap.add_argument("--concurrency", type=int, nargs="+",
                    default=list(DEFAULT_CONCURRENCY))
    ap.add_argument("--trials", type=int, default=5,
                    help="reported trials per cell (one extra is run and "
                         "discarded unless --no-discard)")
    ap.add_argument("--requests", type=int, default=500,
                    help="requests per trial, split across workers")
    ap.add_argument("--warmup", type=int, default=50)
    ap.add_argument("--processes", type=int, default=4,
                    help="generator processes per trial (workers are threads "
                         "spread across them; never more than workers)")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--out-dir", type=Path, default=REPO_ROOT / "results")
    ap.add_argument("--no-discard", action="store_true",
                    help="keep the first trial of each cell")
    ap.add_argument("--no-write", action="store_true",
                    help="print only; do not write CSV/markdown")
    ap.add_argument("--smoke", action="store_true",
                    help="one cell (b=8, w=0, c=8), 1 trial, no discard, "
                         "no files: sanity-check the harness")
    args = ap.parse_args()
    if args.smoke:
        args.batch_sizes = [8]
        args.wait_ms = [0.0]
        args.concurrency = [8]
        args.trials = 1
        args.no_discard = True
        args.no_write = True
    return args


def main() -> None:
    args = parse_args()
    commit = git_commit()
    started_at = time.strftime("%Y-%m-%d %H:%M %Z")
    t0 = time.perf_counter()

    n_cells = len(args.batch_sizes) * len(args.wait_ms) * len(args.concurrency)
    per_cell = args.trials + (0 if args.no_discard else 1)
    print(f"commit {commit} | {n_cells} cells × {per_cell} trials × "
          f"{args.requests} requests | queue_capacity={SERVER_QUEUE_CAPACITY}",
          flush=True)

    rows: list[TrialRow] = []
    for b in args.batch_sizes:
        for w in args.wait_ms:
            print(f"server: max_batch_size={b} max_wait_ms={w:g}", flush=True)
            with ServerProcess(
                port=args.port,
                max_batch_size=b,
                max_wait_ms=w,
                queue_capacity=SERVER_QUEUE_CAPACITY,
            ) as server:
                for c in args.concurrency:
                    rows.extend(run_cell(
                        server,
                        commit=commit,
                        concurrency=c,
                        requests=args.requests,
                        trials=args.trials,
                        warmup_requests=args.warmup,
                        discard_first=not args.no_discard,
                        processes=args.processes,
                    ))

    elapsed = time.perf_counter() - t0
    print(f"done in {elapsed:.1f}s", flush=True)

    if args.no_write:
        return
    raw_path = args.out_dir / "scheduler_sweep_raw.csv"
    md_path = args.out_dir / "scheduler_sweep.md"
    write_raw_csv(raw_path, rows)
    write_summary_md(
        md_path, rows,
        args=args, commit=commit, started_at=started_at, elapsed_s=elapsed,
    )
    print(f"wrote {raw_path}\nwrote {md_path}")


if __name__ == "__main__":
    main()

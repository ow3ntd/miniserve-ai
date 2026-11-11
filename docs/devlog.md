# miniserve-ai — Devlog

Reverse-chronological. Same rules as `lob-latency-lab`: entries record what
was actually done, decisions made and why, and problems hit — no
retroactive polish.

---

## 2026-10-15 — Day 8: scheduler sweep, load-generator collapse, corrected baseline

**Done**

- Instrumentation for benchmarking (committed as `0e20563`): `AsyncScheduler`
  counts `batches_dispatched` / `requests_dispatched` (live requests in
  model calls only; all-stale skipped batches don't count); `GET /stats`
  returns both, their ratio as `mean_batch_size`, and the effective config;
  the module-level `app` reads `MINISERVE_QUEUE_CAPACITY`,
  `MINISERVE_MAX_BATCH_SIZE`, `MINISERVE_MAX_WAIT_MS`,
  `MINISERVE_REQUEST_TIMEOUT_S` with `create_app()`'s signature unchanged.
  10 new tests; suite at **134 passed**.
- `load_tests/sweep_scheduler.py`: max_batch_size ∈ {1, 8, 32} ×
  max_wait_ms ∈ {0, 1, 5} × closed-loop c ∈ {1, 8, 32}; one fresh uvicorn
  per config via env vars (`MINISERVE_QUEUE_CAPACITY=256`), 50 warmup,
  6 trials × 500 requests per cell with the first trial discarded as cold,
  `/stats` differenced around each trial for realized batch size. Verifies
  the server picked up its config before sending load.
- First sweep, with the asyncio/httpx generator: every c=32 cell with
  max_batch_size > 1 collapsed (~600 req/s, p99 > 200 ms, realized batch
  ~1.08) and several were bimodal (single trials at ~1,800 req/s). Those
  results are kept as evidence under
  `results/superseded/scheduler_sweep_httpx_2026-10-15.{md,csv}`.
- Diagnosis (no code changes kept; trace instrumentation applied behind
  `MINISERVE_TRACE=1`, then reverted):
  - httpx `Limits(max_connections=64, max_keepalive_connections=64)` did
    not remove the collapse; it got worse (390 req/s, p99 > 350 ms).
  - Per-batch trace: queue depth at pop was 1 for 96–98% of batches;
    supervision-loop iterations in `_execute_batch` were 1 for 91–94% of
    batches and never above 4; formation 6 µs; event-loop lag probe p99
    ≈ 45 µs (loop idle); 1.3 ms p50 idle gap between batches waiting for
    the next admit. Per-request: client-send → handler-entry 15.4 ms p50 /
    221 ms p99; everything inside the handler 0.25 ms p50. In-handler
    concurrency was 1 for 79% of requests (vs. ≥ 2 for 65% at
    max_batch_size=1).
  - Kernel socket sampling (`netstat`) during a slow trial: server-side
    Recv-Q = 0 in every sample, client-side Send-Q = 0. Established
    connections pinned at exactly 20 = httpx's default
    `max_keepalive_connections`; with max_batch_size=1 they churned 17–31.
  - Controls: `OMP_NUM_THREADS=1` (no change); uvicorn `--loop asyncio
    --http h11` instead of uvloop/httptools (no change).
  - Replacing the client with 32 OS threads on persistent `http.client`
    connections: the "collapsed" cell served **10,227 ± 112 req/s**,
    p50 2.94 ms, p99 3.86 ms, realized batch 7.67; max_batch_size=1 gave
    3,467 ± 749 req/s. The scheduler was never the problem.
- `load_tests/load_test.py` rewritten around that design: W workers, one
  persistent HTTP/1.1 connection each, closed-loop, same CLI and trial /
  warmup / percentile structure, `run_trial()` kept as the import the
  sweep uses. Added `--method GET` and repeatable `--url` for calibration,
  and `--processes` after calibration showed one CPython process tops out
  at 24–27k req/s (GIL-bound).
- Generator calibrated against `GET /health` (recorded with exact commands
  in `results/environment.md`): 35,068 ± 2,062 req/s at c=32 against one
  uvicorn (uvicorn's event loop near one core is the limiter there);
  42,113 ± 1,612 req/s against two uvicorns (3.5 client cores). Both
  clear the ~3× rule against the best scheduler cell (11,359 req/s → 3.1×).
- Sweep re-run with the new generator (`results/scheduler_sweep.md`,
  raw CSV alongside): 162 trials, 0 errors, 104 s. Headlines, c=32:
  max_batch=1 4,297 ± 133 req/s p50 7.19 ms; max_batch=8 w=0
  11,164 ± 96 req/s p50 2.75 ms p99 4.47 ms, realized batch 7.87;
  max_batch=32 w=5 realized batch 31.6. c=1: ~3,050–3,185 req/s at w=0
  for every max_batch, and the expected full-window penalty when
  max_wait_ms > 0 with a single client (430 and 129 req/s).
- `results/benchmark_summary.md`: Day 4 table kept and labeled historical;
  its "server saturates ~1,700 req/s" interpretation replaced with the
  finding that this was the httpx generator's ceiling, confirmed today.
  Interpretation of the new sweep deliberately not written yet.

**Decisions**

- **Replace the generator rather than tune httpx.** The collapse was a
  property of one event loop plus one connection pool serving 32
  closed-loop workers; raising pool limits made it worse. OS threads
  (and processes) with private persistent connections have no shared
  state a slow worker can stall. The threaded design is also what proved
  the server healthy, so the production generator is the instrument that
  was already validated.
- **Calibrate the generator and record the ceiling before trusting any
  server number.** Rule adopted: single-server `GET /health` ceiling at
  c=32 must be ≥ ~3× the best scheduler cell. The first threaded version
  (one process) came in at 27.3k vs. 10.2k = 2.7×, so it went
  multi-process (`--processes 4` default in the sweep) before the re-run.
- **Two ceilings are recorded, and they mean different things.** ~35k
  req/s against one uvicorn is the HTTP layer's ceiling and bounds every
  `/predict` number; ≥ 42k against two uvicorns is the generator's. Only
  the second is a client limit.
- **Discard the first trial of every cell, not just of every server.**
  One server serves c=1, 8, 32 in sequence; the first cell absorbs
  process cold start and later cells absorb the concurrency step. Treating
  them alike keeps the reported sample size uniform (5 of 6).
- **Keep superseded results, don't rewrite them.** Same rule as
  `performance_history.md`: the httpx numbers stay as evidence with a
  header saying what they measured.
- **Do not present the httpcore pool mechanism as established.** The
  pinned-at-20 observation and a reading of httpcore 1.0.9's idle-pruning
  code (`_assign_requests_to_connections` appears to count all
  connections, not idle ones, when deciding to close an idle connection)
  are *consistent with* the churn seen; they were not traced to the
  bimodality instruction by instruction, and the fix did not depend on
  them.

**Problems**

- Two hypotheses rejected by measurement before the right one: event-loop
  starvation from the supervision loop's per-admission task churn (1
  iteration per batch in >90% of batches; loop idle), and torch OpenMP
  threads spinning after batches > 1 (`OMP_NUM_THREADS=1` changed
  nothing).
- The Day 4 baseline's central claim was wrong for three months. Its
  Little's Law check "closed" only because the in-system time it used was
  dominated by the generator. The httpx c=1 rows were also inflated by
  client overhead (p50 0.69 ms vs. 0.33 ms with the threaded client), so
  none of the httpx rows are quoted.
- uvicorn's single event loop caps the HTTP layer near 35k req/s here, so
  the sweep's top cells (~11k req/s) sit at roughly a third of that
  ceiling; the margin is adequate for this grid but not for much larger
  batches or a faster model.
- Several c=32 cells with max_wait_ms=5 (and max_batch=1 w=5) have one
  outlier trial each, visible as p99 stddevs as large as their means
  (e.g. 13.62 ± 20.85 ms). Raw rows are in the CSV; whether to report
  medians is an interpretation question for the write-up.
- The re-run necessarily executed on a dirty tree (`0e20563-dirty`)
  because results were reviewed before the generator/sweep commit;
  `environment.md` records this.
- Unrelated but relevant to benchmark hygiene: an 8-day-old orphaned
  profiling script from another session was pinning one core until it
  was killed before the first run.

**Next**

- Write the sweep interpretation (README, `docs/design.md`,
  `results/benchmark_summary.md`, `performance_history.md` entry) with
  the numbers now in hand; decide how to present the outlier trials.
- Consider an open-loop generator for overload experiments; the closed
  loop cannot exceed the server's rate by construction.
- Before citing the httpcore behavior anywhere, check whether it is a
  known or already-fixed upstream issue.
- Graceful bounded shutdown and drain (carried from Day 7).

---

## 2026-08-12 — Day 7: end-to-end request deadlines

**Done**

- Added domain-specific `RequestDeadlineExceeded` and made the absolute
  `PendingRequest.deadline` authoritative across the full admitted lifecycle.
- Exposed validated `request_timeout_s` through `create_app()` and translated
  deadline expiration to HTTP 504.
- Exposed the registry's monotonic clock for deadline creation and comparison;
  asyncio wait durations are derived from the same absolute clock domain.
- Made dynamic batch waits wake at the earlier of the fixed `max_wait_ms`
  deadline and the earliest live request deadline.
- Added queued-request expiration, stale-ID handling, and final pre-dispatch
  deadline filtering.
- Supervised all registry-owned deadlines during active model inference,
  including requests queued behind that invocation.
- Added selective timeout within a model batch without shifting positional
  output correspondence; late results for timed-out IDs are safe no-ops.
- Defined exactly-once result/deadline/cancellation races through the
  registry's first-successful-removal rule.
- Hardened scheduler stop to cancel and consume the asyncio inference wrapper
  without waiting for or pretending to terminate its executor thread.
- Disambiguated domain `RequestDeadlineExceeded` from `wait_for()` timeout by
  checking whether the owned future already completed.
- Final hardened Python suite: **124 passed**. Changed-file compilation and
  `git diff --check` passed.

**Decisions**

- **One end-to-end deadline, not phase-local timeouts.** Registration creates
  the single authority used through queueing, formation, inference, and HTTP
  waiting.
- **`request_timeout_s` is separate from `max_wait_ms`.** The former bounds the
  client lifecycle; the latter bounds only batch formation latency.
- **Timeout is individual, not a whole-batch failure.** One expired request
  must not fail unrelated batch-mates.
- **Do not terminate running model threads.** Python cannot forcibly stop an
  executor thread safely; timed-out ownership is removed and late output is
  ignored instead.
- **Terminal stop prevents restart-created overlap.** A stopped scheduler
  cannot launch a new model call beside an old thread still finishing.
- **Own and consume wrapper tasks.** Explicit cancellation and awaiting avoids
  unhandled asyncio task exceptions after stop.

**Deferred**

- Graceful drain of queued and in-flight work.
- Metrics and structured tracing.
- Scheduler/open-loop benchmarks.
- Multiple workers and GPU serving.

No benchmark results or production-readiness claims were added.

**Next**

- Implement graceful bounded shutdown and drain behavior.

---

## 2026-08-11 — Day 6: dynamic batch formation

**Done**

- Added configurable, validated `max_wait_ms`; zero preserves immediate Day 5
  dispatch.
- Forming batches now dispatch on `max_batch_size` live requests or elapsed
  `max_wait_ms`, whichever happens first.
- Used the asyncio event loop's monotonic clock and fixed the batch deadline
  when the first live request enters an empty forming batch.
- Allowed later requests to join available capacity without resetting or
  extending the original deadline.
- Made stale and cancelled IDs consume no live capacity; formation continues
  through them to find later live FIFO work, and all-stale batches skip the
  model.
- Extended `FixedBatchWorker.prepare_batch(max_items)` so the scheduler can
  dequeue only a forming batch's remaining capacity while preserving
  `process_once()` and zero-argument preparation behavior.
- Kept event-driven wake-up during partial timed waits, with no polling or
  production sleeps.
- Added a real HTTP concurrency test proving two `/predict` calls can coalesce
  into one model invocation.
- Added timeout-boundary lost-wakeup coverage and preserved serialized model
  execution.
- Final Python suite: **105 passed**. Changed-file compilation and
  `git diff --check` also passed.

**Decisions**

- **The first live request owns the window.** A fixed absolute deadline gives
  a hard batching-latency bound; resetting it on later arrivals could postpone
  dispatch indefinitely under steady traffic.
- **Live capacity, not dequeued-ID count, determines fullness.** Cancelled IDs
  are implementation debris, not work, so they cannot force undersized model
  calls or block live requests behind them.
- **One event serves idle and timed waiting.** Waiting on `asyncio.Event` with
  the deadline's remaining time avoids polling and preserves admissions that
  race timeout dispatch.
- **One scheduler task still owns inference sequencing.** It awaits each
  `asyncio.to_thread()` invocation before forming the next executing batch, so
  model calls cannot overlap.
- **Batch wait and request timeout remain separate.** `max_wait_ms` controls
  formation latency only; stored request deadlines are not yet enforced.

**Deferred**

- Request deadline enforcement and HTTP 504.
- Graceful drain.
- Metrics and tracing.
- Scheduler/open-loop benchmarks.
- Multiple workers and GPU serving.

No benchmark results or production-readiness claims were added.

**Next**

- Implement request deadline enforcement and HTTP 504 timeout behavior.

---

## 2026-08-08 — Day 5: asynchronous scheduler integration

**Done**

- Added `AsyncScheduler`, owning one background asyncio task awakened by an
  `asyncio.Event` and draining immediately available fixed-size batches.
- Integrated scheduler start/stop through FastAPI lifespan and replaced the
  synchronous `/predict` handler with async admission plus per-request future
  waiting.
- Wired the native bounded queue into HTTP admission; `QueueFullError` now
  produces HTTP 503.
- Offloaded only blocking `ModelRunner.predict()` calls with
  `asyncio.to_thread()`. Registry preparation and completion remain on the
  event-loop thread.
- Serialized model calls by awaiting each inference before preparing another
  batch.
- Added pre-admission `ModelRunner.validate_request()` so invalid model input
  returns 422 without failing valid requests in a shared batch.
- Added shielded future waiting and explicit registry cancellation when a
  request coroutine is cancelled.
- Made stop terminal and non-restartable. Pending registry entries are
  cancelled; restart is forbidden because cancelling a `to_thread()` await
  cannot stop its already-running inference thread.
- Added regressions for concurrent result routing, event-loop responsiveness,
  non-overlapping model calls, queue saturation, model failures, cancellation,
  stop during inference, validation isolation, and the drain-to-wait lost-
  wake-up boundary.
- Final Python suite: **86 passed**. Changed-file compilation and
  `git diff --check` also passed.

**Deliberate limitations**

- No graceful drain. A running inference thread may finish after stop, and
  queued native IDs are not drained.
- Dynamic `max_wait_ms` batch formation, deadline enforcement, metrics, and
  scheduler benchmarks remain future work.
- Existing measured results remain the older synchronous baseline; no Day 5
  performance numbers were invented.

**Next**

- Add dynamic batch formation using `max_wait_ms` and measure it separately
  from the synchronous baseline.

---

## 2026-07-25 — Day 4: fixed-size batch worker

**Done**

- Added `FixedBatchWorker.process_once()` to pop up to `max_batch_size` opaque
  IDs, recover Python payloads, execute one model batch, and route FIFO outputs
  to their matching futures.
- Added stale-ID skipping, output-cardinality validation, whole-batch model
  failure propagation, and exactly-once handling of late completion.
- Added integration coverage across `AdmissionController`, the native queue,
  `RequestRegistry`, and the real `ModelRunner`.

**Decisions**

- This milestone was synchronous and one-shot. It did not include a background
  task or FastAPI integration.
- Empty and all-stale batches do not invoke the model. A genuine model error or
  wrong output count fails every live request in that batch.
- FIFO input/output correspondence is the routing contract.

**Next**

- Add the asynchronous scheduler loop and connect it to `/predict` while
  keeping registry access on the event-loop thread.

---

## 2026-07-24 — Day 3: scheduler admission transaction

**Done**

- Added `AdmissionController` to connect Python-owned request state to the
  bounded native queue.
- Added non-blocking admission through opaque request IDs.
- Added immediate `QueueFullError` rejection when native capacity is reached.
- Added rollback for queue rejection and native enqueue exceptions.
- Added tests using controlled queues and the real pybind11 queue.
- Verified FIFO admission order and that rejected requests leave no registry
  entries or pending futures.

**Decisions**

- **Admission is a transaction.** A successful call leaves both a native
  queued ID and a Python registry record; a failed call leaves neither.
- **Rollback cancels the internal future.** The caller receives
  `QueueFullError`; future cancellation is internal cleanup that prevents an
  orphaned pending future.
- **Rejected IDs are not reused.** Monotonic allocation makes request identity
  unambiguous in future logs, metrics, and race investigations.
- **HTTP behavior remains outside this module.** A later FastAPI integration
  will translate `QueueFullError` into HTTP 503.
- **No worker behavior yet.** Dequeueing, inference, timeout enforcement, and
  result routing remain separate milestones.

**Next**

- Add one fixed-size scheduler worker that dequeues request IDs, retrieves
  Python-owned payloads, performs one model batch, and routes each output to
  the correct future.

---

## 2026-07-24 — Python request registry

**Done**

- Added `app/request_registry.py` with Python-owned pending request state.
- Added monotonically increasing positive request IDs compatible with the
  native queue's `uint64_t` contract.
- Added immutable token ownership, event-loop futures, monotonic arrival
  timestamps, and absolute monotonic deadlines.
- Added exactly-once `resolve`, `fail`, and `cancel` lifecycle operations.
- Added focused tests covering ID allocation, timeout validation, immutable
  token copies, deadlines, success, failure, cancellation, duplicate
  completion, ID exhaustion, unknown IDs, and a 5,000-request leak check.

**Decisions**

- **Python owns payloads and futures; C++ owns only request IDs.** This keeps
  Python object lifetime and reference counting outside the concurrent native
  queue.
- **Event-loop-thread confinement instead of locks.** Registry state will be
  mutated only by scheduler code running on one asyncio event loop.
- **Remove before completing.** A request record is removed before its future
  is resolved, failed, or cancelled so completion callbacks cannot re-enter
  the registry and complete the same request again.
- **Late completion is a safe no-op.** Lifecycle methods return `False` for
  unknown or previously completed IDs rather than raising
  `InvalidStateError`.
- **Store deadlines now; enforce them later.** Registration records the full
  end-to-end monotonic deadline, while scheduler timeout enforcement remains
  outside this milestone.

**Next**

- Add the scheduler admission layer that registers a request, atomically
  attempts to enqueue its ID in the bounded C++ queue, and rolls back the
  registry entry immediately when the queue is full.

---

## 2026-07-21 — C++ bounded queue + pybind11 boundary

**Done**

- Added `miniserve::BoundedQueue`, a fixed-capacity FIFO of opaque request IDs.
  `try_push()` atomically checks capacity and inserts or returns `false`;
  `try_pop()` and `pop_batch()` preserve FIFO order.
- Added a pybind11 module (`miniserve_core`) and release the GIL around every
  queue operation. Python conversion happens outside the native critical
  section.
- Added native contract tests plus a four-producer/four-consumer stress test
  checking no loss, no duplicates, full drain, and bounded completion.
- Added Python binding tests and CMake/CTest build wiring.

**Decisions**

- **Store `uint64_t` request IDs, not `py::object`.** Python will own payloads
  and futures in a registry. This prevents Python reference-count mutations in
  the queue and keeps the C++ core safe while the GIL is released.
- **Non-blocking admission only.** Immediate `false` on full is the primitive
  required by the already-documented 503 backpressure policy. Blocking waits,
  close state, and condition variables would add behavior the scheduler does
  not yet use.
- **One mutex over a deque.** This is intentionally simpler and easier to prove
  than a lock-free queue. The project needs defensible invariants and measured
  behavior, not a prematurely sophisticated data structure.

**Next**

- Build the Python async scheduler around this primitive: request registry,
  monotonically assigned IDs, exactly-once future resolution, one worker, and
  fixed-size batch execution. Add `max_wait_ms`, deadlines, and shutdown only
  after that minimal path is correct.

---

## 2026-07-14 — Day 4: baseline benchmark (no batching)

**Done**

- `load_tests/load_test.py`: closed-loop async load generator (httpx +
  asyncio), CLI args for url/concurrency/requests/trials/warmup,
  nearest-rank percentiles, per-trial rows plus mean ± stddev across
  trials, cold-start captured separately from steady state.
- `results/environment.md`: Apple M4, 10 cores, 24 GB, macOS 26.2,
  Python 3.12, pinned package versions, exact server invocation.
- `results/benchmark_summary.md` + `results/performance_history.md`
  with measured v0.1 baseline:
  - c=1: 1583.8 ± 59.2 req/s, p50 0.61 ms, p99 0.96 ms, 0 errors
  - c=8: 1712.2 ± 47.2 req/s, p50 3.09 ms, p99 23.25 ms, 0 errors
  - cold start 25.55 ms (~40x steady-state p50)

**Observations**

- 8x concurrency bought ~8% throughput and a ~5x p50 increase; p99 went
  from <1 ms to ~23 ms. Server saturates ~1,700 req/s; past that,
  concurrency converts to queueing delay. Little's Law closes:
  1712 req/s x ~4.7 ms in-system ≈ 8 in flight = the 8 workers.
- This is the "before" picture: per-request model invocation pays full
  overhead 8x for work one batched pass could amortize. Days 6–9 exist
  to beat this table.

**Decisions**

- **Closed-loop generator first.** Offered load adapts to server speed,
  which is the right shape for a baseline; open-loop (fixed arrival
  rate) is deferred to the scheduler experiments where overload is the
  point. The distinction is recorded in the methodology preamble.
- **5 trials, mean ± stddev, warmup excluded, benchmark on AC power,
  no access logging.** Same rigor bar as lob-latency-lab; single-run
  numbers don't go in results files.
- **Nearest-rank percentiles** over interpolation: simple, defensible,
  and stable at these sample sizes.

**Next**

- Day 1.5 (C++ bounded queue): macOS update has landed (26.2), so the
  toolchain blocker should be gone — verify clang/CMake, then build the
  queue core. Then Day 5 scheduler design doc.
---

## 2026-07-11 — Day 3: synchronous /predict endpoint

**Done**

- `app/schemas.py`: `PredictRequest` (non-empty `list[int]`, unknown
  fields rejected) and `PredictResponse` (`outputs: list[float]`).
- `app/main.py`: `create_app()` now builds and eagerly loads a
  `ModelRunner` (or accepts an injected one for tests) and exposes
  `POST /predict` — a batch of one, run inline. This is the "before"
  picture the Day 4 baseline benchmark measures.
- `tests/test_predict.py`: 12 tests covering the happy path (status,
  output shape, determinism, exact agreement with a directly-invoked
  runner), structural 422s (empty/missing/mistyped/unknown fields),
  model-dependent 422s (out-of-vocab token), 503 on an unloaded runner,
  and 405 on GET.

**Decisions**

- **Validation split across two layers, deliberately.** Structural
  validation (presence, types, non-empty) lives in Pydantic and never
  reaches app code; model-dependent validation (vocab range) stays in
  `ModelRunner._validate()`, since the runner owns the vocab size.
  The endpoint translates the runner's `ValueError` into a 422 so both
  layers look identical to clients. The alternative — duplicating the
  vocab bound in the schema — creates two sources of truth that can
  drift.
- **`extra = "forbid"` on the request schema.** A typo'd field name
  should fail loudly as a 422, not be silently ignored.
- **Eager model load inside `create_app()` rather than a lifespan
  hook.** Keeps the factory self-contained and lets tests use
  `TestClient` without a context manager (lifespan hooks only run when
  the client is entered as one). Revisit at Day 6: the scheduler needs
  real startup/shutdown ordering, and load-time work moves to a
  lifespan context then.
- **`def`, not `async def`, for the handler.** FastAPI runs sync
  handlers in its threadpool, so the (CPU-bound) model call doesn't
  stall the event loop — and the endpoint makes no async promises it
  can't keep. The scheduler replaces this path entirely.
- **503 vs 422 boundary.** An unloaded model is a server-side readiness
  problem (503); everything the client can fix is a 422.

**Next**

- Day 4: baseline benchmark of this unbatched path (rps, p50/p95/p99,
  error rate; 5+ trials; environment logged to `results/environment.md`)
  — or Day 1.5 (C++ bounded queue) if the macOS update happens first.

## 2026-07-09 — Day 2: model runner abstraction

**Done**

- `app/model_runner.py`: `ModelRunner` with load-once lifecycle and a
  `predict(batch)` interface taking variable-length token-id sequences and
  returning plain Python floats. Batch assembly pads to the longest request
  in the batch with a parallel real-token mask; disassembly splits the
  `[B, n_outputs]` output back into per-request rows in input order.
- In-repo model: embedding → mask-aware mean pool → linear head, seeded
  weights, eval mode, CPU-only, `torch.inference_mode()`.
- 16 tests covering lifecycle (predict-before-load, idempotent load,
  cross-runner determinism), output shape, validation (empty batch/sequence,
  out-of-range and non-integer tokens), and — the load-bearing one —
  **batch invariance**: every request's solo output must match its output
  when batched with unrelated, differently sized requests.
- `docs/design.md` section 4 written (batch shape assembly, why padding
  can't leak, output-ordering contract); later sections renumbered.
- `torch==2.13.0` pinned.

**Decisions**

- **Tiny in-repo model over a pretrained checkpoint.** The model is
  incidental to this project; in-repo keeps startup instant, tests
  deterministic via seeding, and the repo free of downloads. The batching
  correctness problem is identical either way.
- **Mask-aware mean pooling as the leak-proofing mechanism.** Padded
  embeddings are zeroed by the mask and the mean divides by real-token
  count, so the pad id is mathematically irrelevant — `PAD_ID = 0` may
  also appear as a real token, which the invariance test exercises.
- **Float tolerance (`1e-5`) rather than exact equality in the invariance
  test.** Batched vs. single-row matmuls can take different BLAS kernel
  paths with different summation orders; demanding bitwise equality would
  make the test flaky across BLAS backends for no correctness gain.
- **Ordering note for later:** row *i* of the batch output belongs to
  request *i* — this is the contract the Day 6–9 scheduler will use to
  route results to per-request futures, so it's asserted by test now.

**Ordering deviation**

- Day 1.5 (C++ bounded queue) is intentionally deferred until after a
  macOS update: Day 1 setup surfaced that this machine's OS is behind
  (Homebrew's Python bottle failed with a libexpat symbol mismatch —
  `_XML_SetAllocTrackerActivationThreshold` missing from the system
  `libexpat.1.dylib` — resolved by switching to the python.org build).
  The C++ toolchain will be sensitive to the same OS-version issues.
  Day 2 has no dependency on the queue core, so it was pulled forward.

**Next**

- Day 3: synchronous `/predict` endpoint wiring the model runner to
  FastAPI with Pydantic schemas and invalid-request handling — or Day 1.5
  if the macOS update happens first.

---

## 2026-07-08 — Day 1: scaffold + health endpoint

**Done**

- Repo scaffold matching the v0.1 plan (`app/`, `core/`, `tests/` incl.
  `tests/cpp/`, `docs/`, `load_tests/traces/`, `results/`,
  `.github/workflows/` — future directories held with `.gitkeep`).
- Minimal FastAPI app with `GET /health` returning status, service name,
  version, and uptime. No prediction, queueing, or batching logic exists.
- pytest setup (`pyproject.toml` config, `pythonpath` rooted at repo) with
  4 passing tests: 200 status, identity/version payload, non-negative
  uptime, and a 404 check confirming `/predict` does not exist yet.
- README skeleton framed around the scheduler positioning; `docs/design.md`
  skeleton with TBD sections mapped to the day they get built.

**Decisions**

- **App factory (`create_app()`) over a bare module-level app.** Tests get
  isolated instances, and later config injection (queue capacity, batch
  parameters) has a natural seam instead of module-level globals.
- **Pinned `requirements.txt` from Day 1.** Benchmark reproducibility later
  depends on knowing exactly what was installed; cheaper to start pinned
  than to reconstruct.
- **`uptime_s` in `/health` via `time.monotonic()`.** Trivial now, but gives
  the shutdown-drain tests (Day 12) an existing probe to reason about
  process lifetime, and monotonic avoids wall-clock jump artifacts.

**Problems**

- None of substance; Day 1 is deliberately small.

**Next**

- Day 1.5: C++ bounded, thread-safe queue in `core/` (capacity limit,
  blocking/non-blocking push, pop-batch), native unit tests, pybind11
  bindings, and the "why C++" section of `design.md`.

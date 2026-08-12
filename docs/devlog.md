# miniserve-ai — Devlog

Reverse-chronological. Same rules as `lob-latency-lab`: entries record what
was actually done, decisions made and why, and problems hit — no
retroactive polish.

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

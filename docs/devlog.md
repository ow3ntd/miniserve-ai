# miniserve-ai — Devlog

Reverse-chronological. Same rules as `lob-latency-lab`: entries record what
was actually done, decisions made and why, and problems hit — no
retroactive polish.

---

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

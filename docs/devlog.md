# miniserve-ai — Devlog

Reverse-chronological. Same rules as `lob-latency-lab`: entries record what
was actually done, decisions made and why, and problems hit — no
retroactive polish.

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

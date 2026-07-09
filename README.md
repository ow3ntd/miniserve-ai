# miniserve-ai

**A concurrent request scheduler with a pluggable model backend** — a hand-built C++ bounded-queue core exposed to Python via pybind11, an async dynamic-batching scheduler on top of it, and failure handling (backpressure, timeouts, graceful drain) exercised under real load and measured with statistical rigor.

The model is incidental; the scheduler is the point.

> **Status: v0.1 in progress — Day 1 (scaffold + health endpoint).** Everything below marked *(planned)* does not exist yet. This README will only ever describe what has actually been built and measured.

## Why this project exists

Serving systems live or die on their concurrency core: how requests queue, how batches form, what happens when the queue is full, and what happens to in-flight work at shutdown. This repo builds that core deliberately — including intentionally inducing and fixing race conditions — rather than assembling framework defaults. It is the serving-side companion to [`lob-latency-lab`](https://github.com/ow3ntd/lob-latency-lab), which applied the same approach (C++ hot path, stress testing, benchmark methodology) to a limit order book.

## Architecture *(planned)*

```
                        ┌────────────────────────────────────────┐
  HTTP requests ──────► │  FastAPI (Python)                      │
                        │    └─ async scheduler (orchestration)  │
                        │         │            ▲                 │
                        │   ┌─────▼────────────┴─────┐           │
                        │   │  C++ bounded queue /   │  pybind11 │
                        │   │  batch-formation core  │  boundary │
                        │   └─────┬──────────────────┘           │
                        │         ▼                              │
                        │   model runner (batched inference)     │
                        └────────────────────────────────────────┘
```

Diagram to be replaced with the real one once the boundary exists (Day 6–7).

## Features

- [x] Health endpoint (`GET /health`)
- [ ] C++ bounded, thread-safe queue with pybind11 bindings *(Day 1.5)*
- [ ] Model runner abstraction with explicit batch assembly/splitting *(Day 2)*
- [ ] `POST /predict` *(Day 3)*
- [ ] Async scheduler with dynamic batching (`max_batch_size`, `max_wait_ms`) *(Days 6–9)*
- [ ] Deliberate concurrency bug hunt, documented in `docs/concurrency_bugs.md` *(Day 10)*
- [ ] Backpressure (503 on full queue), request timeouts, graceful shutdown drain *(Days 11–12)*
- [ ] Prometheus `/metrics` + structured tracing with correlation IDs *(Day 13)*
- [ ] Reproducible load testing with recorded trace replay *(Day 14)*

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# run the server
uvicorn app.main:app --reload

# hit the health endpoint
curl http://127.0.0.1:8000/health
```

## Running tests

```bash
pytest
```

## Benchmarks

TBD — no numbers are published until they have been measured under the methodology in `results/` (5+ trials with mean/median/stddev, environment logged in `results/environment.md`, warm/cold separated, load shape stated). First baseline lands on Day 4.

## Key engineering lessons

TBD — populated as the devlog (`docs/devlog.md`) and `docs/concurrency_bugs.md` accumulate real content.

## Limitations

- CPU-only, single process, local machine. No GPU, no distributed serving, no Kubernetes/Redis/Ray — deliberately out of scope for v0.1.
- Not production-ready and not claimed to be; this is a concurrency-first serving lab.

## Roadmap

See the daily progression in `docs/design.md` (once written). v0.2 candidates: GPU support, multiple model backends, priority scheduling.

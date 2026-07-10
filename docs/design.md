# miniserve-ai — Design

> Skeleton (Day 1). Each section is filled in on the day its subject is built,
> not before. Sections marked TBD contain no invented content.

## 1. Overview and positioning

miniserve-ai is a concurrent request scheduler with a pluggable model backend.
The scheduling core — a bounded, thread-safe queue with batch-formation
semantics — is implemented in C++ and exposed to Python via pybind11. Python
(FastAPI + asyncio) owns orchestration; C++ owns the hot path.

## 2. Request lifecycle

TBD (Day 5, scheduler design doc). Will cover: request admission → queue →
batch formation → inference → result routing to per-request futures →
response, including every failure exit (rejection, timeout, shutdown).

## 3. Why the queue core lives in C++

TBD (Day 1.5). Will cover: GIL constraints on true multi-producer
concurrency, lock granularity, and the throughline from the
`lob-latency-lab` matching engine.

## 4. Model runner and batch shape assembly

*(Written Day 2, when this was built.)*

The model runner (`app/model_runner.py`) owns the model lifecycle and the
batch tensor shape work. The model itself is a deliberately tiny, in-repo
network — embedding → mask-aware mean pooling → linear head — because the
model is incidental to this project; keeping it in-repo makes startup
instant, tests deterministic (seeded weights), and the repo free of
checkpoint downloads.

**The shape problem.** Requests arrive as variable-length token-id
sequences. Tensors are rectangular, so a batch of lengths {3, 12, 1, 4}
cannot be stacked directly. Assembly (`_assemble`) pads every request to
the longest length *in that batch* (not a global maximum — short batches
shouldn't pay for a worst case that isn't present) and builds a parallel
{1.0, 0.0} mask marking which positions are real.

**Why padding can't leak.** Two mechanisms, both in the forward pass:
padded embeddings are multiplied by the 0.0 mask before the sum, and the
mean divides by the count of *real* tokens per row rather than the padded
length. A consequence worth stating explicitly: the pad token's id is
mathematically irrelevant (its embedding is zeroed), so `PAD_ID = 0` can
also legitimately appear as a real token.

**Disassembly and ordering.** The model returns `[B, n_outputs]`; row *i*
belongs to request *i*. That ordering is the contract the future scheduler
will rely on to route each result back to the correct caller's future.
`_disassemble` converts to plain Python floats so nothing above the runner
ever handles tensors.

**The property that matters.** Batching must never change a request's
result — a padding leak wouldn't crash anything, it would just silently
corrupt outputs, which is the worst failure mode a batching server can
have. This is enforced by test
(`test_batch_invariance_padding_does_not_leak`): each request's solo
output must match its output when batched with unrelated, differently
sized requests. Tolerance is `1e-5`, not exact equality, because batched
and single-row matrix multiplies may take different BLAS kernel paths with
different floating-point summation orders.

Everything runs on CPU under `torch.inference_mode()`.

## 5. Batching design

TBD (Days 5, 8–9). Will cover: `max_batch_size`, `max_wait_ms`, the
latency/throughput tradeoff with measured numbers, and the race between
batch-timeout timers and producers.

## 6. Backpressure, timeouts, and graceful shutdown

TBD (Days 5, 11–12). Will cover: bounded queue rejection (503) semantics,
per-request timeout without orphaned futures, and SIGTERM drain behavior.

## 7. Metrics and tracing

TBD (Day 13). Will cover: counter/histogram choices, latency decomposition
(queue wait vs. inference time), and correlation-ID threading.

## 8. Concurrency bugs found and fixed

TBD (Day 10). Summary here; full root-cause writeups in
[`concurrency_bugs.md`](concurrency_bugs.md).

## 9. Benchmark methodology

TBD (Day 4). Will restate the rigor requirements (trial counts, warm/cold
separation, load shape, environment logging) and link `results/`.

## 10. Limitations

TBD. Known now: CPU-only, single-process, local; no distributed
infrastructure by design in v0.1.

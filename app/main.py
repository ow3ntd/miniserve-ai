"""miniserve-ai FastAPI application."""

import os
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from app.admission import QueueFullError
from app.model_runner import ModelRunner
from app.scheduler import AsyncScheduler
from app.schemas import PredictRequest, PredictResponse
from app.request_registry import RequestDeadlineExceeded

APP_NAME = "miniserve-ai"
VERSION = "0.1.0.dev0"

# Process start time, used to report uptime from /health.
_STARTED_AT = time.monotonic()

DEFAULT_QUEUE_CAPACITY = 128
DEFAULT_MAX_BATCH_SIZE = 8
DEFAULT_MAX_WAIT_MS = 0.0
DEFAULT_REQUEST_TIMEOUT_S = 30.0

# Environment variable -> (create_app keyword, parser, default). Read only by
# the module-level `app` so benchmarks can configure `uvicorn app.main:app`
# without code changes; create_app() itself is unaffected.
_ENV_SETTINGS = (
    ("MINISERVE_QUEUE_CAPACITY", "queue_capacity", int, DEFAULT_QUEUE_CAPACITY),
    ("MINISERVE_MAX_BATCH_SIZE", "max_batch_size", int, DEFAULT_MAX_BATCH_SIZE),
    ("MINISERVE_MAX_WAIT_MS", "max_wait_ms", float, DEFAULT_MAX_WAIT_MS),
    (
        "MINISERVE_REQUEST_TIMEOUT_S",
        "request_timeout_s",
        float,
        DEFAULT_REQUEST_TIMEOUT_S,
    ),
)


def settings_from_env(
    environ: Mapping[str, str] | None = None,
) -> dict[str, int | float]:
    """Return create_app() keyword overrides read from MINISERVE_* vars.

    Unset variables fall back to create_app()'s defaults. A set variable
    that does not parse raises ValueError naming the variable, so a typo
    fails at startup instead of silently benchmarking the wrong config.
    """
    if environ is None:
        environ = os.environ
    settings: dict[str, int | float] = {}
    for name, keyword, parse, default in _ENV_SETTINGS:
        raw = environ.get(name)
        if raw is None:
            settings[keyword] = default
            continue
        try:
            settings[keyword] = parse(raw)
        except ValueError as exc:
            raise ValueError(
                f"{name}={raw!r} is not a valid {parse.__name__}"
            ) from exc
    return settings


def create_app(
    runner: ModelRunner | None = None,
    *,
    queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
    max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
    max_wait_ms: float = DEFAULT_MAX_WAIT_MS,
    request_timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
) -> FastAPI:
    """Application factory.

    Accepts an optional pre-built ModelRunner so tests can inject a
    custom-configured (or deliberately unloaded) runner. By default,
    builds one and loads it eagerly.

    The model remains eagerly loaded for compatibility. The scheduler task is
    started and stopped by FastAPI lifespan.
    """
    if runner is None:
        runner = ModelRunner()
        runner.load()

    scheduler = AsyncScheduler[list[float]](
        runner,
        queue_capacity=queue_capacity,
        max_batch_size=max_batch_size,
        max_wait_ms=max_wait_ms,
        request_timeout_s=request_timeout_s,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await scheduler.start()
        try:
            yield
        finally:
            await scheduler.stop()

    app = FastAPI(
        title=APP_NAME,
        version=VERSION,
        description=(
            "A concurrent request scheduler with a pluggable model backend. "
            "C++ bounded-queue core with asynchronous dynamic batching."
        ),
        lifespan=lifespan,
    )
    app.state.scheduler = scheduler

    @app.get("/health")
    async def health() -> dict:
        """Liveness probe. Returns service identity and uptime."""
        return {
            "status": "ok",
            "service": APP_NAME,
            "version": VERSION,
            "uptime_s": round(time.monotonic() - _STARTED_AT, 3),
        }

    @app.get("/stats")
    async def stats() -> dict:
        """Scheduler dispatch counters plus the effective configuration.

        Counters are monotonic for the process lifetime. mean_batch_size is
        requests_dispatched / batches_dispatched (0.0 before any dispatch);
        a load generator can difference two snapshots to get the realized
        mean batch size over an interval.
        """
        batches = scheduler.batches_dispatched
        requests = scheduler.requests_dispatched
        return {
            "batches_dispatched": batches,
            "requests_dispatched": requests,
            "mean_batch_size": (requests / batches) if batches else 0.0,
            "queue_capacity": scheduler.queue_capacity,
            "max_batch_size": scheduler.max_batch_size,
            "max_wait_ms": scheduler.max_wait_ms,
            "request_timeout_s": scheduler.request_timeout_s,
        }

    @app.post("/predict", response_model=PredictResponse)
    async def predict(request: PredictRequest) -> PredictResponse:
        """Admit one request and await only its corresponding result."""
        if not runner.is_loaded:
            # Server-side readiness problem, not a client error.
            raise HTTPException(status_code=503, detail="model not loaded")
        try:
            runner.validate_request(request.tokens)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        # FastAPI lifespan is the production lifecycle. Starting idempotently
        # here also supports lightweight TestClient usage without a context
        # manager, where Starlette does not enter lifespan.
        await scheduler.start()
        try:
            pending = scheduler.admit(request.tokens)
        except QueueFullError as exc:
            raise HTTPException(
                status_code=503,
                detail="request queue is full",
            ) from exc

        try:
            outputs = await scheduler.wait(pending)
        except RequestDeadlineExceeded as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except ValueError as exc:
            # Model-dependent validation the schema can't know about
            # (e.g. token ids outside the model's vocab size).
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return PredictResponse(outputs=outputs)

    return app


# Module-level app for `uvicorn app.main:app`. Configurable via MINISERVE_*
# environment variables; see settings_from_env().
app = create_app(**settings_from_env())

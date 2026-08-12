"""miniserve-ai FastAPI application."""

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from app.admission import QueueFullError
from app.model_runner import ModelRunner
from app.scheduler import AsyncScheduler
from app.schemas import PredictRequest, PredictResponse

APP_NAME = "miniserve-ai"
VERSION = "0.1.0.dev0"

# Process start time, used to report uptime from /health.
_STARTED_AT = time.monotonic()


def create_app(
    runner: ModelRunner | None = None,
    *,
    queue_capacity: int = 128,
    max_batch_size: int = 8,
    max_wait_ms: float = 0,
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
        except ValueError as exc:
            # Model-dependent validation the schema can't know about
            # (e.g. token ids outside the model's vocab size).
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return PredictResponse(outputs=outputs)

    return app


# Module-level app for `uvicorn app.main:app`.
app = create_app()

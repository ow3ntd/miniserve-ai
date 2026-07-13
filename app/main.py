"""miniserve-ai -- a concurrent request scheduler with a pluggable model backend.

Day 3 scope: application factory, health endpoint, and a synchronous
/predict endpoint wired directly to the ModelRunner. Every request is a
batch of one, executed inline -- no queueing or batching exists yet.
This is deliberately the "before" picture the Day 4 baseline benchmark
will measure.
"""

import time

from fastapi import FastAPI, HTTPException

from app.model_runner import ModelRunner
from app.schemas import PredictRequest, PredictResponse

APP_NAME = "miniserve-ai"
VERSION = "0.1.0.dev0"

# Process start time, used to report uptime from /health.
_STARTED_AT = time.monotonic()


def create_app(runner: ModelRunner | None = None) -> FastAPI:
    """Application factory.

    Accepts an optional pre-built ModelRunner so tests can inject a
    custom-configured (or deliberately unloaded) runner. By default,
    builds one and loads it eagerly.

    Eager load in the factory -- rather than a FastAPI lifespan hook --
    keeps the factory self-contained and lets tests use TestClient
    without a context manager. When the scheduler lands (Day 6) and real
    startup/shutdown ordering matters, this moves to a lifespan context;
    that tradeoff is recorded in the devlog.
    """
    if runner is None:
        runner = ModelRunner()
        runner.load()

    app = FastAPI(
        title=APP_NAME,
        version=VERSION,
        description=(
            "A concurrent request scheduler with a pluggable model backend. "
            "C++ bounded-queue core (planned), async dynamic batching (planned)."
        ),
    )

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
    def predict(request: PredictRequest) -> PredictResponse:
        """Synchronous single-request prediction (a batch of one).

        Declared `def`, not `async def`, on purpose: FastAPI runs sync
        handlers in its threadpool, so a slow model call doesn't stall
        the event loop -- and the endpoint makes no async promises it
        can't keep. The scheduler replaces this path entirely later.
        """
        if not runner.is_loaded:
            # Server-side readiness problem, not a client error.
            raise HTTPException(status_code=503, detail="model not loaded")
        try:
            outputs = runner.predict([request.tokens])
        except ValueError as exc:
            # Model-dependent validation the schema can't know about
            # (e.g. token ids outside the model's vocab size).
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return PredictResponse(outputs=outputs[0])

    return app


# Module-level app for `uvicorn app.main:app`.
app = create_app()

"""miniserve-ai -- a concurrent request scheduler with a pluggable model backend.

Day 1 scope: application factory + health endpoint only.
No prediction, scheduling, or batching logic exists yet.
"""

import time

from fastapi import FastAPI

APP_NAME = "miniserve-ai"
VERSION = "0.1.0.dev0"

# Process start time, used to report uptime from /health.
_STARTED_AT = time.monotonic()


def create_app() -> FastAPI:
    """Application factory.

    Using a factory (rather than a module-level singleton) so tests can
    construct isolated app instances, and so later config injection
    (queue sizes, batch parameters) has an obvious seam.
    """
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

    return app


# Module-level app for `uvicorn app.main:app`.
app = create_app()

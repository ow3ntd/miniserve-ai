"""Request admission between Python-owned state and the native queue.

Admission is a small transaction:

1. Register Python-owned request state.
2. Attempt one non-blocking enqueue of its opaque request ID.
3. Keep the registry entry on success.
4. Roll it back on queue rejection or enqueue failure.

HTTP status handling remains outside this module.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Generic, Protocol, TypeVar

from app.request_registry import PendingRequest, RequestRegistry


ResultT = TypeVar("ResultT")


class RequestIdQueue(Protocol):
    """Structural interface required from the native bounded queue."""

    def try_push(self, request_id: int, /) -> bool:
        """Attempt to enqueue one request ID without blocking."""


class QueueFullError(RuntimeError):
    """Raised when the bounded native queue rejects admission."""


class AdmissionController(Generic[ResultT]):
    """Coordinates request registration with bounded queue admission."""

    def __init__(
        self,
        queue: RequestIdQueue,
        *,
        registry: RequestRegistry[ResultT] | None = None,
    ) -> None:
        self._queue = queue
        self._registry = (
            registry if registry is not None else RequestRegistry()
        )

    @property
    def registry(self) -> RequestRegistry[ResultT]:
        """Return the Python-owned request registry."""

        return self._registry

    def admit(
        self,
        tokens: Sequence[int],
        *,
        timeout_s: float,
    ) -> PendingRequest[ResultT]:
        """Register and enqueue a request, or roll it back completely."""

        request = self._registry.register(
            tokens,
            timeout_s=timeout_s,
        )

        try:
            accepted = self._queue.try_push(request.request_id)
        except Exception:
            self._rollback(request.request_id)
            raise

        if not accepted:
            self._rollback(request.request_id)
            raise QueueFullError("request queue is full")

        return request

    def _rollback(self, request_id: int) -> None:
        """Remove a request that did not complete admission."""

        if not self._registry.cancel(request_id):
            raise RuntimeError(
                "admission rollback failed for "
                f"request ID {request_id}"
            )

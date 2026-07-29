"""One-shot fixed-size batch execution for the inference scheduler.

The worker connects three existing components:

1. Pop opaque request IDs from the bounded native queue.
2. Recover Python-owned payloads from RequestRegistry.
3. Execute one model batch and route each output to its matching future.

This milestone deliberately implements one synchronous `process_once()` call.
A continuous background loop, dynamic wait window, deadlines, and shutdown
behavior are separate milestones.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from app.request_registry import PendingRequest, RequestRegistry


ResultT = TypeVar("ResultT")


class RequestIdBatchQueue(Protocol):
    """Queue interface required by the fixed-size worker."""

    def pop_batch(self, max_items: int, /) -> list[int]:
        """Pop up to max_items request IDs in FIFO order."""


class BatchModel(Protocol[ResultT]):
    """Model interface required by the fixed-size worker."""

    def predict(
        self,
        batch: Sequence[Sequence[int]],
        /,
    ) -> Sequence[ResultT]:
        """Return one result per input request, in input order."""


class BatchResultCountError(RuntimeError):
    """Raised when a model violates the one-output-per-request contract."""


@dataclass(frozen=True, slots=True)
class BatchOutcome:
    """Summary of one non-blocking worker iteration."""

    dequeued: int
    stale: int
    completed: int
    failed: int


class FixedBatchWorker(Generic[ResultT]):
    """Executes at most one fixed-size model batch per call."""

    def __init__(
        self,
        queue: RequestIdBatchQueue,
        registry: RequestRegistry[ResultT],
        model: BatchModel[ResultT],
        *,
        max_batch_size: int,
    ) -> None:
        if (
            not isinstance(max_batch_size, int)
            or isinstance(max_batch_size, bool)
            or max_batch_size <= 0
        ):
            raise ValueError("max_batch_size must be a positive integer")

        self._queue = queue
        self._registry = registry
        self._model = model
        self._max_batch_size = max_batch_size

    @property
    def max_batch_size(self) -> int:
        """Return the configured maximum number of requests per batch."""

        return self._max_batch_size

    def process_once(self) -> BatchOutcome:
        """Process at most one immediately available batch.

        Empty queues return without invoking the model. IDs whose registry
        records have already been removed are stale and are skipped safely.

        Model errors are propagated to every live request future rather than
        raised from this method. This keeps one failed batch from terminating
        the future background worker loop.
        """

        request_ids = self._queue.pop_batch(self._max_batch_size)

        if len(request_ids) > self._max_batch_size:
            raise RuntimeError(
                "queue returned more request IDs than requested"
            )

        if not request_ids:
            return BatchOutcome(
                dequeued=0,
                stale=0,
                completed=0,
                failed=0,
            )

        live_requests: list[PendingRequest[ResultT]] = []
        stale_count = 0

        for request_id in request_ids:
            request = self._registry.get(request_id)

            if request is None:
                stale_count += 1
                continue

            live_requests.append(request)

        if not live_requests:
            return BatchOutcome(
                dequeued=len(request_ids),
                stale=stale_count,
                completed=0,
                failed=0,
            )

        batch = [request.tokens for request in live_requests]

        try:
            outputs = list(self._model.predict(batch))

            if len(outputs) != len(live_requests):
                raise BatchResultCountError(
                    "model returned "
                    f"{len(outputs)} outputs for "
                    f"{len(live_requests)} requests"
                )
        except Exception as error:
            failed_count = sum(
                self._registry.fail(request.request_id, error)
                for request in live_requests
            )

            return BatchOutcome(
                dequeued=len(request_ids),
                stale=stale_count,
                completed=0,
                failed=failed_count,
            )

        completed_count = sum(
            self._registry.resolve(request.request_id, output)
            for request, output in zip(
                live_requests,
                outputs,
                strict=True,
            )
        )

        return BatchOutcome(
            dequeued=len(request_ids),
            stale=stale_count,
            completed=completed_count,
            failed=0,
        )

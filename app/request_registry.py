"""Async request ownership for the inference scheduler.

Python owns request payloads and asyncio futures. The native C++ queue stores
only opaque unsigned 64-bit request IDs.

RequestRegistry is intended to be mutated only from one asyncio event-loop
thread. It does not provide cross-thread synchronization.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar


ResultT = TypeVar("ResultT")

UINT64_MAX = (1 << 64) - 1


@dataclass(frozen=True, slots=True)
class PendingRequest(Generic[ResultT]):
    """State owned by Python for one admitted inference request."""

    request_id: int
    tokens: tuple[int, ...]
    future: asyncio.Future[ResultT]
    arrival_time: float
    deadline: float


class RequestRegistry(Generic[ResultT]):
    """Maps native-compatible request IDs to Python-owned request state."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self._next_request_id = 1
        self._requests: dict[int, PendingRequest[ResultT]] = {}

    def register(
        self,
        tokens: Sequence[int],
        *,
        timeout_s: float,
    ) -> PendingRequest[ResultT]:
        """Create and store a pending request.

        The caller must invoke this method while an asyncio event loop is
        running because the request future belongs to that loop.
        """

        if not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError(
                "timeout_s must be finite and greater than zero"
            )

        if self._next_request_id > UINT64_MAX:
            raise OverflowError("request ID space exhausted")

        # Perform fallible setup before mutating ID allocation state.
        loop = asyncio.get_running_loop()
        immutable_tokens = tuple(tokens)
        arrival_time = self._clock()
        request_id = self._next_request_id

        request = PendingRequest(
            request_id=request_id,
            tokens=immutable_tokens,
            future=loop.create_future(),
            arrival_time=arrival_time,
            deadline=arrival_time + timeout_s,
        )

        self._requests[request_id] = request
        self._next_request_id += 1
        return request

    def get(self, request_id: int) -> PendingRequest[ResultT] | None:
        """Return a pending request without removing it."""

        return self._requests.get(request_id)

    def resolve(self, request_id: int, result: ResultT) -> bool:
        """Resolve and remove a request exactly once.

        Returns False when the request is unknown or its future was already
        completed by another lifecycle path.
        """

        request = self._requests.pop(request_id, None)
        if request is None or request.future.done():
            return False

        request.future.set_result(result)
        return True

    def fail(self, request_id: int, error: BaseException) -> bool:
        """Fail and remove a request exactly once."""

        request = self._requests.pop(request_id, None)
        if request is None or request.future.done():
            return False

        request.future.set_exception(error)
        return True

    def cancel(self, request_id: int) -> bool:
        """Cancel and remove a request exactly once."""

        request = self._requests.pop(request_id, None)
        if request is None or request.future.done():
            return False

        request.future.cancel()
        return True

    def __len__(self) -> int:
        return len(self._requests)

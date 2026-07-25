"""Tests for the registry-to-native-queue admission transaction."""

import asyncio
from collections.abc import Sequence

import pytest

import miniserve_core
from app.admission import AdmissionController, QueueFullError
from app.request_registry import PendingRequest, RequestRegistry


def run(coro):
    """Run one async test scenario without requiring pytest-asyncio."""

    return asyncio.run(coro)


class StubQueue:
    """Controlled non-blocking queue used to test admission outcomes."""

    def __init__(
        self,
        outcomes: Sequence[bool | BaseException],
    ) -> None:
        self._outcomes = iter(outcomes)
        self.request_ids: list[int] = []

    def try_push(self, request_id: int, /) -> bool:
        self.request_ids.append(request_id)
        outcome = next(self._outcomes)

        if isinstance(outcome, BaseException):
            raise outcome

        return outcome


class CapturingRegistry(RequestRegistry[str]):
    """Registry that exposes the most recently created request for tests."""

    def __init__(self) -> None:
        super().__init__()
        self.last_request: PendingRequest[str] | None = None

    def register(
        self,
        tokens: Sequence[int],
        *,
        timeout_s: float,
    ) -> PendingRequest[str]:
        request = super().register(tokens, timeout_s=timeout_s)
        self.last_request = request
        return request


def test_successful_admission_keeps_registry_entry_and_enqueues_id():
    async def scenario():
        queue = StubQueue([True])
        registry = RequestRegistry[str](clock=lambda: 10.0)
        controller = AdmissionController[str](
            queue,
            registry=registry,
        )

        request = controller.admit([10, 20], timeout_s=2.5)

        assert request.request_id == 1
        assert request.tokens == (10, 20)
        assert request.arrival_time == pytest.approx(10.0)
        assert request.deadline == pytest.approx(12.5)
        assert queue.request_ids == [1]
        assert registry.get(1) is request
        assert not request.future.done()
        assert len(registry) == 1

        assert registry.cancel(request.request_id) is True

    run(scenario())


def test_queue_full_rolls_back_registry_and_cancels_future():
    async def scenario():
        queue = StubQueue([False])
        registry = CapturingRegistry()
        controller = AdmissionController[str](
            queue,
            registry=registry,
        )

        with pytest.raises(
            QueueFullError,
            match="request queue is full",
        ):
            controller.admit([1, 2, 3], timeout_s=1.0)

        assert queue.request_ids == [1]
        assert registry.last_request is not None
        assert registry.last_request.future.cancelled()
        assert registry.get(1) is None
        assert len(registry) == 0

    run(scenario())


def test_native_enqueue_exception_rolls_back_and_is_preserved():
    async def scenario():
        error = RuntimeError("native enqueue failed")
        queue = StubQueue([error])
        registry = CapturingRegistry()
        controller = AdmissionController[str](
            queue,
            registry=registry,
        )

        with pytest.raises(RuntimeError) as caught:
            controller.admit([1], timeout_s=1.0)

        assert caught.value is error
        assert queue.request_ids == [1]
        assert registry.last_request is not None
        assert registry.last_request.future.cancelled()
        assert len(registry) == 0

    run(scenario())


def test_invalid_timeout_never_reaches_queue():
    async def scenario():
        queue = StubQueue([True])
        controller = AdmissionController[str](queue)

        with pytest.raises(
            ValueError,
            match="finite and greater than zero",
        ):
            controller.admit([1], timeout_s=0.0)

        assert queue.request_ids == []
        assert len(controller.registry) == 0

    run(scenario())


def test_rejected_request_id_is_not_reused():
    async def scenario():
        queue = StubQueue([False, True])
        controller = AdmissionController[str](queue)

        with pytest.raises(QueueFullError):
            controller.admit([1], timeout_s=1.0)

        accepted = controller.admit([2], timeout_s=1.0)

        assert queue.request_ids == [1, 2]
        assert accepted.request_id == 2
        assert len(controller.registry) == 1

        assert controller.registry.cancel(accepted.request_id) is True

    run(scenario())


def test_real_native_queue_preserves_admission_fifo_order():
    async def scenario():
        queue = miniserve_core.BoundedQueue(2)
        controller = AdmissionController[str](queue)

        first = controller.admit([10], timeout_s=1.0)
        second = controller.admit([20], timeout_s=1.0)

        assert queue.full is True
        assert queue.pop_batch(2) == [
            first.request_id,
            second.request_id,
        ]

        # Dequeueing IDs does not complete Python-owned requests.
        assert len(controller.registry) == 2
        assert not first.future.done()
        assert not second.future.done()

        assert controller.registry.resolve(
            first.request_id,
            "first result",
        )
        assert controller.registry.resolve(
            second.request_id,
            "second result",
        )

        assert await first.future == "first result"
        assert await second.future == "second result"
        assert len(controller.registry) == 0

    run(scenario())


def test_real_native_queue_full_rejects_without_leaking_registry_state():
    async def scenario():
        queue = miniserve_core.BoundedQueue(1)
        controller = AdmissionController[str](queue)

        accepted = controller.admit([1], timeout_s=1.0)

        with pytest.raises(QueueFullError):
            controller.admit([2], timeout_s=1.0)

        assert queue.pop_batch(1) == [accepted.request_id]
        assert controller.registry.get(accepted.request_id) is accepted
        assert len(controller.registry) == 1

        assert controller.registry.cancel(accepted.request_id) is True

    run(scenario())

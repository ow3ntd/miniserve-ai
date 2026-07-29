"""Tests for one-shot fixed-size scheduler batch execution."""

import asyncio
from collections.abc import Sequence

import pytest

import miniserve_core
from app.batch_worker import (
    BatchOutcome,
    BatchResultCountError,
    FixedBatchWorker,
)
from app.request_registry import RequestRegistry


def run(coro):
    """Run one asynchronous test scenario without pytest-asyncio."""

    return asyncio.run(coro)


class RecordingModel:
    """Model stub that records batches and returns configured outputs."""

    def __init__(
        self,
        *,
        outputs: Sequence[str] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.outputs = outputs
        self.error = error
        self.calls: list[list[tuple[int, ...]]] = []

    def predict(
        self,
        batch: Sequence[Sequence[int]],
        /,
    ) -> Sequence[str]:
        copied_batch = [tuple(tokens) for tokens in batch]
        self.calls.append(copied_batch)

        if self.error is not None:
            raise self.error

        if self.outputs is not None:
            return list(self.outputs)

        return [
            f"result-{tokens[0]}"
            for tokens in copied_batch
        ]


class StubBatchQueue:
    """Controlled queue for testing malformed or stale dequeue results."""

    def __init__(self, batches: Sequence[Sequence[int]]) -> None:
        self._batches = iter(batches)
        self.requested_sizes: list[int] = []

    def pop_batch(self, max_items: int, /) -> list[int]:
        self.requested_sizes.append(max_items)
        return list(next(self._batches, []))


@pytest.mark.parametrize(
    "max_batch_size",
    [0, -1, 1.5, True],
)
def test_worker_rejects_invalid_batch_size(max_batch_size):
    queue = StubBatchQueue([])
    registry = RequestRegistry[str]()
    model = RecordingModel()

    with pytest.raises(
        ValueError,
        match="positive integer",
    ):
        FixedBatchWorker(
            queue,
            registry,
            model,
            max_batch_size=max_batch_size,
        )


def test_empty_queue_does_not_invoke_model():
    queue = StubBatchQueue([[]])
    registry = RequestRegistry[str]()
    model = RecordingModel()
    worker = FixedBatchWorker(
        queue,
        registry,
        model,
        max_batch_size=4,
    )

    outcome = worker.process_once()

    assert outcome == BatchOutcome(
        dequeued=0,
        stale=0,
        completed=0,
        failed=0,
    )
    assert queue.requested_sizes == [4]
    assert model.calls == []


def test_queue_cannot_return_more_ids_than_requested():
    queue = StubBatchQueue([[1, 2]])
    registry = RequestRegistry[str]()
    model = RecordingModel()
    worker = FixedBatchWorker(
        queue,
        registry,
        model,
        max_batch_size=1,
    )

    with pytest.raises(
        RuntimeError,
        match="more request IDs than requested",
    ):
        worker.process_once()

    assert model.calls == []


def test_real_queue_routes_fifo_results_to_correct_futures():
    async def scenario():
        queue = miniserve_core.BoundedQueue(3)
        registry = RequestRegistry[str]()
        model = RecordingModel()
        worker = FixedBatchWorker(
            queue,
            registry,
            model,
            max_batch_size=2,
        )

        first = registry.register([10], timeout_s=10.0)
        second = registry.register([20], timeout_s=10.0)
        third = registry.register([30], timeout_s=10.0)

        assert queue.try_push(first.request_id)
        assert queue.try_push(second.request_id)
        assert queue.try_push(third.request_id)

        first_outcome = worker.process_once()

        assert first_outcome == BatchOutcome(
            dequeued=2,
            stale=0,
            completed=2,
            failed=0,
        )
        assert model.calls == [[(10,), (20,)]]
        assert await first.future == "result-10"
        assert await second.future == "result-20"
        assert not third.future.done()
        assert queue.size == 1
        assert len(registry) == 1

        second_outcome = worker.process_once()

        assert second_outcome == BatchOutcome(
            dequeued=1,
            stale=0,
            completed=1,
            failed=0,
        )
        assert model.calls == [
            [(10,), (20,)],
            [(30,)],
        ]
        assert await third.future == "result-30"
        assert queue.empty is True
        assert len(registry) == 0

    run(scenario())


def test_stale_ids_are_skipped_without_disturbing_live_requests():
    async def scenario():
        queue = miniserve_core.BoundedQueue(2)
        registry = RequestRegistry[str]()
        model = RecordingModel()
        worker = FixedBatchWorker(
            queue,
            registry,
            model,
            max_batch_size=2,
        )

        live = registry.register([7, 8], timeout_s=10.0)

        assert queue.try_push(999)
        assert queue.try_push(live.request_id)

        outcome = worker.process_once()

        assert outcome == BatchOutcome(
            dequeued=2,
            stale=1,
            completed=1,
            failed=0,
        )
        assert model.calls == [[(7, 8)]]
        assert await live.future == "result-7"
        assert len(registry) == 0

    run(scenario())


def test_all_stale_ids_skip_model_execution():
    queue = StubBatchQueue([[100, 200]])
    registry = RequestRegistry[str]()
    model = RecordingModel()
    worker = FixedBatchWorker(
        queue,
        registry,
        model,
        max_batch_size=2,
    )

    outcome = worker.process_once()

    assert outcome == BatchOutcome(
        dequeued=2,
        stale=2,
        completed=0,
        failed=0,
    )
    assert model.calls == []


def test_model_exception_fails_every_live_request():
    async def scenario():
        queue = miniserve_core.BoundedQueue(2)
        registry = RequestRegistry[str]()
        error = RuntimeError("model execution failed")
        model = RecordingModel(error=error)
        worker = FixedBatchWorker(
            queue,
            registry,
            model,
            max_batch_size=2,
        )

        first = registry.register([1], timeout_s=10.0)
        second = registry.register([2], timeout_s=10.0)

        assert queue.try_push(first.request_id)
        assert queue.try_push(second.request_id)

        outcome = worker.process_once()

        assert outcome == BatchOutcome(
            dequeued=2,
            stale=0,
            completed=0,
            failed=2,
        )
        assert first.future.exception() is error
        assert second.future.exception() is error
        assert len(registry) == 0

    run(scenario())


def test_wrong_model_output_count_fails_entire_batch():
    async def scenario():
        queue = miniserve_core.BoundedQueue(2)
        registry = RequestRegistry[str]()
        model = RecordingModel(outputs=["only-one-output"])
        worker = FixedBatchWorker(
            queue,
            registry,
            model,
            max_batch_size=2,
        )

        first = registry.register([1], timeout_s=10.0)
        second = registry.register([2], timeout_s=10.0)

        assert queue.try_push(first.request_id)
        assert queue.try_push(second.request_id)

        outcome = worker.process_once()

        assert outcome == BatchOutcome(
            dequeued=2,
            stale=0,
            completed=0,
            failed=2,
        )

        first_error = first.future.exception()
        second_error = second.future.exception()

        assert isinstance(first_error, BatchResultCountError)
        assert isinstance(second_error, BatchResultCountError)
        assert "1 outputs for 2 requests" in str(first_error)
        assert "1 outputs for 2 requests" in str(second_error)
        assert len(registry) == 0

    run(scenario())


def test_late_result_does_not_complete_request_twice():
    async def scenario():
        queue = miniserve_core.BoundedQueue(2)
        registry = RequestRegistry[str]()

        first = registry.register([1], timeout_s=10.0)
        second = registry.register([2], timeout_s=10.0)

        assert queue.try_push(first.request_id)
        assert queue.try_push(second.request_id)

        class RacingModel:
            def predict(
                self,
                batch: Sequence[Sequence[int]],
                /,
            ) -> Sequence[str]:
                assert registry.resolve(
                    first.request_id,
                    "completed-before-routing",
                )
                return ["late-first", "normal-second"]

        worker = FixedBatchWorker(
            queue,
            registry,
            RacingModel(),
            max_batch_size=2,
        )

        outcome = worker.process_once()

        assert outcome == BatchOutcome(
            dequeued=2,
            stale=0,
            completed=1,
            failed=0,
        )
        assert await first.future == "completed-before-routing"
        assert await second.future == "normal-second"
        assert len(registry) == 0

    run(scenario())

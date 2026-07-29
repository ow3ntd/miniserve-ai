"""Integration test for admission, native queue, worker, and real model."""

import asyncio

import pytest

import miniserve_core
from app.admission import AdmissionController
from app.batch_worker import BatchOutcome, FixedBatchWorker
from app.model_runner import ModelConfig, ModelRunner
from app.request_registry import RequestRegistry


def run(coro):
    """Run one asynchronous test scenario without pytest-asyncio."""

    return asyncio.run(coro)


def test_real_scheduler_components_execute_and_route_fixed_batches():
    async def scenario():
        queue = miniserve_core.BoundedQueue(3)
        registry = RequestRegistry[list[float]]()

        model = ModelRunner(
            ModelConfig(
                vocab_size=128,
                embed_dim=8,
                n_outputs=4,
                seed=20260725,
            )
        )
        model.load()

        admission = AdmissionController[list[float]](
            queue,
            registry=registry,
        )
        worker = FixedBatchWorker[list[float]](
            queue,
            registry,
            model,
            max_batch_size=2,
        )

        token_batches = [
            [1, 2, 3],
            [4],
            [5, 6, 7, 8],
        ]

        # Compute the deterministic reference results directly from the same
        # loaded model. The worker must route these rows to matching futures.
        expected = model.predict(token_batches)

        first = admission.admit(
            token_batches[0],
            timeout_s=10.0,
        )
        second = admission.admit(
            token_batches[1],
            timeout_s=10.0,
        )
        third = admission.admit(
            token_batches[2],
            timeout_s=10.0,
        )

        assert queue.size == 3
        assert len(registry) == 3

        first_outcome = worker.process_once()

        assert first_outcome == BatchOutcome(
            dequeued=2,
            stale=0,
            completed=2,
            failed=0,
        )

        first_result = await first.future
        second_result = await second.future

        assert first_result == pytest.approx(expected[0])
        assert second_result == pytest.approx(expected[1])
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

        third_result = await third.future

        assert third_result == pytest.approx(expected[2])
        assert queue.empty is True
        assert len(registry) == 0

        empty_outcome = worker.process_once()

        assert empty_outcome == BatchOutcome(
            dequeued=0,
            stale=0,
            completed=0,
            failed=0,
        )

    run(scenario())

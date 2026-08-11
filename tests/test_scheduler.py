"""Focused tests for the asynchronous fixed-batch scheduler."""

import asyncio
import threading
import time
from collections.abc import Sequence

import pytest

from app.scheduler import AsyncScheduler


def run(coro):
    return asyncio.run(coro)


class TokenEchoModel:
    def predict(
        self,
        batch: Sequence[Sequence[int]],
        /,
    ) -> list[str]:
        return [f"result-{tokens[0]}" for tokens in batch]


def test_start_and_stop_are_deterministic_and_idempotent():
    async def scenario():
        scheduler = AsyncScheduler(TokenEchoModel())

        assert not scheduler.is_running
        await scheduler.start()
        assert scheduler.is_running

        await scheduler.start()
        assert scheduler.is_running

        await scheduler.stop()
        assert not scheduler.is_running

        await scheduler.stop()
        with pytest.raises(RuntimeError, match="cannot be restarted"):
            await scheduler.start()

    run(scenario())


def test_concurrent_requests_receive_their_individual_fifo_outputs():
    async def scenario():
        scheduler = AsyncScheduler(
            TokenEchoModel(),
            queue_capacity=4,
            max_batch_size=4,
        )
        await scheduler.start()
        try:
            requests = [scheduler.admit([token]) for token in (10, 20, 30, 40)]
            results = await asyncio.gather(
                *(request.future for request in requests)
            )
        finally:
            await scheduler.stop()

        assert results == [
            "result-10",
            "result-20",
            "result-30",
            "result-40",
        ]
        assert len(scheduler.registry) == 0

    run(scenario())


def test_slow_inference_does_not_block_event_loop():
    class SlowModel(TokenEchoModel):
        def predict(self, batch, /):
            time.sleep(0.15)
            return super().predict(batch)

    async def scenario():
        scheduler = AsyncScheduler(SlowModel())
        await scheduler.start()
        try:
            request = scheduler.admit([7])
            ticked = False

            async def tick():
                nonlocal ticked
                await asyncio.sleep(0.02)
                ticked = True

            tick_task = asyncio.create_task(tick())
            await asyncio.sleep(0.05)
            assert ticked
            assert not request.future.done()
            assert await request.future == "result-7"
            await tick_task
        finally:
            await scheduler.stop()

    run(scenario())


def test_model_calls_never_overlap():
    class OverlapDetectingModel(TokenEchoModel):
        def __init__(self):
            self._lock = threading.Lock()
            self.active = 0
            self.max_active = 0

        def predict(self, batch, /):
            with self._lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            try:
                time.sleep(0.02)
                return super().predict(batch)
            finally:
                with self._lock:
                    self.active -= 1

    async def scenario():
        model = OverlapDetectingModel()
        scheduler = AsyncScheduler(
            model,
            queue_capacity=6,
            max_batch_size=1,
        )
        await scheduler.start()
        try:
            requests = [scheduler.admit([token]) for token in range(6)]
            await asyncio.gather(*(request.future for request in requests))
        finally:
            await scheduler.stop()

        assert model.max_active == 1

    run(scenario())


def test_model_exception_fails_all_futures_and_clears_registry():
    error = RuntimeError("inference exploded")

    class FailingModel:
        def predict(self, batch, /):
            raise error

    async def scenario():
        scheduler = AsyncScheduler(
            FailingModel(),
            queue_capacity=3,
            max_batch_size=3,
        )
        await scheduler.start()
        try:
            requests = [scheduler.admit([token]) for token in (1, 2, 3)]
            results = await asyncio.gather(
                *(request.future for request in requests),
                return_exceptions=True,
            )
        finally:
            await scheduler.stop()

        assert results == [error, error, error]
        assert all(request.future.done() for request in requests)
        assert len(scheduler.registry) == 0

    run(scenario())


def test_stop_during_inference_is_terminal_and_prevents_overlap():
    class BlockingModel(TokenEchoModel):
        def __init__(self):
            self.entered = threading.Event()
            self.release = threading.Event()
            self.finished = threading.Event()
            self.active = 0
            self.max_active = 0

        def predict(self, batch, /):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.entered.set()
            try:
                assert self.release.wait(timeout=2.0)
                return super().predict(batch)
            finally:
                self.active -= 1
                self.finished.set()

    async def scenario():
        model = BlockingModel()
        scheduler = AsyncScheduler(model)
        await scheduler.start()
        request = scheduler.admit([1])

        assert await asyncio.to_thread(model.entered.wait, 2.0)
        await scheduler.stop()

        assert request.future.cancelled()
        assert len(scheduler.registry) == 0
        with pytest.raises(RuntimeError, match="cannot be restarted"):
            await scheduler.start()
        assert model.active == 1
        assert model.max_active == 1

        model.release.set()
        assert await asyncio.to_thread(model.finished.wait, 2.0)
        assert model.max_active == 1

    run(scenario())


def test_cancelled_waiter_cleans_registry_without_corrupting_future():
    class BlockingModel(TokenEchoModel):
        def __init__(self):
            self.entered = threading.Event()
            self.release = threading.Event()

        def predict(self, batch, /):
            self.entered.set()
            assert self.release.wait(timeout=2.0)
            return super().predict(batch)

    async def scenario():
        model = BlockingModel()
        scheduler = AsyncScheduler(model)
        await scheduler.start()
        request = scheduler.admit([9])
        waiter = asyncio.create_task(scheduler.wait(request))

        assert await asyncio.to_thread(model.entered.wait, 2.0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        assert request.future.cancelled()
        assert len(scheduler.registry) == 0

        model.release.set()
        await asyncio.sleep(0.05)
        assert len(scheduler.registry) == 0
        await scheduler.stop()

    run(scenario())


def test_admission_during_drain_does_not_lose_wakeup():
    async def scenario():
        loop = asyncio.get_running_loop()
        admitted = asyncio.Event()
        second_request = []
        scheduler = None

        class AdmitDuringInferenceModel(TokenEchoModel):
            def predict(self, batch, /):
                if batch[0][0] == 1:
                    def admit_second():
                        assert scheduler is not None
                        second_request.append(scheduler.admit([2]))
                        admitted.set()

                    loop.call_soon_threadsafe(admit_second)
                return super().predict(batch)

        scheduler = AsyncScheduler(
            AdmitDuringInferenceModel(),
            queue_capacity=2,
            max_batch_size=1,
        )
        await scheduler.start()
        try:
            first = scheduler.admit([1])
            await admitted.wait()
            assert await scheduler.wait(first) == "result-1"
            assert await asyncio.wait_for(
                scheduler.wait(second_request[0]),
                timeout=1.0,
            ) == "result-2"
        finally:
            await scheduler.stop()

        assert len(scheduler.registry) == 0

    run(scenario())

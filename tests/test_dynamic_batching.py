"""Focused tests for Day 6 dynamic batch formation."""

import asyncio
import math
import threading
import time
from collections.abc import Sequence

import pytest

from app.scheduler import AsyncScheduler


def run(coro):
    return asyncio.run(coro)


class RecordingModel:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.called = asyncio.Event()
        self.calls: list[list[tuple[int, ...]]] = []
        self.call_times: list[float] = []

    def predict(
        self,
        batch: Sequence[Sequence[int]],
        /,
    ) -> list[str]:
        copied = [tuple(tokens) for tokens in batch]
        self.calls.append(copied)
        self.call_times.append(time.monotonic())
        self._loop.call_soon_threadsafe(self.called.set)
        return [f"result-{tokens[0]}" for tokens in copied]


def test_zero_wait_dispatches_immediately():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=4, max_wait_ms=0)
        await scheduler.start()
        try:
            request = scheduler.admit([1])
            assert await asyncio.wait_for(scheduler.wait(request), 0.5) == "result-1"
            assert model.calls == [[(1,)]]
        finally:
            await scheduler.stop()

    run(scenario())


def test_partial_batch_waits_for_window_then_executes():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=3, max_wait_ms=80)
        await scheduler.start()
        try:
            request = scheduler.admit([1])
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(model.called.wait(), timeout=0.02)
            assert await asyncio.wait_for(scheduler.wait(request), 0.5) == "result-1"
            assert model.calls == [[(1,)]]
        finally:
            await scheduler.stop()

    run(scenario())


def test_full_batch_dispatches_before_long_window_expires():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=3, max_wait_ms=500)
        await scheduler.start()
        try:
            requests = [scheduler.admit([token]) for token in (1, 2, 3)]
            results = await asyncio.wait_for(
                asyncio.gather(*(scheduler.wait(request) for request in requests)),
                timeout=0.3,
            )
            assert results == ["result-1", "result-2", "result-3"]
            assert model.calls == [[(1,), (2,), (3,)]]
        finally:
            await scheduler.stop()

    run(scenario())


def test_later_arrival_does_not_reset_original_deadline():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=3, max_wait_ms=200)
        await scheduler.start()
        try:
            first = scheduler.admit([1])
            await asyncio.sleep(0.14)
            second = scheduler.admit([2])

            results = await asyncio.wait_for(
                asyncio.gather(scheduler.wait(first), scheduler.wait(second)),
                timeout=0.13,
            )
            assert results == ["result-1", "result-2"]
            assert model.calls == [[(1,), (2,)]]
        finally:
            await scheduler.stop()

    run(scenario())


def test_arrivals_during_window_join_same_batch_in_fifo_order():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=3, max_wait_ms=150)
        await scheduler.start()
        try:
            first = scheduler.admit([10])
            await asyncio.sleep(0.02)
            second = scheduler.admit([20])
            await asyncio.sleep(0.02)
            third = scheduler.admit([30])
            results = await asyncio.gather(
                scheduler.wait(first),
                scheduler.wait(second),
                scheduler.wait(third),
            )
            assert results == ["result-10", "result-20", "result-30"]
            assert model.calls == [[(10,), (20,), (30,)]]
        finally:
            await scheduler.stop()

    run(scenario())


def test_more_than_capacity_forms_multiple_batches():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=2, max_wait_ms=30)
        await scheduler.start()
        try:
            requests = [scheduler.admit([token]) for token in range(1, 6)]
            results = await asyncio.gather(
                *(scheduler.wait(request) for request in requests)
            )
            assert results == [f"result-{token}" for token in range(1, 6)]
            assert model.calls == [
                [(1,), (2,)],
                [(3,), (4,)],
                [(5,)],
            ]
        finally:
            await scheduler.stop()

    run(scenario())


def test_cancelled_id_does_not_count_toward_batch_capacity():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=2, max_wait_ms=60)
        await scheduler.start()
        try:
            cancelled = scheduler.admit([1])
            assert scheduler.registry.cancel(cancelled.request_id)
            live = scheduler.admit([2])
            assert await scheduler.wait(live) == "result-2"
            assert model.calls == [[(2,)]]
        finally:
            await scheduler.stop()

    run(scenario())


def test_stale_id_does_not_prevent_later_live_request_filling_batch():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=2, max_wait_ms=300)
        await scheduler.start()
        try:
            cancelled = scheduler.admit([0])
            assert scheduler.registry.cancel(cancelled.request_id)
            first = scheduler.admit([1])
            await asyncio.sleep(0.02)
            second = scheduler.admit([2])
            results = await asyncio.wait_for(
                asyncio.gather(scheduler.wait(first), scheduler.wait(second)),
                timeout=0.2,
            )
            assert results == ["result-1", "result-2"]
            assert model.calls == [[(1,), (2,)]]
        finally:
            await scheduler.stop()

    run(scenario())


def test_dynamic_batches_keep_model_invocations_serialized():
    class SerialModel:
        def __init__(self):
            self._lock = threading.Lock()
            self.active = 0
            self.max_active = 0

        def predict(self, batch, /):
            with self._lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            try:
                time.sleep(0.03)
                return [f"result-{tokens[0]}" for tokens in batch]
            finally:
                with self._lock:
                    self.active -= 1

    async def scenario():
        model = SerialModel()
        scheduler = AsyncScheduler(model, max_batch_size=2, max_wait_ms=10)
        await scheduler.start()
        try:
            requests = [scheduler.admit([token]) for token in range(6)]
            await asyncio.gather(*(scheduler.wait(request) for request in requests))
            assert model.max_active == 1
        finally:
            await scheduler.stop()

    run(scenario())


def test_admission_at_timeout_boundary_is_not_stranded():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=2, max_wait_ms=50)
        await scheduler.start()
        try:
            first = scheduler.admit([1])
            await asyncio.sleep(0.05)
            second = scheduler.admit([2])
            results = await asyncio.wait_for(
                asyncio.gather(scheduler.wait(first), scheduler.wait(second)),
                timeout=0.5,
            )
            assert results == ["result-1", "result-2"]
            assert [tokens for call in model.calls for tokens in call] == [(1,), (2,)]
        finally:
            await scheduler.stop()

    run(scenario())


def test_stop_while_batch_is_forming_cancels_all_pending_requests():
    async def scenario():
        model = RecordingModel(asyncio.get_running_loop())
        scheduler = AsyncScheduler(model, max_batch_size=4, max_wait_ms=1_000)
        await scheduler.start()
        request = scheduler.admit([1])
        await asyncio.sleep(0)

        await scheduler.stop()

        assert request.future.cancelled()
        assert len(scheduler.registry) == 0
        assert model.calls == []

    run(scenario())


@pytest.mark.parametrize(
    "max_wait_ms",
    [-1, math.inf, -math.inf, math.nan, True, "10", None],
)
def test_invalid_max_wait_ms_is_rejected(max_wait_ms):
    with pytest.raises(ValueError, match="finite non-negative"):
        AsyncScheduler(object(), max_wait_ms=max_wait_ms)

"""Focused tests for authoritative end-to-end request deadlines."""

import asyncio
import math
import threading
import time
from collections.abc import Sequence

import pytest

from app.request_registry import RequestDeadlineExceeded
from app.scheduler import AsyncScheduler


def run(coro):
    return asyncio.run(coro)


class RecordingModel:
    def __init__(self) -> None:
        self.calls: list[list[tuple[int, ...]]] = []

    def predict(
        self,
        batch: Sequence[Sequence[int]],
        /,
    ) -> list[str]:
        copied = [tuple(tokens) for tokens in batch]
        self.calls.append(copied)
        return [f"result-{tokens[0]}" for tokens in copied]


def test_request_expires_while_dynamic_batch_is_forming():
    async def scenario():
        model = RecordingModel()
        scheduler = AsyncScheduler(
            model,
            max_batch_size=4,
            max_wait_ms=500,
            request_timeout_s=0.05,
        )
        await scheduler.start()
        try:
            request = scheduler.admit([1])
            with pytest.raises(RequestDeadlineExceeded):
                await asyncio.wait_for(request.future, timeout=0.3)
            assert len(scheduler.registry) == 0
            assert model.calls == []
        finally:
            await scheduler.stop()

    run(scenario())


def test_request_deadline_shortens_batch_wait_before_max_wait():
    async def scenario():
        model = RecordingModel()
        scheduler = AsyncScheduler(
            model,
            max_batch_size=2,
            max_wait_ms=1_000,
            request_timeout_s=0.06,
        )
        await scheduler.start()
        started = time.monotonic()
        try:
            request = scheduler.admit([1])
            with pytest.raises(RequestDeadlineExceeded):
                await asyncio.wait_for(request.future, timeout=0.3)
            assert time.monotonic() - started < 0.5
            assert model.calls == []
        finally:
            await scheduler.stop()

    run(scenario())


def test_expired_id_is_stale_and_live_request_behind_it_executes():
    class BlockingFirstModel(RecordingModel):
        def __init__(self):
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()

        def predict(self, batch, /):
            copied = [tuple(tokens) for tokens in batch]
            self.calls.append(copied)
            if copied == [(0,)]:
                self.entered.set()
                assert self.release.wait(timeout=2.0)
            return [f"result-{tokens[0]}" for tokens in copied]

    async def scenario():
        model = BlockingFirstModel()
        scheduler = AsyncScheduler(
            model,
            max_batch_size=1,
            request_timeout_s=0.12,
        )
        await scheduler.start()
        try:
            blocker = scheduler.admit([0])
            assert await asyncio.to_thread(model.entered.wait, 1.0)
            expired = scheduler.admit([1])
            with pytest.raises(RequestDeadlineExceeded):
                await asyncio.wait_for(expired.future, timeout=0.4)

            live = scheduler.admit([2])
            model.release.set()
            assert await scheduler.wait(live) == "result-2"
            with pytest.raises(RequestDeadlineExceeded):
                await blocker.future
            assert model.calls == [[(0,)], [(2,)]]
            assert len(scheduler.registry) == 0
        finally:
            model.release.set()
            await scheduler.stop()

    run(scenario())


def test_timeout_during_inference_returns_promptly_and_late_result_is_ignored():
    class BlockingModel(RecordingModel):
        def __init__(self):
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()
            self.finished = threading.Event()

        def predict(self, batch, /):
            copied = [tuple(tokens) for tokens in batch]
            self.calls.append(copied)
            self.entered.set()
            try:
                assert self.release.wait(timeout=2.0)
                return [f"result-{tokens[0]}" for tokens in copied]
            finally:
                self.finished.set()

    async def scenario():
        model = BlockingModel()
        scheduler = AsyncScheduler(
            model,
            max_batch_size=1,
            request_timeout_s=0.08,
        )
        await scheduler.start()
        try:
            request = scheduler.admit([1])
            assert await asyncio.to_thread(model.entered.wait, 1.0)
            with pytest.raises(RequestDeadlineExceeded):
                await asyncio.wait_for(scheduler.wait(request), timeout=0.3)

            assert not model.finished.is_set()
            assert len(scheduler.registry) == 0
            model.release.set()
            assert await asyncio.to_thread(model.finished.wait, 1.0)
            await asyncio.sleep(0.02)
            assert request.future.done()
            assert isinstance(request.future.exception(), RequestDeadlineExceeded)
            assert len(scheduler.registry) == 0
        finally:
            model.release.set()
            await scheduler.stop()

    run(scenario())


def test_one_inference_member_times_out_while_later_member_succeeds():
    class BlockingModel(RecordingModel):
        def __init__(self):
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()

        def predict(self, batch, /):
            copied = [tuple(tokens) for tokens in batch]
            self.calls.append(copied)
            self.entered.set()
            assert self.release.wait(timeout=2.0)
            return [f"result-{tokens[0]}" for tokens in copied]

    async def scenario():
        model = BlockingModel()
        scheduler = AsyncScheduler(
            model,
            max_batch_size=2,
            max_wait_ms=300,
            request_timeout_s=0.18,
        )
        await scheduler.start()
        try:
            first = scheduler.admit([1])
            await asyncio.sleep(0.10)
            second = scheduler.admit([2])
            assert await asyncio.to_thread(model.entered.wait, 1.0)

            with pytest.raises(RequestDeadlineExceeded):
                await scheduler.wait(first)
            assert not second.future.done()

            model.release.set()
            assert await scheduler.wait(second) == "result-2"
            assert model.calls == [[(1,), (2,)]]
            assert len(scheduler.registry) == 0
        finally:
            model.release.set()
            await scheduler.stop()

    run(scenario())


def test_result_winning_deadline_boundary_is_completed_exactly_once():
    async def scenario():
        scheduler = AsyncScheduler(
            RecordingModel(),
            max_batch_size=1,
            request_timeout_s=1.0,
        )
        await scheduler.start()
        try:
            request = scheduler.admit([7])
            assert await scheduler.wait(request) == "result-7"
            assert scheduler.registry.fail(
                request.request_id,
                RequestDeadlineExceeded("late timeout"),
            ) is False
            assert len(scheduler.registry) == 0
        finally:
            await scheduler.stop()

    run(scenario())


def test_scheduler_deadline_exception_propagates_directly_from_future():
    async def scenario():
        scheduler = AsyncScheduler(
            RecordingModel(),
            max_batch_size=2,
            max_wait_ms=1_000,
            request_timeout_s=1.0,
        )
        await scheduler.start()
        try:
            request = scheduler.admit([1])
            error = RequestDeadlineExceeded("scheduler deadline")
            assert scheduler.registry.fail(request.request_id, error)

            with pytest.raises(RequestDeadlineExceeded) as caught:
                await scheduler.wait(request)

            assert caught.value is error
            assert len(scheduler.registry) == 0
        finally:
            await scheduler.stop()

    run(scenario())


def test_stop_consumes_inference_wrapper_before_late_model_exception():
    class LateFailingModel:
        def __init__(self):
            self.entered = threading.Event()
            self.release = threading.Event()
            self.finished = threading.Event()

        def predict(self, batch, /):
            self.entered.set()
            assert self.release.wait(timeout=2.0)
            self.finished.set()
            raise RuntimeError("late model failure")

    async def scenario():
        loop = asyncio.get_running_loop()
        contexts = []
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: contexts.append(context))
        model = LateFailingModel()
        scheduler = AsyncScheduler(model, max_batch_size=1)
        await scheduler.start()
        scheduler.admit([1])

        try:
            assert await asyncio.to_thread(model.entered.wait, 1.0)
            await asyncio.wait_for(scheduler.stop(), timeout=0.3)

            assert not any(
                task.get_name() == "miniserve-inference"
                for task in asyncio.all_tasks()
            )

            model.release.set()
            assert await asyncio.to_thread(model.finished.wait, 1.0)
            await asyncio.sleep(0.05)
            assert contexts == []
            assert len(scheduler.registry) == 0
        finally:
            model.release.set()
            loop.set_exception_handler(previous_handler)

    run(scenario())


def test_caller_cancellation_remains_distinct_from_deadline():
    async def scenario():
        scheduler = AsyncScheduler(
            RecordingModel(),
            max_batch_size=2,
            max_wait_ms=1_000,
            request_timeout_s=5.0,
        )
        await scheduler.start()
        try:
            request = scheduler.admit([1])
            waiter = asyncio.create_task(scheduler.wait(request))
            await asyncio.sleep(0)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert request.future.cancelled()
            assert len(scheduler.registry) == 0
        finally:
            await scheduler.stop()

    run(scenario())


def test_stop_still_clears_forming_deadline_request():
    async def scenario():
        scheduler = AsyncScheduler(
            RecordingModel(),
            max_batch_size=2,
            max_wait_ms=1_000,
            request_timeout_s=5.0,
        )
        await scheduler.start()
        request = scheduler.admit([1])
        await asyncio.sleep(0)
        await scheduler.stop()
        assert request.future.cancelled()
        assert len(scheduler.registry) == 0

    run(scenario())


@pytest.mark.parametrize(
    "request_timeout_s",
    [0, -1, math.inf, -math.inf, math.nan, True, "1", None],
)
def test_invalid_request_timeout_is_rejected(request_timeout_s):
    with pytest.raises(ValueError, match="finite positive"):
        AsyncScheduler(
            RecordingModel(),
            request_timeout_s=request_timeout_s,
        )

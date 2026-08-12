"""Asynchronous dynamic-batch inference scheduler."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Sequence
from numbers import Real
from typing import Generic, TypeVar

import miniserve_core

from app.admission import AdmissionController
from app.batch_worker import BatchModel, FixedBatchWorker, PreparedBatch
from app.request_registry import PendingRequest, RequestRegistry


ResultT = TypeVar("ResultT")


class AsyncScheduler(Generic[ResultT]):
    """Own one task that serially executes dynamically formed batches."""

    def __init__(
        self,
        model: BatchModel[ResultT],
        *,
        queue_capacity: int = 128,
        max_batch_size: int = 8,
        max_wait_ms: float = 0,
        request_timeout_s: float = 30.0,
    ) -> None:
        if request_timeout_s <= 0:
            raise ValueError("request_timeout_s must be greater than zero")
        if (
            not isinstance(max_wait_ms, Real)
            or isinstance(max_wait_ms, bool)
            or not math.isfinite(max_wait_ms)
            or max_wait_ms < 0
        ):
            raise ValueError(
                "max_wait_ms must be a finite non-negative number"
            )

        self._queue = miniserve_core.BoundedQueue(queue_capacity)
        self._registry = RequestRegistry[ResultT]()
        self._admission = AdmissionController[ResultT](
            self._queue,
            registry=self._registry,
        )
        self._worker = FixedBatchWorker[ResultT](
            self._queue,
            self._registry,
            model,
            max_batch_size=max_batch_size,
        )
        self._request_timeout_s = request_timeout_s
        self._max_wait_s = float(max_wait_ms) / 1_000.0
        self._wake_event: asyncio.Event | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopped = False

    @property
    def registry(self) -> RequestRegistry[ResultT]:
        return self._registry

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Start the sole background task, idempotently until stopped."""

        if self.is_running:
            return
        if self._stopped:
            raise RuntimeError("stopped scheduler cannot be restarted")
        if self._task is not None:
            try:
                self._task.result()
            except (asyncio.CancelledError, Exception):
                pass
        self._wake_event = asyncio.Event()
        self._task = asyncio.create_task(
            self._run(),
            name="miniserve-scheduler",
        )
        self._task.add_done_callback(self._consume_task_result)

    async def stop(self) -> None:
        """Terminally stop scheduling and cancel all pending requests.

        Cancelling an asyncio.to_thread() await cannot stop its underlying
        thread. This scheduler is therefore deliberately non-restartable after
        stop, preventing that old inference from overlapping a new invocation.
        """

        self._stopped = True
        task = self._task
        if task is None:
            self._registry.cancel_all()
            return

        self._task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            self._registry.cancel_all()

    def admit(self, tokens: Sequence[int]) -> PendingRequest[ResultT]:
        """Non-blockingly register and enqueue one request, then wake work."""

        if not self.is_running:
            raise RuntimeError("scheduler is not running")

        request = self._admission.admit(
            tokens,
            timeout_s=self._request_timeout_s,
        )
        wake_event = self._wake_event
        if wake_event is None:
            raise RuntimeError("scheduler wake event is unavailable")
        wake_event.set()
        return request

    async def wait(self, request: PendingRequest[ResultT]) -> ResultT:
        """Await a result without giving caller cancellation its future."""

        try:
            return await asyncio.shield(request.future)
        except asyncio.CancelledError:
            self._registry.cancel(request.request_id)
            raise

    async def _run(self) -> None:
        wake_event = self._wake_event
        if wake_event is None:
            raise RuntimeError("scheduler wake event is unavailable")

        while True:
            await wake_event.wait()
            wake_event.clear()

            while True:
                prepared = await self._form_batch(wake_event)
                if prepared is None:
                    break

                try:
                    outputs = await asyncio.to_thread(
                        self._worker.predict,
                        prepared.inputs,
                    )
                except Exception as error:
                    self._worker.fail_batch(prepared, error)
                else:
                    self._worker.complete_batch(prepared, outputs)

    async def _form_batch(
        self,
        wake_event: asyncio.Event,
    ) -> PreparedBatch[ResultT] | None:
        """Collect live FIFO requests until capacity or the first deadline."""

        loop = asyncio.get_running_loop()
        requests: list[PendingRequest[ResultT]] = []
        dequeued = 0
        stale = 0
        batch_deadline: float | None = None

        while len(requests) < self._worker.max_batch_size:
            remaining = self._worker.max_batch_size - len(requests)
            addition = self._worker.prepare_batch(remaining)
            dequeued += addition.dequeued
            stale += addition.stale

            if addition.requests:
                if batch_deadline is None:
                    batch_deadline = loop.time() + self._max_wait_s
                requests.extend(addition.requests)

            if len(requests) == self._worker.max_batch_size:
                break

            if addition.dequeued > 0:
                # Stale IDs do not consume live capacity. Continue popping so
                # live requests behind them can fill the same batch.
                continue

            if not requests:
                return None

            assert batch_deadline is not None
            remaining_wait = batch_deadline - loop.time()
            if remaining_wait <= 0:
                break

            try:
                await asyncio.wait_for(
                    wake_event.wait(),
                    timeout=remaining_wait,
                )
            except asyncio.TimeoutError:
                break
            else:
                wake_event.clear()

        return PreparedBatch(
            dequeued=dequeued,
            stale=stale,
            requests=tuple(requests),
        )

    @staticmethod
    def _consume_task_result(task: asyncio.Task[None]) -> None:
        """Retrieve terminal exceptions so abandoned test loops stay clean."""

        if task.cancelled():
            return
        task.exception()

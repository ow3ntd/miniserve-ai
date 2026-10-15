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
from app.request_registry import (
    PendingRequest,
    RequestDeadlineExceeded,
    RequestRegistry,
)


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
        if (
            not isinstance(request_timeout_s, Real)
            or isinstance(request_timeout_s, bool)
            or not math.isfinite(request_timeout_s)
            or request_timeout_s <= 0
        ):
            raise ValueError(
                "request_timeout_s must be a finite positive number"
            )
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
        self._max_wait_ms = float(max_wait_ms)
        self._max_wait_s = self._max_wait_ms / 1_000.0
        # Dispatch counters. Incremented only on the event-loop thread, so no
        # locking is needed. Batches whose members all expired before the
        # model call are never dispatched and are not counted.
        self._batches_dispatched = 0
        self._requests_dispatched = 0
        self._wake_event: asyncio.Event | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopped = False

    @property
    def registry(self) -> RequestRegistry[ResultT]:
        return self._registry

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def queue_capacity(self) -> int:
        return self._queue.capacity

    @property
    def max_batch_size(self) -> int:
        return self._worker.max_batch_size

    @property
    def max_wait_ms(self) -> float:
        return self._max_wait_ms

    @property
    def request_timeout_s(self) -> float:
        return self._request_timeout_s

    @property
    def batches_dispatched(self) -> int:
        """Number of model calls issued so far."""

        return self._batches_dispatched

    @property
    def requests_dispatched(self) -> int:
        """Number of live requests included in those model calls."""

        return self._requests_dispatched

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
        """Await one result while enforcing its absolute request deadline."""

        try:
            remaining = request.deadline - self._registry.now()
            if remaining <= 0:
                self._expire(request)
            else:
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(request.future),
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    # RequestDeadlineExceeded is also a TimeoutError. If the
                    # owned future completed, propagate its winning outcome;
                    # only a still-pending future means wait_for timed out.
                    if request.future.done():
                        return await asyncio.shield(request.future)
                    self._expire(request)

            # Timeout and result completion may become runnable together. The
            # registry removal that succeeded owns the outcome.
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

                await self._execute_batch(prepared, wake_event)

    async def _form_batch(
        self,
        wake_event: asyncio.Event,
    ) -> PreparedBatch[ResultT] | None:
        """Collect live FIFO requests until capacity or the first deadline."""

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
                live_additions = self._expire_and_filter(
                    addition.requests
                )
                if batch_deadline is None:
                    if live_additions:
                        batch_deadline = (
                            self._registry.now() + self._max_wait_s
                        )
                requests.extend(live_additions)

            requests = self._expire_and_filter(requests)
            if not requests:
                batch_deadline = None

            if len(requests) == self._worker.max_batch_size:
                break

            if addition.dequeued > 0:
                # Stale IDs do not consume live capacity. Continue popping so
                # live requests behind them can fill the same batch.
                continue

            if not requests:
                return None

            assert batch_deadline is not None
            request_deadline = min(
                request.deadline for request in requests
            )
            wait_deadline = min(batch_deadline, request_deadline)
            remaining_wait = wait_deadline - self._registry.now()
            if remaining_wait <= 0:
                requests = self._expire_and_filter(requests)
                if not requests:
                    batch_deadline = None
                    continue
                if self._registry.now() >= batch_deadline:
                    break
                continue

            try:
                await asyncio.wait_for(
                    wake_event.wait(),
                    timeout=remaining_wait,
                )
            except asyncio.TimeoutError:
                requests = self._expire_and_filter(requests)
                if not requests:
                    batch_deadline = None
                    continue
                if self._registry.now() >= batch_deadline:
                    break
            else:
                wake_event.clear()

        return PreparedBatch(
            dequeued=dequeued,
            stale=stale,
            requests=tuple(requests),
        )

    async def _execute_batch(
        self,
        prepared: PreparedBatch[ResultT],
        wake_event: asyncio.Event,
    ) -> None:
        """Run one model call while expiring its requests independently."""

        live = self._expire_and_filter(prepared.requests)
        if not live:
            return

        dispatched = PreparedBatch(
            dequeued=prepared.dequeued,
            stale=prepared.stale,
            requests=tuple(live),
        )
        self._batches_dispatched += 1
        self._requests_dispatched += len(live)
        inference = asyncio.create_task(
            asyncio.to_thread(
                self._worker.predict,
                dispatched.inputs,
            ),
            name="miniserve-inference",
        )

        try:
            while not inference.done():
                # The scheduler cannot dequeue another batch while this model
                # call is active, but queued requests' clocks keep running.
                # Supervise all registry-owned requests with one timer loop.
                owned = list(self._registry.pending())
                if owned:
                    remaining = min(
                        request.deadline for request in owned
                    ) - self._registry.now()
                else:
                    remaining = None

                if remaining is not None and remaining <= 0:
                    self._expire_and_filter(owned)
                    continue

                admission = asyncio.create_task(wake_event.wait())
                try:
                    done, _ = await asyncio.wait(
                        {inference, admission},
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if inference in done:
                        break
                    if admission in done:
                        wake_event.clear()
                    else:
                        self._expire_and_filter(owned)
                finally:
                    admission.cancel()
                    try:
                        await admission
                    except asyncio.CancelledError:
                        pass

            try:
                outputs = await inference
            except Exception as error:
                self._worker.fail_batch(dispatched, error)
            else:
                self._worker.complete_batch(dispatched, outputs)
        except asyncio.CancelledError:
            # This cancels only the asyncio wrapper. The executor thread may
            # continue, but its wrapper is owned, awaited, and cannot leak an
            # un-retrieved task exception.
            inference.cancel()
            try:
                await inference
            except asyncio.CancelledError:
                pass
            raise

    def _expire_and_filter(
        self,
        requests: Sequence[PendingRequest[ResultT]],
    ) -> list[PendingRequest[ResultT]]:
        """Expire due requests and return entries still owned by registry."""

        now = self._registry.now()
        live: list[PendingRequest[ResultT]] = []
        for request in requests:
            if now >= request.deadline:
                self._expire(request)
            if self._registry.get(request.request_id) is request:
                live.append(request)
        return live

    def _expire(self, request: PendingRequest[ResultT]) -> bool:
        return self._registry.fail(
            request.request_id,
            RequestDeadlineExceeded("request deadline exceeded"),
        )

    @staticmethod
    def _consume_task_result(task: asyncio.Task[None]) -> None:
        """Retrieve terminal exceptions so abandoned test loops stay clean."""

        if task.cancelled():
            return
        task.exception()

"""Tests for Python-owned request lifecycle state."""

import asyncio
import math

import pytest

from app.request_registry import UINT64_MAX, RequestRegistry


def run(coro):
    """Run one async test scenario without requiring pytest-asyncio."""

    return asyncio.run(coro)


def test_ids_are_unique_and_monotonically_increasing():
    async def scenario():
        registry = RequestRegistry[str]()

        first = registry.register([1], timeout_s=1.0)
        second = registry.register([2], timeout_s=1.0)
        third = registry.register([3], timeout_s=1.0)

        assert [first.request_id, second.request_id, third.request_id] == [1, 2, 3]
        assert len(registry) == 3

        registry.cancel(first.request_id)
        registry.cancel(second.request_id)
        registry.cancel(third.request_id)

    run(scenario())


def test_request_ids_fit_uint64_contract():
    async def scenario():
        registry = RequestRegistry[str]()
        request = registry.register([1, 2, 3], timeout_s=1.0)

        assert 1 <= request.request_id <= UINT64_MAX
        registry.cancel(request.request_id)

    run(scenario())


def test_register_copies_tokens_into_immutable_tuple():
    async def scenario():
        registry = RequestRegistry[str]()
        tokens = [10, 20, 30]

        request = registry.register(tokens, timeout_s=1.0)
        tokens.append(40)

        assert request.tokens == (10, 20, 30)
        assert isinstance(request.tokens, tuple)

        registry.cancel(request.request_id)

    run(scenario())


def test_arrival_and_deadline_use_supplied_monotonic_clock():
    async def scenario():
        registry = RequestRegistry[str](clock=lambda: 100.25)

        request = registry.register([1], timeout_s=2.5)

        assert request.arrival_time == pytest.approx(100.25)
        assert request.deadline == pytest.approx(102.75)

        registry.cancel(request.request_id)

    run(scenario())


@pytest.mark.parametrize(
    "timeout_s",
    [0.0, -0.1, math.nan, math.inf, -math.inf],
)
def test_register_rejects_invalid_timeout(timeout_s):
    async def scenario():
        registry = RequestRegistry[str]()

        with pytest.raises(ValueError, match="finite and greater than zero"):
            registry.register([1], timeout_s=timeout_s)

        assert len(registry) == 0

    run(scenario())


def test_failed_registration_without_event_loop_does_not_consume_id():
    registry = RequestRegistry[str]()

    with pytest.raises(RuntimeError, match="no running event loop"):
        registry.register([1], timeout_s=1.0)

    async def scenario():
        request = registry.register([1], timeout_s=1.0)

        assert request.request_id == 1
        assert registry.cancel(request.request_id) is True

    run(scenario())


def test_resolve_completes_future_and_removes_request():
    async def scenario():
        registry = RequestRegistry[str]()
        request = registry.register([1], timeout_s=1.0)

        assert registry.resolve(request.request_id, "result") is True
        assert await request.future == "result"
        assert registry.get(request.request_id) is None
        assert len(registry) == 0

    run(scenario())


def test_fail_completes_future_with_exception_and_removes_request():
    async def scenario():
        registry = RequestRegistry[str]()
        request = registry.register([1], timeout_s=1.0)
        error = RuntimeError("inference failed")

        assert registry.fail(request.request_id, error) is True

        with pytest.raises(RuntimeError, match="inference failed"):
            await request.future

        assert registry.get(request.request_id) is None
        assert len(registry) == 0

    run(scenario())


def test_duplicate_resolution_is_safe_no_op():
    async def scenario():
        registry = RequestRegistry[str]()
        request = registry.register([1], timeout_s=1.0)

        assert registry.resolve(request.request_id, "first") is True
        assert registry.resolve(request.request_id, "second") is False
        assert await request.future == "first"
        assert len(registry) == 0

    run(scenario())


def test_success_after_failure_is_safe_no_op():
    async def scenario():
        registry = RequestRegistry[str]()
        request = registry.register([1], timeout_s=1.0)

        assert registry.fail(request.request_id, ValueError("failed")) is True
        assert registry.resolve(request.request_id, "late result") is False

        with pytest.raises(ValueError, match="failed"):
            await request.future

        assert len(registry) == 0

    run(scenario())


def test_failure_after_success_is_safe_no_op():
    async def scenario():
        registry = RequestRegistry[str]()
        request = registry.register([1], timeout_s=1.0)

        assert registry.resolve(request.request_id, "success") is True
        assert registry.fail(
            request.request_id,
            RuntimeError("late failure"),
        ) is False

        assert await request.future == "success"
        assert len(registry) == 0

    run(scenario())


def test_unknown_request_ids_return_false():
    async def scenario():
        registry = RequestRegistry[str]()

        assert registry.resolve(999, "result") is False
        assert registry.fail(999, RuntimeError("failure")) is False
        assert registry.cancel(999) is False
        assert len(registry) == 0

    run(scenario())


def test_cancel_cancels_future_and_removes_request():
    async def scenario():
        registry = RequestRegistry[str]()
        request = registry.register([1], timeout_s=1.0)

        assert registry.cancel(request.request_id) is True
        assert request.future.cancelled()
        assert registry.get(request.request_id) is None
        assert registry.cancel(request.request_id) is False
        assert len(registry) == 0

    run(scenario())


def test_request_id_exhaustion_is_rejected():
    async def scenario():
        registry = RequestRegistry[str]()
        registry._next_request_id = UINT64_MAX

        last = registry.register([1], timeout_s=1.0)
        assert last.request_id == UINT64_MAX

        with pytest.raises(OverflowError, match="request ID space exhausted"):
            registry.register([2], timeout_s=1.0)

        registry.cancel(last.request_id)
        assert len(registry) == 0

    run(scenario())


def test_large_create_and_complete_cycle_leaves_no_entries():
    async def scenario():
        registry = RequestRegistry[int]()
        requests = [
            registry.register([index], timeout_s=10.0)
            for index in range(5_000)
        ]

        assert len(registry) == 5_000

        for index, request in enumerate(requests):
            assert registry.resolve(request.request_id, index) is True

        results = await asyncio.gather(
            *(request.future for request in requests)
        )

        assert results == list(range(5_000))
        assert len(registry) == 0

    run(scenario())

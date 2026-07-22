"""Contract tests for the pybind11 bounded-queue boundary."""

import pytest

miniserve_core = pytest.importorskip(
    "miniserve_core",
    reason=(
        "C++ extension is not built; run cmake --build build and set "
        "PYTHONPATH=build/python:."
    ),
)


def test_zero_capacity_is_rejected():
    with pytest.raises(ValueError, match="capacity must be positive"):
        miniserve_core.BoundedQueue(0)


def test_full_queue_rejects_without_growing():
    queue = miniserve_core.BoundedQueue(2)

    assert queue.try_push(101)
    assert queue.try_push(102)
    assert not queue.try_push(103)
    assert queue.size == 2
    assert queue.full


def test_pop_and_pop_batch_preserve_fifo_order():
    queue = miniserve_core.BoundedQueue(6)
    for request_id in range(1, 7):
        assert queue.try_push(request_id)

    assert queue.try_pop() == 1
    assert queue.pop_batch(3) == [2, 3, 4]
    assert queue.pop_batch(99) == [5, 6]
    assert queue.try_pop() is None
    assert queue.empty


@pytest.mark.parametrize("request_id", [-1, 1 << 80])
def test_python_integer_outside_uint64_is_rejected(request_id):
    queue = miniserve_core.BoundedQueue(1)
    with pytest.raises(OverflowError):
        queue.try_push(request_id)

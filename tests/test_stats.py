"""Tests for environment-variable configuration and the /stats endpoint.

Both exist for the scheduler benchmark: the env vars let a sweep configure
`uvicorn app.main:app` per server process, and /stats lets the load
generator measure the batch size the scheduler actually realized rather
than the maximum it was allowed.
"""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import (
    DEFAULT_MAX_BATCH_SIZE,
    DEFAULT_MAX_WAIT_MS,
    DEFAULT_QUEUE_CAPACITY,
    DEFAULT_REQUEST_TIMEOUT_S,
    create_app,
    settings_from_env,
)
from app.model_runner import ModelRunner
from app.request_registry import RequestDeadlineExceeded
from app.scheduler import AsyncScheduler


# --------------------------------------------------------------------- #
# settings_from_env                                                      #
# --------------------------------------------------------------------- #


def test_unset_env_vars_give_create_app_defaults():
    assert settings_from_env({}) == {
        "queue_capacity": DEFAULT_QUEUE_CAPACITY,
        "max_batch_size": DEFAULT_MAX_BATCH_SIZE,
        "max_wait_ms": DEFAULT_MAX_WAIT_MS,
        "request_timeout_s": DEFAULT_REQUEST_TIMEOUT_S,
    }


def test_defaults_match_a_default_app():
    # The env-var fallbacks and create_app()'s own defaults must agree, or
    # `uvicorn app.main:app` with no env vars would differ from create_app().
    scheduler = create_app().state.scheduler
    assert settings_from_env({}) == {
        "queue_capacity": scheduler.queue_capacity,
        "max_batch_size": scheduler.max_batch_size,
        "max_wait_ms": scheduler.max_wait_ms,
        "request_timeout_s": scheduler.request_timeout_s,
    }


def test_set_env_vars_override_with_correct_types():
    settings = settings_from_env(
        {
            "MINISERVE_QUEUE_CAPACITY": "256",
            "MINISERVE_MAX_BATCH_SIZE": "32",
            "MINISERVE_MAX_WAIT_MS": "2.5",
            "MINISERVE_REQUEST_TIMEOUT_S": "7",
        }
    )
    assert settings == {
        "queue_capacity": 256,
        "max_batch_size": 32,
        "max_wait_ms": 2.5,
        "request_timeout_s": 7.0,
    }
    assert isinstance(settings["queue_capacity"], int)
    assert isinstance(settings["max_batch_size"], int)
    assert isinstance(settings["max_wait_ms"], float)
    assert isinstance(settings["request_timeout_s"], float)


def test_partial_env_overrides_leave_other_defaults():
    settings = settings_from_env({"MINISERVE_MAX_WAIT_MS": "5"})
    assert settings["max_wait_ms"] == 5.0
    assert settings["queue_capacity"] == DEFAULT_QUEUE_CAPACITY
    assert settings["max_batch_size"] == DEFAULT_MAX_BATCH_SIZE
    assert settings["request_timeout_s"] == DEFAULT_REQUEST_TIMEOUT_S


def test_unparseable_env_var_names_the_variable():
    with pytest.raises(ValueError, match="MINISERVE_MAX_BATCH_SIZE"):
        settings_from_env({"MINISERVE_MAX_BATCH_SIZE": "eight"})


def test_env_settings_feed_create_app():
    settings = settings_from_env(
        {
            "MINISERVE_QUEUE_CAPACITY": "64",
            "MINISERVE_MAX_BATCH_SIZE": "4",
            "MINISERVE_MAX_WAIT_MS": "3",
            "MINISERVE_REQUEST_TIMEOUT_S": "9",
        }
    )
    body = TestClient(create_app(**settings)).get("/stats").json()
    assert body["queue_capacity"] == 64
    assert body["max_batch_size"] == 4
    assert body["max_wait_ms"] == 3.0
    assert body["request_timeout_s"] == 9.0


# --------------------------------------------------------------------- #
# /stats                                                                 #
# --------------------------------------------------------------------- #


def test_stats_on_fresh_app_reports_zero_counters_and_config():
    application = create_app(
        queue_capacity=16,
        max_batch_size=4,
        max_wait_ms=1.5,
        request_timeout_s=2.0,
    )
    body = TestClient(application).get("/stats").json()
    assert body == {
        "batches_dispatched": 0,
        "requests_dispatched": 0,
        "mean_batch_size": 0.0,
        "queue_capacity": 16,
        "max_batch_size": 4,
        "max_wait_ms": 1.5,
        "request_timeout_s": 2.0,
    }


def test_two_coalesced_predicts_count_one_batch_of_two():
    class RecordingRunner(ModelRunner):
        def __init__(self):
            super().__init__()
            self.calls = []
            self._lock = threading.Lock()

        def predict(self, batch):
            copied = [tuple(tokens) for tokens in batch]
            with self._lock:
                self.calls.append(copied)
            return [
                [float(tokens[0])] * self.config.n_outputs
                for tokens in copied
            ]

    runner = RecordingRunner()
    runner.load()
    application = create_app(runner, max_batch_size=2, max_wait_ms=200)

    with TestClient(application) as client, ThreadPoolExecutor(2) as pool:
        first = pool.submit(client.post, "/predict", json={"tokens": [11]})
        second = pool.submit(client.post, "/predict", json={"tokens": [22]})
        assert first.result(timeout=2.0).status_code == 200
        assert second.result(timeout=2.0).status_code == 200
        body = client.get("/stats").json()

    assert len(runner.calls) == 1
    assert body["batches_dispatched"] == 1
    assert body["requests_dispatched"] == 2
    assert body["mean_batch_size"] == 2.0


def test_sequential_predicts_count_separate_batches():
    with TestClient(create_app(max_batch_size=8)) as client:
        for _ in range(3):
            assert client.post("/predict", json={"tokens": [1]}).status_code == 200
        body = client.get("/stats").json()

    assert body["batches_dispatched"] == 3
    assert body["requests_dispatched"] == 3
    assert body["mean_batch_size"] == 1.0


def test_all_stale_batch_is_not_counted_as_a_dispatch():
    # A request that expires while queued behind active inference leaves a
    # stale ID in the native queue. Popping it forms no model call, so it
    # must not inflate batches_dispatched or deflate mean_batch_size.
    class BlockingFirstModel:
        def __init__(self):
            self.entered = threading.Event()
            self.release = threading.Event()

        def predict(self, batch, /):
            copied = [tuple(tokens) for tokens in batch]
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

            # Dispatched: [0] and [2]. Never dispatched: [1].
            assert scheduler.batches_dispatched == 2
            assert scheduler.requests_dispatched == 2
        finally:
            model.release.set()
            await scheduler.stop()

    asyncio.run(scenario())

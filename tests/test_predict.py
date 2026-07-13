"""Tests for the synchronous /predict endpoint (Day 3).

Structural validation failures (missing/empty/mistyped fields) are
handled by Pydantic; model-dependent failures (vocab range) are raised
by ModelRunner and translated to 422 by the endpoint. Both must present
to the client identically as 422s -- that boundary is what these tests
pin down.
"""

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.model_runner import ModelConfig, ModelRunner


def make_client(runner: ModelRunner | None = None) -> TestClient:
    return TestClient(create_app(runner))


VALID_TOKENS = [1, 5, 9, 200]


# --------------------------------------------------------------------- #
# Happy path                                                             #
# --------------------------------------------------------------------- #


def test_predict_returns_200_for_valid_request():
    resp = make_client().post("/predict", json={"tokens": VALID_TOKENS})
    assert resp.status_code == 200


def test_predict_returns_n_outputs_floats():
    body = make_client().post("/predict", json={"tokens": VALID_TOKENS}).json()
    outputs = body["outputs"]
    assert isinstance(outputs, list)
    assert len(outputs) == ModelConfig().n_outputs
    assert all(isinstance(x, float) for x in outputs)


def test_predict_is_deterministic_across_calls():
    client = make_client()
    first = client.post("/predict", json={"tokens": VALID_TOKENS}).json()
    second = client.post("/predict", json={"tokens": VALID_TOKENS}).json()
    assert first["outputs"] == second["outputs"]


def test_predict_matches_direct_runner_output():
    # The endpoint must be a thin wrapper: same runner, same tokens,
    # same numbers. Inject the runner so weights are shared by identity,
    # not just by seed.
    runner = ModelRunner()
    runner.load()
    expected = runner.predict([VALID_TOKENS])[0]

    body = make_client(runner).post(
        "/predict", json={"tokens": VALID_TOKENS}
    ).json()
    assert body["outputs"] == pytest.approx(expected)


# --------------------------------------------------------------------- #
# Structural validation (rejected by Pydantic before app code runs)      #
# --------------------------------------------------------------------- #


def test_predict_empty_tokens_is_422():
    resp = make_client().post("/predict", json={"tokens": []})
    assert resp.status_code == 422


def test_predict_missing_tokens_field_is_422():
    resp = make_client().post("/predict", json={})
    assert resp.status_code == 422


def test_predict_non_integer_token_is_422():
    resp = make_client().post("/predict", json={"tokens": [1, "abc", 3]})
    assert resp.status_code == 422


def test_predict_float_token_is_422():
    resp = make_client().post("/predict", json={"tokens": [1, 2.5, 3]})
    assert resp.status_code == 422


def test_predict_unknown_field_is_422():
    resp = make_client().post(
        "/predict", json={"tokens": VALID_TOKENS, "tokenz": [1]}
    )
    assert resp.status_code == 422


# --------------------------------------------------------------------- #
# Model-dependent validation (raised by ModelRunner, mapped to 422)      #
# --------------------------------------------------------------------- #


def test_predict_out_of_range_token_is_422():
    too_big = ModelConfig().vocab_size
    resp = make_client().post("/predict", json={"tokens": [too_big]})
    assert resp.status_code == 422
    assert "out of range" in resp.json()["detail"]


# --------------------------------------------------------------------- #
# Server-side readiness and method handling                              #
# --------------------------------------------------------------------- #


def test_predict_with_unloaded_runner_is_503():
    unloaded = ModelRunner()
    resp = make_client(unloaded).post(
        "/predict", json={"tokens": VALID_TOKENS}
    )
    assert resp.status_code == 503


def test_get_predict_is_405():
    resp = make_client().get("/predict")
    assert resp.status_code == 405

"""Tests for the model runner (Day 2).

The load-bearing test here is batch invariance: a request's output must be
the same whether it runs alone or padded inside a batch of unrelated,
differently-sized requests. If padding ever leaks into the math, batching
silently corrupts results -- the worst failure mode a batching server can
have, because nothing crashes.
"""

import pytest

from app.model_runner import ModelConfig, ModelRunner


@pytest.fixture(scope="module")
def runner() -> ModelRunner:
    r = ModelRunner()
    r.load()
    return r


# --------------------------------------------------------------------- #
# Lifecycle                                                             #
# --------------------------------------------------------------------- #


def test_predict_before_load_raises():
    with pytest.raises(RuntimeError):
        ModelRunner().predict([[1, 2, 3]])


def test_load_is_idempotent(runner):
    before = runner.predict([[1, 2, 3]])
    runner.load()  # second call must not rebuild/reseed the model
    after = runner.predict([[1, 2, 3]])
    assert before == after


def test_two_runners_same_config_agree():
    # Seeded weights: independently-loaded runners are interchangeable.
    a, b = ModelRunner(), ModelRunner()
    a.load()
    b.load()
    assert a.predict([[5, 6, 7]]) == b.predict([[5, 6, 7]])


# --------------------------------------------------------------------- #
# Output shape                                                          #
# --------------------------------------------------------------------- #


def test_output_shape_matches_batch(runner):
    batch = [[1], [2, 3, 4, 5], [9, 8, 7, 6, 5, 4, 3]]  # varied lengths
    out = runner.predict(batch)
    assert len(out) == len(batch)
    k = runner.config.n_outputs
    assert all(len(row) == k for row in out)


def test_output_is_plain_python_floats(runner):
    # The scheduler/API layer should never receive tensors.
    out = runner.predict([[1, 2]])
    assert isinstance(out, list)
    assert all(isinstance(v, float) for v in out[0])


def test_single_request_batch(runner):
    out = runner.predict([[42]])
    assert len(out) == 1


# --------------------------------------------------------------------- #
# Batch-splitting correctness (the property that matters)               #
# --------------------------------------------------------------------- #


def test_batch_invariance_padding_does_not_leak(runner):
    requests = [
        [1, 2, 3],
        [700] * 12,          # long request forces heavy padding of the others
        [5],
        [1023, 0, 1023, 0],  # boundary token ids, incl. the pad id as a REAL token
    ]
    solo = [runner.predict([r])[0] for r in requests]
    batched = runner.predict(requests)
    for i, (s, b) in enumerate(zip(solo, batched)):
        assert b == pytest.approx(s, abs=1e-5), f"request {i} changed under batching"


def test_results_routed_in_input_order(runner):
    a, b = [1, 2, 3], [900, 901]
    out_ab = runner.predict([a, b])
    out_ba = runner.predict([b, a])
    assert out_ab[0] == pytest.approx(out_ba[1], abs=1e-5)
    assert out_ab[1] == pytest.approx(out_ba[0], abs=1e-5)


def test_repeated_identical_requests_get_identical_results(runner):
    out = runner.predict([[3, 1, 4], [3, 1, 4]])
    assert out[0] == pytest.approx(out[1], abs=1e-6)


# --------------------------------------------------------------------- #
# Validation                                                            #
# --------------------------------------------------------------------- #


def test_rejects_empty_batch(runner):
    with pytest.raises(ValueError, match="at least one request"):
        runner.predict([])


def test_rejects_empty_sequence(runner):
    with pytest.raises(ValueError, match="empty sequence"):
        runner.predict([[1, 2], []])


@pytest.mark.parametrize("bad_token", [-1, 1024, 10_000])
def test_rejects_out_of_range_token(runner, bad_token):
    with pytest.raises(ValueError, match="out of range"):
        runner.predict([[1, bad_token]])


def test_rejects_non_integer_token(runner):
    with pytest.raises(ValueError, match="non-integer"):
        runner.predict([[1, 2.5]])


def test_custom_config_output_width():
    r = ModelRunner(ModelConfig(vocab_size=64, embed_dim=8, n_outputs=3))
    r.load()
    out = r.predict([[0, 1, 63]])
    assert len(out[0]) == 3

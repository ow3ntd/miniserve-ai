"""Tests for the /health endpoint (Day 1)."""

from fastapi.testclient import TestClient

from app.main import APP_NAME, VERSION, create_app


def make_client() -> TestClient:
    return TestClient(create_app())


def test_health_returns_200():
    resp = make_client().get("/health")
    assert resp.status_code == 200


def test_health_reports_ok_status_and_identity():
    body = make_client().get("/health").json()
    assert body["status"] == "ok"
    assert body["service"] == APP_NAME
    assert body["version"] == VERSION


def test_health_reports_nonnegative_uptime():
    body = make_client().get("/health").json()
    assert isinstance(body["uptime_s"], (int, float))
    assert body["uptime_s"] >= 0


def test_unknown_route_returns_404():
    resp = make_client().get("/predict")  # does not exist yet (Day 3)
    assert resp.status_code == 404

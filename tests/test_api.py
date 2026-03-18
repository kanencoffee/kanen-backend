import os

os.environ.setdefault("DATABASE_URL", "sqlite:///./kanen_test.db")

from fastapi.testclient import TestClient
from sqlmodel import SQLModel

from app.db.session import engine
from app.main import app

SQLModel.metadata.create_all(engine)

client = TestClient(app)


def test_status_endpoint():
    resp = client.get("/v1/status")
    assert resp.status_code == 200
    body = resp.json()
    assert "message" in body


def test_dashboard_summary():
    resp = client.get("/v1/dashboard/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert {"active_repairs", "low_stock_parts", "avg_lead_time_days", "vendor_score"}.issubset(data.keys())


def test_sync_runs_empty():
    resp = client.get("/v1/system/sync-runs")
    assert resp.status_code == 200
    assert "items" in resp.json()


def test_run_sync_requires_job():
    resp = client.post("/v1/system/run-sync", json={"simulate_only": True})
    assert resp.status_code == 400


def test_run_sync_simulation_quickbooks():
    resp = client.post("/v1/system/run-sync", json={"quickbooks": True, "simulate_only": True})
    assert resp.status_code == 202
    body = resp.json()
    assert body["jobs"] == ["quickbooks"]

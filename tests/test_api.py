from __future__ import annotations

from fastapi.testclient import TestClient

from eurostream.api import create_app
from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.warehouse import Warehouse


def _app(tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "eurocart.duckdb",
        audit_log_path=tmp_path / "data" / "logs" / "audit.jsonl",
        metrics_path=tmp_path / "data" / "logs" / "metrics.jsonl",
        pii_manifest_path=tmp_path / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
    )
    bus = open_bus(tmp_path / "events.db")
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    metrics = Metrics(tmp_path / "metrics.jsonl")
    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "api-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    app = create_app(erasure, metrics, settings, warehouse)
    return app, bus, warehouse


def test_erasure_request_success(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    r = c.post("/erasure-requests", json={"customer_id": "cust_12345"})
    assert r.status_code == 200
    assert r.json()["customer_id"] == "cust_12345"
    assert "request_id" in r.json()
    bus.close()
    wh.close()


def test_erasure_request_validation(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    r = c.post("/erasure-requests", json={"customer_id": "ab"})
    assert r.status_code == 422
    bus.close()
    wh.close()


def test_health_and_metrics(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    assert c.get("/health").status_code == 200
    assert c.get("/health").json()["status"] == "ok"
    assert c.get("/metrics").status_code == 200
    assert "counters" in c.get("/metrics").json()
    bus.close()
    wh.close()


def test_metrics_prometheus(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    r = c.get("/metrics/prometheus")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]
    bus.close()
    wh.close()


def test_gold_and_audit_endpoints(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    assert c.get("/governance/erasure-audit").status_code == 200
    assert c.get("/gold/customer-360").status_code == 200
    bus.close()
    wh.close()

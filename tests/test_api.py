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
    assert c.get("/stats").status_code == 200
    bus.close()
    wh.close()


def test_api_interactive_triggers_and_verification(tmp_path):
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
        consumer=bus.consumer("erasure_requests", "api-test2", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    app = create_app(erasure, metrics, settings, warehouse, bus)
    c = TestClient(app)

    # 1. Produce
    r_prod = c.post("/produce?events=10")
    assert r_prod.status_code == 200
    assert r_prod.json()["events_produced"] == 10

    # 2. Stream fraud
    r_str = c.post("/stream?max_events=20")
    assert r_str.status_code == 200
    assert "alerts_emitted" in r_str.json()

    # 3. Transform
    r_tr = c.post("/transform?incremental=false")
    assert r_tr.status_code == 200
    assert r_tr.json()["status"] == "ok"

    # 4. Quality Gate
    r_dq = c.post("/quality-gate")
    assert r_dq.status_code == 200
    assert r_dq.json()["all_passed"] is True

    # 5. Customer 360 query
    r_c360 = c.get("/gold/customer-360")
    assert r_c360.status_code == 200
    custs = r_c360.json()
    assert len(custs) > 0
    target_cust = custs[0]["customer_id"]

    # 6. Synchronous Erase
    r_erase = c.post(f"/erase/{target_cust}")
    assert r_erase.status_code == 200
    assert r_erase.json()["status"] == "completed"
    assert len(r_erase.json()["confirmation_hash"]) == 16

    # 7. Verify Erasure
    r_ver = c.get(f"/verify-erasure/{target_cust}")
    assert r_ver.status_code == 200
    ver_data = r_ver.json()
    assert ver_data["verified"] is True
    assert ver_data["gold_rows_remaining"] == 0
    assert ver_data["silver_rows_remaining"] == 0
    assert ver_data["audit_log_entries"] >= 1

    bus.close()
    warehouse.close()

from __future__ import annotations

import re
import time

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
    app = create_app(erasure, metrics, settings, warehouse, bus)
    return app, bus, warehouse


def test_erasure_request_success(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    r = c.post("/erasure-requests", json={"customer_id": "cust_12345"})
    # Queued work is 202 Accepted, not 200 OK — the cascade runs on the worker.
    assert r.status_code == 202
    assert r.json()["customer_id"] == "cust_12345"
    assert "request_id" in r.json()
    assert r.json()["status"] in {"queued", "succeeded"}
    bus.close()
    wh.close()


def test_erasure_request_sync_returns_200(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    r = c.post("/erasure-requests", json={"customer_id": "cust_99999", "sync": True})
    assert r.status_code == 200
    assert r.json()["status"] in {"completed", "failed"}
    bus.close()
    wh.close()


def test_verify_erasure_rejects_injection(tmp_path):
    """A quote in the path must be a 422, never a 500 or a forged verdict."""
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    r = c.get("/verify-erasure/cust'%20OR%201=1--")
    assert r.status_code == 422
    r = c.get("/verify-erasure/cust' UNION SELECT 1--")
    assert r.status_code == 422
    bus.close()
    wh.close()


def test_gold_customer_360_search_is_bound(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    ok = c.get("/gold/customer-360", params={"search": "cust_"})
    assert ok.status_code == 200
    # A quote in the search term filters nothing rather than erroring.
    r = c.get("/gold/customer-360", params={"search": "x' OR '1'='1"})
    assert r.status_code == 200
    assert r.json() == []
    # The limit is validated, not interpolated.
    assert c.get("/gold/customer-360", params={"limit": 0}).status_code == 422
    assert c.get("/gold/customer-360", params={"limit": 5000}).status_code == 422
    bus.close()
    wh.close()


def test_erasure_request_validation(tmp_path):
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    r = c.post("/erasure-requests", json={"customer_id": "ab"})
    assert r.status_code == 422
    bus.close()
    wh.close()


def test_concurrent_quality_gate_is_isolated(tmp_path):
    """Regression: one shared DuckDB connection used to make concurrent
    /quality-gate calls return 500 or leak rows across requests."""
    from concurrent.futures import ThreadPoolExecutor

    app, bus, wh = _app(tmp_path)
    clients = [TestClient(app) for _ in range(8)]

    def hit(i: int) -> tuple[int, dict]:
        # Alternate between two read endpoints so a cross-request row leak
        # shows up as a wrong-shaped body rather than just a crash.
        if i % 2 == 0:
            r = clients[i].post("/quality-gate")
        else:
            r = clients[i].get("/gold/customer-360")
        return r.status_code, r.json()

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(hit, range(8)))

    assert all(code == 200 for code, _ in results), results
    # Every quality-gate run reports the same checks (run_id differs by design).
    gate = [results[i][1] for i in range(0, 8, 2)]
    gold = [results[i][1] for i in range(1, 8, 2)]
    fingerprints = {(g["all_passed"], tuple(r["check_name"] for r in g["results"])) for g in gate}
    assert len(fingerprints) == 1, "quality-gate answers diverged under concurrency"
    assert len({len(g["results"]) for g in gate}) == 1, "quality-gate rows leaked across requests"
    # The thresholds the verdict was judged against travel with the report.
    assert {g["thresholds"]["freshness_seconds"] for g in gate} == {3600.0}
    assert {g["thresholds"]["volume_drop_pct"] for g in gate} == {50.0}
    assert {g["thresholds"]["k_anonymity"] for g in gate} == {1}
    assert len({str(g) for g in gold}) == 1, "gold.customer-360 answers diverged under concurrency"
    for c in clients:
        c.close()
    bus.close()
    wh.close()


def test_suppressed_customer_does_not_resurrect_on_rebuild(tmp_path):
    """Regression: build_silver/build_gold used to ignore the suppression
    registry, so a `transform` after erasure brought the erased customer back."""
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)

    # Build a customer, then erase them through the public API.
    assert c.post("/produce?events=20").status_code == 200
    assert c.post("/transform?incremental=false").status_code == 200
    customers = c.get("/gold/customer-360").json()
    assert customers
    victim = customers[0]["customer_id"]
    assert c.post(f"/erase/{victim}").status_code == 200
    assert c.get(f"/verify-erasure/{victim}").json()["verified"] is True

    # A later full rebuild (what /transform runs) must not resurrect them.
    assert c.post("/transform?incremental=false").status_code == 200
    rebuilt = {row["customer_id"] for row in c.get("/gold/customer-360").json()}
    assert victim not in rebuilt
    silver = {row["customer_id"] for row in wh.query("SELECT customer_id FROM silver.customers")}
    assert victim not in silver
    alerts = {row["customer_id"] for row in wh.query("SELECT customer_id FROM gold.fraud_summary")}
    assert victim not in alerts

    # And the incremental path must agree.
    assert c.post("/transform?incremental=true").status_code == 200
    again = {row["customer_id"] for row in c.get("/gold/customer-360").json()}
    assert victim not in again
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


def test_dashboard_fetchtabdata_matches_real_tabs():
    """Regression: fetchTabData branched on medallion/quality/metrics while the
    real tabs are overview|fraud|warehouse|erasure|ops, leaving three tabs
    permanently empty."""
    from eurostream.dashboard import get_dashboard_html

    html = get_dashboard_html()
    real_tabs = set(re.findall(r"setTab\('([a-z]+)'\)", html))
    assert real_tabs == {"overview", "fraud", "warehouse", "erasure", "ops"}

    fetch_branches = set(re.findall(r"currentTab === '([a-z]+)'", html))
    # Every branch that fetches must be a tab that exists, and every tab that
    # has data behind it must be fetched.
    assert fetch_branches <= real_tabs
    assert {"warehouse", "erasure", "ops"} <= fetch_branches

    # No stale branch names left behind from the old tab set.
    assert not ({"medallion", "quality", "metrics"} & fetch_branches)


def test_dashboard_defaults_to_same_origin():
    """A deployed instance must not be pointed at localhost:7860."""
    from eurostream.dashboard import get_dashboard_html

    html = get_dashboard_html()
    assert "window.location.origin" in html
    # The buggy expression itself, not the comment that mentions it.
    assert "origin.includes(':')" not in html
    assert "'http://localhost:7860'" not in html


def test_dashboard_surfaces_http_errors():
    from eurostream.dashboard import get_dashboard_html

    html = get_dashboard_html()
    assert "async apiJson" in html
    assert "Failed to load" in html


def test_stats_reports_lake_file_count(tmp_path):
    """`Lake: N Parquet` in the dashboard must be counted, not invented."""
    settings = Settings(
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "eurocart.duckdb",
        audit_log_path=tmp_path / "data" / "logs" / "audit.jsonl",
        metrics_path=tmp_path / "data" / "logs" / "metrics.jsonl",
        pii_manifest_path=tmp_path / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
    )
    app, bus, wh = _app(tmp_path)
    c = TestClient(app)
    stats = c.get("/stats").json()
    expected = (
        sum(1 for p in settings.lake_root.rglob("*.parquet") if p.is_file())
        if settings.lake_root.exists()
        else 0
    )
    assert stats["lake_files"] == expected
    bus.close()
    wh.close()
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


def _app_with_erasure(tmp_path):
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
        consumer=bus.consumer("erasure_requests", "lifecycle-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    app = create_app(erasure, metrics, settings, warehouse, bus, start_worker=False)
    return app, bus, warehouse, erasure


def test_erasure_request_status_lifecycle(tmp_path):
    """202 leaves a readable `queued` record; the audit row finishes it."""
    app, bus, wh, erasure = _app_with_erasure(tmp_path)
    c = TestClient(app)

    accepted = c.post("/erasure-requests", json={"customer_id": "cust_lifecycle"})
    assert accepted.status_code == 202
    rid = accepted.json()["request_id"]

    status = c.get(f"/erasure-requests/{rid}")
    assert status.status_code == 200
    body = status.json()
    assert body["status"] == "queued"
    assert body["customer_id"] == "cust_lifecycle"
    assert body["completed_at"] is None
    assert body["layers_touched"] == []

    from eurostream.models import ErasureRequested

    audit = erasure.execute(
        ErasureRequested(
            event_id=rid,
            occurred_at=time.time(),
            request_id=rid,
            customer_id="cust_lifecycle",
        )
    )
    assert audit.status == "completed"

    status = c.get(f"/erasure-requests/{rid}")
    assert status.status_code == 200
    body = status.json()
    assert body["status"] == "completed"
    assert body["completed_at"] is not None
    assert body["duration_seconds"] >= 0
    assert "suppression_registry" in body["layers_touched"]
    assert body["confirmation_hash"]
    # Once the audit row exists the intake entry is gone, not duplicated.
    assert erasure.pending_request(rid) is None
    bus.close()
    wh.close()


def test_erasure_request_status_unknown_id_is_404_problem_json(tmp_path):
    app, bus, wh, _ = _app_with_erasure(tmp_path)
    c = TestClient(app)
    r = c.get("/erasure-requests/nope-does-not-exist")
    assert r.status_code == 404
    assert r.headers["content-type"].startswith("application/problem+json")
    assert r.json()["title"] == "Not found"
    bus.close()
    wh.close()


def test_erasure_request_status_rejects_injection(tmp_path):
    app, bus, wh, _ = _app_with_erasure(tmp_path)
    c = TestClient(app)
    assert c.get("/erasure-requests/abc'%20OR%201=1--").status_code == 422
    assert c.get("/erasure-requests/" + "x" * 200).status_code == 422
    bus.close()
    wh.close()


def test_list_erasure_requests_filters_and_pages(tmp_path):
    app, bus, wh, erasure = _app_with_erasure(tmp_path)
    c = TestClient(app)

    for customer in ("cust_a", "cust_b", "cust_c"):
        assert c.post("/erasure-requests", json={"customer_id": customer}).status_code == 202

    everything = c.get("/erasure-requests")
    assert everything.status_code == 200
    assert everything.json()["total"] == 3
    assert len(everything.json()["items"]) == 3

    # Paging
    page = c.get("/erasure-requests", params={"limit": 2, "offset": 2})
    assert page.json()["total"] == 3
    assert len(page.json()["items"]) == 1

    # Status filter (all three are still queued: no worker runs here)
    queued = c.get("/erasure-requests", params={"status": "queued"})
    assert queued.json()["total"] == 3
    empty = c.get("/erasure-requests", params={"status": "failed"})
    assert empty.json()["total"] == 0

    # Customer filter
    one = c.get("/erasure-requests", params={"customer_id": "cust_b"})
    assert one.json()["total"] == 1
    assert one.json()["items"][0]["customer_id"] == "cust_b"

    # An impossible status is a validation error, not a silent empty list
    assert c.get("/erasure-requests", params={"status": "invented"}).status_code == 422
    assert c.get("/erasure-requests", params={"limit": 0}).status_code == 422

    # Executed requests join the list with their verdict
    c.post("/erasure-requests", json={"customer_id": "cust_d", "sync": True})
    done = c.get("/erasure-requests", params={"status": "completed"})
    assert done.json()["total"] >= 1
    assert done.json()["items"][0]["duration_seconds"] >= 0
    bus.close()
    wh.close()


def test_idempotent_erasure_intake_replays_the_first_response(tmp_path):
    app, bus, wh, erasure = _app_with_erasure(tmp_path)
    c = TestClient(app)
    key = "dsar-2026-000001"
    headers = {"Idempotency-Key": key}

    first = c.post("/erasure-requests", json={"customer_id": "cust_idem"}, headers=headers)
    second = c.post("/erasure-requests", json={"customer_id": "cust_idem"}, headers=headers)
    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers.get("Idempotent-Replay") == "true"
    assert first.headers.get("Idempotent-Replay") is None

    # Exactly one DSAR opened, not two.
    assert erasure.pending_requests()[0]["request_id"] == first.json()["request_id"]
    assert len(erasure.pending_requests()) == 1
    bus.close()
    wh.close()


def test_idempotent_sync_erasure_executes_once(tmp_path):
    app, bus, wh, erasure = _app_with_erasure(tmp_path)
    c = TestClient(app)
    headers = {"Idempotency-Key": "dsar-sync-000001"}
    body = {"customer_id": "cust_sync_idem", "sync": True}

    first = c.post("/erasure-requests", json=body, headers=headers)
    second = c.post("/erasure-requests", json=body, headers=headers)
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.headers.get("Idempotent-Replay") == "true"
    assert second.json()["confirmation_hash"] == first.json()["confirmation_hash"]
    # The cascade ran once: one audit row, one tombstone.
    rows = wh.query(
        "SELECT count(*) AS c FROM governance.erasure_audit_log WHERE customer_id = ?",
        ("cust_sync_idem",),
        local_only=True,
    )
    assert rows[0]["c"] == 1
    assert erasure.is_suppressed("cust_sync_idem")
    bus.close()
    wh.close()


def test_idempotency_key_reused_for_another_customer_is_409(tmp_path):
    app, bus, wh, _ = _app_with_erasure(tmp_path)
    c = TestClient(app)
    headers = {"Idempotency-Key": "dsar-2026-000002"}
    assert (
        c.post("/erasure-requests", json={"customer_id": "cust_one"}, headers=headers).status_code
        == 202
    )

    conflict = c.post("/erasure-requests", json={"customer_id": "cust_two"}, headers=headers)
    assert conflict.status_code == 409
    assert conflict.headers["content-type"].startswith("application/problem+json")
    assert conflict.json()["title"] == "Conflict"
    bus.close()
    wh.close()


def test_malformed_idempotency_key_is_422(tmp_path):
    app, bus, wh, _ = _app_with_erasure(tmp_path)
    c = TestClient(app)
    r = c.post(
        "/erasure-requests",
        json={"customer_id": "cust_x"},
        headers={"Idempotency-Key": "no spaces"},
    )
    assert r.status_code == 422
    short = c.post(
        "/erasure-requests",
        json={"customer_id": "cust_x"},
        headers={"Idempotency-Key": "abc"},
    )
    assert short.status_code == 422
    bus.close()
    wh.close()


def test_without_a_key_each_post_is_a_new_request(tmp_path):
    app, bus, wh, erasure = _app_with_erasure(tmp_path)
    c = TestClient(app)
    first = c.post("/erasure-requests", json={"customer_id": "cust_free"})
    second = c.post("/erasure-requests", json={"customer_id": "cust_free"})
    assert first.json()["request_id"] != second.json()["request_id"]
    assert len(erasure.pending_requests()) == 2
    bus.close()
    wh.close()

from __future__ import annotations

import time

from fastapi.testclient import TestClient

from eurostream.api import create_app
from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.warehouse import Warehouse


def _app(tmp_path, **overrides):
    settings = Settings(
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "eurocart.duckdb",
        audit_log_path=tmp_path / "data" / "logs" / "audit.jsonl",
        metrics_path=tmp_path / "data" / "logs" / "metrics.jsonl",
        pii_manifest_path=tmp_path / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
        **overrides,
    )
    bus = open_bus(tmp_path / "events.db")
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    metrics = Metrics(tmp_path / "metrics.jsonl")
    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "worker-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    app = create_app(erasure, metrics, settings, warehouse, bus)
    return app, bus, warehouse


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.1) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def wait_until_stable(value, quiet_for: float = 1.0, timeout: float = 10.0) -> int:
    """Wait until `value()` stops changing for `quiet_for` seconds."""
    deadline = time.time() + timeout
    last = value()
    stable_since = time.time()
    while time.time() < deadline:
        time.sleep(0.1)
        current = value()
        if current != last:
            last, stable_since = current, time.time()
        elif time.time() - stable_since >= quiet_for:
            return current
    return value()


def test_worker_scores_produce_without_a_manual_stream(tmp_path):
    """POST /produce should be enough: the worker drains and scores."""
    app, bus, wh = _app(tmp_path)
    broker = app.state.alert_broker
    client = TestClient(app)
    with client:
        assert client.post("/produce?events=25").status_code == 200
        scored = wait_until(lambda: broker.published > 0)
        assert scored, "background fraud worker never published an alert"

        # Ingested as well, not just published in-process.
        assert wait_until(
            lambda: wh.scalar("SELECT count(*) FROM bronze.fraud_alerts", local_only=True) > 0
        )
        stats = client.get("/stats").json()
        assert stats["fraud_worker"]["enabled"] is True
        assert stats["fraud_worker"]["running"] is True
        assert stats["fraud_worker"]["group"] == "api-fraud-stream"
        assert stats["erasure_worker"]["running"] is True
    bus.close()
    wh.close()


def test_worker_can_be_switched_off(tmp_path):
    app, bus, wh = _app(tmp_path, fraud_worker_enabled=False)
    broker = app.state.alert_broker
    client = TestClient(app)
    with client:
        assert client.post("/produce?events=10").status_code == 200
        time.sleep(1.5)  # long enough for a worker, had one been started
        assert broker.published == 0
        stats = client.get("/stats").json()
        assert stats["fraud_worker"]["enabled"] is False
        assert stats["fraud_worker"]["running"] is False
        # The manual trigger still works with the worker off.
        assert client.post("/stream?max_events=50").status_code == 200
        assert broker.published > 0
    bus.close()
    wh.close()


def test_stream_and_worker_share_one_consumer_group(tmp_path):
    """The endpoint must not re-score what the worker already consumed."""
    app, bus, wh = _app(tmp_path)
    broker = app.state.alert_broker
    client = TestClient(app)
    with client:
        assert client.post("/produce?events=25").status_code == 200
        # Let the worker finish; a growing counter would make the assertion
        # below meaningless rather than proving anything.
        # 2.5s of quiet spans a whole worker cycle (drain, idle, sleep), so a
        # still-draining worker cannot be mistaken for a finished one.
        first = wait_until_stable(lambda: broker.published, quiet_for=2.5, timeout=20)
        assert first > 0

        drained = client.post("/stream?max_events=100")
        assert drained.status_code == 200
        time.sleep(1.5)
        # Same consumer, same committed offsets: nothing left to score, so
        # the total must not have moved.
        assert broker.published == first
        assert drained.json()["alerts_emitted"] == 0

        warehouse_rows = wh.scalar("SELECT count(*) FROM bronze.fraud_alerts", local_only=True)
        assert warehouse_rows == first
    bus.close()
    wh.close()


def test_stats_reports_workers_as_stopped_outside_lifespan(tmp_path):
    app, bus, wh = _app(tmp_path)
    client = TestClient(app)
    # No lifespan without the context manager: nothing is running, and the
    # API says so instead of claiming the configured intent.
    stats = client.get("/stats").json()
    assert stats["fraud_worker"]["enabled"] is True
    assert stats["fraud_worker"]["running"] is False
    assert stats["erasure_worker"]["running"] is False
    bus.close()
    wh.close()

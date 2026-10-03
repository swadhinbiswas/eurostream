from __future__ import annotations

import io
import json
import logging
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eurostream import logging as es_logging
from eurostream.api import create_app
from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings
from eurostream.governance.erasure import ErasureService
from eurostream.logging import (
    ContextTextFormatter,
    JsonFormatter,
    configure_logging,
    get_request_id,
    is_secret_key,
    reset_request_id,
    set_request_id,
)
from eurostream.metrics import Metrics
from eurostream.warehouse import Warehouse


def _record(message: str, **extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="eurostream.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    record.__dict__.update(extra)
    return record


def test_json_formatter_is_one_parseable_object() -> None:
    token = set_request_id("rid-42")
    try:
        line = JsonFormatter().format(_record("hello"))
    finally:
        reset_request_id(token)
    payload = json.loads(line)
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "eurostream.test"
    assert payload["request_id"] == "rid-42"
    assert payload["ts"].endswith("Z")


def test_json_formatter_redacts_secret_keys() -> None:
    record = _record(
        "connecting",
        password="hunter2",
        turso_auth_token="tok_abc",
        customer_id="cust_1",
    )
    payload = json.loads(JsonFormatter().format(record))
    assert payload["password"] == es_logging.REDACTED
    assert payload["turso_auth_token"] == es_logging.REDACTED
    assert payload["customer_id"] == "cust_1"


def test_json_formatter_serialises_non_json_values() -> None:
    record = _record("values", lake_root=Path("/var/lib/eurostream/lake"), error=ValueError("boom"))
    payload = json.loads(JsonFormatter().format(record))
    assert payload["lake_root"] == "/var/lib/eurostream/lake"
    assert payload["error"] == "boom"


def test_secret_key_detection() -> None:
    assert is_secret_key("KAFKA_PASSWORD")
    assert is_secret_key("api_token")
    assert is_secret_key("pii_salt")
    assert not is_secret_key("customer_id")


def test_text_formatter_carries_the_request_id() -> None:
    token = set_request_id("rid-7")
    try:
        line = ContextTextFormatter("%(message)s [%(request_id)s]").format(_record("hi"))
    finally:
        reset_request_id(token)
    assert line == "hi [rid-7]"


def test_text_formatter_defaults_to_dash_outside_a_request() -> None:
    line = ContextTextFormatter("%(message)s [%(request_id)s]").format(_record("hi"))
    assert line == "hi [-]"


def test_configure_logging_is_idempotent() -> None:
    root = configure_logging("INFO", "text")
    configure_logging("DEBUG", "json")
    mine = [h for h in root.handlers if h in es_logging.installed_handlers()]
    assert len(mine) == 1
    assert isinstance(mine[0].formatter, JsonFormatter)
    assert root.level == logging.DEBUG


def test_configure_logging_rejects_an_unknown_level() -> None:
    with pytest.raises(ValueError, match="invalid log level"):
        configure_logging("LOUD", "text")


def _app(tmp_path: Path):
    settings = Settings(
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "eurocart.duckdb",
        audit_log_path=tmp_path / "data" / "logs" / "audit.jsonl",
        metrics_path=tmp_path / "data" / "logs" / "metrics.jsonl",
        pii_manifest_path=tmp_path / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
        log_format="json",
    )
    bus = open_bus(tmp_path / "events.db")
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    metrics = Metrics(tmp_path / "metrics.jsonl")
    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "log-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    return create_app(erasure, metrics, settings, warehouse, bus), bus, warehouse


def test_request_logs_carry_the_client_request_id(tmp_path: Path) -> None:
    app, bus, warehouse = _app(tmp_path)

    buf = io.StringIO()
    configure_logging("INFO", "json", stream=buf)
    with TestClient(app) as client:
        response = client.post(
            "/erasure-requests",
            json={"customer_id": "cust_logtest", "sync": True},
            headers={"X-Request-ID": "rid-from-client"},
        )
    assert response.status_code == 200

    lines = [json.loads(line) for line in buf.getvalue().splitlines() if line.startswith("{")]
    correlated = [row for row in lines if row.get("request_id") == "rid-from-client"]
    assert correlated, "no log line carried the client-supplied request id"
    assert any(row.get("path") == "/erasure-requests" for row in correlated)
    assert any("erasure completed" in str(row.get("message")) for row in correlated)
    # Outside the request scope nothing may claim that id.
    assert get_request_id() is None
    bus.close()
    warehouse.close()


def test_a_generated_request_id_is_returned_in_the_header(tmp_path: Path) -> None:
    app, bus, warehouse = _app(tmp_path)
    with TestClient(app) as client:
        response = client.get("/health")
    assert response.headers["X-Request-ID"]
    bus.close()
    warehouse.close()

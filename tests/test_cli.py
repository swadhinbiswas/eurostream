from __future__ import annotations

import json as _json

from typer.testing import CliRunner

from eurostream.cli import app
from eurostream.portal import build_portal_html

runner = CliRunner()


def test_cli_contracts(tmp_path):
    out = tmp_path / "contracts.json"
    res = runner.invoke(app, ["contracts", "--out", str(out)])
    assert res.exit_code == 0
    assert out.exists()


def test_cli_produce_and_transform(tmp_path, monkeypatch):
    monkeypatch.setenv("EUROSTREAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EUROSTREAM_WAREHOUSE_PATH", str(tmp_path / "data" / "warehouse.duckdb"))
    monkeypatch.setenv("EUROSTREAM_LAKE_ROOT", str(tmp_path / "data" / "lake"))
    monkeypatch.setenv("EUROSTREAM_AUDIT_LOG_PATH", str(tmp_path / "data" / "audit.jsonl"))
    monkeypatch.setenv("EUROSTREAM_METRICS_PATH", str(tmp_path / "data" / "metrics.jsonl"))

    # 1. Produce
    res_produce = runner.invoke(app, ["produce", "--events", "5"])
    assert res_produce.exit_code == 0
    assert "produced 5 events" in res_produce.stdout

    # 2. Stream
    res_stream = runner.invoke(app, ["stream", "--max-events", "5"])
    assert res_stream.exit_code == 0

    # 3. Transform
    res_transform = runner.invoke(app, ["transform", "--incremental"])
    assert res_transform.exit_code == 0
    assert "quality_gate: ok" in res_transform.stdout


def test_portal_html():
    html = build_portal_html()
    assert "<!DOCTYPE html>" in html
    assert "EuroStream" in html


def _point_cli_at(tmp_path, monkeypatch):
    monkeypatch.setenv("EUROSTREAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EUROSTREAM_WAREHOUSE_PATH", str(tmp_path / "data" / "warehouse.duckdb"))
    monkeypatch.setenv("EUROSTREAM_LAKE_ROOT", str(tmp_path / "data" / "lake"))
    monkeypatch.setenv("EUROSTREAM_AUDIT_LOG_PATH", str(tmp_path / "data" / "audit.jsonl"))
    monkeypatch.setenv("EUROSTREAM_METRICS_PATH", str(tmp_path / "data" / "metrics.jsonl"))
    return tmp_path / "data" / "audit.jsonl"


def test_cli_verify_audit_passes_after_a_real_erasure(tmp_path, monkeypatch):
    audit_path = _point_cli_at(tmp_path, monkeypatch)

    erased = runner.invoke(app, ["erase", "cust_999999"])
    assert erased.exit_code == 0, erased.output

    res = runner.invoke(app, ["verify-audit"])
    assert res.exit_code == 0, res.output
    assert "audit chain is intact" in res.output
    assert str(audit_path) in res.output
    assert "1 chained" in res.output


def test_cli_verify_audit_json_is_machine_readable(tmp_path, monkeypatch):
    _point_cli_at(tmp_path, monkeypatch)
    assert runner.invoke(app, ["erase", "cust_123123"]).exit_code == 0

    res = runner.invoke(app, ["verify-audit", "--json"])
    assert res.exit_code == 0, res.output
    payload = _json.loads(res.stdout)
    assert payload["ok"] is True
    assert payload["errors"] == []
    assert payload["entries"] == 1
    assert payload["hashed"] == 1
    assert payload["last_seq"] == 1
    assert payload["chain"]["seq"] == 1
    assert payload["tip"] == payload["chain"]["tip"]


def test_cli_verify_audit_fails_on_an_edited_record(tmp_path, monkeypatch):
    audit_path = _point_cli_at(tmp_path, monkeypatch)
    assert runner.invoke(app, ["erase", "cust_555555"]).exit_code == 0

    lines = audit_path.read_text().splitlines()
    record = _json.loads(lines[0])
    record["status"] = "failed"  # rewrite history
    audit_path.write_text(_json.dumps(record) + "\n")

    res = runner.invoke(app, ["verify-audit"])
    assert res.exit_code == 1
    assert "BROKEN" in res.output
    assert "hash mismatch" in res.output

    json_res = runner.invoke(app, ["verify-audit", "--json"])
    assert json_res.exit_code == 1
    payload = _json.loads(json_res.stdout)
    assert payload["ok"] is False
    assert any("hash mismatch" in e for e in payload["errors"])


def test_cli_verify_audit_missing_file_exits_one(tmp_path, monkeypatch):
    _point_cli_at(tmp_path, monkeypatch)
    res = runner.invoke(app, ["verify-audit", "--path", str(tmp_path / "never-written.jsonl")])
    assert res.exit_code == 1
    assert "does not exist" in res.output


def test_cli_verify_audit_strict_rejects_unhashed_records(tmp_path, monkeypatch):
    audit_path = _point_cli_at(tmp_path, monkeypatch)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        _json.dumps({"request_id": "old-1", "customer_id": "cust_old", "status": "completed"})
        + "\n"
    )

    lenient = runner.invoke(app, ["verify-audit", "--no-cross-check"])
    assert lenient.exit_code == 0, lenient.output
    assert "predate the hash chain" in lenient.output
    assert "legacy" in lenient.output

    strict = runner.invoke(app, ["verify-audit", "--no-cross-check", "--strict"])
    assert strict.exit_code == 1
    assert "strict mode" in strict.output


def test_cli_verify_audit_can_skip_the_warehouse_copy(tmp_path, monkeypatch):
    _point_cli_at(tmp_path, monkeypatch)
    assert runner.invoke(app, ["erase", "cust_777777"]).exit_code == 0

    res = runner.invoke(app, ["verify-audit", "--no-cross-check"])
    assert res.exit_code == 0, res.output
    assert "scope: file only" in res.output
    assert "cross_checked" not in res.output  # human mode stays human

from __future__ import annotations

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

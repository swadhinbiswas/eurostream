from __future__ import annotations

import json
import shutil
from pathlib import Path

from typer.testing import CliRunner

from eurostream.chaos import SCENARIOS, discard_sandbox, new_sandbox, run_scenarios
from eurostream.cli import app

runner = CliRunner()


def _point_at(tmp_path: Path, monkeypatch) -> None:
    """Point the deployment's own paths at an empty directory: if any drill
    opened them, this test would see the directory appear."""
    monkeypatch.setenv("EUROSTREAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EUROSTREAM_WAREHOUSE_PATH", str(tmp_path / "data" / "warehouse.duckdb"))
    monkeypatch.setenv("EUROSTREAM_LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("EUROSTREAM_AUDIT_LOG_PATH", str(tmp_path / "data" / "audit.jsonl"))
    monkeypatch.setenv("EUROSTREAM_METRICS_PATH", str(tmp_path / "data" / "metrics.jsonl"))


def _combined(result) -> str:
    parts = [result.output]
    try:
        parts.append(result.stderr)
    except (ValueError, AttributeError):  # pragma: no cover - depends on click version
        pass
    return "".join(parts)


def test_every_scenario_runs_and_its_guardrail_holds(tmp_path):
    results = run_scenarios(tmp_path / "sandbox")

    assert [result.name for result in results] == list(SCENARIOS)
    assert len(results) >= 6
    for result in results:
        assert result.ok, f"{result.name} did not hold: {result.detail}"
        assert result.detail, f"{result.name} passed without saying what happened"


def test_each_scenario_gets_its_own_directory(tmp_path):
    sandbox = tmp_path / "sandbox"
    run_scenarios(sandbox)
    assert {path.name for path in sandbox.iterdir()} == set(SCENARIOS)


def test_a_scenario_that_raises_is_a_failed_drill_not_a_pass(tmp_path, monkeypatch):
    def boom(root: Path) -> tuple[bool, str]:
        raise RuntimeError("kaboom")

    monkeypatch.setitem(SCENARIOS, "boom", boom)
    results = run_scenarios(tmp_path, ["boom"])

    assert len(results) == 1
    assert results[0].ok is False
    assert "scenario crashed" in results[0].detail
    assert "kaboom" in results[0].detail


def test_cli_chaos_reports_every_drill_and_leaves_the_deployment_alone(tmp_path, monkeypatch):
    _point_at(tmp_path, monkeypatch)

    res = runner.invoke(app, ["chaos", "--json"])
    assert res.exit_code == 0, _combined(res)

    payload = json.loads(res.stdout)
    assert payload["held"] == len(SCENARIOS)
    assert payload["failed"] == 0
    assert [r["name"] for r in payload["results"]] == list(SCENARIOS)
    assert all(r["ok"] for r in payload["results"])
    assert payload["kept"] is False

    # The sandbox is somewhere else entirely, and the paths this deployment
    # actually uses were never created.
    sandbox = Path(payload["sandbox"])
    assert not str(sandbox).startswith(str(tmp_path))
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "lake").exists()
    # Default behaviour cleans up after itself.
    assert not sandbox.exists()


def test_cli_chaos_keep_retains_the_sandbox(tmp_path, monkeypatch):
    _point_at(tmp_path, monkeypatch)

    res = runner.invoke(app, ["chaos", "--keep", "--json"])
    assert res.exit_code == 0, _combined(res)
    payload = json.loads(res.stdout)
    assert payload["kept"] is True
    sandbox = Path(payload["sandbox"])
    try:
        assert sandbox.exists()
        assert {path.name for path in sandbox.iterdir()} == set(SCENARIOS)
    finally:
        shutil.rmtree(sandbox, ignore_errors=True)


def test_cli_chaos_runs_one_scenario_when_asked(tmp_path, monkeypatch):
    _point_at(tmp_path, monkeypatch)

    res = runner.invoke(app, ["chaos", "--scenario", "audit-tamper", "--json"])
    assert res.exit_code == 0, _combined(res)
    payload = json.loads(res.stdout)
    assert [r["name"] for r in payload["results"]] == ["audit-tamper"]
    assert payload["results"][0]["ok"] is True
    assert "verifier reported" in payload["results"][0]["detail"]


def test_cli_chaos_rejects_an_unknown_scenario_by_name(tmp_path, monkeypatch):
    _point_at(tmp_path, monkeypatch)

    res = runner.invoke(app, ["chaos", "--scenario", "definitely-not-a-drill"])
    assert res.exit_code == 2
    output = _combined(res)
    assert "unknown scenario" in output
    assert "definitely-not-a-drill" in output
    # The operator is told what the real names are instead of guessing.
    for name in SCENARIOS:
        assert name in output


def test_cli_chaos_list_describes_the_drills_without_running_them(tmp_path, monkeypatch):
    _point_at(tmp_path, monkeypatch)

    res = runner.invoke(app, ["chaos", "--list"])
    assert res.exit_code == 0, _combined(res)
    for name, fn in SCENARIOS.items():
        assert name in res.output
        assert (fn.__doc__ or "").strip() in res.output
    # Listing is not running: no sandbox, no report.
    assert "guardrails held" not in res.output


def test_sandbox_helper_creates_and_removes_a_directory(tmp_path):
    sandbox = new_sandbox()
    try:
        assert sandbox.is_dir()
        assert sandbox.name.startswith("eurostream-chaos-")
    finally:
        discard_sandbox(sandbox)
    assert not sandbox.exists()

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from typer.testing import CliRunner

from eurostream.cli import app
from eurostream.loadgen import percentile, run_load, served

runner = CliRunner()


# ------------------------------------------------------------- percentile


def test_percentile_is_nearest_rank_and_never_interpolates():
    ten = [float(i) for i in range(1, 11)]
    assert percentile(ten, 50) == 5.0
    assert percentile(ten, 95) == 10.0
    assert percentile(ten, 99) == 10.0
    assert percentile(ten, 100) == 10.0
    assert percentile(ten, 0) == 1.0
    assert percentile([42.0], 95) == 42.0
    assert percentile([], 95) == 0.0


# ------------------------------------------------------------- http target


class _Handler(BaseHTTPRequestHandler):
    """A stand-in API: mostly 200, a failing endpoint, and a slow one."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's name
        if self.path == "/boom":
            body, status = b'{"detail": "boom"}', 500
        elif self.path == "/slow":
            time.sleep(1.0)
            body, status = b'{"status": "ok"}', 200
        else:
            body, status = b'{"status": "ok"}', 200
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:  # keep the test output clean
        return


@pytest.fixture()
def target() -> str:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_run_load_reports_percentiles_and_statuses(target):
    report = run_load(target, requests=40, concurrency=8, paths=("/ok", "/other"))

    assert report.requests == 40
    assert len(report.endpoints) == 2
    assert report.status_counts == {200: 40}
    assert report.server_errors == 0
    assert report.rate_limited == 0
    assert report.passed is True
    assert report.throughput > 0
    assert report.duration_s > 0
    for endpoint in report.endpoints:
        assert endpoint.total == 20
        assert endpoint.p50_ms > 0
        assert endpoint.p95_ms >= endpoint.p50_ms
        assert endpoint.max_ms >= endpoint.p99_ms


def test_run_load_fails_the_run_on_server_errors(target):
    report = run_load(target, requests=16, concurrency=4, paths=("/ok", "/boom"))

    assert report.status_counts[500] == 8
    assert report.server_errors == 8
    assert report.passed is False
    boom = next(e for e in report.endpoints if e.path == "/boom")
    assert boom.server_errors == 8
    assert boom.ok == 0


def test_run_load_counts_timeouts_as_failures(target):
    report = run_load(target, requests=4, concurrency=4, paths=("/slow",), timeout=0.2)

    assert report.timeouts == 4
    assert report.passed is False
    assert report.endpoints[0].total == 4


def test_run_load_rejects_an_empty_path_list(target):
    with pytest.raises(ValueError, match="at least one path"):
        run_load(target, paths=())


# ------------------------------------------------------------------- cli


def _point_at(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("EUROSTREAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EUROSTREAM_WAREHOUSE_PATH", str(tmp_path / "data" / "warehouse.duckdb"))
    monkeypatch.setenv("EUROSTREAM_LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("EUROSTREAM_AUDIT_LOG_PATH", str(tmp_path / "data" / "audit.jsonl"))
    monkeypatch.setenv("EUROSTREAM_METRICS_PATH", str(tmp_path / "data" / "metrics.jsonl"))


def test_cli_load_test_against_an_existing_server(target):
    res = runner.invoke(
        app,
        ["load-test", "--url", target, "--requests", "20", "--concurrency", "4"],
    )
    assert res.exit_code == 0, res.output
    assert "load test: 20 requests" in res.output
    assert "throughput" in res.output
    assert "verdict: PASS" in res.output


def test_cli_load_test_exits_one_on_a_5xx(target):
    res = runner.invoke(
        app,
        [
            "load-test",
            "--url",
            target,
            "--requests",
            "16",
            "--concurrency",
            "4",
            "--path",
            "/boom",
            "--json",
        ],
    )
    assert res.exit_code == 1, res.output
    payload = json.loads(res.stdout)
    assert payload["passed"] is False
    assert payload["server_errors"] == 16
    assert payload["endpoints"][0]["path"] == "/boom"


def test_cli_load_test_starts_and_stops_its_own_server(tmp_path, monkeypatch):
    _point_at(tmp_path, monkeypatch)

    res = runner.invoke(app, ["load-test", "--requests", "24", "--concurrency", "8", "--json"])
    assert res.exit_code == 0, res.output

    payload = json.loads(res.stdout)
    assert payload["requests"] == 24
    assert payload["passed"] is True
    assert payload["concurrency"] == 8
    assert payload["base_url"].startswith("http://127.0.0.1:")
    # The mix includes a warehouse read, so the app really did serve.
    assert {e["path"] for e in payload["endpoints"]} == {
        "/health",
        "/health/ready",
        "/gold/customer-360",
        "/metrics",
    }

    # The context manager shut the server down behind us.
    with pytest.raises(httpx.HTTPError):
        httpx.get(f"{payload['base_url']}/health", timeout=2)


def test_served_context_yields_a_working_url_and_frees_the_port(tmp_path, monkeypatch):
    _point_at(tmp_path, monkeypatch)

    with served() as base:
        response = httpx.get(f"{base}/health", timeout=5)
        assert response.json()["status"] == "ok"
        in_use = base

    with pytest.raises(httpx.HTTPError):
        httpx.get(f"{in_use}/health", timeout=2)

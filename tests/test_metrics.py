from __future__ import annotations

import time

import pytest

from eurostream.metrics import Metrics


def test_metrics_counters_and_snapshot(tmp_path):
    m = Metrics(tmp_path / "m.jsonl")
    m.incr("x")
    m.incr("x", 2)
    assert m.snapshot()["counters"]["x"] == 3


def test_metrics_gauge_and_histogram(tmp_path):
    m = Metrics(tmp_path / "m.jsonl")
    m.set_gauge("g", 1.5)
    m.observe("lat", 0.1)
    m.observe("lat", 0.3)
    snap = m.snapshot()
    assert snap["gauges"]["g"] == 1.5
    hist = snap["histograms"]["lat"]
    assert hist["count"] == 2
    assert hist["sum"] == 0.4


def test_metrics_flush_and_render(tmp_path):
    path = tmp_path / "m.jsonl"
    m = Metrics(path)
    m.incr("c", 5)
    m.flush()
    assert path.exists()
    text = path.read_text()
    assert "c" in text
    prom = m.render_prometheus()
    # Counters are suffixed _total and carry HELP/TYPE headers per the
    # Prometheus text exposition format.
    assert "# HELP eurostream_c_total c" in prom
    assert "# TYPE eurostream_c_total counter" in prom
    assert "eurostream_c_total 5" in prom
    # The baseline `up` gauge makes an empty scrape distinguishable from a
    # dead exporter.
    assert "# TYPE eurostream_up gauge" in prom
    assert "eurostream_up 1" in prom
    # Every scrape body ends in a newline, as the format requires.
    assert prom.endswith("\n")


def test_slo_window_counts_5xx_but_not_4xx(tmp_path):
    m = Metrics(tmp_path / "m.jsonl", slo_target=0.99, slo_window_seconds=60)
    base = time.time()
    m.record_request(200, ts=base)
    m.record_request(404, ts=base + 1)
    m.record_request(503, ts=base + 2)
    slo = m.slo()
    assert slo["requests"] == 3
    assert slo["errors"] == 1
    # 1/3 observed against a 1% allowance is 33x the budget.
    assert slo["success_ratio"] == pytest.approx(2 / 3)
    assert slo["burn_rate"] == pytest.approx((1 / 3) / 0.01)
    assert slo["budget_remaining"] == 0.0


def test_slo_window_expires_old_requests(tmp_path):
    m = Metrics(tmp_path / "m.jsonl", slo_target=0.99, slo_window_seconds=60)
    base = time.time()
    m.record_request(500, ts=base)
    m.record_request(500, ts=base + 120)  # two minutes later: the first is out
    slo = m.slo()
    assert slo["requests"] == 1
    assert slo["errors"] == 1


def test_slo_with_no_traffic_has_a_full_budget(tmp_path):
    m = Metrics(tmp_path / "m.jsonl")
    slo = m.slo()
    assert slo["requests"] == 0
    assert slo["success_ratio"] == 1.0
    assert slo["burn_rate"] == 0.0
    assert slo["budget_remaining"] == 1.0


def test_slo_is_clean_after_only_successful_requests(tmp_path):
    m = Metrics(tmp_path / "m.jsonl", slo_target=0.99)
    for _ in range(10):
        m.record_request(200)
    slo = m.slo()
    assert slo["burn_rate"] == 0.0
    assert slo["budget_remaining"] == 1.0


def _gauge(prom: str, name: str) -> float:
    for line in prom.splitlines():
        if line.startswith(name + "{"):
            return float(line.rsplit(" ", 1)[1])
    raise AssertionError(f"{name} missing from exposition")


def test_prometheus_exposes_the_error_budget(tmp_path):
    m = Metrics(tmp_path / "m.jsonl", slo_target=0.99, slo_window_seconds=300)
    m.record_request(200)
    m.record_request(500)
    prom = m.render_prometheus()
    assert "# HELP eurostream_http_success_ratio" in prom
    assert "# TYPE eurostream_http_success_ratio gauge" in prom
    assert 'eurostream_http_success_ratio{window="300s"} 0.5' in prom
    # Whole numbers render without a decimal point, as the exposition format
    # allows: 0.0 -> 0. Floats are rounded rather than left as 49.99999999999996.
    burn = _gauge(prom, "eurostream_http_error_budget_burn_rate")
    assert burn == pytest.approx(50)
    assert _gauge(prom, "eurostream_http_error_budget_remaining") == 0
    assert m.snapshot()["slo"]["requests"] == 2

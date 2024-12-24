from __future__ import annotations

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
    assert "# TYPE c counter" in prom
    assert "c 5" in prom

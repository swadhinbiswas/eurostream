from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from pathlib import Path


class Metrics:
    """Tiny prometheus-shaped metric sink. Counters and histograms are kept
    in memory and snapshotted to a JSONL file so local observability works
    without spinning up a whole Prometheus+Grafana stack. The JSONL output
    is intentionally shaped like scrape output for a later swap to a real
    exporter."""

    def __init__(self, path: Path | None = None) -> None:
        self._counters: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = defaultdict(float)
        self._histograms: dict[str, list[float]] = defaultdict(list)
        self._lock = threading.Lock()
        self._path = path

    def incr(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] += amount

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            self._histograms[name].append(value)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {
                    k: {"count": len(v), "sum": sum(v), "max": max(v) if v else 0.0}
                    for k, v in self._histograms.items()
                },
            }

    def flush(self) -> None:
        if self._path is None:
            return
        with self._lock:
            snapshot = {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {
                    k: {"count": len(v), "sum": sum(v), "max": max(v) if v else 0.0}
                    for k, v in self._histograms.items()
                },
            }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        row = {"ts": time.time(), **snapshot}
        # Lock file append to avoid interleaved JSONL under concurrency.
        with self._lock:
            with self._path.open("a") as fh:
                fh.write(json.dumps(row) + "\n")

    def render_prometheus(self) -> str:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histograms = {k: list(v) for k, v in self._histograms.items()}
        lines: list[str] = []
        for name, count in counters.items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {count}")
        for name, gauge in gauges.items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {gauge}")
        for name, hist in histograms.items():
            if not hist:
                continue
            lines.append(f"# TYPE {name} summary")
            lines.append(f"{name}_sum {sum(hist)}")
            lines.append(f"{name}_count {len(hist)}")
        return "\n".join(lines)

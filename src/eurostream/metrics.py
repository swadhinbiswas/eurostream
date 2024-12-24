from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# Every metric this process exposes is namespaced so it never collides with
# another exporter on the same scrape job.
NAMESPACE = "eurostream"

_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_LABEL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# HELP text for the metrics other people will actually be asked to alert on.
_HELP = {
    "up": "Whether the process is running.",
    "build_info": "Build information for this EuroStream process.",
    "http_requests_total": "HTTP requests handled, by method, path and status.",
    "http_request_duration_seconds": "Wall-clock time spent serving HTTP requests.",
    "events_produced": "Source events written to the bus.",
    "erasure_requested": "Right-to-erasure requests accepted.",
    "erasure_completed": "Right-to-erasure cascades completed.",
    "erasure_failed": "Right-to-erasure cascades that raised an error.",
    "erasure_sla_breach": "Erasures that finished after the documented SLA.",
    "erasure_latency": "End-to-end latency of a right-to-erasure request.",
    "erasure_queue_depth": "Erasure requests waiting to be executed.",
    "malformed_erasure_requests": "Erasure records dropped as unparseable.",
}


@dataclass
class _Histogram:
    """Running aggregates instead of raw samples: a long-lived worker must not
    grow a list of one entry per erasure forever."""

    count: int = 0
    total: float = 0.0
    max_value: float = 0.0

    def observe(self, value: float) -> None:
        self.count += 1
        self.total += value
        if value > self.max_value:
            self.max_value = value


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _series_key(name: str, labels: dict[str, str] | None) -> str:
    """Build a stable ``name{k="v"}`` identifier.

    The full identifier is the dict key, so labelled series stay separate in
    memory, in the JSON snapshot and in the exposition output.
    """
    if not name or not _NAME_RE.match(name):
        raise ValueError(f"invalid metric name: {name!r}")
    if not labels:
        return name
    parts = []
    for key in sorted(labels):
        if not _LABEL_RE.match(key):
            raise ValueError(f"invalid label name: {key!r}")
        parts.append(f'{key}="{_escape_label_value(str(labels[key]))}"')
    return f"{name}{{{','.join(parts)}}}"


def _split_key(key: str) -> tuple[str, str]:
    if "{" not in key:
        return key, ""
    name, _, rest = key.partition("{")
    return name, "{" + rest


def _exported_name(metric_name: str, kind: str) -> str:
    name = f"{NAMESPACE}_{metric_name}"
    if kind == "counter" and not name.endswith("_total"):
        name += "_total"
    return name


class Metrics:
    """In-process metric sink with a Prometheus text exposition endpoint.

    Counters, gauges and histograms are held in memory and snapshotted to a
    JSONL file so local observability works without a Prometheus+Grafana
    stack. ``render_prometheus`` emits the standard exposition format —
    ``# HELP``/``# TYPE``, a namespace prefix, ``_total`` on counters and a
    trailing newline — so the endpoint can be scraped as-is.
    """

    def __init__(self, path: Path | None = None) -> None:
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, _Histogram] = {}
        self._lock = threading.Lock()
        self._path = path
        # Baseline series: a scrape must never come back empty, even before
        # the first request, otherwise monitoring reads it as a dead exporter.
        self._gauges[_series_key("up", None)] = 1.0

    def incr(self, name: str, amount: int = 1, labels: dict[str, str] | None = None) -> None:
        key = _series_key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount

    def set_gauge(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        key = _series_key(name, labels)
        with self._lock:
            self._gauges[key] = float(value)

    def observe(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        key = _series_key(name, labels)
        with self._lock:
            self._histograms.setdefault(key, _Histogram()).observe(value)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {
                    k: {"count": h.count, "sum": h.total, "max": h.max_value}
                    for k, h in self._histograms.items()
                },
            }

    def flush(self) -> None:
        if self._path is None:
            return
        snapshot = self.snapshot()
        row = {"ts": time.time(), **snapshot}
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # File I/O happens outside the lock so a slow disk never blocks incr().
        try:
            with self._path.open("a") as fh:
                fh.write(json.dumps(row) + "\n")
        except OSError:
            logger.exception("could not append metric snapshot to %s", self._path)

    def render_prometheus(self) -> str:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histograms = {k: (h.count, h.total, h.max_value) for k, h in self._histograms.items()}

        lines: list[str] = []
        emitted: set[str] = set()

        def header(metric_name: str, kind: str, target: str) -> None:
            if target in emitted:
                return
            emitted.add(target)
            lines.append(f"# HELP {target} {_HELP.get(metric_name, metric_name)}")
            lines.append(f"# TYPE {target} {kind}")

        for key in sorted(counters):
            metric_name, labelset = _split_key(key)
            target = _exported_name(metric_name, "counter")
            header(metric_name, "counter", target)
            lines.append(f"{target}{labelset} {counters[key]}")

        for key in sorted(gauges):
            metric_name, labelset = _split_key(key)
            target = _exported_name(metric_name, "gauge")
            header(metric_name, "gauge", target)
            value = gauges[key]
            rendered = int(value) if float(value).is_integer() else value
            lines.append(f"{target}{labelset} {rendered}")

        for key in sorted(histograms):
            metric_name, labelset = _split_key(key)
            count, total, _ = histograms[key]
            target = _exported_name(metric_name, "summary")
            header(metric_name, "summary", target)
            lines.append(f"{target}_sum{labelset} {total}")
            lines.append(f"{target}_count{labelset} {count}")

        return "\n".join(lines) + "\n"

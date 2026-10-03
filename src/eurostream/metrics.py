from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from eurostream import __version__

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
    "rate_limited_requests": "Mutating requests rejected by the per-client rate limiter.",
    "alert_streams_opened": "Server-Sent Event connections opened on the live alert feed.",
    "alert_stream_events": "Alert frames delivered over the live SSE feed.",
    "idempotent_replays": "Erasure POSTs answered from the idempotency cache.",
    "malformed_events": "Bus records dropped as unparseable.",
    "suppressed_for_erasure": ("Fraud scores suppressed because the customer asked to be erased."),
    "erasure_worker_failures": "Erasure executions that raised and were dead-lettered.",
    "erasure_lake_export_failed": (
        "Completed erasures whose lake re-export failed (the cascade still finished)."
    ),
    "idempotency_conflicts": "Erasure POSTs rejected for reusing a key on another customer.",
    "http_success_ratio": "Share of requests in the rolling window that were not 5xx.",
    "http_error_budget_burn_rate": (
        "Observed 5xx rate divided by the rate the SLO allows; "
        "1.0 is spending the budget exactly as planned, above 1.0 faster."
    ),
    "http_error_budget_remaining": (
        "Share of the rolling window's error budget still unspent, from 1 down to 0."
    ),
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

    def __init__(
        self,
        path: Path | None = None,
        *,
        slo_target: float = 0.99,
        slo_window_seconds: float = 300.0,
    ) -> None:
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, _Histogram] = {}
        self._lock = threading.Lock()
        self._path = path
        # Rolling request outcomes for the SLO. Individual events (not just a
        # count) are kept so the window can expire: a counter cannot say how
        # the last five minutes went, only how all time went.
        self._requests: deque[tuple[float, int]] = deque()
        self._slo_target = float(slo_target)
        self._slo_window = float(slo_window_seconds)
        # Baseline series: a scrape must never come back empty, even before
        # the first request, otherwise monitoring reads it as a dead exporter.
        self.set_gauge("up", 1.0)
        # Prometheus convention: build info is a gauge pinned at 1 with the
        # build's identity in labels, so `eurostream_build_info` can be used
        # to join a dashboard or an alert to the version that emitted it.
        self.set_gauge("build_info", 1.0, {"version": __version__})

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

    def record_request(self, status: int, *, ts: float | None = None) -> None:
        """Feed one HTTP outcome into the SLO window.

        5xx is an availability error; 4xx is the caller's problem and must not
        spend the budget (otherwise a client sending bad IDs looks like an
        outage).
        """
        now = time.time() if ts is None else ts
        with self._lock:
            self._requests.append((now, 1 if status >= 500 else 0))
            self._prune_locked(now)

    def _prune_locked(self, now: float) -> None:
        # The window ends at the later of wall-clock and the newest sample, so
        # injected or clock-skewed timestamps cannot expire live traffic early.
        if self._requests:
            now = max(now, self._requests[-1][0])
        cutoff = now - self._slo_window
        requests = self._requests
        while requests and requests[0][0] < cutoff:
            requests.popleft()

    def slo(self) -> dict[str, float]:
        """SLO state for the rolling window.

        ``burn_rate`` is observed error rate over the rate the objective
        allows: 1.0 means the budget is being spent exactly as planned, 2.0
        twice as fast (an hour of budget gone in thirty minutes). No traffic
        is a full budget and a perfect ratio, not a divide-by-zero.
        """
        with self._lock:
            self._prune_locked(time.time())
            total = len(self._requests)
            errors = sum(error for _, error in self._requests)
        allowed_error_rate = 1.0 - self._slo_target
        success_ratio = 1.0 if total == 0 else (total - errors) / total
        observed_rate = 0.0 if total == 0 else errors / total
        burn_rate = 0.0 if allowed_error_rate <= 0 else observed_rate / allowed_error_rate
        remaining = 1.0 if total == 0 else max(0.0, min(1.0, 1.0 - burn_rate))
        return {
            "window_seconds": self._slo_window,
            "target": self._slo_target,
            "requests": float(total),
            "errors": float(errors),
            "success_ratio": success_ratio,
            "burn_rate": burn_rate,
            "budget_remaining": remaining,
        }

    def _slo_gauges(self) -> dict[str, float]:
        """The SLO numbers as labelled series keys, ready to render."""
        slo = self.slo()
        window = {"window": f"{int(slo['window_seconds'])}s"}
        return {
            _series_key("http_success_ratio", window): slo["success_ratio"],
            _series_key("http_error_budget_burn_rate", window): slo["burn_rate"],
            _series_key("http_error_budget_remaining", window): slo["budget_remaining"],
        }

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histograms = {
                k: {"count": h.count, "sum": h.total, "max": h.max_value}
                for k, h in self._histograms.items()
            }
        # Read after releasing the lock: slo() takes it itself.
        return {
            "counters": counters,
            "gauges": gauges,
            "histograms": histograms,
            "slo": self.slo(),
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
        # Derived at scrape time: an error budget that is only recomputed when
        # traffic arrives would read stale the moment it matters.
        gauges.update(self._slo_gauges())

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
            # Integers render bare; floats are rounded so a budget of
            # 49.99999999999996 does not reach the scrape payload.
            rendered = int(value) if float(value).is_integer() else round(value, 6)
            lines.append(f"{target}{labelset} {rendered}")

        for key in sorted(histograms):
            metric_name, labelset = _split_key(key)
            count, total, _ = histograms[key]
            target = _exported_name(metric_name, "summary")
            header(metric_name, "summary", target)
            lines.append(f"{target}_sum{labelset} {total}")
            lines.append(f"{target}_count{labelset} {count}")

        return "\n".join(lines) + "\n"

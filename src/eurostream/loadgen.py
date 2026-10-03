"""A small HTTP load generator for the running API.

Deliberately boring: a thread pool, one shared httpx client, percentiles
computed from the samples actually taken. It exists to answer questions
this project can otherwise only assert — how the p95 of a warehouse read
behaves under concurrency, and whether the token bucket sheds load with
429s instead of letting the server fall over.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx

#: A representative mix: liveness, readiness (touches the warehouse), a
#: real warehouse read, and the metrics snapshot.
DEFAULT_PATHS = ("/health", "/health/ready", "/gold/customer-360", "/metrics")


def percentile(samples: Sequence[float], p: float) -> float:
    """The ``p``-th percentile of ``samples`` (nearest-rank, no interpolation).

    Interpolated percentiles look more precise than the measurements they
    summarise; a latency budget is about a request that really happened.
    """
    if not samples:
        return 0.0
    ordered = sorted(samples)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


@dataclass(frozen=True)
class EndpointReport:
    """Latency and outcome of one endpoint over the run."""

    path: str
    status_counts: dict[int, int] = field(default_factory=dict)
    timeouts: int = 0
    network_errors: int = 0
    samples_ms: tuple[float, ...] = ()

    @property
    def total(self) -> int:
        return sum(self.status_counts.values()) + self.timeouts + self.network_errors

    @property
    def ok(self) -> int:
        return sum(count for status, count in self.status_counts.items() if status < 400)

    @property
    def rate_limited(self) -> int:
        return self.status_counts.get(429, 0)

    @property
    def server_errors(self) -> int:
        return sum(count for status, count in self.status_counts.items() if status >= 500)

    @property
    def p50_ms(self) -> float:
        return percentile(self.samples_ms, 50)

    @property
    def p95_ms(self) -> float:
        return percentile(self.samples_ms, 95)

    @property
    def p99_ms(self) -> float:
        return percentile(self.samples_ms, 99)

    @property
    def max_ms(self) -> float:
        return max(self.samples_ms, default=0.0)


@dataclass(frozen=True)
class LoadReport:
    """The whole run, aggregated across endpoints."""

    base_url: str
    concurrency: int
    requests: int
    duration_s: float
    endpoints: tuple[EndpointReport, ...]

    @property
    def status_counts(self) -> dict[int, int]:
        totals: dict[int, int] = {}
        for endpoint in self.endpoints:
            for status, count in endpoint.status_counts.items():
                totals[status] = totals.get(status, 0) + count
        return dict(sorted(totals.items()))

    @property
    def server_errors(self) -> int:
        return sum(endpoint.server_errors for endpoint in self.endpoints)

    @property
    def rate_limited(self) -> int:
        return sum(endpoint.rate_limited for endpoint in self.endpoints)

    @property
    def timeouts(self) -> int:
        return sum(endpoint.timeouts for endpoint in self.endpoints)

    @property
    def network_errors(self) -> int:
        return sum(endpoint.network_errors for endpoint in self.endpoints)

    @property
    def throughput(self) -> float:
        return self.requests / self.duration_s if self.duration_s > 0 else 0.0

    @property
    def all_p95_ms(self) -> float:
        samples = [s for endpoint in self.endpoints for s in endpoint.samples_ms]
        return percentile(samples, 95)

    @property
    def passed(self) -> bool:
        """5xx and transport failures fail a run; 429 does not.

        A rate-limited request is the token bucket doing exactly what it
        was configured to do — shedding load at the edge rather than
        letting the warehouse queue behind it. Silently passing on 5xx
        would be the dishonest version of the same flag."""
        return self.server_errors == 0 and self.network_errors == 0 and self.timeouts == 0


def run_load(
    base_url: str,
    *,
    requests: int = 500,
    concurrency: int = 16,
    paths: Sequence[str] = DEFAULT_PATHS,
    timeout: float = 10.0,
) -> LoadReport:
    """Fire ``requests`` GETs across ``paths`` from ``concurrency`` workers."""
    if not paths:
        raise ValueError("at least one path is required")
    targets = [path if path.startswith("/") else f"/{path}" for path in paths]

    status_counts: dict[str, dict[int, int]] = {path: {} for path in targets}
    samples: dict[str, list[float]] = {path: [] for path in targets}
    timeouts: dict[str, int] = dict.fromkeys(targets, 0)
    network_errors: dict[str, int] = dict.fromkeys(targets, 0)
    lock = threading.Lock()

    limits = httpx.Limits(
        max_connections=max(concurrency, 1), max_keepalive_connections=concurrency
    )
    started = time.perf_counter()
    with httpx.Client(base_url=base_url, timeout=timeout, limits=limits) as client:

        def one(index: int) -> None:
            path = targets[index % len(targets)]
            begin = time.perf_counter()
            try:
                response = client.get(path)
                elapsed = (time.perf_counter() - begin) * 1000
            except httpx.TimeoutException:
                with lock:
                    timeouts[path] += 1
                return
            except httpx.HTTPError:
                with lock:
                    network_errors[path] += 1
                return
            with lock:
                status_counts[path][response.status_code] = (
                    status_counts[path].get(response.status_code, 0) + 1
                )
                samples[path].append(elapsed)

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(one, range(requests)))

    duration = time.perf_counter() - started

    return LoadReport(
        base_url=base_url,
        concurrency=concurrency,
        requests=requests,
        duration_s=duration,
        endpoints=tuple(
            EndpointReport(
                path=path,
                status_counts=status_counts[path],
                timeouts=timeouts[path],
                network_errors=network_errors[path],
                samples_ms=tuple(samples[path]),
            )
            for path in targets
        ),
    )


def free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def served(port: int | None = None, *, log_level: str = "warning") -> Iterator[str]:
    """Run the API in-process and yield its base URL.

    Uvicorn runs on a worker thread (it installs signal handlers only on
    the main thread), and the context manager waits for ``server.started``
    rather than for a fixed sleep — a load test that races its own server
    measures the startup, not the app."""
    import uvicorn

    chosen = port or free_port()
    config = uvicorn.Config(
        "eurostream.api:app",
        host="127.0.0.1",
        port=chosen,
        log_level=log_level,
        access_log=False,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True, name="eurostream-load-server")
    thread.start()

    deadline = time.time() + 60
    while not server.started and time.time() < deadline:
        if not thread.is_alive():
            raise RuntimeError("the server thread exited before startup completed")
        time.sleep(0.05)
    if not server.started:
        raise RuntimeError("server did not start within 60s")

    try:
        yield f"http://127.0.0.1:{chosen}"
    finally:
        server.should_exit = True
        thread.join(timeout=20)


def render(report: LoadReport) -> str:
    """The report as lines a human reads at a terminal."""
    lines = [
        f"load test: {report.requests} requests, concurrency {report.concurrency} "
        f"against {report.base_url}",
    ]
    for endpoint in report.endpoints:
        statuses = " ".join(
            f"{status}x{count}" for status, count in sorted(endpoint.status_counts.items())
        )
        lines.append(
            f"  {endpoint.path:<24} {endpoint.total:>5} req  {statuses or '-':<16} "
            f"p50={endpoint.p50_ms:.1f}ms p95={endpoint.p95_ms:.1f}ms "
            f"p99={endpoint.p99_ms:.1f}ms max={endpoint.max_ms:.1f}ms"
        )
    lines.append(
        "  status codes: "
        + " ".join(f"{status}={count}" for status, count in report.status_counts.items())
    )
    lines.append(
        f"  throughput {report.throughput:.1f} req/s over {report.duration_s:.2f}s "
        f"(p95 overall {report.all_p95_ms:.1f}ms)"
    )
    if report.rate_limited:
        lines.append(
            f"  rate-limited: {report.rate_limited} "
            f"({report.rate_limited / report.requests * 100:.1f}%) — the token bucket shed load"
        )
    if report.server_errors or report.network_errors or report.timeouts:
        lines.append(
            f"  failures: {report.server_errors} server error(s), "
            f"{report.network_errors} network error(s), {report.timeouts} timeout(s)"
        )
    else:
        lines.append("  failures: none")
    lines.append(
        f"  verdict: {'PASS' if report.passed else 'FAIL'} (5xx/transport fail, 429 does not)"
    )
    return "\n".join(lines)


def to_dict(report: LoadReport) -> dict[str, Any]:
    return {
        "base_url": report.base_url,
        "concurrency": report.concurrency,
        "requests": report.requests,
        "duration_s": report.duration_s,
        "throughput_rps": report.throughput,
        "status_counts": report.status_counts,
        "server_errors": report.server_errors,
        "rate_limited": report.rate_limited,
        "timeouts": report.timeouts,
        "network_errors": report.network_errors,
        "p95_ms": report.all_p95_ms,
        "passed": report.passed,
        "endpoints": [
            {
                "path": endpoint.path,
                "total": endpoint.total,
                "status_counts": endpoint.status_counts,
                "p50_ms": endpoint.p50_ms,
                "p95_ms": endpoint.p95_ms,
                "p99_ms": endpoint.p99_ms,
                "max_ms": endpoint.max_ms,
                "rate_limited": endpoint.rate_limited,
                "server_errors": endpoint.server_errors,
            }
            for endpoint in report.endpoints
        ],
    }

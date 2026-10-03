from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from eurostream.api import create_app
from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    RetryPolicy,
    call_with_retry,
    http_status_is_retryable,
)
from eurostream.turso import TursoClient
from eurostream.warehouse import Warehouse

# ------------------------------------------------------------------- retry


def _fail(exc: Exception) -> None:
    """A callable that always raises ``exc`` (a failure, not a bug)."""
    raise exc


def test_retry_policy_delays_are_exponential_without_jitter() -> None:
    policy = RetryPolicy(attempts=4, base_delay=0.1, max_delay=1.0, jitter=0.0)
    assert policy.delays() == pytest.approx([0.1, 0.2, 0.4])


def test_retry_policy_caps_at_max_delay() -> None:
    policy = RetryPolicy(attempts=6, base_delay=1.0, max_delay=2.5, jitter=0.0)
    assert policy.delays() == pytest.approx([1.0, 2.0, 2.5, 2.5, 2.5])


def test_full_jitter_spreads_the_whole_window() -> None:
    policy = RetryPolicy(attempts=3, base_delay=1.0, max_delay=8.0, jitter=1.0)
    # rng()=0 -> the full ceiling; rng()=1 -> no delay at all.
    assert policy.delays(rng=lambda: 0.0) == pytest.approx([1.0, 2.0])
    assert policy.delays(rng=lambda: 1.0) == pytest.approx([0.0, 0.0])


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"attempts": 0}, "attempts"),
        ({"base_delay": -1.0}, "delays"),
        ({"jitter": 1.5}, "jitter"),
    ],
)
def test_retry_policy_rejects_nonsense(kwargs: dict[str, float], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        RetryPolicy(**kwargs)


def test_retry_recovers_from_transient_failures() -> None:
    calls: list[int] = []
    slept: list[float] = []
    attempts_made: list[tuple[int, str, float]] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("reset by peer")
        return "ok"

    result = call_with_retry(
        flaky,
        policy=RetryPolicy(attempts=3, base_delay=0.5, jitter=0.0),
        sleep=slept.append,
        on_retry=lambda n, exc, delay: attempts_made.append((n, str(exc), delay)),
    )

    assert result == "ok"
    assert len(calls) == 3
    assert slept == pytest.approx([0.5, 1.0])  # exponential, jittered off
    assert [n for n, _, _ in attempts_made] == [1, 2]
    assert all("reset by peer" in msg for _, msg, _ in attempts_made)


def test_retry_gives_up_and_reraises_the_last_error() -> None:
    calls: list[int] = []

    def always_down() -> None:
        calls.append(1)
        raise TimeoutError("still down")

    with pytest.raises(TimeoutError, match="still down"):
        call_with_retry(
            always_down,
            policy=RetryPolicy(attempts=3, base_delay=0.0),
            sleep=lambda _d: None,
        )
    assert len(calls) == 3


def test_retry_stops_immediately_for_a_non_transient_error() -> None:
    calls: list[int] = []

    def bug() -> None:
        calls.append(1)
        raise ValueError("bad sql")

    with pytest.raises(ValueError, match="bad sql"):
        call_with_retry(
            bug,
            policy=RetryPolicy(attempts=5, base_delay=0.0),
            should_retry=http_status_is_retryable,
            sleep=lambda _d: None,
        )
    assert len(calls) == 1  # a programming error is not a flake


def test_retry_policy_of_one_never_sleeps() -> None:
    calls: list[int] = []

    def boom() -> None:
        calls.append(1)
        raise ConnectionError("nope")

    with pytest.raises(ConnectionError):
        call_with_retry(boom, policy=RetryPolicy(attempts=1), sleep=lambda _d: None)
    assert len(calls) == 1


def test_retryable_statuses_cover_timeouts_overload_and_5xx() -> None:
    def status_error(code: int) -> httpx.HTTPStatusError:
        req = httpx.Request("POST", "https://db.example/v2/pipeline")
        return httpx.HTTPStatusError(
            "boom", request=req, response=httpx.Response(code, request=req)
        )

    for code in (408, 425, 429, 500, 502, 503, 504):
        assert http_status_is_retryable(status_error(code)), code
    for code in (400, 401, 403, 404, 409, 422):
        assert not http_status_is_retryable(status_error(code)), code

    assert http_status_is_retryable(httpx.ConnectError("dns"))
    assert http_status_is_retryable(httpx.ReadTimeout("read timed out"))
    assert not http_status_is_retryable(ValueError("sql syntax"))


# -------------------------------------------------------------- breaker


class FakeClock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_breaker_stays_closed_below_the_threshold() -> None:
    breaker = CircuitBreaker("dep", failure_threshold=3, clock=FakeClock())
    for _ in range(2):
        with pytest.raises(RuntimeError):
            breaker.call(lambda: _fail(RuntimeError("x")))
    assert breaker.state == CircuitBreaker.CLOSED
    assert breaker.failures == 2
    assert breaker.call(lambda: "fine") == "fine"
    assert breaker.failures == 0  # success clears the count


def test_breaker_opens_and_refuses_without_calling() -> None:
    calls: list[int] = []
    clock = FakeClock()
    breaker = CircuitBreaker("dep", failure_threshold=2, recovery_seconds=30, clock=clock)

    def fail() -> None:
        calls.append(1)
        raise ConnectionError("down")

    for _ in range(2):
        with pytest.raises(ConnectionError):
            breaker.call(fail)

    assert breaker.state == CircuitBreaker.OPEN
    assert breaker.trips == 1
    assert breaker.retry_after == pytest.approx(30.0)

    with pytest.raises(CircuitOpenError, match="open"):
        breaker.call(fail)
    assert len(calls) == 2  # the refused call never reached the dependency


def test_breaker_half_open_probe_closes_it_on_success() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker("dep", failure_threshold=1, recovery_seconds=10, clock=clock)
    with pytest.raises(ConnectionError):
        breaker.call(lambda: _fail(ConnectionError("down")))

    clock.advance(11)
    assert breaker.state == CircuitBreaker.HALF_OPEN
    assert breaker.retry_after == 0.0
    assert breaker.call(lambda: "recovered") == "recovered"
    assert breaker.state == CircuitBreaker.CLOSED
    assert breaker.failures == 0


def test_breaker_half_open_failure_reopens_and_counts_another_trip() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker("dep", failure_threshold=1, recovery_seconds=10, clock=clock)
    with pytest.raises(ConnectionError):
        breaker.call(lambda: _fail(ConnectionError("down")))

    clock.advance(11)
    with pytest.raises(ConnectionError):
        breaker.call(lambda: _fail(ConnectionError("still down")))
    assert breaker.state == CircuitBreaker.OPEN
    assert breaker.trips == 2
    assert breaker.retry_after == pytest.approx(10.0)


def test_breaker_lets_only_one_probe_through_while_half_open() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker("dep", failure_threshold=1, recovery_seconds=5, clock=clock)
    with pytest.raises(ConnectionError):
        breaker.call(lambda: _fail(ConnectionError("down")))
    clock.advance(6)

    def nested_probe() -> str:
        # A second caller during the probe is refused, not queued.
        with pytest.raises(CircuitOpenError, match="half-open"):
            breaker.call(lambda: "inner")
        return "outer"

    assert breaker.call(nested_probe) == "outer"


def test_breaker_rejects_a_useless_threshold() -> None:
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreaker("dep", failure_threshold=0)
    with pytest.raises(ValueError, match="recovery_seconds"):
        CircuitBreaker("dep", recovery_seconds=-1)


def test_breaker_stats_expose_what_an_operator_needs() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker("turso", failure_threshold=1, recovery_seconds=7, clock=clock)
    with pytest.raises(ConnectionError):
        breaker.call(lambda: _fail(ConnectionError("down")))
    stats = breaker.stats()
    assert stats["name"] == "turso"
    assert stats["state"] == CircuitBreaker.OPEN
    assert stats["trips"] == 1
    assert stats["failure_threshold"] == 1
    assert stats["recovery_seconds"] == 7
    assert stats["retry_after"] == pytest.approx(7.0)


# ----------------------------------------------------- TursoClient transport


def _ok_execute() -> dict[str, object]:
    return {"results": [{"type": "ok", "response": {"result": {"affected_row_count": 1}}}]}


def _int_cell(value: int) -> dict[str, object]:
    return {"type": "integer", "value": str(value)}


def _text_cell(value: str) -> dict[str, object]:
    return {"type": "text", "value": value}


def _ok_query(cols: list[str], rows: list[list[object]]) -> dict[str, object]:
    return {
        "results": [
            {
                "type": "ok",
                "response": {
                    "result": {
                        "cols": [{"name": c} for c in cols],
                        "rows": rows,
                    }
                },
            }
        ]
    }


def test_turso_retries_a_transient_503_then_succeeds() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if len(calls) < 3:
            return httpx.Response(503, json={"error": "unavailable"})
        return httpx.Response(200, json=_ok_execute())

    client = TursoClient(
        "libsql://my-db.turso.io",
        "token",
        transport=httpx.MockTransport(handler),
        retry_attempts=3,
        retry_base_delay=0.01,
    )
    try:
        assert client.execute("INSERT INTO bronze.orders VALUES (1)") == 1
        assert len(calls) == 3
        status = client.status()
        assert status["retries"] == 2
        assert status["state"] == CircuitBreaker.CLOSED
        assert status["native"] is False
        assert status["endpoint"] == "https://my-db.turso.io/v2/pipeline"
    finally:
        client.close()


def test_turso_breaker_refuses_further_calls_once_it_opens() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(500, json={"error": "boom"})

    client = TursoClient(
        "libsql://my-db.turso.io",
        "token",
        transport=httpx.MockTransport(handler),
        retry_attempts=1,
        failure_threshold=1,
        recovery_seconds=60,
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            client.execute("INSERT INTO bronze.orders VALUES (1)")
        assert client.circuit.state == CircuitBreaker.OPEN
        assert len(calls) == 1

        # Fast-fail: no HTTP round trip, and the error says why.
        with pytest.raises(CircuitOpenError, match="open"):
            client.execute("INSERT INTO bronze.orders VALUES (1)")
        with pytest.raises(CircuitOpenError):
            client.query("SELECT 1")
        with pytest.raises(CircuitOpenError):
            client.executemany("INSERT INTO bronze.orders VALUES (1)", [(1,), (2,)])
        assert len(calls) == 1

        status = client.status()
        assert status["state"] == CircuitBreaker.OPEN
        assert status["trips"] == 1
        assert 59.0 <= float(status["retry_after"]) <= 60.0
    finally:
        client.close()


def test_turso_does_not_retry_a_statement_error() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        # HTTP 200 with a per-statement SQL error: retrying is pointless.
        return httpx.Response(200, json={"results": [{"type": "error", "error": "no such table"}]})

    client = TursoClient(
        "libsql://my-db.turso.io",
        "token",
        transport=httpx.MockTransport(handler),
        retry_attempts=3,
        retry_base_delay=0.01,
    )
    try:
        with pytest.raises(RuntimeError, match="no such table"):
            client.execute("SELECT * FROM missing")
        assert len(calls) == 1
        assert client.circuit.state == CircuitBreaker.CLOSED  # not a dependency failure
    finally:
        client.close()


def test_turso_query_rows_still_parse_after_a_retry() -> None:
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if state["calls"] == 1:
            return httpx.Response(504, json={})
        return httpx.Response(200, json=_ok_query(["customer_id"], [[_text_cell("cust_1")]]))

    client = TursoClient(
        "libsql://my-db.turso.io",
        "token",
        transport=httpx.MockTransport(handler),
        retry_attempts=2,
        retry_base_delay=0.01,
    )
    try:
        assert client.query("SELECT customer_id FROM gold.customer_360") == [
            {"customer_id": "cust_1"}
        ]  # cell decoded from {"type": "text", "value": ...}
        assert state["calls"] == 2
    finally:
        client.close()


# -------------------------------------------------------------- endpoint


def _app(tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "eurocart.duckdb",
        audit_log_path=tmp_path / "data" / "logs" / "audit.jsonl",
        metrics_path=tmp_path / "data" / "logs" / "metrics.jsonl",
        pii_manifest_path=tmp_path / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
    )
    bus = open_bus(tmp_path / "events.db")
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    metrics = Metrics(tmp_path / "metrics.jsonl")
    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "res-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    app = create_app(erasure, metrics, settings, warehouse, bus, start_worker=False)
    return app, bus, warehouse


def test_turso_status_reports_a_closed_circuit(tmp_path) -> None:
    app, bus, wh = _app(tmp_path)
    wh.turso = TursoClient(
        "libsql://my-db.turso.io",
        "token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=_ok_query(["n"], [[_int_cell(0)]]))
        ),
    )
    client = TestClient(app)
    payload = client.get("/turso/status").json()
    assert payload["connected"] is True
    circuit = payload["circuit"]
    assert circuit["state"] == CircuitBreaker.CLOSED
    assert circuit["name"] == "turso"
    assert circuit["trips"] == 0
    assert circuit["retries"] == 0
    assert all(count >= 0 for count in payload["table_counts"].values())
    bus.close()
    wh.close()


def test_turso_status_degrades_honestly_when_the_breaker_opens(tmp_path) -> None:
    app, bus, wh = _app(tmp_path)
    wh.turso = TursoClient(
        "libsql://my-db.turso.io",
        "token",
        transport=httpx.MockTransport(lambda request: httpx.Response(500, json={})),
        retry_attempts=1,
        failure_threshold=1,
        recovery_seconds=60,
    )
    client = TestClient(app)
    payload = client.get("/turso/status").json()
    circuit = payload["circuit"]
    assert circuit["state"] == CircuitBreaker.OPEN
    assert circuit["trips"] == 1
    # Every table read reports the refusal instead of hanging or lying 0.
    assert payload["table_errors"], "expected per-table failures while open"
    assert all(count == -1 for count in payload["table_counts"].values())
    assert any("open" in err for err in payload["table_errors"].values())
    bus.close()
    wh.close()


def test_turso_status_without_a_connection_has_no_circuit(tmp_path) -> None:
    app, bus, wh = _app(tmp_path)
    assert wh.turso is None
    payload = TestClient(app).get("/turso/status").json()
    assert payload["connected"] is False
    assert payload["circuit"] is None
    assert payload["table_counts"] == {}
    bus.close()
    wh.close()

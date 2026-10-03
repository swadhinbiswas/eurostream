from __future__ import annotations

from fastapi.testclient import TestClient

from eurostream.api import create_app
from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.ratelimit import RateLimiter, TokenBucket
from eurostream.warehouse import Warehouse


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_bucket_allows_a_full_burst_then_paces() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=1.0, burst=3, clock=clock)
    assert bucket.allow() == (True, 0.0)
    assert bucket.allow() == (True, 0.0)
    assert bucket.allow() == (True, 0.0)
    allowed, retry_after = bucket.allow()
    assert allowed is False
    assert retry_after == 1.0  # one token at one per second


def test_bucket_refills_continuously() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=2.0, burst=2, clock=clock)
    bucket.allow()
    bucket.allow()
    assert bucket.allow()[0] is False
    clock.advance(0.5)  # 2/s for 0.5s = one token back
    assert bucket.allow()[0] is True
    assert bucket.allow()[0] is False


def test_bucket_never_overshoots_the_burst() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10.0, burst=2, clock=clock)
    clock.advance(60)  # an hour of quiet must not accumulate an hour of tokens
    assert bucket.tokens == 2.0


def test_bucket_rejects_impossible_configuration() -> None:
    import pytest

    with pytest.raises(ValueError, match="rate"):
        TokenBucket(rate=-1, burst=1)
    with pytest.raises(ValueError, match="burst"):
        TokenBucket(rate=1, burst=0)


def test_limiter_disables_itself_at_zero_rate() -> None:
    limiter = RateLimiter(rate=0, burst=1)
    assert limiter.enabled is False
    for _ in range(100):
        allowed, retry_after = limiter.check("client-a")
        assert allowed is True
        assert retry_after == 0.0
    assert limiter.keys() == 0


def test_limiter_keeps_buckets_per_client() -> None:
    clock = FakeClock()
    limiter = RateLimiter(rate=1, burst=1, clock=clock)
    assert limiter.check("client-a")[0] is True
    assert limiter.check("client-a")[0] is False
    # client-b has its own bucket and is not punished for client-a's burst
    assert limiter.check("client-b")[0] is True
    assert limiter.rejected == 1


def test_limiter_buckets_are_lru_bounded() -> None:
    limiter = RateLimiter(rate=1, burst=1, max_buckets=5)
    for index in range(50):
        limiter.check(f"client-{index}")
    assert limiter.keys() == 5


def _app(tmp_path, **overrides):
    settings = Settings(
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "eurocart.duckdb",
        audit_log_path=tmp_path / "data" / "logs" / "audit.jsonl",
        metrics_path=tmp_path / "data" / "logs" / "metrics.jsonl",
        pii_manifest_path=tmp_path / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
        **overrides,
    )
    bus = open_bus(tmp_path / "events.db")
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    metrics = Metrics(tmp_path / "metrics.jsonl")
    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "rl-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    app = create_app(erasure, metrics, settings, warehouse, bus, start_worker=False)
    return app, bus, warehouse, metrics


def test_mutating_routes_rate_limit_and_reads_do_not(tmp_path):
    app, bus, wh, metrics = _app(tmp_path, api_rate_limit=0.01, api_rate_limit_burst=2)
    c = TestClient(app)

    assert c.post("/erasure-requests", json={"customer_id": "cust_r1"}).status_code == 202
    assert c.post("/erasure-requests", json={"customer_id": "cust_r2"}).status_code == 202
    blocked = c.post("/erasure-requests", json={"customer_id": "cust_r3"})
    assert blocked.status_code == 429
    assert blocked.headers["content-type"].startswith("application/problem+json")
    assert int(blocked.headers["Retry-After"]) >= 1
    assert "rate limit exceeded" in blocked.json()["detail"]

    # Reads are unaffected: the same client can still poll the API.
    assert c.get("/health").status_code == 200
    assert c.get("/stats").status_code == 200
    assert metrics.snapshot()["counters"]["rate_limited_requests"] == 1
    bus.close()
    wh.close()


def test_rate_limit_keys_are_per_forwarded_client(tmp_path):
    app, bus, wh, _ = _app(tmp_path, api_rate_limit=0.01, api_rate_limit_burst=1)
    c = TestClient(app)
    one = c.post(
        "/erasure-requests",
        json={"customer_id": "cust_f1"},
        headers={"X-Forwarded-For": "203.0.113.9"},
    )
    two = c.post(
        "/erasure-requests",
        json={"customer_id": "cust_f2"},
        headers={"X-Forwarded-For": "203.0.113.9"},
    )
    other = c.post(
        "/erasure-requests",
        json={"customer_id": "cust_f3"},
        headers={"X-Forwarded-For": "198.51.100.7"},
    )
    assert one.status_code == 202
    assert two.status_code == 429
    assert other.status_code == 202
    bus.close()
    wh.close()


def test_rate_limit_can_be_switched_off(tmp_path):
    app, bus, wh, _ = _app(tmp_path, api_rate_limit=0.0, api_rate_limit_burst=1)
    c = TestClient(app)
    codes = [
        c.post("/erasure-requests", json={"customer_id": f"cust_off{i}"}).status_code
        for i in range(30)
    ]
    assert codes == [202] * 30
    stats = c.get("/stats").json()["rate_limit"]
    assert stats["enabled"] is False
    bus.close()
    wh.close()

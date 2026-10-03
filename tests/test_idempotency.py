from __future__ import annotations

import pytest

from eurostream.idempotency import (
    IDEMPOTENCY_KEY_PATTERN,
    IdempotencyConflictError,
    IdempotencyStore,
)


def test_first_use_returns_none_then_replays() -> None:
    store = IdempotencyStore()
    assert store.begin("key-00000001", "cust_1") is None
    store.complete(
        "key-00000001",
        customer_id="cust_1",
        request_id="req-1",
        status_code=202,
        body={"request_id": "req-1"},
    )
    replay = store.begin("key-00000001", "cust_1")
    assert replay is not None
    assert replay.request_id == "req-1"
    assert replay.status_code == 202
    assert replay.body == {"request_id": "req-1"}
    assert store.replays == 1


def test_key_reused_for_another_customer_is_a_conflict() -> None:
    store = IdempotencyStore()
    store.begin("key-00000002", "cust_1")
    store.complete(
        "key-00000002",
        customer_id="cust_1",
        request_id="req-1",
        status_code=202,
        body={},
    )
    with pytest.raises(IdempotencyConflictError, match="first used for customer"):
        store.begin("key-00000002", "cust_2")


def test_in_flight_key_cannot_be_claimed_twice() -> None:
    store = IdempotencyStore()
    assert store.begin("key-00000003", "cust_1") is None
    with pytest.raises(IdempotencyConflictError, match="still being processed"):
        store.begin("key-00000003", "cust_1")


def test_a_failed_attempt_releases_the_key() -> None:
    store = IdempotencyStore()
    store.begin("key-00000004", "cust_1")
    store.fail("key-00000004")
    # The retry gets to be the owner, and nothing replays a response that
    # was never produced.
    assert store.begin("key-00000004", "cust_1") is None


def test_entries_expire_after_the_ttl() -> None:
    now = [1000.0]
    store = IdempotencyStore(ttl_seconds=60, clock=lambda: now[0])
    store.begin("key-00000005", "cust_1")
    store.complete(
        "key-00000005",
        customer_id="cust_1",
        request_id="req-1",
        status_code=202,
        body={},
    )
    assert len(store) == 1
    now[0] += 61
    assert store.begin("key-00000005", "cust_1") is None
    assert len(store) == 0


def test_the_map_is_bounded_and_evicts_the_oldest() -> None:
    now = [1000.0]
    store = IdempotencyStore(max_entries=3, clock=lambda: now[0])
    for index in range(5):
        store.begin(f"key-000000{index:02d}", "cust_1")
        store.complete(
            f"key-000000{index:02d}",
            customer_id="cust_1",
            request_id=f"req-{index}",
            status_code=202,
            body={},
        )
        now[0] += 1
    assert len(store) == 3
    # The oldest keys are gone; the newest survived.
    assert store.begin("key-00000000", "cust_1") is None
    assert store.begin("key-00000004", "cust_1") is not None


def test_key_pattern_shape() -> None:
    import re

    pattern = re.compile(IDEMPOTENCY_KEY_PATTERN)
    assert pattern.match("8e0f2c1a-4d3b-4f5e-9a70-1b2c3d4e5f60")
    assert pattern.match("order-2026-000001")
    assert not pattern.match("short")  # under the 8 character floor
    assert not pattern.match("has spaces here")
    assert not pattern.match("x" * 129)

from __future__ import annotations

import json
import threading
import time

from fastapi.testclient import TestClient

from eurostream.alerts import (
    AlertBroker,
    drain,
    format_event,
    parse_last_event_id,
)
from eurostream.api import create_app
from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.warehouse import Warehouse

# ----------------------------------------------------------------- broker


def test_publish_assigns_monotonic_event_ids() -> None:
    broker = AlertBroker()
    assert broker.publish({"rule": "A"}) == 1
    assert broker.publish({"rule": "B"}) == 2
    assert broker.last_seq == 2
    assert broker.published == 2
    assert broker.replay(0) == [(1, {"rule": "A"}), (2, {"rule": "B"})]
    assert broker.replay(1) == [(2, {"rule": "B"})]


def test_subscribe_sees_history_then_live_events() -> None:
    broker = AlertBroker()
    broker.publish({"rule": "OLD"})
    subscriber, snapshot = broker.subscribe()
    assert snapshot == [(1, {"rule": "OLD"})]
    broker.publish({"rule": "NEW"})
    assert subscriber.get_nowait() == (2, {"rule": "NEW"})
    broker.unsubscribe(subscriber)
    assert broker.subscribers == 0


def test_buffer_is_bounded_and_replay_only_returns_what_it_kept() -> None:
    broker = AlertBroker(buffer_size=2)
    for index in range(5):
        broker.publish({"rule": str(index)})
    assert broker.buffered == 2
    assert [seq for seq, _ in broker.replay(0)] == [4, 5]


def test_a_full_subscriber_queue_drops_rather_than_blocking_the_publisher() -> None:
    from eurostream.alerts import SUBSCRIBER_QUEUE_SIZE

    broker = AlertBroker()
    subscriber, _ = broker.subscribe()
    for index in range(SUBSCRIBER_QUEUE_SIZE):
        broker.publish({"rule": str(index)})
    # The queue is full now; the publisher must keep going and count the loss.
    broker.publish({"rule": "overflow"})
    assert broker.dropped == 1
    assert broker.published == SUBSCRIBER_QUEUE_SIZE + 1
    assert subscriber.qsize() == SUBSCRIBER_QUEUE_SIZE


def test_stats_reports_every_counter() -> None:
    broker = AlertBroker(buffer_size=10)
    subscriber, _ = broker.subscribe()
    broker.publish({"rule": "A"})
    stats = broker.stats()
    assert stats == {
        "published": 1,
        "dropped": 0,
        "subscribers": 1,
        "buffered": 1,
        "last_seq": 1,
    }
    broker.unsubscribe(subscriber)
    assert broker.stats()["subscribers"] == 0


def test_broker_rejects_a_useless_buffer() -> None:
    import pytest

    with pytest.raises(ValueError, match="buffer_size"):
        AlertBroker(buffer_size=0)


def test_format_event_is_a_valid_sse_frame() -> None:
    frame = format_event(7, {"rule": "VELOCITY", "customer_id": "cust_1"})
    assert frame.startswith("id: 7\nevent: alert\ndata: ")
    assert frame.endswith("\n\n")
    payload = json.loads(frame.split("data: ", 1)[1].strip())
    assert payload == {"rule": "VELOCITY", "customer_id": "cust_1"}


def test_last_event_id_parsing_is_forgiving() -> None:
    assert parse_last_event_id(None) == 0
    assert parse_last_event_id("") == 0
    assert parse_last_event_id(" 12 ") == 12
    assert parse_last_event_id("-5") == 0
    assert parse_last_event_id("not-a-number") == 0


def test_drain_skips_events_the_client_already_had() -> None:
    broker = AlertBroker()
    subscriber, _ = broker.subscribe()
    for index in range(1, 5):
        broker.publish({"rule": str(index)})
    assert list(drain(subscriber, up_to_seq=2)) == [(3, {"rule": "3"}), (4, {"rule": "4"})]


# --------------------------------------------------------------- HTTP/SSE


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
        consumer=bus.consumer("erasure_requests", "sse-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    app = create_app(erasure, metrics, settings, warehouse, bus, start_worker=False)
    return app, bus, warehouse


def _read(url: str, client: TestClient, **kwargs) -> tuple[int, str]:
    headers = kwargs.pop("headers", None) or {}
    with client.stream("GET", url, headers=headers, **kwargs) as response:
        status = response.status_code
        body = "\n".join(response.iter_lines())
    return status, body


def _ids(body: str) -> list[int]:
    return [int(line.split(": ", 1)[1]) for line in body.splitlines() if line.startswith("id: ")]


def test_stream_heartbeats_then_closes(tmp_path) -> None:
    app, bus, wh = _app(tmp_path)
    client = TestClient(app)
    status, body = _read("/stream/alerts?heartbeat=0.4&duration=1.2", client)
    assert status == 200
    assert "retry: 3000" in body
    # Two heartbeats fit in 1.2s at 0.4s apart; one is enough to prove the
    # keep-alive comment is being emitted.
    assert ": ping" in body
    assert _ids(body) == []  # no resume point asked for, so no history
    bus.close()
    wh.close()


def test_reconnect_with_last_event_id_resumes_without_gap_or_repeat(tmp_path) -> None:
    app, bus, wh = _app(tmp_path)
    broker = app.state.alert_broker
    for index in range(1, 4):
        broker.publish({"rule": f"RULE_{index}", "customer_id": f"cust_{index}"})
    client = TestClient(app)

    _, all_body = _read(
        "/stream/alerts?duration=0.3&heartbeat=5", client, headers={"Last-Event-ID": "0"}
    )
    assert _ids(all_body) == [1, 2, 3]
    payload = json.loads(all_body.split("data: ", 1)[1].splitlines()[0])
    assert payload["rule"] == "RULE_1"

    _, resumed = _read(
        "/stream/alerts?duration=0.3&heartbeat=5", client, headers={"Last-Event-ID": "2"}
    )
    assert _ids(resumed) == [3]

    _, query = _read("/stream/alerts?duration=0.3&heartbeat=5&since=1", client)
    assert _ids(query) == [2, 3]

    _, junk = _read(
        "/stream/alerts?duration=0.3&heartbeat=5", client, headers={"Last-Event-ID": "oops"}
    )
    assert _ids(junk) == [1, 2, 3]
    bus.close()
    wh.close()


def test_events_published_while_connected_arrive_live(tmp_path) -> None:
    app, bus, wh = _app(tmp_path)
    broker = app.state.alert_broker
    client = TestClient(app)

    def publish_later() -> None:
        time.sleep(0.3)
        broker.publish({"rule": "VELOCITY", "customer_id": "cust_live"})

    threading.Thread(target=publish_later, daemon=True).start()
    status, body = _read("/stream/alerts?duration=2.0&heartbeat=1.0", client)
    assert status == 200
    frames = [json.loads(line[6:]) for line in body.splitlines() if line.startswith("data: ")]
    assert frames == [{"rule": "VELOCITY", "customer_id": "cust_live"}]
    bus.close()
    wh.close()


def test_posting_the_scoring_run_publishes_to_the_feed(tmp_path) -> None:
    app, bus, wh = _app(tmp_path)
    client = TestClient(app)
    assert client.post("/produce?events=10").status_code == 200
    streamed = client.post("/stream?max_events=20")
    assert streamed.status_code == 200

    broker = app.state.alert_broker
    assert broker.published == streamed.json()["alerts_emitted"]

    _, body = _read("/stream/alerts?duration=0.3&heartbeat=5&since=0", client)
    assert len(_ids(body)) == broker.published
    assert broker.stats()["buffered"] >= 1

    stats = client.get("/stats").json()["alerts"]
    assert stats["published"] >= 0
    assert stats["last_seq"] == broker.last_seq
    bus.close()
    wh.close()


def test_the_feed_counts_streams_and_frames(tmp_path) -> None:
    app, bus, wh = _app(tmp_path)
    client = TestClient(app)
    app.state.alert_broker.publish({"rule": "A"})
    _read("/stream/alerts?duration=0.2&heartbeat=5&since=0", client)
    counters = client.get("/metrics").json()["counters"]
    assert counters["alert_streams_opened"] == 1
    assert counters["alert_stream_events"] == 1
    bus.close()
    wh.close()

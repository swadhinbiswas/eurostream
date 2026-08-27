from __future__ import annotations

from eurostream.bus.sqlite import open_bus


def test_bus_produce_and_poll_earliest(tmp_path):
    bus = open_bus(tmp_path / "events.db")
    bus.produce("orders", key="k1", value='{"event_type":"order_placed"}', headers={"h": "v"})
    c = bus.consumer("orders", "g1", auto_offset_reset="earliest")
    rec = c.poll()
    assert rec is not None
    assert rec.topic == "orders"
    assert rec.key == "k1"
    assert rec.offset == 0
    assert rec.headers["h"] == "v"
    assert rec.json_value()["event_type"] == "order_placed"
    bus.close()


def test_bus_consumer_groups_isolated(tmp_path):
    bus = open_bus(tmp_path / "events.db")
    bus.produce("payments", key="k1", value='{"a":1}')
    bus.produce("payments", key="k2", value='{"a":2}')
    c1 = bus.consumer("payments", "g1", auto_offset_reset="earliest")
    assert c1.poll().offset == 0
    c1.commit()
    # g1 committed offset 1; new consumer in same group resumes at 1
    c1b = bus.consumer("payments", "g1", auto_offset_reset="earliest")
    assert c1b.poll().offset == 1
    # different group starts from 0 with earliest
    c2 = bus.consumer("payments", "g2", auto_offset_reset="earliest")
    assert c2.poll().offset == 0
    bus.close()


def test_bus_latest_vs_earliest(tmp_path):
    bus = open_bus(tmp_path / "events.db")
    bus.produce("clicks", key="k1", value='{"x":1}')
    # latest should start past existing messages
    latest = bus.consumer("clicks", "g-latest", auto_offset_reset="latest")
    assert latest.poll() is None
    earliest = bus.consumer("clicks", "g-earliest", auto_offset_reset="earliest")
    assert earliest.poll() is not None
    bus.close()


def test_bus_replayability_and_headers(tmp_path):
    bus = open_bus(tmp_path / "events.db")
    bus.produce("orders", key="k1", value='{"n":1}', headers={"schema_version": "1"})
    bus.produce("orders", key="k2", value='{"n":2}')
    c = bus.consumer("orders", "replay", auto_offset_reset="earliest")
    r1 = c.poll()
    r2 = c.poll()
    assert r1.offset == 0
    assert r2.offset == 1
    assert r1.headers["schema_version"] == "1"
    assert r2.headers == {}
    bus.close()


def test_bus_topic_offsets(tmp_path):
    bus = open_bus(tmp_path / "events.db")
    bus.produce("orders", key="k1", value='{"a":1}')
    bus.produce("orders", key="k2", value='{"a":2}')
    offs = bus.topic_offsets("orders")
    assert offs[0] == 2
    bus.close()


def test_kafka_bootstrap_parser():
    import pytest

    from eurostream.bus.kafka import _parse_bootstrap

    assert _parse_bootstrap("localhost:9092") == "localhost:9092"
    assert _parse_bootstrap('"localhost:9092"') == "localhost:9092"
    assert _parse_bootstrap("'kafka.aivencloud.com:28362'") == "kafka.aivencloud.com:28362"
    assert (
        _parse_bootstrap("https://user:pass@kafka.aivencloud.com:28362")
        == "kafka.aivencloud.com:28362"
    )
    assert (
        _parse_bootstrap("kafka+ssl://avnadmin:secret@kafka-proj.aiven.com:12345/default?ssl=true")
        == "kafka-proj.aiven.com:12345"
    )

    with pytest.raises(ValueError):
        _parse_bootstrap("")
    with pytest.raises(ValueError):
        _parse_bootstrap("***")

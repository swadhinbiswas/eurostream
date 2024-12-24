from __future__ import annotations

import time

from eurostream.bus.sqlite import open_bus
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.models import ErasureRequested
from eurostream.producers import EventGenerator
from eurostream.warehouse import Warehouse


def test_erasure_anonymizes_bronze_and_deletes_gold(settings, tmp_path):
    bus = open_bus(tmp_path / "events.db")
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    gen = EventGenerator(settings)

    order = gen.order(customer_id="cust_999", consent=True)
    payment = gen.payment(customer_id="cust_999", country="DE", merchant_country="DE")
    warehouse.append_order(order)
    warehouse.append_payment(payment)

    service = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "test-worker", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=Metrics(),
        sla_seconds=60,
    )

    req = ErasureRequested(
        event_id="req-1",
        occurred_at=time.time(),
        request_id="req-1",
        customer_id="cust_999",
    )
    audit = service.execute(req)

    assert audit.layers_touched == ["suppression_registry", "warehouse"]
    assert audit.status == "completed"
    assert len(audit.confirmation_hash) == 16

    bronze_orders = warehouse.query("SELECT email FROM bronze.orders WHERE customer_id='cust_999'")
    assert all(r["email"] == "<anonymized>" for r in bronze_orders)
    gold_remaining = warehouse.query(
        "SELECT count(*) c FROM gold.customer_360 WHERE customer_id='cust_999'"
    )
    assert gold_remaining[0]["c"] == 0

    audit_rows = warehouse.query(
        "SELECT count(*) c FROM governance.erasure_audit_log WHERE customer_id='cust_999'"
    )
    assert audit_rows[0]["c"] == 1
    assert service.is_suppressed("cust_999")
    bus.close()
    warehouse.close()


def test_erasure_request_produces_tombstone(settings, bus, tmp_path):
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    service = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "test-worker2", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=Metrics(),
    )
    rid = service.request_erasure("cust_7")
    consumer = bus.consumer("erasure_requests", "test-reader", auto_offset_reset="earliest")
    rec = consumer.poll(1.0)
    assert rec is not None
    payload = rec.json_value()
    assert payload["request_id"] == rid
    assert payload["customer_id"] == "cust_7"
    assert payload["reason"] == "GDPR_ARTICLE_17"
    bus.close()
    warehouse.close()


def test_tombstone_identity_and_durable_suppression(settings, bus, tmp_path):
    """Auto-generated requests use one ID for event_id and request_id, and
    suppression survives a service restart via the durable registry."""
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    service = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "t-worker", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=Metrics(),
    )
    rid = service.request_erasure("cust_8")
    consumer = bus.consumer("erasure_requests", "t-reader", auto_offset_reset="earliest")
    rec = consumer.poll(1.0)
    assert rec is not None
    payload = rec.json_value()
    # The tombstone carries one identity, not two unrelated UUIDs.
    assert payload["event_id"] == payload["request_id"] == rid

    service.execute(ErasureRequested(**payload))
    assert warehouse.suppressed_ids() == ["cust_8"]

    # A fresh service (fresh process) seeds its registry from the warehouse.
    restarted = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "t-worker2", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=Metrics(),
    )
    assert restarted.is_suppressed("cust_8")
    bus.close()
    warehouse.close()

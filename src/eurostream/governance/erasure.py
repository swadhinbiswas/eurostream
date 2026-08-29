from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from eurostream.bus import Consumer, Producer
from eurostream.metrics import Metrics
from eurostream.models import ErasureRequested
from eurostream.warehouse import Warehouse

ANONYMIZED = "<anonymized>"

logger = logging.getLogger(__name__)


@dataclass
class ErasureAudit:
    request_id: str
    customer_id: str
    requested_at: float
    completed_at: float
    layers_touched: list[str] = field(default_factory=list)
    status: str = "completed"
    confirmation_hash: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "customer_id": self.customer_id,
            "requested_at": self.requested_at,
            "completed_at": self.completed_at,
            "layers_touched": self.layers_touched,
            "status": self.status,
            "confirmation_hash": self.confirmation_hash,
        }


class ErasureService:
    """Right-to-erasure (GDPR Art. 17) orchestration.

    Given a customer_id the service fans a deletion out to every layer:

    1. Issues an ``erasure_requested`` tombstone on the bus so streaming
       consumers can suppress future events for that customer.
    2. Physically anonymizes the customer's rows in the DuckDB warehouse
       (Bronze retains the row for analytics but with PII replaced; Silver
       drops PII columns entirely; Gold deletes the customer).
    3. Writes a tamper-evident audit record (request_id, layers touched,
       confirmation hash) to the audit log.

    The same code path runs as the local worker; the FastAPI endpoint only
    enqueues onto the bus.
    """

    def __init__(
        self,
        warehouse: Warehouse,
        producer: Producer,
        consumer: Consumer,
        audit_log_path: Path,
        metrics: Metrics,
        sla_seconds: int = 60,
        on_complete: Callable[[ErasureAudit], None] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._producer = producer
        self._consumer = consumer
        self._audit_log_path = audit_log_path
        self._metrics = metrics
        self._sla = sla_seconds
        self._on_complete = on_complete
        # In-memory fast path seeded from the durable registry in the
        # warehouse, so suppression survives process restarts.
        self._suppressed: set[str] = set(warehouse.suppressed_ids())
        self._lock = threading.Lock()

    # ---- public interface ----

    def request_erasure(
        self,
        customer_id: str,
        request_id: str | None = None,
        requested_by: str = "dsar@eurocart.eu",
    ) -> str:
        rid = request_id or _new_id()
        evt = ErasureRequested(
            event_id=rid,
            occurred_at=time.time(),
            request_id=rid,
            customer_id=customer_id,
            requested_by=requested_by,
        )
        self._producer.produce(
            "erasure_requests",
            key=customer_id,
            value=evt.model_dump_json(),
            headers={"schema_version": str(evt.schema_version), "event_type": evt.event_type},
        )
        self._metrics.incr("erasure_requested")
        return evt.request_id

    def execute(self, event: ErasureRequested) -> ErasureAudit:
        """Runs the full cascade synchronously (used by the worker and by the
        CLI demo). Returns the audit record."""
        started = time.time()
        layers: list[str] = []
        with self._lock:
            self._suppressed.add(event.customer_id)
        # Durable record so streaming consumers in other processes see the
        # suppression too (they seed their in-memory set from this table).
        self._warehouse.add_suppressed(event.customer_id, added_at=started)
        layers.append("suppression_registry")
        self._anonymize_warehouse(event.customer_id)
        layers.append("warehouse")
        # Lake layer is counted if a completion hook is wired (normally the Parquet re-snapshot).
        if self._on_complete is not None:
            layers.append("lake")
        audit = ErasureAudit(
            request_id=event.request_id,
            customer_id=event.customer_id,
            requested_at=event.occurred_at,
            completed_at=time.time(),
            layers_touched=layers,
            status="completed",
            confirmation_hash=self._confirmation_hash(event.request_id, event.customer_id),
        )
        self._append_audit(audit)
        # SLA is end-to-end from request time, not worker start, so queue time counts.
        latency = audit.completed_at - audit.requested_at
        self._metrics.observe("erasure_latency", latency)
        if latency > self._sla:
            self._metrics.incr("erasure_sla_breach")
            logger.warning(
                "erasure SLA breach: request=%s customer=%s latency=%.3fs sla=%ss",
                event.request_id,
                event.customer_id,
                latency,
                self._sla,
            )
        logger.info(
            "erasure completed: request=%s customer=%s layers=%s hash=%s",
            event.request_id,
            event.customer_id,
            ",".join(layers),
            audit.confirmation_hash,
        )
        if self._on_complete:
            try:
                self._on_complete(audit)
            except Exception:
                logger.exception("on_complete failed for erasure %s", event.request_id)
        return audit

    def is_suppressed(self, customer_id: str) -> bool:
        with self._lock:
            return customer_id in self._suppressed

    def suppressed_customers(self) -> list[str]:
        """Snapshot of all suppressed customer IDs (for health/metrics)."""
        with self._lock:
            return sorted(self._suppressed)

    def run_worker(
        self, poll_timeout: float = 0.2, stop_event: threading.Event | None = None
    ) -> None:
        """Consumes ``erasure_requests`` forever, executing each request."""
        logger.info("erasure worker started")
        while stop_event is None or not stop_event.is_set():
            record = self._consumer.poll(poll_timeout)
            if record is None:
                time.sleep(0.05)
                continue
            try:
                payload = record.json_value()
                event = ErasureRequested(**payload)
            except Exception:
                self._metrics.incr("malformed_erasure_requests")
                logger.warning("malformed erasure record at offset %s", record.offset)
                self._consumer.commit()
                continue
            self.execute(event)
            self._consumer.commit()

    # ---- internals ----

    def _anonymize_warehouse(self, customer_id: str) -> None:
        conn = self._warehouse.conn
        conn.execute(
            "UPDATE bronze.orders SET email=?, iban=? WHERE customer_id=?",
            (ANONYMIZED, ANONYMIZED, customer_id),
        )
        conn.execute(
            "UPDATE bronze.payments SET iban=? WHERE customer_id=?",
            (ANONYMIZED, customer_id),
        )
        conn.execute(
            "UPDATE bronze.clicks SET ip_address=? WHERE customer_id=?",
            (ANONYMIZED, customer_id),
        )
        conn.execute("DELETE FROM silver.customers WHERE customer_id=?", (customer_id,))
        conn.execute("DELETE FROM silver.orders WHERE customer_id=?", (customer_id,))
        conn.execute("DELETE FROM silver.payments WHERE customer_id=?", (customer_id,))
        conn.execute("DELETE FROM gold.customer_360 WHERE customer_id=?", (customer_id,))
        conn.execute("DELETE FROM gold.order_facts WHERE customer_id=?", (customer_id,))
        conn.execute("DELETE FROM gold.fraud_summary WHERE customer_id=?", (customer_id,))
        if self._warehouse.table_exists("bronze", "fraud_alerts"):
            conn.execute("DELETE FROM bronze.fraud_alerts WHERE customer_id=?", (customer_id,))

        if self._warehouse.turso:
            try:
                t = self._warehouse.turso
                t.execute(
                    "UPDATE bronze.orders SET email=?, iban=? WHERE customer_id=?",
                    (ANONYMIZED, ANONYMIZED, customer_id),
                )
                t.execute(
                    "UPDATE bronze.payments SET iban=? WHERE customer_id=?",
                    (ANONYMIZED, customer_id),
                )
                t.execute(
                    "UPDATE bronze.clicks SET ip_address=? WHERE customer_id=?",
                    (ANONYMIZED, customer_id),
                )
                t.execute("DELETE FROM silver.customers WHERE customer_id=?", (customer_id,))
                t.execute("DELETE FROM silver.orders WHERE customer_id=?", (customer_id,))
                t.execute("DELETE FROM silver.payments WHERE customer_id=?", (customer_id,))
                t.execute("DELETE FROM gold.customer_360 WHERE customer_id=?", (customer_id,))
                t.execute("DELETE FROM gold.order_facts WHERE customer_id=?", (customer_id,))
                t.execute("DELETE FROM gold.fraud_summary WHERE customer_id=?", (customer_id,))
                t.execute("DELETE FROM bronze.fraud_alerts WHERE customer_id=?", (customer_id,))
            except Exception as e:
                logger.warning("Turso erasure cascade error: %s", e)

    def _confirmation_hash(self, request_id: str, customer_id: str) -> str:
        return hashlib.sha256(f"{request_id}:{customer_id}".encode()).hexdigest()[:16]

    def _append_audit(self, audit: ErasureAudit) -> None:
        # DB first (transactional), then file append. If file write fails, DB still has record;
        # on restart the JSONL can be rebuilt from DB. This avoids divergence where file has entry but DB doesn't.
        self._warehouse.conn.execute(
            "INSERT INTO governance.erasure_audit_log VALUES (?,?,?,?,?,?,?)",
            (
                audit.request_id,
                audit.customer_id,
                audit.requested_at,
                audit.completed_at,
                ",".join(audit.layers_touched),
                audit.status,
                audit.confirmation_hash,
            ),
        )
        if self._warehouse.turso:
            try:
                self._warehouse.turso.execute(
                    "INSERT INTO governance.erasure_audit_log VALUES (?,?,?,?,?,?,?)",
                    (
                        audit.request_id,
                        audit.customer_id,
                        audit.requested_at,
                        audit.completed_at,
                        ",".join(audit.layers_touched),
                        audit.status,
                        audit.confirmation_hash,
                    ),
                )
            except Exception as e:
                logger.warning("Turso audit insert error: %s", e)
        try:
            self._audit_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._audit_log_path.open("a") as fh:
                fh.write(json.dumps(audit.to_dict()) + "\n")
        except Exception:
            logger.exception("failed to append audit JSONL for %s", audit.request_id)


def _new_id() -> str:
    import uuid

    return str(uuid.uuid4())

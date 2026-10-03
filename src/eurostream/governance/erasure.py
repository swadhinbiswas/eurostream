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
        # Intake state: what has been accepted but has not finished, so
        # `GET /erasure-requests/{id}` can answer "queued" honestly instead of
        # 404ing between the 202 and the audit row. Bounded: a queue nobody
        # drains must not grow without limit.
        self._pending: dict[str, dict[str, object]] = {}
        self._pending_limit = 10_000

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
        with self._lock:
            self._track_locked(
                request_id=evt.request_id,
                customer_id=customer_id,
                requested_at=evt.occurred_at,
                status="queued",
                requested_by=requested_by,
            )
        return evt.request_id

    def execute(self, event: ErasureRequested) -> ErasureAudit:
        """Runs the full cascade synchronously (used by the worker and by the
        CLI demo). Returns the audit record.

        Fail-closed: the suppression flag is set before any deletion, so a
        crash mid-cascade leaves the customer suppressed rather than
        half-erased and re-scoring. The failure is written to the audit log
        with ``status="failed"`` and re-raised for the caller.
        """
        started = time.time()
        layers: list[str] = []
        with self._lock:
            self._suppressed.add(event.customer_id)
            entry = self._pending.get(event.request_id)
            if entry is None:
                # A tombstone can arrive from another process (bus replay, the
                # CLI); track it here so the status endpoint still works.
                self._track_locked(
                    request_id=event.request_id,
                    customer_id=event.customer_id,
                    requested_at=event.occurred_at,
                    status="executing",
                )
            else:
                entry["status"] = "executing"
                entry["started_at"] = started
        # Durable record so streaming consumers in other processes see the
        # suppression too (they seed their in-memory set from this table).
        self._warehouse.add_suppressed(event.customer_id, added_at=started)
        layers.append("suppression_registry")

        try:
            turso_ok = self._anonymize_warehouse(event.customer_id)
            layers.append("warehouse")
            if turso_ok:
                layers.append("turso")
        except Exception:
            failure = ErasureAudit(
                request_id=event.request_id,
                customer_id=event.customer_id,
                requested_at=event.occurred_at,
                completed_at=time.time(),
                layers_touched=layers,
                status="failed",
                confirmation_hash=self._confirmation_hash(event.request_id, event.customer_id),
            )
            try:
                self._append_audit(failure)
            except Exception:
                logger.exception("could not record failed erasure %s", event.request_id)
            finally:
                self._pop_pending(event.request_id)
            self._metrics.incr("erasure_failed")
            logger.exception(
                "erasure cascade failed: request=%s customer=%s layers=%s",
                event.request_id,
                event.customer_id,
                ",".join(layers),
            )
            raise

        audit = ErasureAudit(
            request_id=event.request_id,
            customer_id=event.customer_id,
            requested_at=event.occurred_at,
            completed_at=time.time(),
            layers_touched=layers,
            status="completed",
            confirmation_hash=self._confirmation_hash(event.request_id, event.customer_id),
        )
        # Re-snapshot the lake before the audit is written, so a failed export
        # is recorded instead of being claimed as a layer that was touched.
        if self._on_complete is not None:
            try:
                self._on_complete(audit)
                audit.layers_touched.append("lake")
            except Exception:
                audit.status = "completed_with_errors"
                self._metrics.incr("erasure_lake_export_failed")
                logger.exception("lake re-export failed for erasure %s", event.request_id)
        self._append_audit(audit)
        self._pop_pending(event.request_id)
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
            ",".join(audit.layers_touched),
            audit.confirmation_hash,
        )
        return audit

    def pending_requests(self) -> list[dict[str, object]]:
        """Snapshot of accepted-but-unfinished requests, oldest first."""
        with self._lock:
            entries = [dict(e) for e in self._pending.values()]
        return sorted(entries, key=_requested_at)

    def pending_request(self, request_id: str) -> dict[str, object] | None:
        with self._lock:
            entry = self._pending.get(request_id)
            return dict(entry) if entry is not None else None

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
        """Consumes ``erasure_requests`` forever, executing each request.

        One bad request must not take the worker down: a GDPR intake queue
        that dies on a poison message stops erasing people. Failures are
        dead-lettered to ``erasure_requests_dlq`` and the offset is committed
        so the loop keeps making progress.
        """
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
                self._dead_letter(record.value)
                self._consumer.commit()
                continue
            try:
                self.execute(event)
            except Exception:
                self._metrics.incr("erasure_worker_failures")
                logger.exception(
                    "erasure failed for request=%s customer=%s (dead-lettered)",
                    event.request_id,
                    event.customer_id,
                )
                self._dead_letter(record.value)
            self._consumer.commit()

    def _dead_letter(self, raw: str | None) -> None:
        """Park an unprocessable request where an operator can replay it."""
        if raw is None:
            return
        try:
            self._producer.produce(
                "erasure_requests_dlq",
                key="failed",
                value=raw,
                headers={"reason": "execution_failed"},
            )
            if hasattr(self._producer, "flush"):
                self._producer.flush()
        except Exception:
            logger.exception("could not dead-letter erasure request")

    # ---- internals ----

    def _track_locked(
        self,
        *,
        request_id: str,
        customer_id: str,
        requested_at: float,
        status: str,
        requested_by: str | None = None,
    ) -> None:
        """Record intake state. Caller holds ``self._lock``."""
        while len(self._pending) >= self._pending_limit:
            oldest = min(self._pending, key=lambda k: _requested_at(self._pending[k]))
            evicted = self._pending.pop(oldest, None)
            logger.warning(
                "pending erasure registry full; evicted oldest %s (still unexecuted)",
                oldest,
            )
            if evicted is None:  # pragma: no cover - defensive against a torn dict
                break
        entry: dict[str, object] = {
            "request_id": request_id,
            "customer_id": customer_id,
            "requested_at": requested_at,
            "status": status,
        }
        if requested_by is not None:
            entry["requested_by"] = requested_by
        self._pending[request_id] = entry

    def _pop_pending(self, request_id: str) -> None:
        with self._lock:
            self._pending.pop(request_id, None)

    def _anonymize_warehouse(self, customer_id: str) -> bool:
        """Anonymize Bronze and delete the customer from Silver/Gold.

        Returns ``True`` when the Turso replica was also updated — callers put
        that in ``layers_touched`` so the audit log never claims a layer that
        silently failed. The DuckDB statements run inside one transaction so a
        failure cannot leave the customer erased in Bronze but intact in Gold.
        """
        with self._warehouse.cursor() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
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
                    conn.execute(
                        "DELETE FROM bronze.fraud_alerts WHERE customer_id=?", (customer_id,)
                    )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

        if not self._warehouse.turso:
            return False
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
        except Exception:
            # Not fatal: DuckDB already holds the erased state and the audit
            # will show `turso` missing from layers_touched.
            logger.exception("Turso erasure cascade failed for %s", customer_id)
            return False
        return True

    def _confirmation_hash(self, request_id: str, customer_id: str) -> str:
        return hashlib.sha256(f"{request_id}:{customer_id}".encode()).hexdigest()[:16]

    def _append_audit(self, audit: ErasureAudit) -> None:
        # DB first (transactional), then file append. If file write fails, DB still has record;
        # on restart the JSONL can be rebuilt from DB. This avoids divergence where file has entry but DB doesn't.
        row = (
            audit.request_id,
            audit.customer_id,
            audit.requested_at,
            audit.completed_at,
            ",".join(audit.layers_touched),
            audit.status,
            audit.confirmation_hash,
        )
        with self._warehouse.cursor() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                # Replays of the same request must not stack up duplicate
                # attestations for one DSAR.
                conn.execute(
                    "DELETE FROM governance.erasure_audit_log WHERE request_id=?",
                    (audit.request_id,),
                )
                conn.execute(
                    "INSERT INTO governance.erasure_audit_log VALUES (?,?,?,?,?,?,?)",
                    row,
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")
        if self._warehouse.turso:
            try:
                self._warehouse.turso.execute(
                    "DELETE FROM governance.erasure_audit_log WHERE request_id=?",
                    (audit.request_id,),
                )
                self._warehouse.turso.execute(
                    "INSERT INTO governance.erasure_audit_log VALUES (?,?,?,?,?,?,?)",
                    row,
                )
            except Exception as e:
                logger.warning("Turso audit insert error: %s", e)
        try:
            self._audit_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._audit_log_path.open("a") as fh:
                fh.write(json.dumps(audit.to_dict()) + "\n")
        except Exception:
            logger.exception("failed to append audit JSONL for %s", audit.request_id)


def _requested_at(entry: dict[str, object]) -> float:
    """Intake timestamp as float; a missing field sorts first, not last."""
    value = entry.get("requested_at")
    return float(value) if isinstance(value, (int, float)) else 0.0


def _new_id() -> str:
    import uuid

    return str(uuid.uuid4())

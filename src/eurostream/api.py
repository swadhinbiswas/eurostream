from __future__ import annotations

import logging
import random
import time
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

from eurostream import __version__
from eurostream.config import Settings
from eurostream.dashboard import get_dashboard_html
from eurostream.governance.erasure import ErasureAudit, ErasureService
from eurostream.governance.pii import PIIClassifier
from eurostream.lineage import LineageEmitter
from eurostream.metrics import Metrics
from eurostream.models import ErasureRequested
from eurostream.orchestration import DAG, DAGTask
from eurostream.producers import ClickProducer, OrderProducer, PaymentProducer
from eurostream.quality import DataQualityEngine
from eurostream.streaming import FraudScorer, FraudStreamProcessor
from eurostream.warehouse import Warehouse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


class ErasureRequest(BaseModel):
    customer_id: str = Field(min_length=3)
    reason: str = "GDPR_ARTICLE_17"
    sync: bool = False


def create_app(
    erasure: ErasureService,
    metrics: Metrics,
    settings: Settings,
    warehouse: Warehouse | None = None,
    bus: Any = None,
) -> FastAPI:
    app = FastAPI(
        title="EuroStream Governance API",
        version=__version__,
        description="GDPR-compliant real-time customer analytics, DSAR right-to-erasure cascade, and Prometheus observability.",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/", response_class=HTMLResponse)
    def root_dashboard() -> str:
        return get_dashboard_html()

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard_view() -> str:
        return get_dashboard_html()

    @app.get("/api")
    def api_index() -> dict[str, object]:
        return {
            "name": "EuroStream Governance API",
            "version": __version__,
            "docs": "/docs",
            "health": "/health",
            "stats": "/stats",
            "metrics": "/metrics",
            "prometheus": "/metrics/prometheus",
            "erasure_audit": "/governance/erasure-audit",
            "customer_360": "/gold/customer-360",
            "fraud_summary": "/gold/fraud_summary",
            "fraud_alerts": "/fraud_alerts",
            "data_quality_runs": "/governance/data_quality_runs",
            "suppression_registry": "/governance/suppression-registry",
        }

    @app.get("/stats")
    def pipeline_stats() -> dict[str, object]:
        stats: dict[str, object] = {
            "version": __version__,
            "backend": settings.event_bus_backend,
            "sla_seconds": settings.erasure_sla_seconds,
            "suppressed_customers": len(erasure.suppressed_customers()),
            "turso_connected": warehouse.turso is not None if warehouse else False,
            "source": "duckdb",
        }
        if warehouse is not None:
            try:
                stats["bronze_orders"] = warehouse.scalar("SELECT count(*) FROM bronze.orders")
                stats["bronze_clicks"] = warehouse.scalar("SELECT count(*) FROM bronze.clicks")
                stats["bronze_payments"] = warehouse.scalar("SELECT count(*) FROM bronze.payments")
                stats["bronze_fraud_alerts"] = warehouse.scalar(
                    "SELECT count(*) FROM bronze.fraud_alerts"
                )
                stats["silver_customers"] = warehouse.scalar(
                    "SELECT count(*) FROM silver.customers"
                )
                stats["silver_orders"] = warehouse.scalar("SELECT count(*) FROM silver.orders")
                stats["silver_payments"] = warehouse.scalar("SELECT count(*) FROM silver.payments")
                stats["gold_customers"] = warehouse.scalar("SELECT count(*) FROM gold.customer_360")
                stats["gold_order_facts"] = warehouse.scalar(
                    "SELECT count(*) FROM gold.order_facts"
                )
                stats["fraud_alerts"] = warehouse.scalar("SELECT count(*) FROM gold.fraud_summary")
                try:
                    stats["silver_watermark"] = warehouse.get_watermark("silver")
                    stats["gold_watermark"] = warehouse.get_watermark("gold")
                    lp = settings.data_dir / "lineage.jsonl"
                    if lp.exists():
                        lines = lp.read_text().strip().splitlines()
                        if lines:
                            stats["lineage"] = lines[-1][:400]
                except Exception:  # noqa: S110
                    pass
            except Exception as e:
                logger.debug("Failed to query stats from warehouse: %s", e)

        stats["total_rows"] = sum(
            int(str(stats.get(k, 0) or 0))
            for k in [
                "bronze_orders",
                "bronze_clicks",
                "bronze_payments",
                "silver_customers",
                "gold_customers",
            ]
        )

        # Cloud fallback: query Hugging Face Lake if local storage is completely empty
        if stats["total_rows"] == 0:
            try:
                import duckdb

                hf_base = "hf://datasets/swadhinbiswas/eustream"
                for tbl, key in [
                    ("silver/orders", "silver_customers"),
                    ("gold/customer_360", "gold_customers"),
                    ("gold/order_facts", "gold_order_facts"),
                    ("gold/fraud_summary", "fraud_alerts"),
                ]:
                    try:
                        row = duckdb.query(
                            f"SELECT count(*) FROM read_parquet('{hf_base}/{tbl}.parquet')"  # noqa: S608
                        ).fetchone()
                        cnt = row[0] if row else 0
                        if cnt and int(cnt) > 0:
                            stats[key] = int(cnt)
                    except Exception:  # noqa: S110
                        pass
                stats["total_rows"] = sum(
                    int(str(stats.get(k, 0) or 0))
                    for k in [
                        "bronze_orders",
                        "bronze_clicks",
                        "bronze_payments",
                        "silver_customers",
                        "gold_customers",
                    ]
                )
                if stats["total_rows"] > 0:
                    stats["source"] = "hf_lake"
            except Exception as e:
                logger.debug("HF lake fallback failed: %s", e)
        return stats

    @app.post("/erasure-requests")
    def request_erasure(body: ErasureRequest) -> dict[str, object]:
        """Accepts a GDPR Art. 17 right-to-erasure request. Enqueues tombstone on bus;
        if body.sync is True, executes the full 6-layer deletion cascade synchronously."""
        request_id = erasure.request_erasure(
            body.customer_id,
            requested_by="dsar@eurocart.eu",
        )
        if body.sync:
            event = ErasureRequested(
                event_id=request_id,
                occurred_at=time.time(),
                request_id=request_id,
                customer_id=body.customer_id,
            )
            audit = erasure.execute(event)
            return {
                "request_id": request_id,
                "customer_id": body.customer_id,
                "status": "completed",
                "confirmation_hash": audit.confirmation_hash,
                "layers_touched": audit.layers_touched,
                "sla_seconds": settings.erasure_sla_seconds,
                "latency_seconds": round(audit.completed_at - audit.requested_at, 3),
            }

        return {
            "request_id": request_id,
            "customer_id": body.customer_id,
            "status": "queued",
            "sla_seconds": settings.erasure_sla_seconds,
        }

    @app.post("/erase/{customer_id}")
    def erase_customer_sync(customer_id: str) -> dict[str, object]:
        """Directly executes the 6-layer right-to-erasure cascade for a customer ID."""
        request_id = erasure.request_erasure(customer_id, requested_by="dsar@eurocart.eu")
        event = ErasureRequested(
            event_id=request_id,
            occurred_at=time.time(),
            request_id=request_id,
            customer_id=customer_id,
        )
        audit = erasure.execute(event)
        metrics.flush()
        return {
            "request_id": request_id,
            "customer_id": customer_id,
            "status": "completed",
            "confirmation_hash": audit.confirmation_hash,
            "layers_touched": audit.layers_touched,
            "sla_seconds": settings.erasure_sla_seconds,
            "latency_seconds": round(audit.completed_at - audit.requested_at, 3),
        }

    @app.get("/verify-erasure/{customer_id}")
    def verify_erasure(customer_id: str) -> dict[str, object]:
        """Verifies proof of deletion for a customer across all warehouse layers."""
        is_suppressed = erasure.is_suppressed(customer_id)
        gold_remaining = 0
        silver_remaining = 0
        bronze_clear = 0
        bronze_anon = 0
        audit_count = 0
        if warehouse is not None:
            gold_remaining = warehouse.scalar(
                f"SELECT count(*) FROM gold.customer_360 WHERE customer_id='{customer_id}'"  # noqa: S608
            )
            silver_remaining = warehouse.scalar(
                f"SELECT count(*) FROM silver.customers WHERE customer_id='{customer_id}'"  # noqa: S608
            )
            bronze_clear = warehouse.scalar(
                f"SELECT count(*) FROM bronze.orders WHERE customer_id='{customer_id}' AND email <> '<anonymized>'"  # noqa: S608
            )
            bronze_anon = warehouse.scalar(
                f"SELECT count(*) FROM bronze.orders WHERE customer_id='{customer_id}' AND email = '<anonymized>'"  # noqa: S608
            )
            audit_count = warehouse.scalar(
                f"SELECT count(*) FROM governance.erasure_audit_log WHERE customer_id='{customer_id}'"  # noqa: S608
            )

        verified = (
            is_suppressed
            and gold_remaining == 0
            and silver_remaining == 0
            and bronze_clear == 0
            and audit_count >= 1
        )
        return {
            "customer_id": customer_id,
            "verified": verified,
            "is_suppressed": is_suppressed,
            "gold_rows_remaining": gold_remaining,
            "silver_rows_remaining": silver_remaining,
            "bronze_clear_text_rows": bronze_clear,
            "bronze_anonymized_rows": bronze_anon,
            "audit_log_entries": audit_count,
        }

    @app.post("/produce")
    def trigger_produce(
        events: int = Query(default=100, ge=1, le=5000),
        burst_customer: str | None = None,
    ) -> dict[str, object]:
        """Emits synthetic EU events to the bus topics."""
        if bus is None:
            return {"status": "error", "message": "Bus not initialized"}
        order = OrderProducer(bus, settings)
        click = ClickProducer(bus, settings)
        payment = PaymentProducer(bus, settings)
        for i in range(events):
            order.emit()
            click.emit()
            payment.emit()
            if burst_customer and (i % 3 == 0):
                payment.emit(
                    customer_id=burst_customer,
                    amount=random.uniform(900, 2000),
                    country="DE",
                    merchant_country="NL",
                )
        # Emit a couple of natural anomalies to ensure fraud scorer has alerts to catch
        anom_cust = burst_customer or f"cust_burst_{random.randint(100, 999)}"
        for _ in range(settings.fraud_velocity_threshold + 2):
            payment.emit(
                customer_id=anom_cust,
                amount=random.uniform(700, 1500),
                country="DE",
                merchant_country="FR",
            )
        if hasattr(bus, "flush"):
            bus.flush()
        metrics.incr("events_produced", events * 3)
        metrics.flush()
        return {
            "status": "ok",
            "events_produced": events,
            "topics": ["orders", "clicks", "payments"],
            "anom_target": anom_cust,
        }

    @app.post("/stream")
    def trigger_stream(max_events: int = Query(default=200, ge=1, le=5000)) -> dict[str, object]:
        """Consumes payments and runs real-time FraudScorer."""
        if bus is None or warehouse is None:
            return {"status": "error", "message": "Bus or Warehouse not initialized"}
        scorer = FraudScorer(settings, metrics)
        processor = FraudStreamProcessor(
            consumer=bus.consumer("payments", "api-fraud-stream", auto_offset_reset="earliest"),
            scorer=scorer,
            metrics=metrics,
            output_producer=bus,
            alert_topic="fraud_alerts",
            suppression_check=erasure.is_suppressed,
        )
        alerts = processor.run(max_events=max_events)
        warehouse.ingest_fraud_alerts([a.to_dict() for a in alerts])
        metrics.flush()
        return {
            "status": "ok",
            "alerts_emitted": len(alerts),
            "alerts": [a.to_dict() for a in alerts[:20]],
        }

    @app.post("/transform")
    def trigger_transform(incremental: bool = Query(default=True)) -> dict[str, object]:
        """Runs the Medallion transformation pipeline DAG and updates Lake."""
        if bus is None or warehouse is None:
            return {"status": "error", "message": "Bus or Warehouse not initialized"}
        lineage = LineageEmitter(settings.data_dir / "lineage.jsonl")
        classifier = PIIClassifier(settings.pii_manifest_path)

        def ingest_bronze() -> None:
            lineage.start(
                "ingest_bronze",
                inputs=["bus.orders", "bus.clicks", "bus.payments"],
                outputs=["bronze.orders", "bronze.clicks", "bronze.payments"],
            )
            loaded = {}
            for topic in ("orders", "clicks", "payments"):
                consumer = bus.consumer(topic, "api-bronze-ingest", auto_offset_reset="earliest")
                records = []
                while True:
                    rec = consumer.poll(0.05)
                    if rec is None:
                        break
                    records.append(rec)
                    if len(records) >= 1000:
                        break
                if records:
                    warehouse.load_bronze_from_records(topic, records)
                    consumer.commit()
                loaded[topic] = len(records)
                consumer.close()
            lineage.complete("ingest_bronze", extra=loaded)

        def pii_scan() -> None:
            rows = warehouse.bronze_rows("orders", 200)
            for r in rows:
                r["_table"] = "bronze.orders"
            manifest = classifier.load()
            if not manifest:
                classifier.build_from_rows(rows)
                classifier.save()
                warehouse.save_manifest(classifier.load())
                return
            findings = classifier.detect_unregistered(rows)
            if findings:
                raise RuntimeError(f"unregistered PII columns: {findings}")

        def build_silver() -> None:
            lineage.start(
                "build_silver",
                inputs=["bronze.orders", "bronze.payments"],
                outputs=["silver.customers", "silver.orders", "silver.payments"],
            )
            if incremental:
                stats = warehouse.build_silver_incremental(pii_salt=settings.pii_salt)
                lineage.complete("build_silver", extra=stats)
            else:
                warehouse.build_silver(pii_salt=settings.pii_salt)
                lineage.complete("build_silver")

        def build_gold() -> None:
            lineage.start(
                "build_gold",
                inputs=["silver.customers", "silver.orders"],
                outputs=["gold.customer_360", "gold.order_facts"],
            )
            if incremental:
                stats = warehouse.build_gold_incremental()
                lineage.complete("build_gold", extra=stats)
            else:
                warehouse.build_gold()
                lineage.complete("build_gold")

        def quality_gate() -> None:
            report = DataQualityEngine(warehouse).run_all()
            if not report.all_passed:
                failed = [r.check_name for r in report.results if not r.passed]
                raise RuntimeError(f"data quality gate failed: {failed}")

        def export_lake() -> None:
            warehouse.export_lake(settings.lake_root)

        dag = DAG(
            dag_id="medallion-api",
            tasks=[
                DAGTask("ingest_bronze", ingest_bronze, depends_on=[]),
                DAGTask("pii_scan", pii_scan, depends_on=["ingest_bronze"]),
                DAGTask("build_silver", build_silver, depends_on=["pii_scan"]),
                DAGTask("build_gold", build_gold, depends_on=["build_silver"]),
                DAGTask("quality_gate", quality_gate, depends_on=["build_gold"]),
                DAGTask("export_lake", export_lake, depends_on=["quality_gate"]),
            ],
        )
        results = dag.run()
        metrics.flush()
        return {
            "status": "ok",
            "incremental": incremental,
            "tasks": {
                k: {"ok": v.ok, "duration_s": round(v.duration_s, 3)} for k, v in results.items()
            },
        }

    @app.post("/quality-gate")
    def trigger_quality_gate() -> dict[str, object]:
        """Runs the Data Quality engine and stores run history."""
        if warehouse is None:
            return {"status": "error", "message": "Warehouse not initialized"}
        report = DataQualityEngine(warehouse).run_all()
        return {
            "status": "ok",
            "run_id": report.run_id,
            "all_passed": report.all_passed,
            "results": [
                {"check_name": r.check_name, "passed": r.passed, "detail": r.detail}
                for r in report.results
            ],
        }

    @app.get("/health")
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "version": __version__,
            "backend": settings.event_bus_backend,
            "turso_connected": warehouse.turso is not None if warehouse else False,
            "suppressed": erasure.suppressed_customers(),
        }

    @app.get("/metrics")
    def metrics_endpoint() -> dict[str, object]:
        return metrics.snapshot()

    @app.get(
        "/metrics/prometheus",
        response_class=PlainTextResponse,
        responses={200: {"content": {"text/plain; version=0.0.4": {}}}},
    )
    def metrics_prometheus() -> str:
        """Prometheus scrape target — standard text exposition format."""
        return metrics.render_prometheus()

    if warehouse is not None:

        @app.get("/governance/erasure-audit")
        def erasure_audit() -> list[dict[str, object]]:
            return warehouse.query(
                "SELECT * FROM governance.erasure_audit_log ORDER BY completed_at DESC LIMIT 50"
            )

        @app.get("/gold/customer-360")
        def customer_360(
            limit: int = Query(default=100, ge=1, le=1000),
            search: str | None = None,
        ) -> list[dict[str, object]]:
            sql = "SELECT * FROM gold.customer_360"
            if search:
                safe_s = search.replace("'", "''")
                sql += f" WHERE customer_id LIKE '%{safe_s}%'"
            sql += f" ORDER BY customer_id LIMIT {int(limit)}"
            return warehouse.query(sql)

        @app.get("/gold/fraud_summary")
        def fraud_summary() -> list[dict[str, object]]:
            return warehouse.query("SELECT * FROM gold.fraud_summary ORDER BY last_alert DESC")

        @app.get("/fraud_alerts")
        def fraud_alerts(limit: int = Query(default=50, ge=1, le=500)) -> list[dict[str, object]]:
            try:
                return warehouse.query(
                    f"SELECT * FROM bronze.fraud_alerts ORDER BY alert_ts DESC LIMIT {int(limit)}"  # noqa: S608
                )
            except Exception:
                return warehouse.query(
                    f"SELECT * FROM gold.fraud_summary ORDER BY last_alert DESC LIMIT {int(limit)}"  # noqa: S608
                )

        @app.get("/governance/data_quality_runs")
        def data_quality_runs(
            limit: int = Query(default=20, ge=1, le=200),
        ) -> list[dict[str, object]]:
            return warehouse.query(
                f"SELECT * FROM governance.data_quality_runs ORDER BY checked_at DESC LIMIT {int(limit)}"  # noqa: S608
            )

        @app.get("/governance/suppression-registry")
        def suppression_registry() -> list[dict[str, object]]:
            return warehouse.query(
                "SELECT customer_id, added_at FROM governance.suppression_registry ORDER BY added_at DESC"
            )

        @app.get("/governance/watermarks")
        def watermarks_endpoint() -> list[dict[str, object]]:
            return warehouse.query("SELECT pipeline, last_ts FROM governance.watermarks")

        @app.post("/sync-turso")
        def sync_turso_endpoint(
            seed_lake: bool = Query(default=True),
        ) -> dict[str, object]:
            """Syncs local Medallion state to Turso cloud database."""
            if not warehouse.turso:
                return {
                    "status": "error",
                    "message": "Turso not configured on server (TURSO_DATABASE_URL / TURSO_AUTH_TOKEN missing)",
                }
            if seed_lake:
                warehouse.seed_from_lake(
                    settings.hf_repo if hasattr(settings, "hf_repo") else "swadhinbiswas/eustream"
                )
            warehouse.sync_all_to_turso()
            table_counts = {}
            for tbl in [
                "bronze.orders",
                "bronze.clicks",
                "bronze.payments",
                "bronze.fraud_alerts",
                "silver.customers",
                "silver.orders",
                "silver.payments",
                "gold.customer_360",
                "gold.order_facts",
                "gold.fraud_summary",
            ]:
                try:
                    table_counts[tbl] = warehouse.turso.scalar(f"SELECT count(*) FROM {tbl}")
                except Exception:
                    table_counts[tbl] = 0
            return {
                "status": "ok",
                "endpoint": warehouse.turso.http_endpoint,
                "table_counts": table_counts,
            }

        @app.get("/turso/status")
        def turso_status_endpoint() -> dict[str, object]:
            """Returns Turso connection status and row counts."""
            connected = warehouse.turso is not None
            table_counts = {}
            if connected and warehouse.turso:
                for tbl in [
                    "bronze.orders",
                    "bronze.clicks",
                    "bronze.payments",
                    "bronze.fraud_alerts",
                    "silver.customers",
                    "silver.orders",
                    "silver.payments",
                    "gold.customer_360",
                    "gold.order_facts",
                    "gold.fraud_summary",
                ]:
                    try:
                        table_counts[tbl] = warehouse.turso.scalar(f"SELECT count(*) FROM {tbl}")
                    except Exception:
                        table_counts[tbl] = 0
            return {
                "connected": connected,
                "endpoint": warehouse.turso.http_endpoint
                if connected and warehouse.turso
                else None,
                "table_counts": table_counts,
            }

    return app


def bootstrap() -> FastAPI:
    """Wire the API to the default runtime stack."""
    from eurostream.bus.sqlite import open_bus

    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    bus: Any = None
    if settings.event_bus_backend == "kafka":
        try:
            from eurostream.bus.kafka import KafkaBus

            bus = KafkaBus(settings)
        except (ImportError, ModuleNotFoundError, ValueError) as e:
            logger.warning(
                "Kafka backend requested but unavailable (%s) — falling back to SQLite", e
            )
            bus = None
    if bus is None:
        bus = open_bus(settings.data_dir / "events.db")
    assert bus is not None
    warehouse = Warehouse(
        settings.warehouse_path,
        turso_url=settings.turso_database_url,
        turso_token=settings.turso_auth_token,
    )
    metrics = Metrics(settings.metrics_path)

    def refresh_lake(_audit: ErasureAudit) -> None:
        warehouse.export_lake(settings.lake_root)

    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "api-worker", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
        sla_seconds=settings.erasure_sla_seconds,
        on_complete=refresh_lake,
    )
    return create_app(erasure, metrics, settings, warehouse, bus)


app = bootstrap()

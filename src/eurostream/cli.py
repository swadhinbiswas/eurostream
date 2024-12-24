from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import typer

from eurostream.bus import Consumer, Record
from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings, get_settings
from eurostream.contracts import ContractRegistry
from eurostream.governance.erasure import ErasureAudit, ErasureService
from eurostream.governance.pii import PIIClassifier
from eurostream.lineage import LineageEmitter
from eurostream.metrics import Metrics
from eurostream.models import ErasureRequested
from eurostream.orchestration import DAG, DAGTask
from eurostream.producers import (
    ClickProducer,
    OrderProducer,
    PaymentProducer,
)
from eurostream.quality import DataQualityEngine
from eurostream.streaming import FraudScorer, FraudStreamProcessor
from eurostream.warehouse import Warehouse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

app = typer.Typer(help="EuroStream — GDPR-compliant real-time analytics platform")


def _fresh() -> tuple[Settings, Any, Warehouse, Metrics, PIIClassifier]:
    settings = get_settings()
    for p in [settings.data_dir, settings.lake_root, settings.audit_log_path.parent]:
        p.mkdir(parents=True, exist_ok=True)
    # Factory: sqlite (local, zero deps) vs kafka (Aiven, SASL_SSL).
    # .env controls it: EUROSTREAM_EVENT_BUS_BACKEND=kafka + KAFKA_* vars.
    if settings.event_bus_backend == "kafka":
        from eurostream.bus.kafka import KafkaBus

        bus: Any = KafkaBus(settings)
    else:
        bus = open_bus(settings.data_dir / "events.db")
    warehouse = Warehouse(settings.warehouse_path)
    metrics = Metrics(settings.metrics_path)
    classifier = PIIClassifier(settings.pii_manifest_path)
    return settings, bus, warehouse, metrics, classifier


def _erasure_service(
    settings: Settings,
    bus: Any,
    warehouse: Warehouse,
    metrics: Metrics,
    group: str = "erasure-worker",
) -> ErasureService:
    # Re-snapshot the lake after every erasure so deleted customers
    # cannot survive in a stale Parquet copy.
    def refresh_lake(_audit: ErasureAudit) -> None:
        warehouse.export_lake(settings.lake_root)

    return ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", group, auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
        sla_seconds=settings.erasure_sla_seconds,
        on_complete=refresh_lake,
    )


@app.command()
def contracts(
    baseline: Path = typer.Option(
        None, "--baseline", "-b", help="committed baseline contracts.json to check against"
    ),
    out: Path = typer.Option(None, "--out", "-o", help="where to write the current snapshot"),
) -> None:
    """Snapshot the schema contract and optionally validate against a committed baseline.

    CI passes --baseline governance/contracts.json; the check fails on any
    breaking drift before it can reach production consumers."""
    reg = ContractRegistry()
    target = out or get_settings().data_dir / "contracts.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    reg.save(target)
    typer.echo(f"wrote contract snapshot to {target}")
    if baseline is not None:
        if not baseline.exists():
            typer.echo(f"baseline not found: {baseline}")
            raise typer.Exit(1)
        violations = reg.check_against(reg.load(baseline))
        if violations:
            for v in violations:
                typer.echo(f"BREAKING: {v}")
            raise typer.Exit(1)
        typer.echo("contract check passed")


@app.command()
def produce(
    events: int = typer.Option(50, help="events per topic"),
    burst_customer: str = typer.Option(None, help="customer to target with a fraud burst"),
) -> None:
    """Run the three simulated source systems into the bus."""
    settings, bus, _, _, _ = _fresh()
    order = OrderProducer(bus, settings)
    click = ClickProducer(bus, settings)
    payment = PaymentProducer(bus, settings)
    for i in range(events):
        order.emit()
        click.emit()
        payment.emit()
        if burst_customer and (i % 3 == 0):
            payment.emit(customer_id=burst_customer, amount=random.uniform(900, 2000))
    typer.echo(f"produced {events} events per topic to {settings.data_dir / 'events.db'}")


@app.command()
def stream(
    max_events: int = typer.Option(None, help="stop after N payments consumed"),
    burst_customer: str = typer.Option(None, help="customer to burst payments for"),
) -> None:
    """Run the streaming fraud scorer against the payments topic."""
    settings, bus, warehouse, metrics, _ = _fresh()
    # The erasure service owns the suppression registry; customers who
    # exercised Art. 17 are skipped instead of scored.
    erasure = _erasure_service(settings, bus, warehouse, metrics)
    scorer = FraudScorer(settings, metrics)
    processor = FraudStreamProcessor(
        consumer=bus.consumer("payments", "fraud-stream", auto_offset_reset="earliest"),
        scorer=scorer,
        metrics=metrics,
        output_producer=bus,
        alert_topic="fraud_alerts",
        suppression_check=erasure.is_suppressed,
    )
    if burst_customer:
        payment = PaymentProducer(bus, settings)
        for _ in range(settings.fraud_velocity_threshold + 3):
            payment.emit(customer_id=burst_customer, country="DE", merchant_country="NL")
    typer.echo("streaming fraud scorer running...")
    alerts = processor.run(max_events=max_events)
    for alert in alerts:
        typer.echo(f"[{alert.severity}] {alert.rule} {alert.customer_id}: {alert.detail}")
    warehouse.ingest_fraud_alerts([a.to_dict() for a in alerts])
    metrics.flush()
    typer.echo(f"fraud alerts: {len(alerts)}")


@app.command()
def transform(
    incremental: bool = typer.Option(
        False, "--incremental", help="Incremental MERGE vs full rebuild"
    ),
) -> None:
    """Run the medallion DAG: bronze -> silver -> gold + data quality gate."""
    settings, bus, warehouse, metrics, classifier = _fresh()
    lineage = LineageEmitter(settings.data_dir / "lineage.jsonl")

    def pii_scan() -> None:
        rows = warehouse.bronze_rows("orders", 200)
        for r in rows:
            r["_table"] = "bronze.orders"
        manifest = classifier.load()
        if not manifest:
            classifier.build_from_rows(rows)
            classifier.save()
            warehouse.save_manifest(classifier.load())
            typer.echo("  pii: seeded manifest (first run)")
            return
        findings = classifier.detect_unregistered(rows)
        if findings:
            raise RuntimeError(f"unregistered PII columns: {findings}")
        typer.echo("  pii: manifest ok")

    def build_silver() -> None:
        lineage.start(
            "build_silver",
            inputs=["bronze.orders", "bronze.payments"],
            outputs=["silver.customers", "silver.orders", "silver.payments"],
        )
        if incremental:
            stats = warehouse.build_silver_incremental(pii_salt=settings.pii_salt)
            lineage.complete("build_silver", extra=stats)
            typer.echo(f"  silver incremental: {stats}")
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
            typer.echo(f"  gold incremental: {stats}")
        else:
            warehouse.build_gold()
            lineage.complete("build_gold")

    def quality_gate() -> None:
        report = DataQualityEngine(warehouse).run_all()
        if not report.all_passed:
            failed = [r.check_name for r in report.results if not r.passed]
            raise RuntimeError(f"data quality gate failed: {failed}")
        typer.echo(
            f"data quality: {sum(r.passed for r in report.results)}/{len(report.results)} passed"
        )

    def export_lake() -> None:
        paths = warehouse.export_lake(settings.lake_root)
        typer.echo(f"  lake export: {len(paths)} parquet files under {settings.lake_root}")

    dag = DAG(
        dag_id="medallion",
        tasks=[
            DAGTask("pii_scan", pii_scan, depends_on=[]),
            DAGTask("build_silver", build_silver, depends_on=["pii_scan"]),
            DAGTask("build_gold", build_gold, depends_on=["build_silver"]),
            DAGTask("quality_gate", quality_gate, depends_on=["build_gold"]),
            DAGTask("export_lake", export_lake, depends_on=["quality_gate"]),
        ],
    )
    results = dag.run()
    for task_id, r in results.items():
        typer.echo(f"  {task_id}: {'ok' if r.ok else 'FAILED'} ({r.duration_s:.2f}s)")
    metrics.flush()


@app.command()
def erase(customer_id: str) -> None:
    """Execute a right-to-erasure request for a customer across all layers."""
    settings, bus, warehouse, metrics, _ = _fresh()
    erasure = _erasure_service(settings, bus, warehouse, metrics)
    event = ErasureRequested(
        event_id=f"cli-{customer_id}",
        occurred_at=time.time(),
        request_id=f"cli-{customer_id}",
        customer_id=customer_id,
    )
    audit = erasure.execute(event)
    typer.echo(
        f"erased {customer_id} in {audit.completed_at - audit.requested_at:.2f}s "
        f"layers={audit.layers_touched} confirmation={audit.confirmation_hash}"
    )
    metrics.flush()


@app.command()
def worker(poll_timeout: float = typer.Option(0.2, help="Seconds between polls")) -> None:
    """Run the erasure worker: consume erasure_requests and execute each cascade.

    This is the production intake pattern — the API enqueues tombstones,
    this process fans them out. Ctrl+C for a graceful stop."""
    settings, bus, warehouse, metrics, _ = _fresh()
    erasure = _erasure_service(settings, bus, warehouse, metrics)
    typer.echo("erasure worker running — Ctrl+C to stop")
    try:
        erasure.run_worker(poll_timeout=poll_timeout)
    except KeyboardInterrupt:
        pass
    finally:
        metrics.flush()
        bus.close()
        warehouse.close()
        typer.echo("erasure worker stopped")


@app.command()
def demo() -> None:
    """End-to-end: produce -> stream fraud -> medallion -> erasure -> verify.

    Self-contained: resets the data dir first so a fresh run is deterministic.
    The individual subcommands (produce/stream/transform/erase) are the
    persistent pipeline."""
    settings = get_settings()
    for p in [settings.data_dir, settings.lake_root]:
        if p.exists():
            import shutil

            shutil.rmtree(p)
    settings, bus, warehouse, metrics, classifier = _fresh()
    order = OrderProducer(bus, settings)
    click = ClickProducer(bus, settings)
    payment = PaymentProducer(bus, settings)

    typer.echo("1/6 producing source events...")
    for _ in range(60):
        order.emit()
        click.emit()
        payment.emit()

    victim = "cust_424242"
    for _ in range(60):
        order.emit(customer_id=victim)
        click.emit(customer_id=victim)
        payment.emit(customer_id=victim, amount=random.uniform(50, 300))

    # The erasure service is created before streaming so its suppression
    # registry gates the fraud scorer from the start.
    erasure = _erasure_service(settings, bus, warehouse, metrics, group="demo-erasure")

    typer.echo("2/6 running fraud scoring...")
    scorer = FraudScorer(settings, metrics)
    for _ in range(settings.fraud_velocity_threshold + 3):
        payment.emit(
            customer_id=victim, amount=random.uniform(100, 400), country="DE", merchant_country="NL"
        )
    alerts = FraudStreamProcessor(
        consumer=bus.consumer("payments", "demo-stream", auto_offset_reset="earliest"),
        scorer=scorer,
        metrics=metrics,
        output_producer=bus,
        suppression_check=erasure.is_suppressed,
    ).run()
    warehouse.ingest_fraud_alerts([a.to_dict() for a in alerts])
    typer.echo(f"  fraud alerts emitted: {len(alerts)}")

    typer.echo("3/6 loading bus events into Bronze...")
    for topic in ("orders", "clicks", "payments"):
        consumer = bus.consumer(topic, "bronze-loader", auto_offset_reset="earliest")
        records = _drain(consumer)
        warehouse.load_bronze_from_records(topic, records)
        consumer.commit()

    typer.echo("3b/6 running medallion transform + quality gate...")
    transform_dag = DAG(
        dag_id="demo-medallion",
        tasks=[
            DAGTask("pii_scan", _pii_scan(warehouse, classifier), depends_on=[]),
            DAGTask(
                "build_silver",
                lambda: warehouse.build_silver(pii_salt=settings.pii_salt),
                depends_on=["pii_scan"],
            ),
            DAGTask("build_gold", warehouse.build_gold, depends_on=["build_silver"]),
            DAGTask(
                "quality_gate",
                _quality_gate(warehouse),
                depends_on=["build_gold"],
            ),
            DAGTask(
                "export_lake",
                lambda: warehouse.export_lake(settings.lake_root),
                depends_on=["quality_gate"],
            ),
        ],
    )
    transform_dag.run()

    bronze_before = warehouse.scalar(
        "SELECT count(*) c FROM bronze.orders WHERE customer_id = 'cust_424242' AND email <> '<anonymized>'"
    )

    typer.echo("4/6 executing GDPR right-to-erasure...")
    request_id = erasure.request_erasure(victim)
    event = ErasureRequested(
        event_id=request_id,
        occurred_at=time.time(),
        request_id=request_id,
        customer_id=victim,
    )
    audit = erasure.execute(event)

    typer.echo("5/6 verifying cascade...")
    bronze_after = warehouse.scalar(
        "SELECT count(*) c FROM bronze.orders WHERE customer_id = 'cust_424242' AND email = '<anonymized>'"
    )
    gold_after = warehouse.scalar(
        "SELECT count(*) c FROM gold.customer_360 WHERE customer_id = 'cust_424242'"
    )
    audit_rows = warehouse.scalar(
        "SELECT count(*) c FROM governance.erasure_audit_log WHERE customer_id = 'cust_424242'"
    )
    typer.echo(f"  bronze PII anonymized rows: {bronze_after} (before: {bronze_before} clear-text)")
    typer.echo(f"  gold customer_360 rows remaining: {gold_after} (expect 0)")
    typer.echo(f"  audit log entries: {audit_rows} (expect 1)")

    typer.echo("6/6 summary")
    typer.echo(
        f"  erasure SLA: {settings.erasure_sla_seconds}s, completed in "
        f"{audit.completed_at - audit.requested_at:.2f}s"
    )
    typer.echo(
        "  verification: PASSED"
        if gold_after == 0 and audit_rows == 1 and bronze_after > 0
        else "  verification: FAILED"
    )
    metrics.flush()


def _drain(consumer: Consumer) -> list[Record]:
    records: list[Record] = []
    while True:
        rec = consumer.poll(0.05)
        if rec is None:
            break
        records.append(rec)
    return records


def _pii_scan(warehouse: Warehouse, classifier: PIIClassifier) -> Callable[[], None]:
    def fn() -> None:
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

    return fn


def _quality_gate(warehouse: Warehouse) -> Callable[[], None]:
    def fn() -> None:
        report = DataQualityEngine(warehouse).run_all()
        if not report.all_passed:
            failed = [r.check_name for r in report.results if not r.passed]
            raise RuntimeError(f"data quality gate failed: {failed}")

    return fn


if __name__ == "__main__":
    app()

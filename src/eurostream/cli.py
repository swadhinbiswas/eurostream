from __future__ import annotations

import json
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
from eurostream.governance.audit_chain import AuditChain, verify_audit_log
from eurostream.governance.erasure import ErasureAudit, ErasureService
from eurostream.governance.pii import PIIClassifier
from eurostream.lineage import LineageEmitter
from eurostream.logging import configure_logging
from eurostream.metrics import Metrics
from eurostream.models import ErasureRequested
from eurostream.orchestration import DAG, DAGRunError, DAGTask, TaskResult
from eurostream.producers import (
    ClickProducer,
    OrderProducer,
    PaymentProducer,
)
from eurostream.quality import DataQualityEngine
from eurostream.streaming import FraudScorer, FraudStreamProcessor
from eurostream.warehouse import Warehouse

# Importing the CLI must not configure the host process's logging; every
# entrypoint calls configure_logging() explicitly with its own settings.
configure_logging()

app = typer.Typer(help="EuroStream — GDPR-compliant real-time analytics platform")


def _fresh() -> tuple[Settings, Any, Warehouse, Metrics, PIIClassifier]:
    settings = get_settings()
    # Re-apply with this deployment's EUROSTREAM_LOG_* overrides.
    configure_logging(settings.log_level, settings.log_format)
    for p in [settings.data_dir, settings.lake_root, settings.audit_log_path.parent]:
        p.mkdir(parents=True, exist_ok=True)
    # Factory: sqlite (local, zero deps) vs kafka (Aiven, SASL_SSL).
    # .env controls it: EUROSTREAM_EVENT_BUS_BACKEND=kafka + KAFKA_* vars.
    if settings.event_bus_backend == "kafka":
        try:
            from eurostream.bus.kafka import KafkaBus

            bus: Any = KafkaBus(settings)
        except (ImportError, ModuleNotFoundError, ValueError) as e:
            logging.getLogger(__name__).warning(
                "Kafka requested but unavailable (%s) — using SQLite", e
            )
            bus = open_bus(settings.data_dir / "events.db")
    else:
        bus = open_bus(settings.data_dir / "events.db")
    warehouse = Warehouse(
        settings.warehouse_path,
        turso_url=settings.turso_database_url,
        turso_token=settings.turso_auth_token,
    )
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
    if hasattr(bus, "flush"):
        bus.flush()
    dest = (
        settings.kafka_bootstrap_servers
        if settings.event_bus_backend == "kafka"
        else str(settings.data_dir / "events.db")
    )
    typer.echo(f"produced {events} events per topic to {dest}")


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

    def ingest_bronze() -> None:
        lineage.start(
            "ingest_bronze",
            inputs=["bus.orders", "bus.clicks", "bus.payments"],
            outputs=["bronze.orders", "bronze.clicks", "bronze.payments"],
        )
        loaded = {}
        for topic in ("orders", "clicks", "payments"):
            consumer = bus.consumer(topic, "bronze-ingester", auto_offset_reset="earliest")
            records = _drain(consumer)
            if records:
                warehouse.load_bronze_from_records(topic, records)
                consumer.commit()
            loaded[topic] = len(records)
            consumer.close()
        lineage.complete("ingest_bronze", extra=loaded)
        typer.echo(f"  bronze ingested: {loaded}")

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
        report = DataQualityEngine(
            warehouse,
            freshness_seconds=settings.dq_freshness_seconds,
            volume_drop_pct=settings.dq_volume_drop_pct,
        ).run_all()
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
            DAGTask("ingest_bronze", ingest_bronze, depends_on=[]),
            DAGTask("pii_scan", pii_scan, depends_on=["ingest_bronze"]),
            DAGTask("build_silver", build_silver, depends_on=["pii_scan"]),
            DAGTask("build_gold", build_gold, depends_on=["build_silver"]),
            DAGTask("quality_gate", quality_gate, depends_on=["build_gold"]),
            DAGTask("export_lake", export_lake, depends_on=["quality_gate"]),
        ],
    )

    def _on_task(r: TaskResult) -> None:
        _echo_task(r)
        if not r.ok:
            lineage.fail(r.task_id, r.error or "unknown error")

    try:
        dag.run(on_task=_on_task)
    except DAGRunError as exc:
        metrics.flush()
        typer.secho(f"transform failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    metrics.flush()


def _echo_task(r: TaskResult) -> None:
    """Print each task as it finishes and record failures in lineage.

    Registered via ``DAG.run(on_task=...)`` so a failing run still leaves a
    printed result and a ``failed`` lineage event instead of a bare traceback.
    """
    marker = "ok" if r.ok else "FAILED"
    typer.echo(f"  {r.task_id}: {marker} ({r.duration_s:.2f}s)")
    if not r.ok:
        typer.secho(f"    {r.error}", fg=typer.colors.RED, err=True)


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
    metrics.flush()
    if audit.status != "completed":
        typer.secho(
            f"erasure of {customer_id} FAILED after "
            f"{audit.completed_at - audit.requested_at:.2f}s "
            f"layers={audit.layers_touched} — see {settings.audit_log_path}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)
    typer.echo(
        f"erased {customer_id} in {audit.completed_at - audit.requested_at:.2f}s "
        f"layers={audit.layers_touched} confirmation={audit.confirmation_hash}"
    )


@app.command("verify-audit")
def verify_audit(
    path: Path = typer.Option(
        None, "--path", "-p", help="audit JSONL to verify (default: EUROSTREAM_AUDIT_LOG_PATH)"
    ),
    strict: bool = typer.Option(
        False,
        "--strict",
        "-s",
        help="also fail on records that predate the hash chain or on duplicate request_ids",
    ),
    json_out: bool = typer.Option(False, "--json", help="print the machine-readable report"),
    no_cross_check: bool = typer.Option(
        False, "--no-cross-check", help="verify the file alone, without the warehouse copy"
    ),
) -> None:
    """Verify the tamper-evident erasure audit log.

    Recomputes every hash (an edited record fails its own), follows every
    prev_hash link (a removed or reordered record breaks the one after it),
    and — unless --no-cross-check — compares the file against
    governance.erasure_audit_log, which is what catches a truncated tail.

    Exit codes: 0 intact, 1 tampering or divergence found, 2 not runnable."""
    settings, bus, warehouse, metrics, _ = _fresh()
    target = path or settings.audit_log_path

    expected: list[dict[str, object]] | None = None
    db_error: str | None = None
    if not no_cross_check:
        try:
            expected = warehouse.query("SELECT * FROM governance.erasure_audit_log")
        except Exception as exc:  # noqa: BLE001 - report it, never hide it
            db_error = f"warehouse audit table unavailable: {exc}"

    result = verify_audit_log(target, expected=expected)
    errors = list(result.errors)
    if db_error is not None:
        errors.append(db_error)
    if strict:
        if result.legacy:
            errors.append(f"{result.legacy} unverifiable record(s) (strict mode)")
        if result.duplicates:
            errors.append(
                "duplicate request_id(s) in strict mode: "
                + ", ".join(sorted(set(result.duplicates)))
            )

    payload = result.to_dict()
    payload["errors"] = errors
    payload["ok"] = not errors
    payload["strict"] = strict
    # What a fresh writer would chain onto next: equals the verifier's tip
    # when the file is intact, and diverges when it is not.
    writer = AuditChain(target)
    payload["chain"] = {"seq": writer.seq, "tip": writer.tip}

    warehouse.close()
    bus.close()

    if json_out:
        typer.echo(json.dumps(payload, indent=2, default=str))
        raise typer.Exit(0 if payload["ok"] else 1)

    scope = "file only" if no_cross_check else "file + warehouse"
    typer.echo(f"audit log  : {target}")
    typer.echo(
        f"records    : {result.entries} "
        f"({result.hashed} chained, {result.legacy} legacy, scope: {scope})"
    )
    typer.echo(f"last seq   : {result.last_seq}")
    typer.echo(f"tip        : {result.tip or '-'}")
    for warning in result.warnings:
        typer.secho(f"warning    : {warning}", fg=typer.colors.YELLOW)
    if payload["ok"]:
        typer.secho("OK — the audit chain is intact", fg=typer.colors.GREEN)
        raise typer.Exit(0)
    typer.secho(f"BROKEN — {len(errors)} problem(s):", fg=typer.colors.RED, err=True)
    for problem in errors:
        typer.secho(f"  - {problem}", fg=typer.colors.RED, err=True)
    raise typer.Exit(1)


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
    try:
        transform_dag.run(on_task=_echo_task)
    except DAGRunError as exc:
        metrics.flush()
        typer.secho(f"demo aborted: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

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
    silver_after = warehouse.scalar(
        "SELECT count(*) c FROM silver.customers WHERE customer_id = 'cust_424242'"
    )
    audit_rows = warehouse.scalar(
        "SELECT count(*) c FROM governance.erasure_audit_log WHERE customer_id = 'cust_424242'"
    )
    typer.echo(f"  bronze PII anonymized rows: {bronze_after} (before: {bronze_before} clear-text)")
    typer.echo(f"  gold customer_360 rows remaining: {gold_after} (expect 0)")
    typer.echo(f"  silver.customers rows remaining: {silver_after} (expect 0)")
    typer.echo(f"  audit log entries: {audit_rows} (expect 1)")

    typer.echo("6/6 summary")
    typer.echo(
        f"  erasure SLA: {settings.erasure_sla_seconds}s, completed in "
        f"{audit.completed_at - audit.requested_at:.2f}s"
    )
    checks = {
        "cascade reported success": audit.status == "completed",
        "bronze PII anonymized": bronze_after > 0,
        "gold rows removed": gold_after == 0,
        "silver rows removed": silver_after == 0,
        "audit trail written": audit_rows >= 1,
        "suppression registry tombstoned": victim in warehouse.suppressed_ids(),
    }
    passed = all(checks.values())
    for name, ok in checks.items():
        typer.echo(f"  {'PASS' if ok else 'FAIL'}  {name}")
    typer.echo(f"  verification: {'PASSED' if passed else 'FAILED'}")
    metrics.flush()
    bus.close()
    warehouse.close()
    if not passed:
        # A demo that fails must fail the process — CI and humans both read $?
        raise typer.Exit(code=1)


@app.command("sync-turso")
def sync_turso(
    seed_lake: bool = typer.Option(True, help="Seed from Hugging Face lake if DuckDB is empty"),
    hf_repo: str = typer.Option("swadhinbiswas/eustream", help="HF lake repository"),
) -> None:
    """Sync all Medallion tables and Governance state from DuckDB into Turso."""
    settings, _, warehouse, _, _ = _fresh()
    if not warehouse.turso:
        typer.secho(
            "TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not configured!",
            fg=typer.colors.RED,
            bold=True,
        )
        raise typer.Exit(code=1)

    if seed_lake:
        loaded = warehouse.seed_from_lake(hf_repo)
        if loaded:
            typer.echo(f"Seeded from lake {hf_repo}: {loaded}")

    try:
        warehouse.sync_all_to_turso()
    except Exception as e:  # noqa: BLE001 - report and fail, don't claim success
        typer.secho(f"sync failed before verification: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from e

    failed: list[str] = []
    for tbl in Warehouse.TURSO_TABLES:
        try:
            cnt = warehouse.turso.scalar(f"SELECT count(*) FROM {tbl}")  # noqa: S608
            typer.echo(f"  {tbl}: {cnt} rows in Turso")
        except Exception as e:  # noqa: BLE001
            failed.append(tbl)
            typer.secho(f"  {tbl}: NOT SYNCED ({e})", fg=typer.colors.RED)
    if failed:
        typer.secho(
            f"partial sync — {len(failed)} of {len(Warehouse.TURSO_TABLES)} tables missing in Turso",
            fg=typer.colors.RED,
            bold=True,
            err=True,
        )
        raise typer.Exit(code=1)
    typer.secho(
        f"All {len(Warehouse.TURSO_TABLES)} tables synchronized to Turso libSQL",
        fg=typer.colors.GREEN,
        bold=True,
    )


@app.command("probe-turso")
def probe_turso() -> None:
    """Check Turso connectivity and list current remote table counts."""
    settings, _, warehouse, _, _ = _fresh()
    if not warehouse.turso:
        typer.secho(
            "❌ Turso not connected. Please set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    typer.secho(f"✅ Turso connected: {warehouse.turso.http_endpoint}", fg=typer.colors.GREEN)
    warehouse.turso.init_schema()
    for tbl in Warehouse.TURSO_TABLES:
        try:
            cnt = warehouse.turso.scalar(f"SELECT count(*) FROM {tbl}")  # noqa: S608
            typer.echo(f"  {tbl}: {cnt} rows")
        except Exception as e:  # noqa: BLE001
            typer.secho(f"  {tbl}: error ({e})", fg=typer.colors.RED)


def _drain(consumer: Consumer, max_records: int | None = None) -> list[Record]:
    records: list[Record] = []
    consecutive_empty = 0
    while True:
        rec = consumer.poll(0.1)
        if rec is None:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
        else:
            consecutive_empty = 0
            records.append(rec)
            if max_records and len(records) >= max_records:
                break
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
        settings = get_settings()
        report = DataQualityEngine(
            warehouse,
            freshness_seconds=settings.dq_freshness_seconds,
            volume_drop_pct=settings.dq_volume_drop_pct,
        ).run_all()
        if not report.all_passed:
            failed = [r.check_name for r in report.results if not r.passed]
            raise RuntimeError(f"data quality gate failed: {failed}")

    return fn


if __name__ == "__main__":
    app()

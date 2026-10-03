from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import shutil
import time
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

import typer

from eurostream import __version__
from eurostream.bus import Consumer, Record
from eurostream.bus.sqlite import open_bus
from eurostream.chaos import SCENARIOS, discard_sandbox, new_sandbox, run_scenarios
from eurostream.config import Settings, get_settings
from eurostream.contracts import ContractRegistry
from eurostream.governance.audit_chain import AuditChain, verify_audit_log
from eurostream.governance.erasure import ErasureAudit, ErasureService
from eurostream.governance.pii import PIIClassifier
from eurostream.lineage import LineageEmitter
from eurostream.loadgen import DEFAULT_PATHS, LoadReport, render, run_load, served, to_dict
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


def _version_callback(value: bool) -> None:
    """Print the build and leave, before any setting is read or file opened."""
    if value:
        typer.echo(f"eurostream {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the version and exit.",
        ),
    ] = False,
) -> None:
    """EuroStream — GDPR-compliant real-time analytics platform."""


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
            k_anonymity=settings.dq_k_anonymity,
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


# ------------------------------------------------------- backup / restore
BACKUP_FORMAT = 1


#: Table names arrive from a manifest — untrusted input — so they are
#: validated as schema.table identifiers before they reach SQL at all.
_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def _safe_table(name: object) -> str:
    text = str(name)
    if not _TABLE_NAME.match(text):
        raise ValueError(f"not a safe table identifier: {text!r}")
    return text


def _as_int(value: object) -> int:
    """Read a manifest/aggregate value as int; rows are ``dict[str, object]``
    precisely because they came from JSON, so the conversion is explicit."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bare_settings() -> Settings:
    """Settings and logging only — for commands that are about to replace
    the bus and warehouse files, so nothing holds a handle on them."""
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    return settings


def _snapshot_sources(settings: Settings) -> list[tuple[str, Path]]:
    """(name inside the snapshot, source path) for everything worth keeping.

    The lake and the metrics log are deliberately absent: both are derived
    from the warehouse and rebuildable, while the warehouse itself and the
    hash-chained audit trail are what a DSAR or an auditor asks for.
    """
    warehouse_file = Path(settings.warehouse_path)
    wal = Path(str(warehouse_file) + ".wal")
    sources: list[tuple[str, Path]] = [("warehouse.duckdb", warehouse_file)]
    if wal.exists() and wal.stat().st_size:
        sources.append(("warehouse.duckdb.wal", wal))
    sources.append(("erasure_audit.jsonl", Path(settings.audit_log_path)))
    return sources


@app.command()
def backup(
    out: Path = typer.Option(
        None, "--out", "-o", help="Snapshot directory (default: <data_dir>/backups/<timestamp>)"
    ),
    label: str = typer.Option("", "--label", help="Name prefix for the default directory"),
) -> None:
    """Snapshot the warehouse and the audit log into a checksummed directory.

    Writes the files, a manifest of sha256 checksums, per-table row counts
    and the audit chain's seq/tip — enough to prove later that what was
    restored is what was taken. Exits 1 rather than writing over an
    existing snapshot."""
    settings, bus, warehouse, _, _ = _fresh()
    stamp = time.strftime("%Y%m%dT%H%M%S")
    dest = (
        Path(out)
        if out
        else settings.data_dir / "backups" / (f"{label}-{stamp}" if label else stamp)
    )

    if (dest / "manifest.json").exists():
        warehouse.close()
        bus.close()
        typer.secho(
            f"refusing to overwrite an existing backup: {dest}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(1)
    dest.mkdir(parents=True, exist_ok=True)

    try:
        # Flush the WAL into the file we are about to copy, or the snapshot
        # would be a frame behind the database it claims to represent.
        warehouse.conn.execute("CHECKPOINT")
    except Exception as exc:  # noqa: BLE001 - report and fail, never ship a torn backup
        warehouse.close()
        bus.close()
        typer.secho(f"could not checkpoint the warehouse: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from None

    files: dict[str, dict[str, object]] = {}
    for name, source in _snapshot_sources(settings):
        if not source.exists():
            continue
        target = dest / name
        shutil.copy2(source, target)
        files[name] = {
            "sha256": _sha256(target),
            "bytes": target.stat().st_size,
            "source": str(source),
        }

    tables: dict[str, int] = {}
    for table in Warehouse.TURSO_TABLES:
        try:
            tables[table] = int(
                warehouse.scalar(f"SELECT count(*) FROM {_safe_table(table)}", local_only=True)  # noqa: S608
            )
        except Exception as exc:  # noqa: BLE001 - an uncountable table is not fatal to the copy
            logging.getLogger(__name__).debug("backup could not count %s: %s", table, exc)

    audit_path = Path(settings.audit_log_path)
    chain = AuditChain(audit_path)
    audit_records = 0
    if audit_path.exists():
        audit_records = sum(1 for line in audit_path.read_text().splitlines() if line.strip())

    manifest = {
        "format": BACKUP_FORMAT,
        "created_at": time.time(),
        "files": files,
        "tables": tables,
        "rows_total": sum(tables.values()),
        "audit": {"records": audit_records, "seq": chain.seq, "tip": chain.tip},
    }
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))

    warehouse.close()
    bus.close()

    typer.secho(f"backup written to {dest}", fg=typer.colors.GREEN)
    for name, meta in files.items():
        typer.echo(
            f"  {name:<24} {_as_int(meta['bytes']):>9} bytes  sha256={str(meta['sha256'])[:12]}"
        )
    typer.echo(
        f"  tables: {len(tables)} captured, {sum(tables.values())} rows total; "
        f"audit: {audit_records} record(s), chain seq {chain.seq}"
    )
    typer.echo(f"manifest: {dest / 'manifest.json'}")


@app.command()
def restore(
    snapshot: Path = typer.Argument(..., help="Directory produced by `eurostream backup`"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Overwrite an existing warehouse or audit log"
    ),
) -> None:
    """Restore a snapshot, refusing to touch anything if its checksums fail.

    Checksums are verified *before* the first byte is written over the live
    data — restoring a corrupt backup destroys the good copy it replaces —
    and the result is verified afterwards: row counts against the manifest,
    then the audit chain and its agreement with the warehouse copy.
    Targets come from this deployment's settings, never from the manifest,
    so a hand-edited manifest cannot redirect a restore elsewhere."""
    settings = _bare_settings()
    snapshot = Path(snapshot)
    manifest_path = snapshot / "manifest.json"
    if not manifest_path.exists():
        typer.secho(
            f"no manifest.json in {snapshot} — not a EuroStream backup",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)
    try:
        manifest = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        typer.secho(f"manifest is not valid JSON: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from None
    if int(manifest.get("format") or 0) != BACKUP_FORMAT:
        typer.secho(
            f"unsupported backup format: {manifest.get('format')!r} "
            f"(this build reads {BACKUP_FORMAT})",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)

    files = manifest.get("files") or {}
    if not isinstance(files, dict) or not files:
        typer.secho("manifest lists no files", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)

    targets = {
        "warehouse.duckdb": Path(settings.warehouse_path),
        "warehouse.duckdb.wal": Path(str(settings.warehouse_path) + ".wal"),
        "erasure_audit.jsonl": Path(settings.audit_log_path),
    }
    for name, meta in files.items():
        # A manifest is untrusted input: the name is only ever a file
        # inside the snapshot, and the target only ever a path we chose.
        if Path(str(name)).name != name or name not in targets:
            typer.secho(f"refusing to restore unknown file {name!r}", fg=typer.colors.RED, err=True)
            raise typer.Exit(1)
        path = snapshot / str(name)
        if not path.exists():
            typer.secho(f"snapshot is missing {name}", fg=typer.colors.RED, err=True)
            raise typer.Exit(1)
        digest = _sha256(path)
        if digest != str(meta.get("sha256", "")):
            typer.secho(
                f"checksum mismatch for {name}: snapshot is corrupt — nothing was restored",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(1)
    typer.echo(f"verified {len(files)} file checksum(s)")

    existing = [str(t) for n, t in targets.items() if n in files and t.exists()]
    if existing and not force:
        typer.secho(
            "refusing to overwrite existing data:\n  " + "\n  ".join(existing) + "\nuse --force",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)

    stale_wal = Path(str(settings.warehouse_path) + ".wal")
    if "warehouse.duckdb.wal" not in files and stale_wal.exists():
        # A WAL left by the database being replaced would be replayed on top
        # of the restored file and corrupt it.
        stale_wal.unlink()

    for name, meta in files.items():
        target = targets[str(name)]
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        shutil.copy2(snapshot / str(name), target)
        typer.echo(f"restored {name} -> {target} ({_as_int(meta.get('bytes'))} bytes)")

    problems: list[str] = []
    warehouse = Warehouse(settings.warehouse_path)
    try:
        for table, expected in (manifest.get("tables") or {}).items():
            try:
                actual = int(
                    warehouse.scalar(f"SELECT count(*) FROM {_safe_table(table)}", local_only=True)  # noqa: S608
                )
            except Exception as exc:  # noqa: BLE001 - unreadable is a restore failure
                problems.append(f"{table}: unreadable after restore ({exc})")
                continue
            if actual != _as_int(expected):
                problems.append(f"{table}: {actual} rows restored, manifest says {expected}")

        expected_rows: list[dict[str, object]] | None = None
        try:
            expected_rows = warehouse.query(
                "SELECT * FROM governance.erasure_audit_log", local_only=True
            )
        except Exception as exc:  # noqa: BLE001 - say so rather than skip silently
            problems.append(f"warehouse audit table unreadable: {exc}")
        audit = verify_audit_log(settings.audit_log_path, expected=expected_rows)
        if not audit.ok:
            problems.extend(audit.errors)
    finally:
        warehouse.close()

    if problems:
        typer.secho("restore finished with problems:", fg=typer.colors.RED, err=True)
        for problem in problems:
            typer.secho(f"  - {problem}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)

    typer.secho(
        "restore verified: row counts match the manifest and the audit chain "
        "is intact in both copies",
        fg=typer.colors.GREEN,
    )


# ----------------------------------------------------------------- replay
REPLAY_TOPICS = ("orders", "clicks", "payments")
#: Layer totals reported by a replay. Governance tables are absent by
#: design: replay rebuilds data, it never rewrites audit or suppression
#: state — that is `backup restore`'s job.
REPLAY_LAYERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("bronze", ("bronze.orders", "bronze.clicks", "bronze.payments")),
    ("silver", ("silver.customers", "silver.orders", "silver.payments")),
    ("gold", ("gold.customer_360", "gold.order_facts", "gold.fraud_summary")),
)


def _layer_totals(warehouse: Warehouse) -> dict[str, int]:
    totals: dict[str, int] = {}
    for layer, tables in REPLAY_LAYERS:
        total = 0
        for table in tables:
            try:
                total += int(
                    warehouse.scalar(f"SELECT count(*) FROM {_safe_table(table)}", local_only=True)  # noqa: S608
                )
            except Exception as exc:  # noqa: BLE001 - a missing table counts as zero rows
                logging.getLogger(__name__).debug("replay could not count %s: %s", table, exc)
        totals[layer] = total
    return totals


@app.command()
def replay(
    limit: int = typer.Option(
        0, min=0, help="Stop after N records per topic (0 = read to the end)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Report what the log holds without touching the warehouse"
    ),
    lake: bool = typer.Option(
        False, "--lake", help="Re-export the de-identified lake after the rebuild"
    ),
) -> None:
    """Rebuild Bronze from the event log, then rebuild Silver and Gold.

    The log, not the warehouse, is the source of truth here: each topic is
    read from the front by a throwaway consumer group that never commits,
    so no worker's or operator's cursor moves and the run is repeatable.
    Bronze keys on event_id with INSERT OR IGNORE, so replaying events the
    warehouse already holds changes nothing — safe against a live warehouse
    after a bad transform, and a full recovery against an emptied one.

    Erasure survives a replay: records for suppressed customers are skipped
    rather than re-ingested, and Silver/Gold rebuilds filter through the
    suppression registry as they always do. When that registry is itself
    empty — a warehouse lost, not just wiped — the run says so instead of
    quietly resurrecting people the log still remembers."""
    settings, bus, warehouse, _, _ = _fresh()

    suppressed: set[str] = set()
    try:
        suppressed = {
            str(row["customer_id"])
            for row in warehouse.query(
                "SELECT customer_id FROM governance.suppression_registry", local_only=True
            )
        }
    except Exception as exc:  # noqa: BLE001 - unreadable registry degrades to "empty"
        logging.getLogger(__name__).debug("suppression registry unreadable: %s", exc)

    before = _layer_totals(warehouse)
    read: dict[str, int] = {}
    skipped = 0
    for topic in REPLAY_TOPICS:
        consumer = bus.consumer(topic, f"replay-{uuid4()}", auto_offset_reset="earliest")
        records: list[Record] = []
        while not limit or len(records) < limit:
            record = consumer.poll(0.1)
            if record is None:
                break
            if suppressed:
                try:
                    if str(record.json_value().get("customer_id", "")) in suppressed:
                        skipped += 1
                        continue
                except Exception as exc:  # noqa: BLE001 - a malformed body is load's problem
                    logging.getLogger(__name__).debug("replay could not read a record: %s", exc)
            records.append(record)
        # Deliberately never closed: close() commits, and this group exists
        # only to read from the front of the log.
        read[topic] = len(records)
        if not dry_run and records:
            warehouse.load_bronze_from_records(topic, records)

    summary = " ".join(f"{topic}={count}" for topic, count in read.items())
    if dry_run:
        typer.echo(f"[dry-run] log holds {summary} record(s); warehouse untouched")
        if skipped:
            typer.echo(f"[dry-run] {skipped} record(s) belong to suppressed customers")
        warehouse.close()
        bus.close()
        return

    warehouse.build_silver()
    warehouse.build_gold()
    if lake:
        warehouse.export_lake(settings.lake_root)

    after = _layer_totals(warehouse)
    suffix = f" ({skipped} skipped for suppressed customers)" if skipped else ""
    typer.echo(f"read {summary} record(s) from the log{suffix}")
    for layer, _tables in REPLAY_LAYERS:
        typer.echo(f"  {layer}: {after[layer]} rows ({after[layer] - before[layer]:+d})")
    if lake:
        typer.echo(f"lake re-exported to {settings.lake_root}")
    if not suppressed and any(read.values()):
        typer.secho(
            "WARNING: the suppression registry is empty, so anyone erased before this "
            "rebuild would come straight back. Restore a snapshot instead "
            "(eurostream restore <dir> --force) — it carries the governance tables with it.",
            fg=typer.colors.YELLOW,
        )
    warehouse.close()
    bus.close()


# ------------------------------------------------------------------ chaos


@app.command()
def chaos(
    scenario: list[str] | None = typer.Option(
        None, "--scenario", "-s", help="Run only these drills (repeatable); default: all"
    ),
    list_only: bool = typer.Option(False, "--list", help="List the drills and exit"),
    keep: bool = typer.Option(False, "--keep", help="Keep the sandbox directory for inspection"),
    json_out: bool = typer.Option(False, "--json", help="Machine-readable report"),
) -> None:
    """Break things on purpose and report whether a guardrail caught it.

    Every drill runs in its own directory under a fresh tempdir, so the
    warehouse, audit log and event log this deployment uses are never even
    opened — safe to run against a production checkout. A drill passes
    when the guardrail *fires*: the run exits 1 if any protection the
    platform claims did not hold, which is the opposite of a test suite
    where green means nothing happened."""
    if list_only:
        for name, fn in SCENARIOS.items():
            typer.echo(f"  {name:<18} {(fn.__doc__ or '').strip()}")
        return

    unknown = [name for name in (scenario or []) if name not in SCENARIOS]
    if unknown:
        typer.secho(f"unknown scenario(s): {', '.join(unknown)}", fg=typer.colors.RED, err=True)
        typer.echo("known: " + ", ".join(SCENARIOS))
        raise typer.Exit(2)

    sandbox = new_sandbox()
    results = run_scenarios(sandbox, scenario or None)
    held = sum(1 for result in results if result.ok)
    failed = [result for result in results if not result.ok]

    if json_out:
        typer.echo(
            json.dumps(
                {
                    "sandbox": str(sandbox),
                    "kept": keep,
                    "results": [{"name": r.name, "ok": r.ok, "detail": r.detail} for r in results],
                    "held": held,
                    "failed": len(failed),
                },
                indent=2,
            )
        )
    else:
        typer.echo("guardrail drills — each breaks something on purpose, in a sandbox:")
        for result in results:
            colour = typer.colors.GREEN if result.ok else typer.colors.RED
            mark = "✓" if result.ok else "✗"
            typer.secho(f"  {mark} {result.name:<18} {result.detail}", fg=colour)
        typer.echo(f"{held}/{len(results)} guardrails held")
        typer.echo(f"sandbox: {sandbox}{' (kept)' if keep else ' (removed)'}")

    if not keep:
        discard_sandbox(sandbox)
    if failed:
        raise typer.Exit(1)


# ------------------------------------------------------------- load test


@app.command("load-test")
def load_test(
    url: str = typer.Option(
        "", "--url", "-u", help="Base URL of a server already running (default: start one)"
    ),
    count: int = typer.Option(200, "--requests", "-n", min=1, help="Total requests to send"),
    concurrency: int = typer.Option(16, "--concurrency", "-c", min=1, help="Concurrent workers"),
    path: list[str] | None = typer.Option(
        None, "--path", help="Endpoint to hit (repeatable); default: a mix of four"
    ),
    timeout: float = typer.Option(10.0, "--timeout", min=0.1, help="Per-request timeout"),
    json_out: bool = typer.Option(False, "--json", help="Machine-readable report"),
) -> None:
    """Drive HTTP load at the API and report latency percentiles.

    Point it at a server with --url, or leave --url out and it starts the
    app in-process on a free port, measures it, and shuts it down — one
    command from a cold checkout to a p95. Exits 1 on any 5xx, timeout or
    transport failure; 429s are counted and reported but never fail a run,
    because shedding load is the rate limiter working, not the server
    breaking."""
    targets = path or list(DEFAULT_PATHS)

    def measure(base: str) -> LoadReport:
        return run_load(
            base,
            requests=count,
            concurrency=concurrency,
            paths=targets,
            timeout=timeout,
        )

    try:
        if url:
            report = measure(url)
        else:
            with served() as base:
                if not json_out:
                    # In --json mode the base URL is in the payload; an extra
                    # banner line would make the whole output unparseable.
                    typer.echo(f"started the API in-process at {base}")
                report = measure(base)
    except (ValueError, RuntimeError) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from None

    if json_out:
        typer.echo(json.dumps(to_dict(report), indent=2))
    else:
        typer.echo(render(report))

    if not report.passed:
        raise typer.Exit(1)


# ------------------------------------------------------------------- dlq
DLQ_TOPIC = "erasure_requests_dlq"
ERASURE_TOPIC = "erasure_requests"
#: The operator's cursor in the dead-letter log. It only moves when someone
#: runs `dlq requeue` or `dlq ack`, which is what makes `dlq list` repeatable
#: and makes requeueing idempotent against an append-only log that has no
#: delete.
DLQ_GROUP = "eurostream-dlq"

dlq_app = typer.Typer(
    help=(
        "Inspect and replay dead-lettered erasure requests. Records stay in "
        "the log for audit; only this group's position decides what counts "
        "as still unhandled."
    )
)
app.add_typer(dlq_app, name="dlq")


def _poll_dlq(consumer: Consumer, limit: int) -> list[Record]:
    """Read up to ``limit`` records from the consumer's current position."""
    records: list[Record] = []
    while len(records) < limit:
        record = consumer.poll(0.05)
        if record is None:
            break
        records.append(record)
    return records


def _describe(record: Record) -> dict[str, object]:
    """A dead letter as an operator wants to see it: who failed, why, when."""
    try:
        payload = record.json_value()
        request_id = str(payload.get("request_id", ""))
        customer_id = str(payload.get("customer_id", ""))
        raw: str | None = None
    except Exception as exc:  # noqa: BLE001 - the payload is the evidence
        request_id, customer_id, raw = "", "", f"{type(exc).__name__}: {exc}"
    return {
        "offset": record.offset,
        "reason": record.headers.get("reason", "unknown"),
        "request_id": request_id,
        "customer_id": customer_id,
        "timestamp": record.timestamp,
        "parse_error": raw,
        "value": record.value,
    }


def _format_item(item: dict[str, object]) -> str:
    who = f"customer={item['customer_id']}" if item["customer_id"] else "customer=?"
    request = f"request={item['request_id']}" if item["request_id"] else "request=?"
    line = f"  #{item['offset']:<5} {item['reason']}  {who}  {request}"
    if item["parse_error"]:
        line += f"\n          unparseable payload ({item['parse_error']})"
    return line


@dlq_app.command("list")
def dlq_list(
    limit: int = typer.Option(50, min=1, max=1000, help="Maximum records to show"),
    check: bool = typer.Option(
        False, "--check", help="exit 1 when unhandled dead letters exist (for a monitor)"
    ),
    json_out: bool = typer.Option(False, "--json", help="print the machine-readable list"),
) -> None:
    """Show dead-lettered erasure requests nobody has handled yet."""
    _, bus, warehouse, _, _ = _fresh()
    # Read only: this consumer is never committed or closed, because
    # close() commits and would silently mark the letters as handled.
    consumer = bus.consumer(DLQ_TOPIC, DLQ_GROUP, auto_offset_reset="earliest")
    records = _poll_dlq(consumer, limit)
    items = [_describe(r) for r in records]

    if json_out:
        typer.echo(json.dumps({"count": len(items), "records": items}, indent=2, default=str))
    elif not items:
        typer.secho("no unhandled dead letters — the queue is clear", fg=typer.colors.GREEN)
    else:
        for item in items:
            typer.secho(_format_item(item), fg=typer.colors.YELLOW)
        typer.echo(
            f"{len(items)} unhandled dead letter(s) in {DLQ_TOPIC} "
            f"(handled cursor at offset {consumer.current_offset()})"
        )

    warehouse.close()
    bus.close()
    if check and items:
        raise typer.Exit(1)


@dlq_app.command("requeue")
def dlq_requeue(
    limit: int = typer.Option(50, min=1, max=1000, help="Maximum records to republish"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="show what would be republished without moving the cursor"
    ),
) -> None:
    """Republish dead letters onto the intake topic so the worker retries them."""
    _, bus, warehouse, _, _ = _fresh()
    consumer = bus.consumer(DLQ_TOPIC, DLQ_GROUP, auto_offset_reset="earliest")
    records = _poll_dlq(consumer, limit)
    if not records:
        warehouse.close()
        bus.close()
        typer.secho("nothing to requeue — the queue is clear", fg=typer.colors.GREEN)
        return

    if dry_run:
        # Nothing is published and the cursor does not move: a dry run that
        # republished would be worse than no dry run at all.
        for record in records:
            typer.echo(f"[dry-run] would republish offset {record.offset}")
        typer.echo(f"[dry-run] {len(records)} record(s) left untouched")
        warehouse.close()
        bus.close()
        return

    for record in records:
        bus.produce(
            ERASURE_TOPIC,
            key=record.key or "requeued",
            value=record.value,
            headers={**record.headers, "requeued_from": "dlq", "dlq_offset": str(record.offset)},
        )
    if hasattr(bus, "flush"):
        bus.flush()

    # Only now does the cursor move: a republish that failed half way
    # leaves the remaining letters visible instead of swallowing them.
    consumer.commit()
    consumer.close()
    warehouse.close()
    bus.close()
    typer.secho(
        f"republished {len(records)} dead letter(s) to {ERASURE_TOPIC} — "
        "the erasure worker will retry them",
        fg=typer.colors.GREEN,
    )


@dlq_app.command("ack")
def dlq_ack(
    limit: int = typer.Option(50, min=1, max=1000, help="Maximum records to acknowledge"),
    dry_run: bool = typer.Option(False, "--dry-run", help="show what would be acknowledged"),
) -> None:
    """Mark dead letters as handled without retrying them.

    The records stay in the log for audit — this only moves the operator's
    cursor past them, for failures that are known-bad input nobody intends
    to retry."""
    _, bus, warehouse, _, _ = _fresh()
    consumer = bus.consumer(DLQ_TOPIC, DLQ_GROUP, auto_offset_reset="earliest")
    records = _poll_dlq(consumer, limit)
    if not records:
        warehouse.close()
        bus.close()
        typer.secho("nothing to acknowledge — the queue is clear", fg=typer.colors.GREEN)
        return

    if dry_run:
        for record in records:
            typer.echo(f"[dry-run] would acknowledge offset {record.offset}")
        typer.echo(f"[dry-run] {len(records)} record(s) left untouched")
        warehouse.close()
        bus.close()
        return

    consumer.commit()
    consumer.close()
    warehouse.close()
    bus.close()
    typer.secho(
        f"acknowledged {len(records)} dead letter(s); they stay in {DLQ_TOPIC} for audit",
        fg=typer.colors.GREEN,
    )


def _serve_kwargs(
    host: str, port: int, *, reload: bool, workers: int, log_level: str | None
) -> dict[str, Any]:
    """Build uvicorn's arguments, rejecting combinations it cannot honour."""
    if not 1 <= port <= 65535:
        raise ValueError(f"port must be between 1 and 65535, got {port}")
    if reload and workers > 1:
        raise ValueError("--reload supervises a single process; drop --workers or --reload")
    kwargs: dict[str, Any] = {"app": "eurostream.api:app", "host": host, "port": port}
    if reload:
        # Reload spawns its own reloader process; workers would nest.
        kwargs["reload"] = True
    else:
        kwargs["workers"] = workers
    if log_level:
        kwargs["log_level"] = log_level
    return kwargs


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", "-h", help="Address to bind"),
    port: int = typer.Option(
        7860, "--port", "-p", help="Port to bind (7860, as the container does)"
    ),
    reload: bool = typer.Option(False, "--reload", help="Restart on source changes (development)"),
    workers: int = typer.Option(1, "--workers", min=1, help="Worker processes"),
    log_level: str = typer.Option(
        None, "--log-level", help="uvicorn level: critical/error/warning/info/debug/trace"
    ),
) -> None:
    """Run the API server — the same app the container runs.

    `uvicorn eurostream.api:app` typed by hand, the Dockerfile CMD and
    this command are now one thing, so the README, the load-test harness
    and the Prometheus scrape config all start the server the same way."""
    try:
        kwargs = _serve_kwargs(host, port, reload=reload, workers=workers, log_level=log_level)
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from None

    typer.echo(f"serving eurostream.api:app on http://{host}:{port}")
    typer.echo(f"  health: http://{host}:{port}/health     api: http://{host}:{port}/docs")
    import uvicorn

    uvicorn.run(**kwargs)


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
            k_anonymity=settings.dq_k_anonymity,
        ).run_all()
        if not report.all_passed:
            failed = [r.check_name for r in report.results if not r.passed]
            raise RuntimeError(f"data quality gate failed: {failed}")

    return fn


if __name__ == "__main__":
    app()

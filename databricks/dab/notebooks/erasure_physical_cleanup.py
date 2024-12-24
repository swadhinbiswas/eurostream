# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # Article 17 final stage — export verification and bounded physical cleanup
#
# This DAB-local stage runs after the Silver and Gold pipeline updates and after
# the governed export Job.  It verifies current Silver/Gold rows and the Volume
# snapshot, then performs explicit physical cleanup only for the standalone
# fraud sink. Bronze streaming tables and Silver/Gold materialized views remain
# pipeline-owned; this stage never issues DELETE, REORG, or VACUUM against them.
#
# `confirm_quiesced_physical_cleanup=false` is fail closed.  A true value is an
# operator assertion that all showcase writers/readers are quiesced.

# COMMAND ----------

import hashlib
import json
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("customer_id", "")
dbutils.widgets.text("ticket_id", "manual")
dbutils.widgets.text("request_id", "")
dbutils.widgets.text("idempotency_key", "")
dbutils.widgets.text("requested_by", "")
dbutils.widgets.text("reason", "GDPR_ARTICLE_17")
dbutils.widgets.text("actor", "databricks_workflow")
dbutils.widgets.text("workflow_run_id", "")
dbutils.widgets.text("internal_slo_seconds", "60")
dbutils.widgets.text("confirm_quiesced_physical_cleanup", "false")

CUSTOMER_ID = dbutils.widgets.get("customer_id").strip()
TICKET_ID = dbutils.widgets.get("ticket_id").strip() or "manual"
REQUEST_ID_INPUT = dbutils.widgets.get("request_id").strip()
IDEMPOTENCY_INPUT = dbutils.widgets.get("idempotency_key").strip()
REQUESTED_BY = dbutils.widgets.get("requested_by").strip() or "unknown"
REASON = dbutils.widgets.get("reason").strip() or "GDPR_ARTICLE_17"
ACTOR = dbutils.widgets.get("actor").strip() or "databricks_workflow"
WORKFLOW_RUN_ID = dbutils.widgets.get("workflow_run_id").strip() or str(
    getattr(dbutils.context, "jobRunId", "")
)
INTERNAL_SLO_RAW = dbutils.widgets.get("internal_slo_seconds").strip() or "60"
CONFIRM_PHYSICAL = (
    dbutils.widgets.get("confirm_quiesced_physical_cleanup").strip().lower() == "true"
)

if not CUSTOMER_ID:
    raise ValueError("customer_id is required")
try:
    INTERNAL_SLO_SECONDS = int(INTERNAL_SLO_RAW)
except ValueError as exc:
    raise ValueError("internal_slo_seconds must be an integer") from exc
if INTERNAL_SLO_SECONDS <= 0:
    raise ValueError("internal_slo_seconds must be greater than zero")


def _confirmation_hash(request_id: str, customer_id: str) -> str:
    return hashlib.sha256(f"{request_id}:{customer_id}".encode()).hexdigest()[:16]


REQUEST_ID = REQUEST_ID_INPUT or IDEMPOTENCY_INPUT
if not REQUEST_ID:
    seed = f"{CUSTOMER_ID}|{TICKET_ID}|{REQUESTED_BY}|{REASON}"
    REQUEST_ID = f"manual-{hashlib.sha256(seed.encode()).hexdigest()[:24]}"
IDEMPOTENCY_KEY = IDEMPOTENCY_INPUT or REQUEST_ID

STATE_TABLE = "eurostream.governance.erasure_command_state"
ANONYMIZED = "<anonymized>"
EXPORT_ROOT = "/Volumes/eurostream/lake/exports"

# These are current-row checks for Lakeflow-owned materialized views.  They are
# intentionally separate from PHYSICAL_TABLES below.
SILVER_GOLD_TABLES = (
    "eurostream.silver.customers",
    "eurostream.silver.orders",
    "eurostream.silver.payments",
    "eurostream.silver.orders_quarantine",
    "eurostream.silver.payments_quarantine",
    "eurostream.gold.customer_360",
    "eurostream.gold.order_facts",
    "eurostream.gold.fraud_summary",
)

# Only the standalone fraud sink is eligible for direct physical cleanup.
# Bronze REPLACE USING tables are still pipeline-owned streaming tables; Silver
# and Gold are pipeline-owned materialized views. Their storage lifecycle is not
# claimed by this stage.
PHYSICAL_TABLES = {
    "eurostream.bronze.fraud_alerts": ("absent", ()),
}
assert set(PHYSICAL_TABLES) == {"eurostream.bronze.fraud_alerts"}, (
    "Only the job-owned fraud sink may enter direct physical cleanup"
)

EXPORT_MANIFEST = {
    "silver/orders": "eurostream.silver.orders",
    "silver/payments": "eurostream.silver.payments",
    "silver/customers": "eurostream.silver.customers",
    "gold/customer_360": "eurostream.gold.customer_360",
    "gold/order_facts": "eurostream.gold.order_facts",
    "gold/fraud_summary": "eurostream.gold.fraud_summary",
}


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _load_state() -> dict[str, object]:
    rows = (
        spark.table(STATE_TABLE)
        .where(
            (F.col("command_id") == F.lit(REQUEST_ID))
            | (F.col("idempotency_key") == F.lit(IDEMPOTENCY_KEY))
        )
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise RuntimeError("Expected exactly one durable erasure command state")
    state = rows[0].asDict()
    if str(state["customer_id"]) != CUSTOMER_ID:
        raise ValueError("durable state belongs to a different customer_id")
    if str(state["ticket_id"]) != TICKET_ID:
        raise ValueError("durable state belongs to a different ticket_id")
    if str(state["idempotency_key"]) != IDEMPOTENCY_KEY:
        raise ValueError("durable state belongs to a different idempotency_key")
    if int(state["internal_slo_seconds"]) != INTERNAL_SLO_SECONDS:
        raise ValueError("durable state has a different internal_slo_seconds")
    if not WORKFLOW_RUN_ID or state.get("silver_gold_refresh_run_id") != WORKFLOW_RUN_ID:
        raise ValueError("durable state is not bound to this Job's Silver/Gold refresh run")
    return state


def _update_state(
    *,
    status: str,
    logical_verified: bool,
    physical_verified: bool,
    export_verified: bool,
    layers: str,
    failed_layer: str | None = None,
    failure_message: str | None = None,
    physical_detail: str | None = None,
    blocked_layer: str | None = None,
    next_action: str | None = None,
) -> None:
    now = _utc_now()
    completed_at = now if status == "failed" else None
    spark.sql(
        """
        UPDATE eurostream.governance.erasure_command_state
        SET status = :status,
            updated_at = :updated_at,
            completed_at = :completed_at,
            logical_verified = :logical_verified,
            physical_verified = :physical_verified,
            export_verified = :export_verified,
            layers_completed = :layers,
            failed_layer = :failed_layer,
            failure_message = :failure_message,
            physical_verification_detail = :physical_detail,
            blocked_layer = :blocked_layer,
            next_action = :next_action
        WHERE command_id = :command_id
        """,
        args={
            "status": status,
            "updated_at": now,
            "completed_at": completed_at,
            "logical_verified": logical_verified,
            "physical_verified": physical_verified,
            "export_verified": export_verified,
            "layers": layers,
            "failed_layer": failed_layer,
            "failure_message": failure_message,
            "physical_detail": physical_detail,
            "blocked_layer": blocked_layer,
            "next_action": next_action,
            "command_id": REQUEST_ID,
        },
    )


def _mirror_audit() -> None:
    (
        spark.table(STATE_TABLE)
        .filter(F.col("command_id") == F.lit(REQUEST_ID))
        .select(
            "request_id",
            "customer_id",
            F.unix_timestamp("requested_at").alias("requested_at"),
            F.unix_timestamp(F.coalesce(F.col("completed_at"), F.col("updated_at"))).alias(
                "completed_at"
            ),
            "layers_completed",
            "status",
            "confirmation_hash",
        )
        .createOrReplaceTempView("eurostream_erasure_physical_audit")
    )
    spark.sql(
        """
        MERGE INTO eurostream.governance.erasure_audit_log AS target
        USING eurostream_erasure_physical_audit AS source
        ON target.request_id = source.request_id
           AND target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET
          requested_at = source.requested_at,
          completed_at = source.completed_at,
          layers_touched = source.layers_completed,
          status = source.status,
          confirmation_hash = source.confirmation_hash
        WHEN NOT MATCHED THEN INSERT (
          request_id, customer_id, requested_at, completed_at, layers_touched,
          status, confirmation_hash
        ) VALUES (
          source.request_id, source.customer_id, source.requested_at,
          source.completed_at, source.layers_completed, source.status,
          source.confirmation_hash
        )
        """
    )


def _mark_completed(
    state: dict[str, object],
    *,
    layers: str,
    logical_detail: str,
    export_detail: str,
    physical_detail: str,
) -> None:
    now = _utc_now()
    requested_at = state["requested_at"]
    slo_seconds = int(state["internal_slo_seconds"])
    slo_deadline = requested_at + timedelta(seconds=slo_seconds)
    latency = max(0.0, (now - requested_at).total_seconds())
    slo_state = "met" if now <= slo_deadline else "breached"
    spark.sql(
        """
        UPDATE eurostream.governance.erasure_command_state
        SET status = 'completed',
            updated_at = :updated_at,
            completed_at = :completed_at,
            internal_slo_state = :internal_slo_state,
            slo_deadline_at = :slo_deadline_at,
            latency_seconds = :latency_seconds,
            logical_verified = TRUE,
            physical_verified = TRUE,
            export_verified = TRUE,
            logical_verification_detail = :logical_detail,
            export_verification_detail = :export_detail,
            physical_verification_detail = :physical_detail,
            silver_gold_refresh_run_id = :silver_gold_refresh_run_id,
            layers_completed = :layers,
            failed_layer = NULL,
            failure_message = NULL,
            blocked_layer = NULL,
            next_action = NULL
        WHERE command_id = :command_id
        """,
        args={
            "updated_at": now,
            "completed_at": now,
            "internal_slo_state": slo_state,
            "slo_deadline_at": slo_deadline,
            "latency_seconds": latency,
            "logical_detail": logical_detail,
            "export_detail": export_detail,
            "physical_detail": physical_detail,
            "silver_gold_refresh_run_id": WORKFLOW_RUN_ID,
            "layers": layers,
            "command_id": REQUEST_ID,
        },
    )
    persisted = spark.table(STATE_TABLE).filter(F.col("command_id") == F.lit(REQUEST_ID)).first()
    if (
        persisted is None
        or persisted["status"] != "completed"
        or persisted["silver_gold_refresh_run_id"] != WORKFLOW_RUN_ID
        or persisted["blocked_layer"] is not None
        or persisted["next_action"] is not None
        or not all(
            bool(persisted[field])
            for field in ("logical_verified", "physical_verified", "export_verified")
        )
    ):
        raise RuntimeError("Durable erasure completion postcondition failed")
    _mirror_audit()


def _mark_failure(state: dict[str, object], layer: str, exc: BaseException) -> None:
    try:
        _update_state(
            status="failed",
            logical_verified=False,
            physical_verified=False,
            export_verified=False,
            layers=str(state.get("layers_completed") or ""),
            failed_layer=layer,
            failure_message=f"{type(exc).__name__}: {exc}"[:4000],
            blocked_layer=layer,
            next_action="fix_and_resume_same_request",
        )
    except Exception as state_exc:
        print(f"failed to persist physical-stage failure: {state_exc}")


def _count(table: str) -> int:
    return spark.table(table).filter(F.col("customer_id") == F.lit(CUSTOMER_ID)).count()


def _verify_current_views() -> dict[str, int]:
    counts = {table: _count(table) for table in SILVER_GOLD_TABLES}
    remaining = {table: count for table, count in counts.items() if count}
    if remaining:
        raise RuntimeError(f"Materialized views still contain target rows: {remaining}")
    return counts


def _verify_bronze_logical() -> dict[str, object]:
    suppression_count = _count("eurostream.governance.suppression_registry")
    if suppression_count < 1:
        raise RuntimeError("suppression tombstone is missing")

    details: dict[str, object] = {"suppression_rows": suppression_count}
    for table, columns in (
        ("eurostream.bronze.orders", ("email", "iban")),
        ("eurostream.bronze.orders_files", ("email", "iban")),
        ("eurostream.bronze.payments", ("iban",)),
        ("eurostream.bronze.clicks", ("ip_address",)),
    ):
        target = spark.table(table).filter(F.col("customer_id") == F.lit(CUSTOMER_ID))
        invalid = target.filter(
            F.reduce(
                [
                    F.col(column).isNull() | (F.col(column) != F.lit(ANONYMIZED))
                    for column in columns
                ],
                F.or_,
            )
        ).count()
        details[table] = {"target_rows": target.count(), "invalid_pii_rows": invalid}
        if invalid:
            raise RuntimeError(f"Bronze PII masking is incomplete for {table}")

    quarantine_count = (
        spark.table("eurostream.bronze.ingest_quarantine")
        .filter(
            (F.col("customer_id") == F.lit(CUSTOMER_ID))
            | (F.instr(F.coalesce(F.col("raw_payload"), F.lit("")), CUSTOMER_ID) > 0)
        )
        .count()
    )
    fraud_count = _count("eurostream.bronze.fraud_alerts")
    details["bronze.ingest_quarantine"] = quarantine_count
    details["bronze.fraud_alerts"] = fraud_count
    if quarantine_count or fraud_count:
        raise RuntimeError("Bronze quarantine or fraud rows remain")
    return details


def _verify_export() -> dict[str, object]:
    details: dict[str, object] = {}
    for relative_path, table in EXPORT_MANIFEST.items():
        source = spark.read.table(table)
        exported = spark.read.parquet(f"{EXPORT_ROOT}/{relative_path}")
        if exported.schema != source.schema:
            raise RuntimeError(f"Export schema mismatch for {relative_path}")
        source_count = source.count()
        exported_count = exported.count()
        if source_count != exported_count:
            raise RuntimeError(f"Export row count mismatch for {relative_path}")
        target_count = exported.filter(F.col("customer_id") == F.lit(CUSTOMER_ID)).count()
        if target_count:
            raise RuntimeError(f"Export still contains target rows for {relative_path}")
        details[relative_path] = {
            "rows": exported_count,
            "target_rows": target_count,
        }
    return details


def _remaining_data_files(table: str) -> list[str]:
    detail = DeltaTable.forName(spark, table).detail().first().asDict()
    if detail.get("format") != "delta":
        raise RuntimeError(f"Expected a Delta table for physical scan: {table}")
    location = str(detail["location"])
    files: list[str] = []
    for entry in dbutils.fs.ls(location, recursive=True):
        path = str(entry.path)
        if entry.isDir or "/_delta_log/" in f"{path}/" or path.endswith("/_delta_log"):
            continue
        if entry.name in {"_SUCCESS", ".crc"}:
            continue
        if not path.lower().endswith(".parquet"):
            raise RuntimeError(f"Unexpected non-Parquet file during scan: {path}")
        files.append(path)
    return files


def _physical_violation(table: str, mode: str, columns: tuple[str, ...], file_path: str) -> int:
    physical = spark.read.parquet(file_path)
    if mode == "payload_absent":
        target = physical.filter(
            (F.col("customer_id") == F.lit(CUSTOMER_ID))
            | (F.instr(F.coalesce(F.col("raw_payload"), F.lit("")), CUSTOMER_ID) > 0)
        )
    else:
        target = physical.filter(F.col("customer_id") == F.lit(CUSTOMER_ID))
    if mode in {"absent", "payload_absent"}:
        return target.limit(1).count()
    invalid = target.filter(
        F.reduce(
            [F.col(column).isNull() | (F.col(column) != F.lit(ANONYMIZED)) for column in columns],
            F.or_,
        )
    )
    return invalid.limit(1).count()


def _scan_fraud_sink() -> dict[str, object]:
    details: dict[str, object] = {}
    for table, (mode, columns) in PHYSICAL_TABLES.items():
        files = _remaining_data_files(table)
        violations = sum(
            _physical_violation(table, mode, columns, file_path) for file_path in files
        )
        details[table] = {"parquet_files_scanned": len(files), "violations": violations}
        if violations:
            raise RuntimeError(f"Physical content scan found target data in {table}")
    return details


# COMMAND ----------

state = _load_state()
current_layer = "verification"
deferred_recorded = False
try:
    logical_details = _verify_bronze_logical()
    view_details = _verify_current_views()
    export_details = _verify_export()
    layers = ",".join(item for item in str(state.get("layers_completed") or "").split(",") if item)
    for layer in ("silver_pipeline_refresh", "gold_pipeline_refresh", "export_refresh"):
        if layer not in layers.split(","):
            layers = ",".join(item for item in (layers, layer) if item)

    if not CONFIRM_PHYSICAL:
        physical_detail = json.dumps(
            {
                "passed": False,
                "state": "deferred",
                "reason": "explicit quiesced physical-cleanup confirmation was false",
                "pipeline_storage": "not directly cleaned; Lakeflow owns Bronze/Silver/Gold",
            },
            sort_keys=True,
        )
        _update_state(
            status="failed",
            logical_verified=True,
            physical_verified=False,
            export_verified=True,
            layers=layers,
            failed_layer="physical_cleanup_and_verification",
            failure_message="Physical verification is deferred by explicit operator guard",
            physical_detail=physical_detail,
            blocked_layer="fraud_sink_physical_cleanup",
            next_action="rerun_same_request_in_quiesced_window_with_confirmation",
        )
        _mirror_audit()
        deferred_recorded = True
        raise RuntimeError(
            "Physical verification is deferred; rerun the same request only in a "
            "quiesced window with confirm_quiesced_physical_cleanup=true"
        )

    current_layer = "fraud_sink_physical_cleanup"
    # No pipeline-owned Bronze, Silver, or Gold relation is present in this list.
    for table in PHYSICAL_TABLES:
        spark.sql(f"REORG TABLE {table} APPLY (PURGE)")
        spark.sql(f"VACUUM {table} RETAIN 0 HOURS")

    current_layer = "fraud_sink_physical_verification"
    physical_details = _scan_fraud_sink()
    physical_detail = json.dumps(
        {
            "passed": True,
            "tables": physical_details,
            "scope": "job-owned bronze.fraud_alerts sink only",
            "silver_gold": "current rows verified; no direct DELETE/REORG/VACUUM",
        },
        sort_keys=True,
    )
    logical_detail = json.dumps(
        {
            "passed": True,
            "bronze": logical_details,
            "materialized_views": view_details,
        },
        sort_keys=True,
    )
    export_detail = json.dumps({"passed": True, "tables": export_details}, sort_keys=True)
    completed_layers = ",".join(
        item for item in (layers, "physical_cleanup_and_verification") if item
    )
    _mark_completed(
        state,
        layers=completed_layers,
        logical_detail=logical_detail,
        export_detail=export_detail,
        physical_detail=physical_detail,
    )

    dbutils.jobs.taskValues.set(key="erasure_stage", value="verify_export_cleanup")
    dbutils.jobs.taskValues.set(key="erasure_status", value="completed")
    dbutils.jobs.taskValues.set(key="erasure_request_id", value=REQUEST_ID)
    dbutils.jobs.taskValues.set(key="erasure_ticket_id", value=TICKET_ID)
    dbutils.jobs.taskValues.set(key="erasure_idempotency_key", value=IDEMPOTENCY_KEY)
    dbutils.jobs.taskValues.set(
        key="erasure_confirmation_hash",
        value=str(state["confirmation_hash"]),
    )
    print(
        json.dumps(
            {
                "stage": "verify_export_cleanup",
                "request_id": REQUEST_ID,
                "customer_id": CUSTOMER_ID,
                "logical": {**logical_details, "materialized_views": view_details},
                "export": export_details,
                "physical": physical_details,
            },
            sort_keys=True,
        )
    )
    dbutils.notebook.exit("ok")
except Exception as exc:
    if not deferred_recorded:
        _mark_failure(state, current_layer, exc)
    raise

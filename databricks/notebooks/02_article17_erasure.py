# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # GDPR Article 17 — Fail-Closed Erasure Command
# #
# This notebook executes an idempotent, durable erasure command:
# `queued → running → completed | failed`. Every state transition is upserted
# into `governance.erasure_command_state`; the legacy audit table is mirrored
# idempotently for existing consumers. App ticket_id, request_id,
# idempotency_key, requested_by, and reason are retained in the returned state.
# The native Workflow graph must run `mode=prepare`, synchronously refresh the
# Silver and Gold Lakeflow pipelines, then run `mode=verify_only` with the
# refresh run ID. This notebook cannot refresh another pipeline.
# %
# The governed cascade is:
# 1. Idempotently suppress future ingestion/streaming events.
# 2. Mask clear-text PII in every Bronze event table and delete matching rows
#    from the restricted Bronze ingest quarantine (including raw payloads).
# 3. Delete persisted fraud alerts from the job-owned Bronze sink.
# 4. Stop and wait for the Workflow to synchronously refresh the Lakeflow-owned
#    Silver and Gold MATERIALIZED VIEWs after suppression.
# 5. Verify both MV layers no longer contain the customer; never DML them here.
# 6. Rebuild and verify the complete `lake.exports` Volume snapshot only after
#    that required refresh/verification succeeds.
# 7. Scope physical cleanup and independent file verification to the job-owned
#    fraud sink; pipeline-owned Bronze/MV storage is explicitly not claimed.
# %
# `VACUUM` is cleanup, not proof. Physical verification is a separate content
# scan of the job-owned fraud sink only. Pipeline-owned Bronze streaming tables
# and Silver/Gold MATERIALIZED VIEWs are never vacuumed or described as
# physically erased here. Fraud-sink cleanup is guarded by
# `confirm_quiesced_physical_cleanup=true`; Delta retention safety remains enabled.
# %
# The 60-second default is an **internal SLO**, not a statutory deadline. The
# durable audit records its latency separately. Physical scope is the job-owned
# fraud sink only; Bronze/MV storage, backups, remote Hugging Face history, and
# streaming checkpoints are not claimed as physically erased.
# %
# Set `mode=verify_only` to verify an existing request without changing source
# tables. A missing command-state row or any failed required check is an error.

# COMMAND ----------

import uuid

dbutils.widgets.text("customer_id", "")
dbutils.widgets.text("ticket_id", "")
dbutils.widgets.text("request_id", "")
dbutils.widgets.text("idempotency_key", "")
dbutils.widgets.text("requested_by", "")
dbutils.widgets.text("reason", "")
dbutils.widgets.text("actor", "")
dbutils.widgets.text("workflow_run_id", "")
dbutils.widgets.text("internal_slo_seconds", "")
dbutils.widgets.text("sla_seconds", "60")  # Workflow alias; value is an internal SLO
dbutils.widgets.text("mode", "prepare")  # prepare | execute | verify_only
dbutils.widgets.text("silver_gold_refresh_run_id", "")
dbutils.widgets.text("confirm_quiesced_physical_cleanup", "false")

CUSTOMER_ID = dbutils.widgets.get("customer_id").strip()
TICKET_WIDGET = dbutils.widgets.get("ticket_id").strip()
REQUEST_ID = dbutils.widgets.get("request_id").strip()
IDEMPOTENCY_WIDGET = dbutils.widgets.get("idempotency_key").strip()
REQUESTED_BY_WIDGET = dbutils.widgets.get("requested_by").strip()
REASON_WIDGET = dbutils.widgets.get("reason").strip()
REFRESH_RUN_ID = dbutils.widgets.get("silver_gold_refresh_run_id").strip()
ACTOR = dbutils.widgets.get("actor").strip()
WORKFLOW_RUN_ID = dbutils.widgets.get("workflow_run_id").strip() or str(
    getattr(dbutils.context, "jobRunId", "")
)
MODE = dbutils.widgets.get("mode").strip().lower()
CONFIRM_PHYSICAL = (
    dbutils.widgets.get("confirm_quiesced_physical_cleanup").strip().lower() == "true"
)
INTERNAL_SLO_WIDGET = dbutils.widgets.get("internal_slo_seconds").strip()
WORKFLOW_SLO_WIDGET = dbutils.widgets.get("sla_seconds").strip()
LAKE_ROOT = "/Volumes/eurostream/lake/exports"
STAGING_ROOT = f"{LAKE_ROOT}/.staging"
ANONYMIZED = "<anonymized>"  # noqa: S105 - fixed masking marker, not a secret

# COMMAND ----------

import hashlib
import json
import time
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructSchema,
    TimestampType,
)

# Keep manual and Job execution independent of the workspace default catalog.
spark.sql("USE CATALOG eurostream")

# COMMAND ----------


def utc_now() -> datetime:
    """Return naive UTC for Spark TimestampType (the value is UTC by convention)."""
    return datetime.now(UTC).replace(tzinfo=None)


def positive_int(raw_value: str, field_name: str) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return value


def required_text(value: str, field_name: str, max_length: int = 512) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} is required")
    if len(cleaned) > max_length:
        raise ValueError(f"{field_name} exceeds {max_length} characters")
    if any(ord(character) < 32 for character in cleaned):
        raise ValueError(f"{field_name} contains control characters")
    return cleaned


def validate_replay_identity(
    existing: dict[str, object],
    *,
    customer_id: str,
    ticket_id: str,
    idempotency_key: str,
) -> None:
    """Reject replay-key reuse across customers, tickets, or logical requests."""
    if existing.get("customer_id") != customer_id:
        raise ValueError("idempotency_key is already bound to a different customer_id")
    existing_ticket = str(existing.get("ticket_id") or "").strip()
    if existing_ticket and existing_ticket != ticket_id:
        raise ValueError("idempotency_key is already bound to a different ticket_id")
    existing_key = str(existing.get("idempotency_key") or "").strip()
    if existing_key and existing_key != idempotency_key:
        raise ValueError("request_id is already bound to a different idempotency_key")


def confirmation_hash(request_id: str, customer_id: str) -> str:
    return hashlib.sha256(f"{request_id}:{customer_id}".encode()).hexdigest()[:16]


def error_detail(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:4000]


def _load_intake_metadata() -> tuple[datetime | None, str | None, str | None]:
    """Resolve queue time, requester, and reason from the Databricks bronze intake."""
    rows = (
        spark.table("bronze.erasure_requests")
        .filter(
            (F.col("request_id") == F.lit(REQUEST_ID))
            & (F.col("customer_id") == F.lit(CUSTOMER_ID))
        )
        .orderBy(F.col("occurred_at").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        return None, None, None
    row = rows[0]
    requested_at = datetime.fromtimestamp(float(row["occurred_at"]), tz=UTC).replace(tzinfo=None)
    requested_by = str(row["requested_by"]).strip() if row["requested_by"] else None
    reason = str(row["reason"]).strip() if row["reason"] else None
    return requested_at, requested_by, reason


def _resolve_verify_command():
    """Resolve verify-only runs even when a Workflow omits request_id."""
    query = spark.table("governance.erasure_command_state")
    query = query.filter(F.col("command_type") == F.lit("article_17_erasure"))
    if REQUEST_ID:
        query = query.filter(F.col("command_id") == F.lit(REQUEST_ID))
    else:
        query = query.filter(F.col("customer_id") == F.lit(CUSTOMER_ID))
    rows = query.orderBy(F.col("requested_at").desc()).limit(2).collect()
    if not rows:
        raise ValueError(
            f"No durable erasure command found for customer_id={CUSTOMER_ID}"
            + (f", request_id={REQUEST_ID}" if REQUEST_ID else "")
        )
    if REQUEST_ID and len(rows) != 1:
        raise RuntimeError(f"Duplicate durable command rows for request_id={REQUEST_ID}")
    return rows[0]


def _find_command_by_idempotency(idempotency_key: str):
    """Return the one durable command bound to an App replay key."""
    if not idempotency_key:
        return None
    rows = (
        spark.table("governance.erasure_command_state")
        .filter(
            (F.col("command_type") == F.lit("article_17_erasure"))
            & (F.col("idempotency_key") == F.lit(idempotency_key))
        )
        .limit(2)
        .collect()
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError(f"Duplicate durable command rows for idempotency_key={idempotency_key}")
    return rows[0]


def _find_resumable_command():
    """Reuse the latest durable request when an execute retry omits request_id."""
    rows = (
        spark.table("governance.erasure_command_state")
        .filter(
            (F.col("customer_id") == F.lit(CUSTOMER_ID))
            & (F.col("command_type") == F.lit("article_17_erasure"))
        )
        .orderBy(F.col("requested_at").desc())
        .limit(1)
        .collect()
    )
    return rows[0] if rows else None


if MODE not in {"execute", "prepare", "verify_only"}:
    raise ValueError("mode must be 'prepare', 'execute', or 'verify_only'")
CUSTOMER_ID = required_text(CUSTOMER_ID, "customer_id")
TICKET_ID = required_text(TICKET_WIDGET or "manual", "ticket_id", 256)
REASON = required_text(REASON_WIDGET or "GDPR_ARTICLE_17", "reason", 128)
REQUESTED_BY = REQUESTED_BY_WIDGET or None
IDEMPOTENCY_KEY = IDEMPOTENCY_WIDGET or None

RESOLVED_VERIFY_COMMAND = None
RESUMED_EXECUTE_COMMAND = None
if MODE == "verify_only":
    if IDEMPOTENCY_KEY:
        RESOLVED_VERIFY_COMMAND = _find_command_by_idempotency(IDEMPOTENCY_KEY)
    if RESOLVED_VERIFY_COMMAND is None:
        RESOLVED_VERIFY_COMMAND = _resolve_verify_command()
    resolved_verify_state = RESOLVED_VERIFY_COMMAND.asDict()
    REQUEST_ID = str(resolved_verify_state["command_id"])
    if not TICKET_WIDGET:
        TICKET_ID = str(resolved_verify_state.get("ticket_id") or TICKET_ID)
    if not IDEMPOTENCY_WIDGET:
        IDEMPOTENCY_KEY = str(resolved_verify_state.get("idempotency_key") or "")
    if not REQUESTED_BY_WIDGET:
        REQUESTED_BY = str(resolved_verify_state.get("requested_by") or "")
    if not REASON_WIDGET:
        REASON = str(resolved_verify_state.get("reason") or REASON)
    TICKET_ID = required_text(TICKET_ID, "ticket_id", 256)
    IDEMPOTENCY_KEY = required_text(IDEMPOTENCY_KEY, "idempotency_key")
    REQUESTED_BY = required_text(REQUESTED_BY, "requested_by", 256)
    REASON = required_text(REASON, "reason", 128)
    validate_replay_identity(
        resolved_verify_state,
        customer_id=CUSTOMER_ID,
        ticket_id=TICKET_ID,
        idempotency_key=IDEMPOTENCY_KEY,
    )
    stored_slo = int(resolved_verify_state["internal_slo_seconds"])
    if (
        INTERNAL_SLO_WIDGET
        and positive_int(INTERNAL_SLO_WIDGET, "internal_slo_seconds") != stored_slo
    ):
        raise ValueError(
            f"request_id={REQUEST_ID} was created with internal_slo_seconds={stored_slo}"
        )
    INTERNAL_SLO_SECONDS = stored_slo
    ACTOR = str(resolved_verify_state["actor"])
else:
    if IDEMPOTENCY_KEY:
        RESUMED_EXECUTE_COMMAND = _find_command_by_idempotency(IDEMPOTENCY_KEY)
    if RESUMED_EXECUTE_COMMAND is None and not REQUEST_ID:
        RESUMED_EXECUTE_COMMAND = _find_resumable_command()
    if RESUMED_EXECUTE_COMMAND:
        REQUEST_ID = str(RESUMED_EXECUTE_COMMAND["command_id"])
    elif not REQUEST_ID:
        REQUEST_ID = str(uuid.uuid4())
    REQUEST_ID = required_text(REQUEST_ID, "request_id")
    resumed_execute_state = RESUMED_EXECUTE_COMMAND.asDict() if RESUMED_EXECUTE_COMMAND else None
    if not IDEMPOTENCY_KEY and resumed_execute_state:
        IDEMPOTENCY_KEY = str(resumed_execute_state.get("idempotency_key") or REQUEST_ID)
    IDEMPOTENCY_KEY = required_text(IDEMPOTENCY_KEY or REQUEST_ID, "idempotency_key")

    if resumed_execute_state:
        if not TICKET_WIDGET:
            TICKET_ID = str(resumed_execute_state.get("ticket_id") or TICKET_ID)
        REQUESTED_BY = str(resumed_execute_state.get("requested_by") or REQUESTED_BY or "")
        REASON = str(resumed_execute_state.get("reason") or REASON)
        validate_replay_identity(
            resumed_execute_state,
            customer_id=CUSTOMER_ID,
            ticket_id=TICKET_ID,
            idempotency_key=IDEMPOTENCY_KEY,
        )
        stored_slo = int(resumed_execute_state["internal_slo_seconds"])
        if (
            INTERNAL_SLO_WIDGET
            and positive_int(INTERNAL_SLO_WIDGET, "internal_slo_seconds") != stored_slo
        ):
            raise ValueError(
                f"request_id={REQUEST_ID} was created with internal_slo_seconds={stored_slo}"
            )
        INTERNAL_SLO_SECONDS = stored_slo
    else:
        requested_slo = INTERNAL_SLO_WIDGET or WORKFLOW_SLO_WIDGET
        INTERNAL_SLO_SECONDS = positive_int(requested_slo, "internal_slo_seconds")

if MODE == "verify_only":
    INTAKE_REQUESTED_AT, INTAKE_REQUESTED_BY, INTAKE_REASON = None, None, None
else:
    INTAKE_REQUESTED_AT, INTAKE_REQUESTED_BY, INTAKE_REASON = _load_intake_metadata()
    resumed_requested_by = (
        str(resumed_execute_state.get("requested_by") or "") if resumed_execute_state else ""
    )
    resumed_reason = str(resumed_execute_state.get("reason") or "") if resumed_execute_state else ""
    REQUESTED_BY = required_text(
        resumed_requested_by or REQUESTED_BY_WIDGET or INTAKE_REQUESTED_BY or "unknown",
        "requested_by",
        256,
    )
    REASON = required_text(
        resumed_reason or REASON_WIDGET or INTAKE_REASON or REASON,
        "reason",
        128,
    )
    resumed_actor = str(resumed_execute_state["actor"]) if resumed_execute_state else None
    ACTOR = required_text(
        ACTOR or resumed_actor or REQUESTED_BY or "databricks_workflow", "actor", 256
    )

# COMMAND ----------

# %md
# ## Durable command state and governed export manifest

# COMMAND ----------

COMMAND_TABLE = "governance.erasure_command_state"
COMMAND_SOURCE_VIEW = "eurostream_erasure_command_source"
LEGACY_SOURCE_VIEW = "eurostream_erasure_legacy_source"

COMMAND_SCHEMA = StructSchema(
    [
        StructField("command_id", StringType(), False),
        StructField("request_id", StringType(), False),
        StructField("ticket_id", StringType(), False),
        StructField("idempotency_key", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("requested_by", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("workflow_run_id", StringType(), True),
        StructField("silver_gold_refresh_run_id", StringType(), True),
        StructField("command_type", StringType(), False),
        StructField("status", StringType(), False),
        StructField("requested_at", TimestampType(), False),
        StructField("started_at", TimestampType(), True),
        StructField("completed_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), False),
        StructField("attempt_count", IntegerType(), False),
        StructField("internal_slo_seconds", LongType(), False),
        StructField("internal_slo_state", StringType(), False),
        StructField("slo_deadline_at", TimestampType(), True),
        StructField("latency_seconds", DoubleType(), True),
        StructField("logical_verified", BooleanType(), True),
        StructField("physical_verified", BooleanType(), True),
        StructField("export_verified", BooleanType(), True),
        StructField("logical_verification_detail", StringType(), True),
        StructField("physical_verification_detail", StringType(), True),
        StructField("export_verification_detail", StringType(), True),
        StructField("layers_completed", StringType(), False),
        StructField("blocked_layer", StringType(), True),
        StructField("next_action", StringType(), True),
        StructField("failed_layer", StringType(), True),
        StructField("failure_message", StringType(), True),
        StructField("confirmation_hash", StringType(), False),
        StructField("export_root", StringType(), False),
    ]
)

EXPORTS = {
    "silver/orders": "silver.orders",
    "silver/payments": "silver.payments",
    "silver/customers": "silver.customers",
    "gold/customer_360": "gold.customer_360",
    "gold/order_facts": "gold.order_facts",
    "gold/fraud_summary": "gold.fraud_summary",
}
EXPORT_TABLES_BY_LAYER = {
    "silver": frozenset(path.split("/", 1)[1] for path in EXPORTS if path.startswith("silver/")),
    "gold": frozenset(path.split("/", 1)[1] for path in EXPORTS if path.startswith("gold/")),
}

BRONZE_MASK_SQL = (
    (
        "bronze.orders",
        """
        UPDATE bronze.orders
        SET email = :marker, iban = :marker
        WHERE customer_id = :customer_id
        """.strip(),
    ),
    (
        "bronze.orders_files",
        """
        UPDATE bronze.orders_files
        SET email = :marker, iban = :marker
        WHERE customer_id = :customer_id
        """.strip(),
    ),
    (
        "bronze.payments",
        """
        UPDATE bronze.payments
        SET iban = :marker
        WHERE customer_id = :customer_id
        """.strip(),
    ),
    (
        "bronze.clicks",
        """
        UPDATE bronze.clicks
        SET ip_address = :marker
        WHERE customer_id = :customer_id
        """.strip(),
    ),
)

BRONZE_QUARANTINE_DELETE_SQL = """
    DELETE FROM bronze.ingest_quarantine
    WHERE customer_id = :customer_id
       OR instr(coalesce(raw_payload, ''), :customer_id) > 0
"""

# Silver and Gold are Lakeflow-owned MATERIALIZED VIEWs. They are read-only to
# this notebook and are removed only by the required downstream pipeline refresh.
# Physical cleanup is limited to the job-owned Bronze fraud sink.
PHYSICAL_TABLES = {
    "bronze.fraud_alerts": ("absent", ()),
}
PHYSICAL_CLEANUP_SQL = (
    "REORG TABLE bronze.fraud_alerts APPLY (PURGE)",
    "VACUUM bronze.fraud_alerts RETAIN 0 HOURS",
)

# COMMAND ----------


def _state_from_inputs(
    *,
    status: str,
    requested_at: datetime,
    started_at: datetime | None,
    updated_at: datetime,
    attempt_count: int,
    logical_verified: bool | None = None,
    physical_verified: bool | None = None,
    export_verified: bool | None = None,
    logical_detail: str | None = None,
    physical_detail: str | None = None,
    export_detail: str | None = None,
    layers_completed: list[str] | None = None,
    refresh_run_id: str | None = None,
    blocked_layer: str | None = None,
    next_action: str | None = None,
    failed_layer: str | None = None,
    failure_message: str | None = None,
    completed_at: datetime | None = None,
) -> dict[str, object]:
    slo_deadline = requested_at + timedelta(seconds=INTERNAL_SLO_SECONDS)
    if completed_at is None:
        slo_state = "pending"
        latency = None
    else:
        latency = max(0.0, (completed_at - requested_at).total_seconds())
        slo_state = "met" if completed_at <= slo_deadline else "breached"
    return {
        "command_id": REQUEST_ID,
        "request_id": REQUEST_ID,
        "ticket_id": TICKET_ID,
        "idempotency_key": IDEMPOTENCY_KEY,
        "customer_id": CUSTOMER_ID,
        "actor": ACTOR,
        "requested_by": REQUESTED_BY,
        "reason": REASON,
        "workflow_run_id": WORKFLOW_RUN_ID or None,
        "silver_gold_refresh_run_id": refresh_run_id,
        "command_type": "article_17_erasure",
        "status": status,
        "requested_at": requested_at,
        "started_at": started_at,
        "completed_at": completed_at,
        "updated_at": updated_at,
        "attempt_count": attempt_count,
        "internal_slo_seconds": INTERNAL_SLO_SECONDS,
        "internal_slo_state": slo_state,
        "slo_deadline_at": slo_deadline,
        "latency_seconds": latency,
        "logical_verified": logical_verified,
        "physical_verified": physical_verified,
        "export_verified": export_verified,
        "logical_verification_detail": logical_detail,
        "physical_verification_detail": physical_detail,
        "export_verification_detail": export_detail,
        "layers_completed": ",".join(layers_completed or []),
        "blocked_layer": blocked_layer,
        "next_action": next_action,
        "failed_layer": failed_layer,
        "failure_message": failure_message,
        "confirmation_hash": confirmation_hash(REQUEST_ID, CUSTOMER_ID),
        "export_root": LAKE_ROOT,
    }


def _upsert_command_state(state: dict[str, object]) -> None:
    # DBR 15.4 has no enforced UNIQUE constraint. The preflight lookup, guarded
    # MERGE, and post-write identity check together prevent duplicate replay keys.
    values = tuple(state[field.name] for field in COMMAND_SCHEMA.fields)
    source = spark.createDataFrame([values], schema=COMMAND_SCHEMA)
    source.createOrReplaceTempView(COMMAND_SOURCE_VIEW)
    spark.sql(
        """
        MERGE INTO governance.erasure_command_state AS target
        USING eurostream_erasure_command_source AS source
        ON target.command_id = source.command_id
           OR target.idempotency_key = source.idempotency_key
        WHEN MATCHED AND target.customer_id = source.customer_id
                      AND target.ticket_id = source.ticket_id
                      AND target.idempotency_key = source.idempotency_key
        THEN UPDATE SET
            actor = source.actor,
            workflow_run_id = source.workflow_run_id,
            silver_gold_refresh_run_id = source.silver_gold_refresh_run_id,
            command_type = source.command_type,
            status = source.status,
            requested_at = source.requested_at,
            started_at = source.started_at,
            completed_at = source.completed_at,
            updated_at = source.updated_at,
            attempt_count = source.attempt_count,
            internal_slo_seconds = source.internal_slo_seconds,
            internal_slo_state = source.internal_slo_state,
            slo_deadline_at = source.slo_deadline_at,
            latency_seconds = source.latency_seconds,
            logical_verified = source.logical_verified,
            physical_verified = source.physical_verified,
            export_verified = source.export_verified,
            logical_verification_detail = source.logical_verification_detail,
            physical_verification_detail = source.physical_verification_detail,
            export_verification_detail = source.export_verification_detail,
            layers_completed = source.layers_completed,
            blocked_layer = source.blocked_layer,
            next_action = source.next_action,
            failed_layer = source.failed_layer,
            failure_message = source.failure_message,
            confirmation_hash = source.confirmation_hash,
            export_root = source.export_root
        WHEN NOT MATCHED THEN INSERT (
            command_id, request_id, ticket_id, idempotency_key, customer_id,
            actor, requested_by, reason, workflow_run_id, silver_gold_refresh_run_id,
            command_type, status, requested_at, started_at, completed_at,
            updated_at, attempt_count, internal_slo_seconds, internal_slo_state,
            slo_deadline_at, latency_seconds, logical_verified, physical_verified,
            export_verified, logical_verification_detail, physical_verification_detail,
            export_verification_detail, layers_completed, blocked_layer, next_action,
            failed_layer, failure_message, confirmation_hash, export_root
        ) VALUES (
            source.command_id, source.request_id, source.ticket_id,
            source.idempotency_key, source.customer_id, source.actor,
            source.requested_by, source.reason, source.workflow_run_id,
            source.silver_gold_refresh_run_id, source.command_type, source.status,
            source.requested_at, source.started_at, source.completed_at,
            source.updated_at, source.attempt_count, source.internal_slo_seconds,
            source.internal_slo_state, source.slo_deadline_at, source.latency_seconds,
            source.logical_verified, source.physical_verified, source.export_verified,
            source.logical_verification_detail, source.physical_verification_detail,
            source.export_verification_detail, source.layers_completed,
            source.blocked_layer, source.next_action,
            source.failed_layer, source.failure_message, source.confirmation_hash,
            source.export_root
        )
        """
    )
    persisted = (
        spark.table(COMMAND_TABLE)
        .filter(
            (F.col("command_id") == F.lit(state["command_id"]))
            | (F.col("idempotency_key") == F.lit(state["idempotency_key"]))
        )
        .limit(2)
        .collect()
    )
    if len(persisted) != 1:
        raise RuntimeError("Erasure command upsert did not produce exactly one durable row")
    durable = persisted[0].asDict()
    for field in ("command_id", "request_id", "ticket_id", "idempotency_key", "customer_id"):
        if durable[field] != state[field]:
            raise RuntimeError(f"Erasure command identity conflict for {field}")


def _mirror_legacy_audit(state: dict[str, object]) -> None:
    """Mirror one durable row without duplicating retries of the same request."""
    source = (
        spark.table(COMMAND_TABLE)
        .filter(F.col("command_id") == F.lit(state["command_id"]))
        .select(
            "request_id",
            "customer_id",
            F.unix_timestamp("requested_at").alias("requested_at"),
            F.unix_timestamp(F.coalesce(F.col("completed_at"), F.col("updated_at"))).alias(
                "completed_at"
            ),
            F.col("layers_completed").alias("layers_touched"),
            "status",
            "confirmation_hash",
        )
    )
    source.createOrReplaceTempView(LEGACY_SOURCE_VIEW)
    spark.sql(
        """
        MERGE INTO governance.erasure_audit_log AS target
        USING eurostream_erasure_legacy_source AS source
        ON target.request_id = source.request_id
           AND target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET
            requested_at = source.requested_at,
            completed_at = source.completed_at,
            layers_touched = source.layers_touched,
            status = source.status,
            confirmation_hash = source.confirmation_hash
        WHEN NOT MATCHED THEN INSERT (
            request_id, customer_id, requested_at, completed_at,
            layers_touched, status, confirmation_hash
        ) VALUES (
            source.request_id, source.customer_id, source.requested_at,
            source.completed_at, source.layers_touched, source.status,
            source.confirmation_hash
        )
        """
    )


def _load_command() -> dict[str, object] | None:
    rows = (
        spark.table(COMMAND_TABLE)
        .filter(F.col("command_id") == F.lit(REQUEST_ID))
        .limit(2)
        .collect()
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError(f"Duplicate durable command rows for request_id={REQUEST_ID}")
    return rows[0].asDict()


def _persist_and_mirror(state: dict[str, object]) -> None:
    _upsert_command_state(state)
    _mirror_legacy_audit(state)


def _begin_command() -> dict[str, object]:
    global TICKET_ID, IDEMPOTENCY_KEY, REQUESTED_BY, REASON
    existing = _load_command()
    if existing and existing["customer_id"] != CUSTOMER_ID:
        raise ValueError(f"request_id={REQUEST_ID} is already bound to a different customer_id")
    if existing:
        if not TICKET_WIDGET:
            TICKET_ID = str(existing.get("ticket_id") or TICKET_ID)
        if not IDEMPOTENCY_WIDGET:
            IDEMPOTENCY_KEY = str(existing.get("idempotency_key") or IDEMPOTENCY_KEY)
        REQUESTED_BY = str(existing.get("requested_by") or REQUESTED_BY or "")
        REASON = str(existing.get("reason") or REASON)
        TICKET_ID = required_text(TICKET_ID, "ticket_id", 256)
        IDEMPOTENCY_KEY = required_text(IDEMPOTENCY_KEY, "idempotency_key")
        REQUESTED_BY = required_text(REQUESTED_BY, "requested_by", 256)
        REASON = required_text(REASON, "reason", 128)
        validate_replay_identity(
            existing,
            customer_id=CUSTOMER_ID,
            ticket_id=TICKET_ID,
            idempotency_key=IDEMPOTENCY_KEY,
        )
    if existing and int(existing["internal_slo_seconds"]) != INTERNAL_SLO_SECONDS:
        raise ValueError(
            f"request_id={REQUEST_ID} was created with internal_slo_seconds="
            f"{existing['internal_slo_seconds']}"
        )
    if existing and existing["status"] == "completed":
        return existing
    requested_at = existing["requested_at"] if existing else INTAKE_REQUESTED_AT or utc_now()
    attempt_count = int(existing["attempt_count"]) + 1 if existing else 1
    started_at = existing["started_at"] if existing else utc_now()

    queued = _state_from_inputs(
        status="queued",
        requested_at=requested_at,
        started_at=None,
        updated_at=utc_now(),
        attempt_count=attempt_count,
    )
    _persist_and_mirror(queued)

    running = _state_from_inputs(
        status="running",
        requested_at=requested_at,
        started_at=started_at,
        updated_at=utc_now(),
        attempt_count=attempt_count,
    )
    _persist_and_mirror(running)
    return running


def _mark_layer(state: dict[str, object], layer: str) -> None:
    layers = [item for item in str(state["layers_completed"]).split(",") if item]
    if layer not in layers:
        layers.append(layer)
    state.update(
        _state_from_inputs(
            status="running",
            requested_at=state["requested_at"],
            started_at=state["started_at"],
            updated_at=utc_now(),
            attempt_count=int(state["attempt_count"]),
            logical_verified=state["logical_verified"],
            physical_verified=state["physical_verified"],
            export_verified=state["export_verified"],
            logical_detail=state["logical_verification_detail"],
            physical_detail=state["physical_verification_detail"],
            export_detail=state["export_verification_detail"],
            layers_completed=layers,
            refresh_run_id=state["silver_gold_refresh_run_id"],
            blocked_layer=state["blocked_layer"],
            next_action=state["next_action"],
        )
    )
    _persist_and_mirror(state)


def _mark_failed(state: dict[str, object], failed_layer: str, exc: BaseException) -> None:
    completed_at = utc_now()
    layers = [item for item in str(state["layers_completed"]).split(",") if item]
    failed = _state_from_inputs(
        status="failed",
        requested_at=state["requested_at"],
        started_at=state["started_at"],
        updated_at=completed_at,
        attempt_count=int(state["attempt_count"]),
        logical_verified=state["logical_verified"],
        physical_verified=state["physical_verified"],
        export_verified=state["export_verified"],
        logical_detail=state["logical_verification_detail"],
        physical_detail=state["physical_verification_detail"],
        export_detail=state["export_verification_detail"],
        layers_completed=layers,
        refresh_run_id=state["silver_gold_refresh_run_id"],
        failed_layer=failed_layer,
        failure_message=error_detail(exc),
        completed_at=completed_at,
    )
    _persist_and_mirror(failed)
    state.clear()
    state.update(failed)


def _mark_completed(state: dict[str, object]) -> None:
    if not all(
        state[field] is True
        for field in ("logical_verified", "physical_verified", "export_verified")
    ):
        raise RuntimeError("Completion refused: logical, physical, and export checks must pass")
    if not state["silver_gold_refresh_run_id"]:
        raise RuntimeError("Completion refused: Silver/Gold refresh run ID is missing")
    if state["blocked_layer"] or state["next_action"] or state["failed_layer"]:
        raise RuntimeError("Completion refused: command still has a blocked or failed layer")
    completed_at = utc_now()
    layers = [item for item in str(state["layers_completed"]).split(",") if item]
    completed = _state_from_inputs(
        status="completed",
        requested_at=state["requested_at"],
        started_at=state["started_at"],
        updated_at=completed_at,
        attempt_count=int(state["attempt_count"]),
        logical_verified=True,
        physical_verified=True,
        export_verified=True,
        logical_detail=state["logical_verification_detail"],
        physical_detail=state["physical_verification_detail"],
        export_detail=state["export_verification_detail"],
        layers_completed=layers,
        refresh_run_id=state["silver_gold_refresh_run_id"],
        completed_at=completed_at,
    )
    _persist_and_mirror(completed)
    state.clear()
    state.update(completed)


# COMMAND ----------

# %md
# ## Governed `lake.exports` rewrite
# The same manifest and exact schema/count checks are used by
# `04_export_lake.py`. Restricted Bronze and quarantine data never enter staging.

# COMMAND ----------


def _remove(path: str) -> None:
    dbutils.fs.rm(path, recurse=True)


def _validate_export_manifest() -> None:
    for relative_path, table in EXPORTS.items():
        if not relative_path.startswith(("silver/", "gold/")):
            raise PermissionError(f"Only Silver/Gold export paths are allowed: {relative_path}")
        if not table.startswith(("silver.", "gold.")) or table.rsplit(".", 1)[-1].endswith(
            "_quarantine"
        ):
            raise PermissionError(f"Restricted table export blocked: {table}")


def _write_staged_table(relative_path: str, table: str) -> int:
    source = spark.read.table(table)
    destination = f"{STAGING_ROOT}/{relative_path}"
    (
        source.repartition(F.col("customer_id"))
        .write.mode("overwrite")
        .option("maxRecordsPerFile", 500_000)
        .parquet(destination)
    )
    exported = spark.read.parquet(destination)
    source_count = source.count()
    exported_count = exported.count()
    if source_count != exported_count:
        raise RuntimeError(
            f"Staged export count mismatch for {table}: source={source_count}, "
            f"staged={exported_count}"
        )
    if exported.schema != source.schema:
        raise RuntimeError(f"Staged export schema mismatch for {table}")
    return exported_count


def _remove_stale_local_paths() -> None:
    for entry in dbutils.fs.ls(LAKE_ROOT):
        if entry.name not in {"silver", "gold"}:
            _remove(entry.path)
    for layer, expected_tables in EXPORT_TABLES_BY_LAYER.items():
        for entry in dbutils.fs.ls(f"{LAKE_ROOT}/{layer}"):
            if entry.name not in expected_tables:
                _remove(entry.path)


def rewrite_governed_export() -> None:
    _validate_export_manifest()
    dbutils.fs.mkdirs(LAKE_ROOT)
    _remove(STAGING_ROOT)
    staged: list[tuple[str, str]] = []
    try:
        dbutils.fs.mkdirs(STAGING_ROOT)
        for relative_path, table in EXPORTS.items():
            row_count = _write_staged_table(relative_path, table)
            staged.append((relative_path, table))
            print(f"staged {table:28s} ({row_count:,} rows)")
        dbutils.fs.mkdirs(f"{LAKE_ROOT}/silver")
        dbutils.fs.mkdirs(f"{LAKE_ROOT}/gold")
        for relative_path, _table in staged:
            destination = f"{LAKE_ROOT}/{relative_path}"
            _remove(destination)
            dbutils.fs.mv(f"{STAGING_ROOT}/{relative_path}", destination)
    finally:
        _remove(STAGING_ROOT)
    _remove_stale_local_paths()


def verify_governed_export(customer_id: str) -> dict[str, object]:
    details: dict[str, object] = {}
    for relative_path, table in EXPORTS.items():
        source = spark.read.table(table)
        exported = spark.read.parquet(f"{LAKE_ROOT}/{relative_path}")
        if exported.schema != source.schema:
            raise RuntimeError(f"Published export schema mismatch for {table}")
        source_count = source.count()
        exported_count = exported.count()
        if source_count != exported_count:
            raise RuntimeError(
                f"Published export count mismatch for {table}: source={source_count}, "
                f"exported={exported_count}"
            )
        customer_rows = exported.filter(F.col("customer_id") == F.lit(customer_id)).count()
        if customer_rows:
            raise RuntimeError(
                f"Published export still contains {customer_rows} target rows: {table}"
            )
        details[relative_path] = {"rows": exported_count, "target_rows": customer_rows}
    return {
        "root": LAKE_ROOT,
        "tables": details,
        "scope": "Unity Catalog Volume current files only; remote Hugging Face not verified",
    }


# COMMAND ----------

# %md
# ## Logical verification
# Bronze event tables are retained only with every configured PII column
# masked; Bronze/Silver quarantine, Gold, and fraud targets must contain no
# matching rows or raw payload. The durable
# suppression key is intentionally retained to prevent replay/re-ingestion.

# COMMAND ----------


def _customer_count(table: str, customer_id: str) -> int:
    return spark.table(table).filter(F.col("customer_id") == F.lit(customer_id)).count()


def _bronze_mask_counts(table: str, columns: tuple[str, ...], customer_id: str) -> dict[str, int]:
    target = spark.table(table).filter(F.col("customer_id") == F.lit(customer_id))
    target_count = target.count()
    invalid = target.filter(
        F.reduce(
            [F.col(column).isNull() | (F.col(column) != F.lit(ANONYMIZED)) for column in columns],
            F.or_,
        )
    ).count()
    return {"rows": target_count, "invalid_pii_rows": invalid}


def verify_direct_erasure(customer_id: str) -> dict[str, object]:
    """Verify only mutation targets owned by this Bronze/fraud workflow."""
    bronze_counts = {
        table: _bronze_mask_counts(table, columns, customer_id)
        for table, columns in (
            ("bronze.orders", ("email", "iban")),
            ("bronze.orders_files", ("email", "iban")),
            ("bronze.payments", ("iban",)),
            ("bronze.clicks", ("ip_address",)),
        )
    }
    ingest_quarantine_remaining = (
        spark.table("bronze.ingest_quarantine")
        .filter(
            (F.col("customer_id") == F.lit(customer_id))
            | (F.instr(F.coalesce(F.col("raw_payload"), F.lit("")), F.lit(customer_id)) > 0)
        )
        .count()
    )
    fraud_alert_rows = _customer_count("bronze.fraud_alerts", customer_id)
    suppression_count = _customer_count("governance.suppression_registry", customer_id)
    passed = (
        suppression_count > 0
        and all(counts["invalid_pii_rows"] == 0 for counts in bronze_counts.values())
        and ingest_quarantine_remaining == 0
        and fraud_alert_rows == 0
    )
    return {
        "passed": passed,
        "suppression_rows": suppression_count,
        "bronze": bronze_counts,
        "ingest_quarantine_rows_remaining": ingest_quarantine_remaining,
        "fraud_alert_rows_remaining": fraud_alert_rows,
    }


def verify_silver_gold_refresh(customer_id: str) -> dict[str, object]:
    """Read-only proof that the required Lakeflow MV refresh removed the target."""
    rows_remaining = {
        table: _customer_count(table, customer_id)
        for table in (
            "silver.customers",
            "silver.orders",
            "silver.payments",
            "silver.orders_quarantine",
            "silver.payments_quarantine",
            "gold.customer_360",
            "gold.order_facts",
            "gold.fraud_summary",
        )
    }
    return {
        "passed": all(count == 0 for count in rows_remaining.values()),
        "rows_remaining": rows_remaining,
        "refresh_run_id": REFRESH_RUN_ID,
        "ownership": "Lakeflow-owned Silver/Gold MATERIALIZED VIEWs; read-only verification",
    }


# COMMAND ----------

# %md
# ## Physical file verification — fraud sink only
# After guarded `REORG ... APPLY (PURGE)` and zero-retention `VACUUM`, the code
# scans every remaining Parquet file in the job-owned fraud sink. It never runs
# REORG/VACUUM against Lakeflow-owned Bronze streaming tables or Silver/Gold MVs
# and makes no physical-erasure claim for those pipeline-owned relations.

# COMMAND ----------


def _materialize_soft_deletes_and_purge() -> None:
    for statement in PHYSICAL_CLEANUP_SQL:
        spark.sql(statement)
    spark.catalog.clearCache()


def _table_data_files(table: str) -> list[str]:
    detail = DeltaTable.forName(spark, table).detail().first().asDict()
    if detail.get("format") != "delta":
        raise RuntimeError(f"Expected Delta table for physical verification: {table}")
    location = str(detail["location"])
    files: list[str] = []
    for entry in dbutils.fs.ls(location, recursive=True):
        path = str(entry.path)
        if entry.isDir or "/_delta_log/" in f"{path}/" or path.endswith("/_delta_log"):
            continue
        if entry.name in {"_SUCCESS", ".crc"}:
            continue
        if not path.lower().endswith(".parquet"):
            raise RuntimeError(f"Unexpected non-Parquet data file during physical scan: {path}")
        files.append(path)
    return files


def _chunks(values: list[str], size: int = 256):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _physical_violation_count(table: str, customer_id: str, files: list[str]) -> int:
    mode, columns = PHYSICAL_TABLES[table]
    violations = 0
    for file_chunk in _chunks(files):
        physical_df = spark.read.parquet(*file_chunk)
        if mode == "payload_absent":
            target = physical_df.filter(
                (F.col("customer_id") == F.lit(customer_id))
                | (F.instr(F.coalesce(F.col("raw_payload"), F.lit("")), F.lit(customer_id)) > 0)
            )
        else:
            target = physical_df.filter(F.col("customer_id") == F.lit(customer_id))
        if mode in {"absent", "payload_absent"}:
            violations += target.limit(1).count()
        else:
            invalid = target.filter(
                F.reduce(
                    [
                        F.col(column).isNull() | (F.col(column) != F.lit(ANONYMIZED))
                        for column in columns
                    ],
                    F.or_,
                )
            )
            violations += invalid.limit(1).count()
    return violations


def verify_physical_files(customer_id: str) -> dict[str, object]:
    details: dict[str, object] = {}
    all_clean = True
    for table in PHYSICAL_TABLES:
        files = _table_data_files(table)
        violations = _physical_violation_count(table, customer_id, files)
        details[table] = {"parquet_files_scanned": len(files), "violations": violations}
        all_clean = all_clean and violations == 0
    return {
        "passed": all_clean,
        "tables": details,
        "scope": (
            "all remaining Parquet files in the job-owned bronze.fraud_alerts sink only; "
            "no physical claim for Lakeflow-owned Bronze streaming tables, Silver/Gold "
            "MATERIALIZED VIEWs, backups, remote repositories, or checkpoints"
        ),
    }


# COMMAND ----------

# %md
# ## Execute the fail-closed cascade

# COMMAND ----------


def _suppress_customer(customer_id: str) -> None:
    source = spark.createDataFrame(
        [(customer_id, time.time())],
        schema=StructSchema(
            [
                StructField("customer_id", StringType(), False),
                StructField("added_at", DoubleType(), False),
            ]
        ),
    )
    source.createOrReplaceTempView("eurostream_erasure_suppression_source")
    spark.sql(
        """
        MERGE INTO governance.suppression_registry AS target
        USING eurostream_erasure_suppression_source AS source
        ON target.customer_id = source.customer_id
        WHEN NOT MATCHED THEN INSERT (customer_id, added_at)
        VALUES (source.customer_id, source.added_at)
        """
    )


def _record_verification(
    state: dict[str, object], verification_name: str, detail: dict[str, object]
) -> None:
    field = f"{verification_name}_verified"
    detail_field = f"{verification_name}_verification_detail"
    state[field] = bool(detail["passed"])
    state[detail_field] = json.dumps(detail, sort_keys=True, separators=(",", ":"))


def _execute_direct_layers(state: dict[str, object]) -> dict[str, object]:
    """Commit suppression first, then mutate only workflow-owned Bronze targets."""
    current_layer = "command_initialization"
    try:
        current_layer = "suppression_registry"
        _suppress_customer(CUSTOMER_ID)
        _mark_layer(state, current_layer)

        current_layer = "bronze_pii_masking"
        sql_args = {"customer_id": CUSTOMER_ID, "marker": ANONYMIZED}
        for _table, statement in BRONZE_MASK_SQL:
            spark.sql(statement, args=sql_args)
        _mark_layer(state, current_layer)

        current_layer = "bronze_ingest_quarantine_delete"
        spark.sql(BRONZE_QUARANTINE_DELETE_SQL, args={"customer_id": CUSTOMER_ID})
        _mark_layer(state, current_layer)

        current_layer = "fraud_alerts_delete"
        spark.sql(
            "DELETE FROM bronze.fraud_alerts WHERE customer_id = :customer_id",
            args={"customer_id": CUSTOMER_ID},
        )
        _mark_layer(state, current_layer)

        current_layer = "bronze_direct_verification"
        direct = verify_direct_erasure(CUSTOMER_ID)
        state["logical_verification_detail"] = json.dumps(
            {"state": "awaiting_silver_gold_refresh", "direct": direct},
            sort_keys=True,
            separators=(",", ":"),
        )
        _mark_layer(state, current_layer)
        if not direct["passed"]:
            raise RuntimeError(f"Direct Bronze/fraud verification failed: {direct}")
        return direct
    except Exception as exc:
        _mark_failed(state, current_layer, exc)
        raise


def _mark_refresh_required(state: dict[str, object]) -> None:
    """Leave a successful prepare task running with an explicit Workflow handoff."""
    layers = [item for item in str(state["layers_completed"]).split(",") if item]
    blocked = _state_from_inputs(
        status="running",
        requested_at=state["requested_at"],
        started_at=state["started_at"],
        updated_at=utc_now(),
        attempt_count=int(state["attempt_count"]),
        logical_verified=None,
        physical_verified=state["physical_verified"],
        export_verified=state["export_verified"],
        logical_detail=state["logical_verification_detail"],
        physical_detail=state["physical_verification_detail"],
        export_detail=state["export_verification_detail"],
        layers_completed=layers,
        refresh_run_id=state["silver_gold_refresh_run_id"],
        blocked_layer="silver_gold_refresh_required",
        next_action=(
            "Synchronously refresh the Silver and Gold Lakeflow pipelines, then run "
            "mode=verify_only with silver_gold_refresh_run_id."
        ),
    )
    _persist_and_mirror(blocked)
    state.clear()
    state.update(blocked)


def _finish_after_refresh(state: dict[str, object]) -> dict[str, object]:
    """Verify MV refresh, then export; no Silver/Gold mutation occurs here."""
    global REFRESH_RUN_ID
    current_layer = "silver_gold_refresh_contract"
    try:
        if not REFRESH_RUN_ID:
            raise RuntimeError(
                "silver_gold_refresh_run_id is required after the downstream Lakeflow refresh"
            )
        REFRESH_RUN_ID = required_text(REFRESH_RUN_ID, "silver_gold_refresh_run_id", 512)
        state["silver_gold_refresh_run_id"] = REFRESH_RUN_ID
        state["blocked_layer"] = None
        state["next_action"] = None

        current_layer = "direct_post_refresh_verification"
        direct = verify_direct_erasure(CUSTOMER_ID)
        if not direct["passed"]:
            raise RuntimeError(f"Direct Bronze/fraud verification failed: {direct}")

        current_layer = "silver_gold_refresh_verification"
        materialized_views = verify_silver_gold_refresh(CUSTOMER_ID)
        logical = {
            "passed": direct["passed"] and materialized_views["passed"],
            "direct": direct,
            "silver_gold_materialized_views": materialized_views,
        }
        _record_verification(state, "logical", logical)
        _mark_layer(state, current_layer)
        if not logical["passed"]:
            raise RuntimeError(
                f"Silver/Gold MATERIALIZED VIEW refresh verification failed: {materialized_views}"
            )

        current_layer = "governed_export_rewrite"
        rewrite_governed_export()
        _mark_layer(state, current_layer)

        current_layer = "export_verification"
        export = verify_governed_export(CUSTOMER_ID)
        _record_verification(state, "export", export)
        _mark_layer(state, current_layer)

        current_layer = "fraud_sink_physical_cleanup_and_verification"
        if not CONFIRM_PHYSICAL:
            state["physical_verified"] = False
            state["physical_verification_detail"] = json.dumps(
                {
                    "passed": False,
                    "state": "deferred",
                    "scope": "bronze.fraud_alerts only",
                    "reason": "Quiesced fraud-sink cleanup was not explicitly confirmed.",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            raise RuntimeError(
                "Fraud-sink physical verification is deferred; rerun the same request_id "
                "with confirm_quiesced_physical_cleanup=true"
            )
        _materialize_soft_deletes_and_purge()
        physical = verify_physical_files(CUSTOMER_ID)
        _record_verification(state, "physical", physical)
        _mark_layer(state, current_layer)
        if not physical["passed"]:
            raise RuntimeError(f"Fraud-sink physical file verification failed: {physical}")

        _mark_completed(state)
        return state
    except Exception as exc:
        _mark_failed(state, current_layer, exc)
        raise


def run_prepare() -> dict[str, object]:
    """Prepare suppression and direct Bronze/fraud mutations for a downstream refresh."""
    state = _begin_command()
    if state["status"] == "completed":
        return state
    _execute_direct_layers(state)
    _mark_refresh_required(state)
    return state


def run_execute() -> dict[str, object]:
    """Manual combined mode; still requires proof of an external MV refresh."""
    state = run_prepare()
    if state["status"] == "completed":
        return state
    return _finish_after_refresh(state)


# COMMAND ----------

# %md
# ## Verify-only mode
# The Workflow must refresh Silver and Gold after `mode=prepare` and before this
# task. Verification is read-only for those MATERIALIZED VIEWs.

# COMMAND ----------


def run_verify_only() -> dict[str, object]:
    global ACTOR, TICKET_ID, IDEMPOTENCY_KEY, REQUESTED_BY, REASON, REFRESH_RUN_ID
    existing = _load_command()
    if not existing:
        raise ValueError(f"No durable erasure command found for request_id={REQUEST_ID}")
    if existing["customer_id"] != CUSTOMER_ID:
        raise ValueError(f"request_id={REQUEST_ID} is already bound to a different customer_id")
    if int(existing["internal_slo_seconds"]) != INTERNAL_SLO_SECONDS:
        raise ValueError(
            f"request_id={REQUEST_ID} was created with internal_slo_seconds="
            f"{existing['internal_slo_seconds']}"
        )
    ACTOR = str(existing["actor"])
    TICKET_ID = str(existing["ticket_id"])
    IDEMPOTENCY_KEY = str(existing["idempotency_key"])
    REQUESTED_BY = str(existing["requested_by"])
    REASON = str(existing["reason"])
    REFRESH_RUN_ID = REFRESH_RUN_ID or str(existing["silver_gold_refresh_run_id"] or "")
    return _finish_after_refresh(existing)


# COMMAND ----------

if MODE == "verify_only":
    final_state = run_verify_only()
elif MODE == "prepare":
    final_state = run_prepare()
else:
    final_state = run_execute()

print(f"status={final_state['status']}")
print(f"ticket_id={final_state['ticket_id']}")
print(f"request_id={final_state['request_id']}")
print(f"idempotency_key={final_state['idempotency_key']}")
print(f"requested_by={final_state['requested_by']} reason={final_state['reason']}")
print(f"silver_gold_refresh_run_id={final_state['silver_gold_refresh_run_id']}")
print(f"blocked_layer={final_state['blocked_layer']}")
print(f"next_action={final_state['next_action']}")
print(f"layers={final_state['layers_completed']}")
print(
    "verification="
    f"logical:{final_state['logical_verified']},"
    f"physical:{final_state['physical_verified']},"
    f"export:{final_state['export_verified']}"
)
print(
    f"latency_seconds={final_state['latency_seconds']} "
    f"internal_slo_seconds={final_state['internal_slo_seconds']} "
    f"internal_slo_state={final_state['internal_slo_state']} "
    "(internal SLO; not a statutory claim)"
)
print(f"confirmation_hash={final_state['confirmation_hash']}")

dbutils.jobs.taskValues.set(
    key="erasure_confirmation_hash", value=str(final_state["confirmation_hash"])
)
dbutils.jobs.taskValues.set(key="erasure_status", value=str(final_state["status"]))
dbutils.jobs.taskValues.set(key="erasure_ticket_id", value=str(final_state["ticket_id"]))
dbutils.jobs.taskValues.set(
    key="erasure_idempotency_key", value=str(final_state["idempotency_key"])
)
dbutils.jobs.taskValues.set(
    key="silver_gold_refresh_run_id",
    value=str(final_state["silver_gold_refresh_run_id"] or ""),
)
dbutils.jobs.taskValues.set(
    key="erasure_blocked_layer", value=str(final_state["blocked_layer"] or "")
)
dbutils.notebook.exit(json.dumps(final_state, default=str, sort_keys=True))

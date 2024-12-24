# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # Article 17 stage 1 — suppression and Bronze masking
#
# This DAB-local stage is deliberately limited to the durable suppression
# tombstone, clear-text Bronze masking, restricted Bronze quarantine cleanup,
# and the standalone fraud sink.  It never mutates Silver or Gold: those are
# Lakeflow-owned materialized views and are refreshed by the following stages.
#
# The parent Job supplies the same request identity as the App-facing Job.  A
# retry uses the same request/idempotency key and is safe to repeat.

# COMMAND ----------

import hashlib
import json
import time
from datetime import UTC, datetime, timedelta

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


# Keep manual retries stable even when the caller did not provide an ID.
REQUEST_ID = REQUEST_ID_INPUT or IDEMPOTENCY_INPUT
if not REQUEST_ID:
    seed = f"{CUSTOMER_ID}|{TICKET_ID}|{REQUESTED_BY}|{REASON}"
    REQUEST_ID = f"manual-{hashlib.sha256(seed.encode()).hexdigest()[:24]}"
IDEMPOTENCY_KEY = IDEMPOTENCY_INPUT or REQUEST_ID

STATE_TABLE = "eurostream.governance.erasure_command_state"
AUDIT_TABLE = "eurostream.governance.erasure_audit_log"
SUPPRESSION_TABLE = "eurostream.governance.suppression_registry"
EXPORT_ROOT = "/Volumes/eurostream/lake/exports"
ANONYMIZED = "<anonymized>"

STATE_SCHEMA = StructSchema(
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
        StructField("failed_layer", StringType(), True),
        StructField("failure_message", StringType(), True),
        StructField("blocked_layer", StringType(), True),
        StructField("next_action", StringType(), True),
        StructField("confirmation_hash", StringType(), False),
        StructField("export_root", StringType(), False),
    ]
)


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _load_existing() -> dict[str, object] | None:
    rows = (
        spark.table(STATE_TABLE)
        .where(
            (F.col("command_id") == F.lit(REQUEST_ID))
            | (F.col("idempotency_key") == F.lit(IDEMPOTENCY_KEY))
        )
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise RuntimeError("Multiple durable erasure rows match the request/idempotency key")
    if not rows:
        return None
    existing = rows[0].asDict()
    if str(existing["customer_id"]) != CUSTOMER_ID:
        raise ValueError("request identity is already bound to a different customer_id")
    if str(existing["ticket_id"]) != TICKET_ID:
        raise ValueError("request identity is already bound to a different ticket_id")
    if str(existing["idempotency_key"]) != IDEMPOTENCY_KEY:
        raise ValueError("request identity is already bound to a different idempotency_key")
    if int(existing["internal_slo_seconds"]) != INTERNAL_SLO_SECONDS:
        raise ValueError("the request was created with a different internal_slo_seconds")
    return existing


def _initial_state(existing: dict[str, object] | None) -> dict[str, object]:
    now = _utc_now()
    if existing:
        requested_at = existing["requested_at"]
        started_at = existing.get("started_at") or now
        attempt_count = int(existing.get("attempt_count") or 0) + 1
        layers = (
            [item for item in str(existing.get("layers_completed") or "").split(",") if item]
            if str(existing.get("status")) != "completed"
            else []
        )
    else:
        requested_at = now
        started_at = now
        attempt_count = 1
        layers = []
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
        "silver_gold_refresh_run_id": WORKFLOW_RUN_ID or None,
        "command_type": "article_17_erasure",
        "status": "running",
        "requested_at": requested_at,
        "started_at": started_at,
        "completed_at": None,
        "updated_at": now,
        "attempt_count": attempt_count,
        "internal_slo_seconds": INTERNAL_SLO_SECONDS,
        "internal_slo_state": "pending",
        "slo_deadline_at": requested_at + timedelta(seconds=INTERNAL_SLO_SECONDS),
        "latency_seconds": None,
        # A new attempt must not inherit a green verification bit from an
        # earlier attempt.  The final stage will set each bit only after its
        # corresponding check succeeds.
        "logical_verified": False,
        "physical_verified": False,
        "export_verified": False,
        "logical_verification_detail": None,
        "physical_verification_detail": None,
        "export_verification_detail": None,
        "layers_completed": ",".join(layers),
        "failed_layer": None,
        "failure_message": None,
        "blocked_layer": "silver_gold_refresh_required",
        "next_action": "wait_for_synchronous_silver_gold_refresh",
        "confirmation_hash": _confirmation_hash(REQUEST_ID, CUSTOMER_ID),
        "export_root": EXPORT_ROOT,
    }


def _persist_state(state: dict[str, object]) -> None:
    values = tuple(state[field.name] for field in STATE_SCHEMA.fields)
    (
        spark.createDataFrame([values], schema=STATE_SCHEMA).createOrReplaceTempView(
            "eurostream_erasure_stage_state"
        )
    )
    spark.sql(
        """
        MERGE INTO eurostream.governance.erasure_command_state AS target
        USING eurostream_erasure_stage_state AS source
        ON target.command_id = source.command_id
           OR target.idempotency_key = source.idempotency_key
        WHEN MATCHED THEN UPDATE SET
          request_id = source.request_id,
          ticket_id = source.ticket_id,
          idempotency_key = source.idempotency_key,
          customer_id = source.customer_id,
          actor = source.actor,
          requested_by = source.requested_by,
          reason = source.reason,
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
          failed_layer = source.failed_layer,
          failure_message = source.failure_message,
          blocked_layer = source.blocked_layer,
          next_action = source.next_action,
          confirmation_hash = source.confirmation_hash,
          export_root = source.export_root
        WHEN NOT MATCHED THEN INSERT (
          command_id, request_id, ticket_id, idempotency_key, customer_id,
          actor, requested_by, reason, workflow_run_id,
          silver_gold_refresh_run_id, command_type, status,
          requested_at, started_at, completed_at, updated_at, attempt_count,
          internal_slo_seconds, internal_slo_state, slo_deadline_at,
          latency_seconds, logical_verified, physical_verified, export_verified,
          logical_verification_detail, physical_verification_detail,
          export_verification_detail, layers_completed, failed_layer,
          failure_message, blocked_layer, next_action, confirmation_hash, export_root
        ) VALUES (
          source.command_id, source.request_id, source.ticket_id,
          source.idempotency_key, source.customer_id, source.actor,
          source.requested_by, source.reason, source.workflow_run_id,
          source.silver_gold_refresh_run_id, source.command_type, source.status,
          source.requested_at, source.started_at, source.completed_at, source.updated_at,
          source.attempt_count, source.internal_slo_seconds,
          source.internal_slo_state, source.slo_deadline_at, source.latency_seconds,
          source.logical_verified, source.physical_verified, source.export_verified,
          source.logical_verification_detail, source.physical_verification_detail,
          source.export_verification_detail, source.layers_completed,
          source.failed_layer, source.failure_message, source.blocked_layer,
          source.next_action, source.confirmation_hash, source.export_root
        )
        """
    )
    persisted = (
        spark.table(STATE_TABLE)
        .filter(
            (F.col("command_id") == F.lit(REQUEST_ID))
            | (F.col("idempotency_key") == F.lit(IDEMPOTENCY_KEY))
        )
        .limit(2)
        .collect()
    )
    if len(persisted) != 1:
        raise RuntimeError("Erasure stage state upsert did not produce exactly one durable row")


def _mark_layer(state: dict[str, object], layer: str) -> None:
    layers = [item for item in str(state["layers_completed"]).split(",") if item]
    if layer not in layers:
        layers.append(layer)
    state["layers_completed"] = ",".join(layers)
    state["updated_at"] = _utc_now()
    _persist_state(state)


def _mark_failed(state: dict[str, object], layer: str, exc: BaseException) -> None:
    now = _utc_now()
    state.update(
        {
            "status": "failed",
            "updated_at": now,
            "completed_at": now,
            "failed_layer": layer,
            "failure_message": f"{type(exc).__name__}: {exc}"[:4000],
            "blocked_layer": layer,
            "next_action": "fix_and_resume_same_request",
        }
    )
    _persist_state(state)


def _mirror_audit(state: dict[str, object]) -> None:
    source = (
        spark.table(STATE_TABLE)
        .filter(F.col("command_id") == F.lit(REQUEST_ID))
        .select(
            "request_id",
            "customer_id",
            F.unix_timestamp("requested_at").alias("requested_at"),
            F.unix_timestamp(F.coalesce("completed_at", "updated_at")).alias("completed_at"),
            "layers_completed",
            "status",
            "confirmation_hash",
        )
    )
    source.createOrReplaceTempView("eurostream_erasure_stage_audit")
    spark.sql(
        """
        MERGE INTO eurostream.governance.erasure_audit_log AS target
        USING eurostream_erasure_stage_audit AS source
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


# COMMAND ----------

state = _initial_state(_load_existing())
current_layer = "command_initialization"
try:
    _persist_state(state)
    _mirror_audit(state)

    current_layer = "suppression_registry"
    suppression_source = spark.createDataFrame(
        [(CUSTOMER_ID, time.time())],
        schema=StructSchema(
            [
                StructField("customer_id", StringType(), False),
                StructField("added_at", DoubleType(), False),
            ]
        ),
    )
    suppression_source.createOrReplaceTempView("eurostream_erasure_stage_suppression")
    spark.sql(
        """
        MERGE INTO eurostream.governance.suppression_registry AS target
        USING eurostream_erasure_stage_suppression AS source
        ON target.customer_id = source.customer_id
        WHEN NOT MATCHED THEN INSERT (customer_id, added_at)
        VALUES (source.customer_id, source.added_at)
        """
    )
    _mark_layer(state, current_layer)

    current_layer = "bronze_pii_masking"
    mask_statements = (
        """
        UPDATE eurostream.bronze.orders
        SET email = :marker, iban = :marker
        WHERE customer_id = :customer_id
        """,
        """
        UPDATE eurostream.bronze.orders_files
        SET email = :marker, iban = :marker
        WHERE customer_id = :customer_id
        """,
        """
        UPDATE eurostream.bronze.payments
        SET iban = :marker
        WHERE customer_id = :customer_id
        """,
        """
        UPDATE eurostream.bronze.clicks
        SET ip_address = :marker
        WHERE customer_id = :customer_id
        """,
    )
    for statement in mask_statements:
        spark.sql(statement, args={"marker": ANONYMIZED, "customer_id": CUSTOMER_ID})
    _mark_layer(state, current_layer)

    current_layer = "bronze_ingest_quarantine_delete"
    spark.sql(
        """
        DELETE FROM eurostream.bronze.ingest_quarantine
        WHERE customer_id = :customer_id
           OR instr(coalesce(raw_payload, ''), :customer_id) > 0
        """,
        args={"customer_id": CUSTOMER_ID},
    )
    _mark_layer(state, current_layer)

    # The fraud sink is a standalone Delta table, not a Lakeflow materialized
    # view.  Suppression prevents new alerts; this delete removes current rows.
    current_layer = "fraud_alert_delete"
    spark.sql(
        "DELETE FROM eurostream.bronze.fraud_alerts WHERE customer_id = :customer_id",
        args={"customer_id": CUSTOMER_ID},
    )
    _mark_layer(state, current_layer)
    _mirror_audit(state)

    dbutils.jobs.taskValues.set(key="erasure_stage", value="suppression_mask")
    dbutils.jobs.taskValues.set(key="erasure_request_id", value=REQUEST_ID)
    dbutils.jobs.taskValues.set(key="erasure_idempotency_key", value=IDEMPOTENCY_KEY)
    print(
        json.dumps(
            {
                "stage": "suppression_mask",
                "request_id": REQUEST_ID,
                "customer_id": CUSTOMER_ID,
                "silver_gold_mutated": False,
                "layers_completed": state["layers_completed"],
            },
            sort_keys=True,
        )
    )
    dbutils.notebook.exit("ok")
except Exception as exc:
    try:
        _mark_failed(state, current_layer, exc)
        _mirror_audit(state)
    except Exception as state_exc:
        print(f"failed to persist erasure failure: {state_exc}")
    raise

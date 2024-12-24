# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # Article 17 orchestration failure record
#
# This task is conditionally scheduled by the parent Job when a required stage
# fails.  It records the durable failure state and never mutates Bronze, Silver,
# Gold, fraud, or Volume data.  The parent Job also sends an on-failure email;
# this notebook makes the state transition machine-readable for recovery.

# COMMAND ----------

import hashlib
import json
from datetime import UTC, datetime

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
dbutils.widgets.text("failure_stage", "orchestration_failure")

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
FAILURE_STAGE = dbutils.widgets.get("failure_stage").strip() or "orchestration_failure"

if not CUSTOMER_ID:
    raise ValueError("customer_id is required")
try:
    INTERNAL_SLO_SECONDS = int(dbutils.widgets.get("internal_slo_seconds").strip() or "60")
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


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


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
    raise RuntimeError("Failure handler expected exactly one durable erasure state row")
state = rows[0].asDict()
if str(state["customer_id"]) != CUSTOMER_ID:
    raise ValueError("failure handler identity does not match the durable state")

now = _utc_now()
failure_message = (
    f"Article 17 orchestration failed at {FAILURE_STAGE}; "
    f"see the parent Job task logs and resume with the same request_id"
)
spark.sql(
    """
    UPDATE eurostream.governance.erasure_command_state
    SET status = 'failed',
        updated_at = :updated_at,
        completed_at = :completed_at,
        failed_layer = COALESCE(failed_layer, :failure_stage),
        failure_message = :failure_message,
        blocked_layer = COALESCE(blocked_layer, :failure_stage),
        next_action = :next_action
    WHERE command_id = :command_id
    """,
    args={
        "updated_at": now,
        "completed_at": now,
        "failure_stage": FAILURE_STAGE,
        "failure_message": failure_message,
        "next_action": "inspect_parent_task_and_resume_same_request",
        "command_id": REQUEST_ID,
    },
)

# Keep the legacy audit row useful for the UI without claiming completion.
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
    .createOrReplaceTempView("eurostream_erasure_failure_audit")
)
spark.sql(
    """
    MERGE INTO eurostream.governance.erasure_audit_log AS target
    USING eurostream_erasure_failure_audit AS source
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

dbutils.jobs.taskValues.set(key="erasure_failure_stage", value=FAILURE_STAGE)
dbutils.jobs.taskValues.set(key="erasure_request_id", value=REQUEST_ID)
print(
    json.dumps(
        {
            "status": "failed",
            "request_id": REQUEST_ID,
            "failure_stage": FAILURE_STAGE,
            "data_mutation": False,
        },
        sort_keys=True,
    )
)
dbutils.notebook.exit("failure_recorded")

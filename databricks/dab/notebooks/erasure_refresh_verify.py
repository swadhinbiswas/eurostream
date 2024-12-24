# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # Article 17 refresh barrier
# #
# Runs after the synchronous Silver and Gold pipeline tasks and before export.
# It verifies current Bronze logical state plus every Silver/Gold materialized
# view. It never mutates pipeline-owned Bronze streaming tables or Silver/Gold
# materialized views.
#
# %md
# COMMAND ----------

import json
from datetime import UTC, datetime

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("customer_id", "")
dbutils.widgets.text("ticket_id", "manual")
dbutils.widgets.text("request_id", "")
dbutils.widgets.text("idempotency_key", "")
dbutils.widgets.text("requested_by", "unknown")
dbutils.widgets.text("reason", "GDPR_ARTICLE_17")
dbutils.widgets.text("actor", "databricks_workflow")
dbutils.widgets.text("workflow_run_id", "")
dbutils.widgets.text("internal_slo_seconds", "60")

CUSTOMER_ID = dbutils.widgets.get("customer_id").strip()
TICKET_ID = dbutils.widgets.get("ticket_id").strip() or "manual"
REQUEST_ID = dbutils.widgets.get("request_id").strip()
IDEMPOTENCY_KEY = dbutils.widgets.get("idempotency_key").strip()
REQUESTED_BY = dbutils.widgets.get("requested_by").strip() or "unknown"
REASON = dbutils.widgets.get("reason").strip() or "GDPR_ARTICLE_17"
ACTOR = dbutils.widgets.get("actor").strip() or "databricks_workflow"
WORKFLOW_RUN_ID = dbutils.widgets.get("workflow_run_id").strip()
INTERNAL_SLO_SECONDS = int(dbutils.widgets.get("internal_slo_seconds").strip() or "60")

if not CUSTOMER_ID or not REQUEST_ID or not IDEMPOTENCY_KEY or not WORKFLOW_RUN_ID:
    raise ValueError("customer_id, request_id, idempotency_key, and workflow_run_id are required")
if INTERNAL_SLO_SECONDS <= 0:
    raise ValueError("internal_slo_seconds must be greater than zero")

STATE_TABLE = "eurostream.governance.erasure_command_state"
ANONYMIZED = "<anonymized>"
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
        raise ValueError("Durable state belongs to a different customer")
    if str(state["ticket_id"]) != TICKET_ID:
        raise ValueError("Durable state belongs to a different ticket")
    if str(state["idempotency_key"]) != IDEMPOTENCY_KEY:
        raise ValueError("Durable state belongs to a different idempotency key")
    if state.get("silver_gold_refresh_run_id") != WORKFLOW_RUN_ID:
        raise ValueError("Refresh evidence is not bound to this orchestration run")
    if int(state["internal_slo_seconds"]) != INTERNAL_SLO_SECONDS:
        raise ValueError("Durable state has a different internal SLO")
    return state


def _count(table: str) -> int:
    return spark.table(table).filter(F.col("customer_id") == F.lit(CUSTOMER_ID)).count()


def _verify_bronze() -> dict[str, object]:
    if _count("eurostream.governance.suppression_registry") < 1:
        raise RuntimeError("Suppression tombstone is missing")

    details: dict[str, object] = {}
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
    details["eurostream.bronze.ingest_quarantine"] = quarantine_count
    details["eurostream.bronze.fraud_alerts"] = fraud_count
    if quarantine_count or fraud_count:
        raise RuntimeError("Bronze quarantine or fraud rows remain")
    return details


def _verify_views() -> dict[str, int]:
    counts = {table: _count(table) for table in SILVER_GOLD_TABLES}
    remaining = {table: count for table, count in counts.items() if count}
    if remaining:
        raise RuntimeError(f"Materialized views still contain target rows: {remaining}")
    return counts


# COMMAND ----------

state = _load_state()
bronze_details = _verify_bronze()
view_details = _verify_views()
logical_detail = json.dumps(
    {"passed": True, "bronze": bronze_details, "materialized_views": view_details},
    sort_keys=True,
)
layers = [item for item in str(state.get("layers_completed") or "").split(",") if item]
for layer in ("silver_pipeline_refresh", "gold_pipeline_refresh", "refresh_barrier"):
    if layer not in layers:
        layers.append(layer)
now = _utc_now()
spark.sql(
    """
    UPDATE eurostream.governance.erasure_command_state
    SET updated_at = :updated_at,
        logical_verified = TRUE,
        logical_verification_detail = :logical_detail,
        layers_completed = :layers,
        blocked_layer = NULL,
        next_action = 'run_governed_export'
    WHERE command_id = :command_id
    """,
    args={
        "updated_at": now,
        "logical_detail": logical_detail,
        "layers": ",".join(layers),
        "command_id": REQUEST_ID,
    },
)
persisted = spark.table(STATE_TABLE).filter(F.col("command_id") == F.lit(REQUEST_ID)).first()
if (
    persisted is None
    or not bool(persisted["logical_verified"])
    or persisted["blocked_layer"] is not None
    or persisted["next_action"] != "run_governed_export"
):
    raise RuntimeError("Refresh barrier postcondition failed")

dbutils.jobs.taskValues.set(key="erasure_stage", value="refresh_barrier")
dbutils.jobs.taskValues.set(key="erasure_request_id", value=REQUEST_ID)
print(
    json.dumps(
        {
            "stage": "refresh_barrier",
            "request_id": REQUEST_ID,
            "customer_id": CUSTOMER_ID,
            "bronze": bronze_details,
            "materialized_views": view_details,
            "export_started": False,
        },
        sort_keys=True,
    )
)
dbutils.notebook.exit("ok")

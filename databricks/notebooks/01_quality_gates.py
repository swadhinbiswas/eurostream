# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # Data Quality Gates
# #
# Runs the six governance assertions against Unity Catalog and records every
# result to `governance.quality_results`. PII checks scan every row (never a
# sample), and an empty required table is a failed gate.
# #
# | # | Check | Source |
# |---|---|---|
# | 1 | `gold.customer_360.customer_id_unique` | uniqueness |
# | 2 | `gold.order_facts.order_id_unique` | uniqueness |
# | 3 | `silver.customers.email_hash_not_clear` | PII not in clear text |
# | 4 | `silver.customers.iban_hash_not_clear` | PII not in clear text |
# | 5 | `consent_gating` | consent mirror integrity |
# | 6 | `referential_integrity` | FK → dimension |
# #
# An assertion that cannot be evaluated is recorded as failed. The notebook
# still fails the Workflows task when `fail_on_error=true`. Gold is read through
# its Lakeflow MATERIALIZED VIEW; this notebook never mutates Silver or Gold.

# COMMAND ----------

import uuid

dbutils.widgets.text("fail_on_error", "true")
dbutils.widgets.text("run_id", "")

FAIL_ON_ERROR = dbutils.widgets.get("fail_on_error").strip().lower() == "true"
RUN_ID = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())

# COMMAND ----------

from datetime import UTC, datetime

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

# Job notebooks must not depend on the workspace's default catalog. All short
# table names below are schema.table names under the governed showcase catalog.
spark.sql("USE CATALOG eurostream")

# COMMAND ----------

HASH_PATTERN = r"^[0-9a-f]{64}$"


def check_uniqueness(table: str, column: str) -> dict[str, object]:
    """Check every group for duplicate values and reject an empty table."""
    grouped = spark.table(table).groupBy(F.col(column)).agg(F.count("*").alias("group_count"))
    summary = grouped.agg(
        F.coalesce(F.sum("group_count"), F.lit(0)).alias("row_count"),
        F.count(F.when(F.col("group_count") > 1, F.lit(1))).alias("duplicate_values"),
        F.count(F.when(F.col(column).isNull(), F.lit(1))).alias("null_values"),
    ).first()
    row_count = int(summary["row_count"])
    duplicate_values = int(summary["duplicate_values"])
    null_values = int(summary["null_values"])
    passed = row_count > 0 and duplicate_values == 0 and null_values == 0
    if row_count == 0:
        detail = "required table is empty"
    elif null_values:
        detail = f"{null_values} null key values"
    elif duplicate_values:
        detail = f"{duplicate_values} duplicate values"
    else:
        detail = ""
    return {
        "check_name": f"{table}.{column}_unique",
        "passed": passed,
        "detail": detail,
    }


# COMMAND ----------


def check_pii_not_clear(table: str, column: str) -> dict[str, object]:
    """Require every value to be one salted SHA-256 digest.

    This is a full-table predicate pushdown, not a `LIMIT` sample. Nulls and
    malformed/clear-text values are violations. An empty table also fails so a
    broken upstream pipeline cannot produce a green governance result.
    """
    value = F.col(column)
    invalid_value = value.isNull() | ~F.rlike(F.lower(F.trim(value)), HASH_PATTERN)
    summary = (
        spark.table(table)
        .agg(
            F.count("*").alias("row_count"),
            F.count(F.when(invalid_value, F.lit(1))).alias("invalid_values"),
        )
        .first()
    )
    row_count = int(summary["row_count"])
    invalid_values = int(summary["invalid_values"])
    passed = row_count > 0 and invalid_values == 0
    if row_count == 0:
        detail = "required table is empty"
    elif invalid_values:
        detail = f"{invalid_values} null, clear-text, or non-hashed PII values"
    else:
        detail = ""
    return {
        "check_name": f"{table}.{column}_not_clear",
        "passed": passed,
        "detail": detail,
    }


# COMMAND ----------


def check_consent_gating() -> dict[str, object]:
    """The two marketing flags must be null-safe equal on every profile."""
    customers = spark.table("gold.customer_360")
    row_count = customers.count()
    mismatches = customers.filter(
        ~F.col("consents_marketing").eqNullSafe(F.col("marketing_consent"))
    ).count()
    null_consent_values = customers.filter(
        F.col("consents_marketing").isNull() | F.col("marketing_consent").isNull()
    ).count()
    passed = row_count > 0 and mismatches == 0 and null_consent_values == 0
    if row_count == 0:
        detail = "required table is empty"
    elif null_consent_values:
        detail = f"{null_consent_values} customers with null consent flags"
    elif mismatches:
        detail = f"{mismatches} customers where consent flags differ"
    else:
        detail = ""
    return {
        "check_name": "consent_gating",
        "passed": passed,
        "detail": detail,
    }


# COMMAND ----------


def check_referential_integrity() -> dict[str, object]:
    """Every fact must resolve to a customer profile; both inputs must be non-empty."""
    facts = spark.table("gold.order_facts")
    customers = spark.table("gold.customer_360").select(
        F.col("customer_id").alias("resolved_customer_id")
    )
    fact_count = facts.count()
    customer_count = spark.table("gold.customer_360").count()
    null_fact_keys = facts.filter(F.col("customer_id").isNull()).count()
    null_customer_keys = (
        spark.table("gold.customer_360").filter(F.col("customer_id").isNull()).count()
    )
    orphans = (
        facts.select(F.col("customer_id"))
        .join(customers, on="customer_id", how="left_anti")
        .limit(1)
        .count()
    )
    passed = (
        fact_count > 0
        and customer_count > 0
        and null_fact_keys == 0
        and null_customer_keys == 0
        and orphans == 0
    )
    if fact_count == 0 or customer_count == 0:
        detail = "gold.order_facts and gold.customer_360 must both be non-empty"
    elif null_fact_keys or null_customer_keys:
        detail = "customer_id must be non-null in both Gold inputs"
    elif orphans:
        detail = f"{orphans} orphan customer references"
    else:
        detail = ""
    return {
        "check_name": "gold.order_facts.customer_id_references_gold.customer_360",
        "passed": passed,
        "detail": detail,
    }


# COMMAND ----------


def run_check(check_name: str, check) -> dict[str, object]:
    """Convert an evaluation error into a durable failed gate, never a pass."""
    try:
        return check()
    except Exception as exc:  # recorded below and re-raised by the gate
        return {
            "check_name": check_name,
            "passed": False,
            "detail": f"evaluation failed: {type(exc).__name__}: {exc}",
        }


# %md
# ## Run all six checks

# COMMAND ----------

results = [
    run_check(
        "gold.customer_360.customer_id_unique",
        lambda: check_uniqueness("gold.customer_360", "customer_id"),
    ),
    run_check(
        "gold.order_facts.order_id_unique",
        lambda: check_uniqueness("gold.order_facts", "order_id"),
    ),
    run_check(
        "silver.customers.email_hash_not_clear",
        lambda: check_pii_not_clear("silver.customers", "email_hash"),
    ),
    run_check(
        "silver.customers.iban_hash_not_clear",
        lambda: check_pii_not_clear("silver.customers", "iban_hash"),
    ),
    run_check("consent_gating", check_consent_gating),
    run_check(
        "gold.order_facts.customer_id_references_gold.customer_360",
        check_referential_integrity,
    ),
]

all_passed = all(bool(result["passed"]) for result in results)

# COMMAND ----------

# %md
# ## Persist results → `governance.quality_results`
# A write failure is intentionally fatal: no result means no governance pass.

# COMMAND ----------

quality_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("detail", StringType(), True),
        StructField("checked_at", TimestampType(), False),
    ]
)

checked_at = datetime.now(UTC).replace(tzinfo=None)
quality_rows = [
    (RUN_ID, result["check_name"], bool(result["passed"]), result["detail"], checked_at)
    for result in results
]

(
    spark.createDataFrame(quality_rows, schema=quality_schema)
    .write.mode("append")
    .format("delta")
    .saveAsTable("governance.quality_results")
)

# COMMAND ----------

for result in results:
    status = "PASS" if result["passed"] else "FAIL"
    suffix = f" — {result['detail']}" if result["detail"] else ""
    print(f"[{status}] {result['check_name']}{suffix}")

print(f"\nrun_id={RUN_ID} all_passed={all_passed}")
dbutils.jobs.taskValues.set(key="quality_passed", value=str(all_passed).lower())

if not all_passed and FAIL_ON_ERROR:
    failed_checks = ", ".join(
        str(result["check_name"]) for result in results if not result["passed"]
    )
    raise ValueError(f"Data quality gate failed: {failed_checks}")

# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # Databricks Direct-Erasure Preparation Benchmark
# #
# Measures the synchronous portion of the Article 17 command that this
# Workflow owns: suppression is committed first, then Bronze PII is masked,
# restricted Bronze quarantine is deleted, and the job-owned fraud sink is
# cleared. Every iteration cleans up in `finally`, including partial failures.
# %
# Silver and Gold are Lakeflow-owned MATERIALIZED VIEWs. This benchmark does not
# seed, mutate, vacuum, or claim erasure for those relations. The production
# Workflow must synchronously refresh both Lakeflow pipelines after this direct
# preparation and before `02_article17_erasure.py` verifies them and exports.
# %
# The benchmark therefore does not measure MV refresh latency, Volume export, or
# physical-file cleanup. The 60-second default is an **internal engineering
# SLO**, not a statutory deadline.

# COMMAND ----------

import uuid

dbutils.widgets.text("iterations", "50")
dbutils.widgets.text("internal_slo_seconds", "")
dbutils.widgets.text("sla_seconds", "60")  # Workflow alias; value is an internal SLO
dbutils.widgets.text("run_id", "")

ITERATIONS_RAW = dbutils.widgets.get("iterations")
SLO_RAW = (
    dbutils.widgets.get("internal_slo_seconds").strip()
    or dbutils.widgets.get("sla_seconds").strip()
)
RUN_ID = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())

# COMMAND ----------

import hashlib
import statistics
import time
from datetime import UTC, datetime

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

# Keep the benchmark independent of the workspace default catalog.
spark.sql("USE CATALOG eurostream")

# COMMAND ----------


def positive_int(raw_value: str, field_name: str) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return value


def percentile(sorted_values: list[float], quantile: float) -> float:
    """Linear-interpolated percentile for a non-empty sorted sample."""
    if not sorted_values:
        raise ValueError("percentile requires at least one value")
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be between zero and one")
    position = (len(sorted_values) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


ITERATIONS = positive_int(ITERATIONS_RAW, "iterations")
INTERNAL_SLO_SECONDS = positive_int(SLO_RAW, "internal_slo_seconds")
ANONYMIZED = "<anonymized>"  # noqa: S105 - fixed marker, not a secret

# COMMAND ----------

STRING = StringType()
DOUBLE = DoubleType()
BOOLEAN = BooleanType()
TIMESTAMP = TimestampType()

BRONZE_ORDERS_SCHEMA = StructSchema(
    [
        StructField("event_id", STRING, False),
        StructField("schema_version", IntegerType(), True),
        StructField("event_type", STRING, False),
        StructField("occurred_at", DOUBLE, False),
        StructField("order_id", STRING, False),
        StructField("customer_id", STRING, False),
        StructField("email", STRING, False),
        StructField("iban", STRING, False),
        StructField("country", STRING, False),
        StructField("amount_eur", DOUBLE, False),
        StructField("marketing_consent", BOOLEAN, False),
        StructField("currency", STRING, True),
        StructField("_occurred_at", TIMESTAMP, False),
        StructField("_ingested_at", TIMESTAMP, False),
        StructField("_source", STRING, False),
        StructField("_kafka_ts", TIMESTAMP, True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_file_path", STRING, True),
        StructField("_file_modification_time", TIMESTAMP, True),
    ]
)
BRONZE_PAYMENTS_SCHEMA = StructSchema(
    [
        StructField("event_id", STRING, False),
        StructField("schema_version", IntegerType(), True),
        StructField("event_type", STRING, False),
        StructField("occurred_at", DOUBLE, False),
        StructField("payment_id", STRING, False),
        StructField("order_id", STRING, False),
        StructField("customer_id", STRING, False),
        StructField("iban", STRING, False),
        StructField("amount_eur", DOUBLE, False),
        StructField("country", STRING, False),
        StructField("merchant_country", STRING, False),
        StructField("status", STRING, True),
        StructField("_occurred_at", TIMESTAMP, False),
        StructField("_ingested_at", TIMESTAMP, False),
        StructField("_source", STRING, False),
        StructField("_kafka_ts", TIMESTAMP, True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_file_path", STRING, True),
        StructField("_file_modification_time", TIMESTAMP, True),
    ]
)
BRONZE_CLICKS_SCHEMA = StructSchema(
    [
        StructField("event_id", STRING, False),
        StructField("schema_version", IntegerType(), True),
        StructField("event_type", STRING, False),
        StructField("occurred_at", DOUBLE, False),
        StructField("click_id", STRING, False),
        StructField("customer_id", STRING, False),
        StructField("session_id", STRING, False),
        StructField("ip_address", STRING, False),
        StructField("page", STRING, False),
        StructField("country", STRING, False),
        StructField("_occurred_at", TIMESTAMP, False),
        StructField("_ingested_at", TIMESTAMP, False),
        StructField("_source", STRING, False),
        StructField("_kafka_ts", TIMESTAMP, True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_file_path", STRING, True),
        StructField("_file_modification_time", TIMESTAMP, True),
    ]
)
BRONZE_INGEST_QUARANTINE_SCHEMA = StructSchema(
    [
        StructField("quarantine_id", STRING, False),
        StructField("source_name", STRING, False),
        StructField("source_partition", IntegerType(), True),
        StructField("source_offset", LongType(), True),
        StructField("file_path", STRING, True),
        StructField("event_id", STRING, True),
        StructField("customer_id", STRING, True),
        StructField("raw_payload", STRING, True),
        StructField("quarantine_reason", STRING, False),
        StructField("suppression_status", STRING, False),
        StructField("occurred_at", TIMESTAMP, True),
        StructField("ingested_at", TIMESTAMP, False),
    ]
)
FRAUD_ALERTS_SCHEMA = StructSchema(
    [
        StructField("alert_id", STRING, False),
        StructField("customer_id", STRING, False),
        StructField("rule", STRING, False),
        StructField("score", DOUBLE, False),
        StructField("detail", STRING, True),
        StructField("alerted_at", TIMESTAMP, False),
        StructField("window_start", TIMESTAMP, True),
        StructField("window_end", TIMESTAMP, True),
        StructField("_created_at", TIMESTAMP, False),
    ]
)

# COMMAND ----------


def _timestamp(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=UTC).replace(tzinfo=None)


def _merge_seed(table: str, merge_column: str, rows: list[tuple], schema: StructSchema) -> None:
    source = spark.createDataFrame(rows, schema=schema)
    (
        DeltaTable.forName(spark, table)
        .alias("target")
        .merge(source, f"target.{merge_column} = source.{merge_column}")
        .whenNotMatched()
        .insertAll()
        .execute()
    )


def seed_synthetic_customer(customer_id: str, iteration: int) -> None:
    """Seed only direct mutation targets; Silver/Gold remain pipeline-owned."""
    prefix = f"{RUN_ID}-{iteration}"
    order_id = f"bench-order-{prefix}"
    payment_id = f"bench-payment-{prefix}"
    click_id = f"bench-click-{prefix}"
    occurred_at = time.time()
    occurred_timestamp = _timestamp(occurred_at)
    email = "benchmark@example.invalid"
    iban = "DE00BENCHMARKONLY"
    quarantine_id = hashlib.sha256(f"ingest|{prefix}|{customer_id}".encode()).hexdigest()

    _merge_seed(
        "bronze.orders",
        "event_id",
        [
            (
                f"bench-order-{prefix}",
                1,
                "benchmark_order",
                occurred_at,
                order_id,
                customer_id,
                email,
                iban,
                "DE",
                42.0,
                True,
                "EUR",
                occurred_timestamp,
                occurred_timestamp,
                "databricks_benchmark",
                None,
                None,
                None,
                None,
                None,
            )
        ],
        BRONZE_ORDERS_SCHEMA,
    )
    _merge_seed(
        "bronze.orders_files",
        "event_id",
        [
            (
                f"bench-order-file-{prefix}",
                1,
                "benchmark_order",
                occurred_at,
                order_id,
                customer_id,
                email,
                iban,
                "DE",
                42.0,
                True,
                "EUR",
                occurred_timestamp,
                occurred_timestamp,
                "databricks_benchmark_file",
                None,
                None,
                None,
                None,
                None,
            )
        ],
        BRONZE_ORDERS_SCHEMA,
    )
    _merge_seed(
        "bronze.payments",
        "event_id",
        [
            (
                f"bench-payment-{prefix}",
                1,
                "benchmark_payment",
                occurred_at,
                payment_id,
                order_id,
                customer_id,
                iban,
                42.0,
                "DE",
                "DE",
                "authorized",
                occurred_timestamp,
                occurred_timestamp,
                "databricks_benchmark",
                None,
                None,
                None,
                None,
                None,
            )
        ],
        BRONZE_PAYMENTS_SCHEMA,
    )
    _merge_seed(
        "bronze.clicks",
        "event_id",
        [
            (
                f"bench-click-{prefix}",
                1,
                "benchmark_click",
                occurred_at,
                click_id,
                customer_id,
                f"bench-session-{prefix}",
                "192.0.2.1",
                "/benchmark",
                "DE",
                occurred_timestamp,
                occurred_timestamp,
                "databricks_benchmark",
                None,
                None,
                None,
                None,
                None,
            )
        ],
        BRONZE_CLICKS_SCHEMA,
    )
    _merge_seed(
        "bronze.ingest_quarantine",
        "quarantine_id",
        [
            (
                quarantine_id,
                "databricks_benchmark",
                0,
                iteration,
                f"/benchmark/{prefix}.json",
                f"bench-malformed-{prefix}",
                customer_id,
                f'{{"customer_id":"{customer_id}","email":"{email}","iban":"{iban}"}}',
                "BENCHMARK_MALFORMED_EVENT",
                "NOT_SUPPRESSED",
                occurred_timestamp,
                occurred_timestamp,
            )
        ],
        BRONZE_INGEST_QUARANTINE_SCHEMA,
    )
    _merge_seed(
        "bronze.fraud_alerts",
        "alert_id",
        [
            (
                f"bench-alert-{prefix}",
                customer_id,
                "VELOCITY",
                1.0,
                "benchmark",
                occurred_timestamp,
                occurred_timestamp,
                occurred_timestamp,
                occurred_timestamp,
            )
        ],
        FRAUD_ALERTS_SCHEMA,
    )


# COMMAND ----------


def _customer_count(table: str, customer_id: str) -> int:
    return spark.table(table).filter(F.col("customer_id") == F.lit(customer_id)).count()


def _suppress(customer_id: str) -> None:
    source = spark.createDataFrame(
        [(customer_id, time.time())],
        schema=StructSchema(
            [
                StructField("customer_id", STRING, False),
                StructField("added_at", DOUBLE, False),
            ]
        ),
    )
    source.createOrReplaceTempView("eurostream_benchmark_suppression")
    spark.sql(
        """
        MERGE INTO governance.suppression_registry AS target
        USING eurostream_benchmark_suppression AS source
        ON target.customer_id = source.customer_id
        WHEN NOT MATCHED THEN INSERT (customer_id, added_at)
        VALUES (source.customer_id, source.added_at)
        """
    )


def verify_direct_customer(customer_id: str, suppression_required: bool) -> None:
    suppression_count = _customer_count("governance.suppression_registry", customer_id)
    if suppression_required and suppression_count != 1:
        raise RuntimeError("Benchmark suppression verification failed")
    if not suppression_required and suppression_count != 0:
        raise RuntimeError("Benchmark cleanup left a suppression row")

    if _customer_count("bronze.fraud_alerts", customer_id):
        raise RuntimeError("Benchmark cascade left fraud alerts")
    quarantine_remaining = (
        spark.table("bronze.ingest_quarantine")
        .filter(
            (F.col("customer_id") == F.lit(customer_id))
            | (F.instr(F.coalesce(F.col("raw_payload"), F.lit("")), F.lit(customer_id)) > 0)
        )
        .limit(1)
        .count()
    )
    if quarantine_remaining:
        raise RuntimeError("Benchmark cascade left restricted quarantine data")

    bronze_rules = {
        "bronze.orders": ("email", "iban"),
        "bronze.orders_files": ("email", "iban"),
        "bronze.payments": ("iban",),
        "bronze.clicks": ("ip_address",),
    }
    for table, columns in bronze_rules.items():
        target = spark.table(table).filter(F.col("customer_id") == F.lit(customer_id))
        if suppression_required:
            invalid = target.filter(
                F.reduce(
                    [
                        F.col(column).isNull() | (F.col(column) != F.lit(ANONYMIZED))
                        for column in columns
                    ],
                    F.or_,
                )
            ).limit(1)
        else:
            invalid = target.limit(1)
        if invalid.count():
            raise RuntimeError(f"Benchmark cleanup left target rows in {table}")


def run_direct_cascade(customer_id: str) -> float:
    started = time.perf_counter()
    _suppress(customer_id)
    args = {"customer_id": customer_id, "marker": ANONYMIZED}
    spark.sql(
        "UPDATE bronze.orders SET email = :marker, iban = :marker WHERE customer_id = :customer_id",
        args=args,
    )
    spark.sql(
        "UPDATE bronze.orders_files SET email = :marker, iban = :marker "
        "WHERE customer_id = :customer_id",
        args=args,
    )
    spark.sql(
        "UPDATE bronze.payments SET iban = :marker WHERE customer_id = :customer_id",
        args=args,
    )
    spark.sql(
        "UPDATE bronze.clicks SET ip_address = :marker WHERE customer_id = :customer_id",
        args=args,
    )
    spark.sql(
        """
        DELETE FROM bronze.ingest_quarantine
        WHERE customer_id = :customer_id
           OR instr(coalesce(raw_payload, ''), :customer_id) > 0
        """,
        args={"customer_id": customer_id},
    )
    spark.sql(
        "DELETE FROM bronze.fraud_alerts WHERE customer_id = :customer_id",
        args={"customer_id": customer_id},
    )
    verify_direct_customer(customer_id, suppression_required=True)
    return time.perf_counter() - started


def cleanup_iteration(customer_id: str) -> None:
    for statement in (
        "DELETE FROM bronze.orders WHERE customer_id = :customer_id",
        "DELETE FROM bronze.orders_files WHERE customer_id = :customer_id",
        "DELETE FROM bronze.payments WHERE customer_id = :customer_id",
        "DELETE FROM bronze.clicks WHERE customer_id = :customer_id",
        """
        DELETE FROM bronze.ingest_quarantine
        WHERE customer_id = :customer_id
           OR instr(coalesce(raw_payload, ''), :customer_id) > 0
        """.strip(),
        "DELETE FROM bronze.fraud_alerts WHERE customer_id = :customer_id",
    ):
        spark.sql(statement, args={"customer_id": customer_id})
    spark.sql(
        "DELETE FROM governance.suppression_registry WHERE customer_id = :customer_id",
        args={"customer_id": customer_id},
    )
    verify_direct_customer(customer_id, suppression_required=False)


# COMMAND ----------

latencies: list[float] = []
run_token = hashlib.sha256(RUN_ID.encode()).hexdigest()[:12]

for iteration in range(ITERATIONS):
    customer_id = f"bench_{run_token}_{iteration:04d}"
    try:
        seed_synthetic_customer(customer_id, iteration)
        latencies.append(run_direct_cascade(customer_id))
    finally:
        cleanup_iteration(customer_id)

# COMMAND ----------

ordered = sorted(latencies)
mean_seconds = statistics.mean(ordered)
p50_seconds = percentile(ordered, 0.50)
p95_seconds = percentile(ordered, 0.95)
min_seconds = ordered[0]
max_seconds = ordered[-1]
passed = p95_seconds <= INTERNAL_SLO_SECONDS

print("=" * 76)
print(" EUROSTREAM DATABRICKS DIRECT ERASURE PREPARATION BENCHMARK")
print("=" * 76)
print(f" Run ID             : {RUN_ID}")
print(f" Iterations Tested  : {len(ordered)}")
print(f" Mean Latency       : {mean_seconds * 1000:.2f} ms")
print(f" Median (p50)       : {p50_seconds * 1000:.2f} ms")
print(f" p95 Latency        : {p95_seconds * 1000:.2f} ms")
print(f" Min / Max          : {min_seconds * 1000:.2f} / {max_seconds * 1000:.2f} ms")
print(f" Internal SLO       : {INTERNAL_SLO_SECONDS * 1000} ms (Passed: {passed})")
print(" SLO Basis          : Internal engineering target; not a statutory claim")
print(" MV/Export Scope    : Excluded; Workflow refresh and verification are required")
print(" Physical Scope     : Not benchmarked or claimed")
print("=" * 76)

dbutils.jobs.taskValues.set(key="benchmark_p95_ms", value=str(int(p95_seconds * 1000)))
dbutils.jobs.taskValues.set(key="benchmark_internal_slo_passed", value=str(passed).lower())
dbutils.jobs.taskValues.set(key="benchmark_run_id", value=RUN_ID)

if not passed:
    raise ValueError(
        f"Internal direct-erasure SLO breached: p95={p95_seconds:.3f}s > {INTERNAL_SLO_SECONDS}s"
    )

dbutils.notebook.exit(f"run_id={RUN_ID} p95={p95_seconds:.4f}s internal_slo_passed={passed}")

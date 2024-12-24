# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # Real-Time Fraud Scoring (Structured Streaming)
#
# Databricks-native Structured Streaming job. It consumes the governed Kafka
# payment stream, scores it, and idempotently writes only to the Delta/Unity
# Catalog table `eurostream.bronze.fraud_alerts`. All runtime services are
# Databricks-native.
#
# Operational prerequisite: the Lakeflow Bronze pipeline must be active so its
# exclusive malformed-input quarantine is the system of record. This notebook
# is a downstream scorer, not a second quarantine owner. Set bootstrap_only=true
# for a one-shot, non-streaming sink-schema bootstrap before starting the job.
#
# ## Safety and idempotency contract
# 1. Parse failures and contract failures are excluded before any rule or state
#    update. The Lakeflow Bronze pipeline owns the exclusive malformed-row
#    quarantine (`eurostream.bronze.ingest_quarantine`); this job never creates
#    a second, unsuppressed raw sink.
# 2. A non-streaming suppression snapshot is read when the query is built,
#    and the sink callback reloads a fresh snapshot for every micro-batch before
#    MERGE. This avoids a stream-stream suppression join while closing the
#    erasure race at the write boundary.
# 3. There is one keyed stateful pass. It owns bounded event-id deduplication,
#    the five-minute velocity timestamp deque, the last 200 z-score amounts,
#    and emits velocity/z-score/geo candidates together.
# 4. `alert_id` is a deterministic SHA-256 key. `foreachBatch` MERGEs on that
#    key, so a retried micro-batch or a Kafka replay cannot append a duplicate.
# 5. All keyed state is bounded and uses a Spark processing-time timeout.
#    Suppression prevents persistence; Spark cannot selectively delete a key
#    from another running query, so the timeout is the delayed eviction path.

# COMMAND ----------

dbutils.widgets.text("kafka_topic", "payments")
dbutils.widgets.text("checkpoint", "/Volumes/eurostream/bronze/_checkpoints/fraud_stream")
dbutils.widgets.text("trigger_seconds", "10")
dbutils.widgets.text("suppression_table", "eurostream.governance.suppression_registry")
dbutils.widgets.text("alert_table", "eurostream.bronze.fraud_alerts")
dbutils.widgets.text("zscore_state_ttl_seconds", "86400")
dbutils.widgets.text("bootstrap_only", "false")

TOPIC = dbutils.widgets.get("kafka_topic")
CHECKPOINT = dbutils.widgets.get("checkpoint")
TRIGGER_SECONDS = int(dbutils.widgets.get("trigger_seconds"))
SUPPRESSION_TABLE = dbutils.widgets.get("suppression_table")
ALERT_TABLE = dbutils.widgets.get("alert_table")
ZSCORE_STATE_TTL_S = int(dbutils.widgets.get("zscore_state_ttl_seconds"))
BOOTSTRAP_ONLY = dbutils.widgets.get("bootstrap_only").strip().lower() == "true"
if TRIGGER_SECONDS < 1 or ZSCORE_STATE_TTL_S < 1:
    raise ValueError("trigger_seconds and zscore_state_ttl_seconds must be positive")

TRIGGER = f"{TRIGGER_SECONDS} seconds"

VELOCITY_THRESHOLD = 5
VELOCITY_WINDOW_S = 300
VELOCITY_MAX_EVENTS = 4096
VELOCITY_WINDOW_MEMORY = 4
ZSCORE_THRESHOLD = 3.0
ZSCORE_STATE_N = 200
ZSCORE_MIN_BASELINE = 3
# These are deliberate memory bounds, not an unbounded replay-history claim;
# deterministic alert MERGE remains the final duplicate guard.
EVENT_ID_STATE_N = 2048

# COMMAND ----------

import math
import re
import statistics
import time
from collections import deque
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ALERT_SINK_SCHEMA = StructType(
    [
        StructField("alert_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("rule", StringType(), False),
        StructField("score", DoubleType(), False),
        StructField("detail", StringType(), True),
        StructField("alerted_at", TimestampType(), False),
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("_created_at", TimestampType(), False),
    ]
)
_ALERT_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){1,2}$")


def _validated_alert_table() -> str:
    if not _ALERT_TABLE_RE.fullmatch(ALERT_TABLE):
        raise ValueError(
            "alert_table must be a simple catalog.schema.table or schema.table identifier"
        )
    return ALERT_TABLE


def ensure_alert_sink() -> None:
    """Idempotently create and validate the standalone job-owned Delta sink."""
    table = _validated_alert_table()
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          alert_id          STRING    NOT NULL,
          customer_id       STRING    NOT NULL,
          rule              STRING    NOT NULL CHECK (rule IN ('VELOCITY','AMOUNT_ZSCORE','GEO_MISMATCH')),
          score             DOUBLE    NOT NULL,
          detail            STRING,
          alerted_at        TIMESTAMP NOT NULL,
          window_start      TIMESTAMP,
          window_end        TIMESTAMP,
          _created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
        )
        USING DELTA
        PARTITIONED BY (rule)
        COMMENT 'Idempotent real-time fraud alerts (from 03_fraud_streaming).'
        TBLPROPERTIES (
          'quality' = 'bronze',
          'pii_classification' = 'confidential',
          'idempotency_key' = 'alert_id'
        )
        """
    )

    observed = [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in spark.table(table).schema.fields
    ]
    expected = [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in ALERT_SINK_SCHEMA.fields
    ]
    if observed != expected:
        raise RuntimeError(
            f"{table} exists with an incompatible schema; refusing to write fraud alerts"
        )

    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    if detail is None or list(detail["partitionColumns"] or []) != ["rule"]:
        raise RuntimeError(f"{table} is not partitioned by rule; refusing to start the sink")


# Bootstrap before any Kafka secret access so the one-shot schema path is
# independent of the streaming source configuration.
ensure_alert_sink()
if BOOTSTRAP_ONLY:
    dbutils.notebook.exit("fraud_alerts sink bootstrap complete")

# %md
# ## Read and validate Kafka payments
#
# The same event contract is used by the Lakeflow Bronze source. We retain the
# original value only in the transient streaming plan; it is never selected
# into the fraud-alert sink or an exported table.

scope = spark.conf.get("eurostream.kafka.secret_scope", "eurostream")
jaas = (
    "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
    f"username='{dbutils.secrets.get(scope, 'kafka_username')}' "
    f"password='{dbutils.secrets.get(scope, 'kafka_password')}';"
)
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", spark.conf.get("eurostream.kafka.bootstrap"))
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option("kafka.sasl.jaas.config", jaas)
    .option("failOnDataLoss", "false")
    .load()
)

payment_schema = StructType(
    [
        StructField("schema_version", IntegerType(), True),
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("occurred_at", DoubleType(), False),
        StructField("payment_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("iban", StringType(), False),
        StructField("amount_eur", DoubleType(), False),
        StructField("country", StringType(), False),
        StructField("merchant_country", StringType(), False),
        StructField("status", StringType(), True),
    ]
)

parsed = (
    raw.select(
        F.col("value").cast("string").alias("_raw_payload"),
        F.from_json(
            F.col("value").cast("string"),
            payment_schema,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        ).alias("event"),
    )
    .select(
        F.col("event.*"),
        F.coalesce(F.col("event._corrupt_record"), F.lit(None).cast("string")).alias(
            "_corrupt_record"
        ),
    )
    .withColumn("event_ts", F.timestamp_seconds(F.col("occurred_at")))
)

required_payment_columns = [
    "event_id",
    "payment_id",
    "order_id",
    "customer_id",
    "iban",
    "amount_eur",
    "country",
    "merchant_country",
]
valid_payment = F.col("_corrupt_record").isNull() & (
    F.col("event_type") == F.lit("payment_processed")
)
for column in required_payment_columns:
    valid_payment = valid_payment & F.col(column).isNotNull()
valid_payment = (
    valid_payment
    & (F.col("occurred_at") > F.lit(0))
    & (F.col("amount_eur") >= F.lit(0))
    & F.col("event_ts").isNotNull()
    & (F.col("status").isNull() | F.col("status").isin("authorized", "declined", "captured"))
)

payments = (
    parsed.where(valid_payment)
    # Fraud rules never need IBAN or the raw payload. Drop clear PII as soon as
    # the contract check is complete so it cannot enter keyed state.
    .drop("iban", "_raw_payload")
)

# %md
# ## Non-streaming suppression snapshot
#
# A streaming registry join would add another stateful streaming operator.
# Read a current snapshot once for the query plan, then reload the same helper in
# foreachBatch for every micro-batch. The latter is the write-time anti-join
# that closes the race with an erasure commit after state processing. A newly
# suppressed customer cannot be removed from already-running keyed state by a
# separate query; the sink anti-join prevents persistence and the state TTL
# removes residual state.


def current_suppression_snapshot():
    return (
        spark.table(SUPPRESSION_TABLE).select("customer_id").where(F.col("customer_id").isNotNull())
    )


initial_suppression = current_suppression_snapshot()
payments_for_state = payments.join(
    initial_suppression,
    on="customer_id",
    how="left_anti",
)

# %md
# ## One keyed stateful pass
#
# All three rules share one bounded keyed state. The state contains a bounded
# event-id list, a bounded five-minute payment timestamp deque, the last 200
# amounts, and a short list of velocity windows already emitted.

alert_output_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), False),
        StructField("rule", StringType(), False),
        StructField("score", DoubleType(), False),
        StructField("detail", StringType(), True),
        StructField("alerted_at", TimestampType(), False),
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
    ]
)


def score_customer(customer_id: str, events, state: GroupState):
    """Update all per-customer rule state and yield typed alert candidates."""
    stored = state.get or {}
    seen_event_ids = [str(value) for value in stored.get("seen_event_ids", [])]
    payment_times = deque(
        (float(value) for value in stored.get("payment_times", [])),
        maxlen=VELOCITY_MAX_EVENTS,
    )
    amounts = [float(value) for value in stored.get("amounts", [])]
    velocity_windows = [int(value) for value in stored.get("velocity_windows", [])]
    saw_row = False

    for row in events:
        saw_row = True
        event_id = str(row["event_id"])
        if event_id in seen_event_ids:
            continue
        seen_event_ids.append(event_id)
        seen_event_ids = seen_event_ids[-EVENT_ID_STATE_N:]

        amount = row["amount_eur"]
        event_ts = row["event_ts"]
        if amount is None or event_ts is None:
            continue
        amount = float(amount)
        if not math.isfinite(amount):
            continue

        event_epoch = event_ts.timestamp()
        payment_times.append(event_epoch)
        payment_times = deque(sorted(payment_times), maxlen=VELOCITY_MAX_EVENTS)
        window_start_epoch = math.floor(event_epoch / VELOCITY_WINDOW_S) * VELOCITY_WINDOW_S
        window_end_epoch = window_start_epoch + VELOCITY_WINDOW_S
        while payment_times and payment_times[0] < window_start_epoch:
            payment_times.popleft()
        window_times = [
            timestamp
            for timestamp in payment_times
            if window_start_epoch <= timestamp < window_end_epoch
        ]

        if len(window_times) > VELOCITY_THRESHOLD and window_start_epoch not in velocity_windows:
            velocity_windows.append(window_start_epoch)
            velocity_windows = velocity_windows[-VELOCITY_WINDOW_MEMORY:]
            window_start = datetime(1970, 1, 1) + timedelta(seconds=window_start_epoch)
            window_end = datetime(1970, 1, 1) + timedelta(seconds=window_end_epoch)
            yield (
                None,
                customer_id,
                "VELOCITY",
                float(len(window_times)),
                f"payments_in_5m={len(window_times)}",
                window_end,
                window_start,
                window_end,
            )

        if len(amounts) >= ZSCORE_MIN_BASELINE:
            mean = sum(amounts) / len(amounts)
            sample_std = statistics.stdev(amounts)
            if sample_std > 0:
                zscore = (amount - mean) / sample_std
                if abs(zscore) > ZSCORE_THRESHOLD:
                    yield (
                        event_id,
                        customer_id,
                        "AMOUNT_ZSCORE",
                        float(zscore),
                        f"amount={amount:.2f};zscore={zscore:.3f}",
                        event_ts,
                        event_ts,
                        event_ts,
                    )
        amounts.append(amount)
        amounts = amounts[-ZSCORE_STATE_N:]

        if row["country"] != row["merchant_country"]:
            yield (
                event_id,
                customer_id,
                "GEO_MISMATCH",
                1.0,
                f"billing={row['country']};merchant={row['merchant_country']}",
                event_ts,
                event_ts,
                event_ts,
            )

    if not saw_row:
        state.remove()
        return

    last_seen_ms = int(time.time() * 1000)
    state.update(
        {
            "seen_event_ids": seen_event_ids,
            "payment_times": list(payment_times),
            "amounts": amounts,
            "velocity_windows": velocity_windows,
            "last_seen_ms": last_seen_ms,
        }
    )
    # ProcessingTimeTimeout receives epoch milliseconds. The group is removed
    # after this many idle seconds, subject to normal micro-batch progress.
    state.setTimeoutTimestamp(last_seen_ms + ZSCORE_STATE_TTL_S * 1000)


scored_stream = payments_for_state.groupByKey(
    lambda row: row["customer_id"]
).flatMapGroupsWithState(
    score_customer,
    outputGroupSchema=alert_output_schema,
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
)

# %md
# ## Normalize alert identity
#
# The single stateful pass emits all rule candidates. Velocity alerts use the
# customer + window identity; event-level rules use the stable event_id.

scored_df = scored_stream.toDF(
    [
        "event_id",
        "customer_id",
        "rule",
        "score",
        "detail",
        "alerted_at",
        "window_start",
        "window_end",
    ]
)
alerts = scored_df.withColumn(
    "alert_id",
    F.sha2(
        F.concat_ws(
            "|",
            F.col("rule"),
            F.col("customer_id"),
            F.coalesce(F.col("event_id"), F.lit("")),
            F.coalesce(F.col("window_start").cast("string"), F.lit("")),
            F.coalesce(F.col("window_end").cast("string"), F.lit("")),
        ),
        256,
    ),
).select(
    "alert_id",
    "customer_id",
    "rule",
    "score",
    "detail",
    "alerted_at",
    "window_start",
    "window_end",
)

# COMMAND ----------

# %md
# ## Idempotent Delta sink
#
# `foreachBatch` is intentionally used for the MERGE. The callback is retried
# by Workflows/Structured Streaming after transient failures; the deterministic
# alert_id makes every retry an update or a no-op, never a duplicate insert.

from pyspark.sql.window import Window


def merge_alert_batch(batch_df, _batch_id):
    if batch_df.isEmpty():
        return

    # Close the race where an erasure commits after the state pass but before
    # this micro-batch is written. The snapshot is re-read for every callback;
    # suppressed candidates are never persisted.
    currently_suppressed = current_suppression_snapshot()
    incoming = batch_df.join(currently_suppressed, on="customer_id", how="left_anti")
    if incoming.isEmpty():
        return

    # A source replay can put the same deterministic key in one batch more
    # than once. Pick one deterministic winner before MERGE (MERGE requires at
    # most one source row per target key).
    winner = Window.partitionBy("alert_id").orderBy(
        F.col("alerted_at").desc_nulls_last(),
        F.col("customer_id").desc_nulls_last(),
        F.col("rule").desc_nulls_last(),
    )
    incoming = (
        incoming.withColumn("_rn", F.row_number().over(winner))
        .where(F.col("_rn") == F.lit(1))
        .drop("_rn")
        .withColumn("_created_at", F.current_timestamp())
        .select(
            "alert_id",
            "customer_id",
            "rule",
            "score",
            "detail",
            "alerted_at",
            "window_start",
            "window_end",
            "_created_at",
        )
        .cache()
    )

    try:
        target = DeltaTable.forName(spark, ALERT_TABLE)
        update_values = {
            column: F.col(f"s.{column}")
            for column in (
                "customer_id",
                "rule",
                "score",
                "detail",
                "alerted_at",
                "window_start",
                "window_end",
            )
        }
        insert_values = {column: F.col(f"s.{column}") for column in incoming.columns}
        (
            target.alias("t")
            .merge(incoming.toDF().alias("s"), "s.alert_id = t.alert_id")
            .whenMatchedUpdate(
                condition=F.col("s.alerted_at") >= F.col("t.alerted_at"),
                set=update_values,
            )
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
    finally:
        incoming.unpersist()


# The fraud table is owned by this standalone job, not by a Lakeflow pipeline.
# The bootstrap above is idempotent and validates an existing table before any
# sink write, so a retry cannot silently target schema drift.
if not BOOTSTRAP_ONLY:
    (
        alerts.writeStream.outputMode("append")
        .foreachBatch(merge_alert_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .queryName("eurostream_fraud_alerts")
        .start()
        .awaitTermination()
    )

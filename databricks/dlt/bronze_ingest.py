# Databricks notebook source
# ruff: noqa: F821, N812
# %md
# # Bronze — Raw Event Capture
#
# Lakeflow Declarative Pipelines source for the Databricks-native `bronze`
# schema. This file depends only on Databricks runtime services and Unity
# Catalog objects.
#
# ## Contracts
# - `event_id` is the stable global key. Every event table uses
#   `replace_using=["event_id"]` and the deterministic
#   `BRONZE_REPLACE_SEQUENCE` expression. REPLACE USING is a Beta DBR 18.2+
#   flow whose target is created inside Lakeflow; it is not a perfect
#   exactly-once replay-dedup guarantee. Same-key/same-sequence rows append by
#   design, so a downstream `row_number()` is not presented as that proof.
# - `orders_files` is a canonical relation with the same event columns as
#   `orders`; Silver consumes both.
# - Invalid JSON/required-field/business-rule rows are split before any event
#   table write. The valid branch and quarantine branch are mutually exclusive.
# - The suppression registry is read as an append-only stream and anti-joined
#   before both branches are materialized. A delayed/replayed event for a
#   suppressed, identifiable customer therefore cannot recreate either an
#   event row or a quarantine row.

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# %md
# ## Event schemas (the versioned event contract is mirrored here so the
# Databricks parser does not infer a weaker schema from JSON).

ORDER_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType(), True),
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("occurred_at", DoubleType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("email", StringType(), False),
        StructField("iban", StringType(), False),
        StructField("country", StringType(), False),
        StructField("amount_eur", DoubleType(), False),
        StructField("marketing_consent", BooleanType(), False),
        StructField("currency", StringType(), True),
    ]
)

CLICK_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType(), True),
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("occurred_at", DoubleType(), False),
        StructField("click_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("ip_address", StringType(), False),
        StructField("page", StringType(), False),
        StructField("country", StringType(), False),
    ]
)

PAYMENT_SCHEMA = StructType(
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

ERASURE_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType(), True),
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("occurred_at", DoubleType(), False),
        StructField("request_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("reason", StringType(), True),
        StructField("requested_by", StringType(), True),
    ]
)

LINEAGE_SCHEMA = StructType(
    [
        StructField("_occurred_at", TimestampType(), False),
        StructField("_ingested_at", TimestampType(), False),
        StructField("_source", StringType(), False),
        StructField("_kafka_ts", TimestampType(), True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_file_path", StringType(), True),
        StructField("_file_modification_time", TimestampType(), True),
    ]
)
ORDER_OUTPUT_SCHEMA = StructType(list(ORDER_SCHEMA.fields) + list(LINEAGE_SCHEMA.fields))
CLICK_OUTPUT_SCHEMA = StructType(list(CLICK_SCHEMA.fields) + list(LINEAGE_SCHEMA.fields))
PAYMENT_OUTPUT_SCHEMA = StructType(list(PAYMENT_SCHEMA.fields) + list(LINEAGE_SCHEMA.fields))
ERASURE_OUTPUT_SCHEMA = StructType(list(ERASURE_SCHEMA.fields) + list(LINEAGE_SCHEMA.fields))
QUARANTINE_SCHEMA = StructType(
    [
        StructField("quarantine_id", StringType(), False),
        StructField("source_name", StringType(), False),
        StructField("source_partition", IntegerType(), True),
        StructField("source_offset", LongType(), True),
        StructField("file_path", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("quarantine_reason", StringType(), False),
        StructField("suppression_status", StringType(), False),
        StructField("occurred_at", TimestampType(), True),
        StructField("ingested_at", TimestampType(), False),
    ]
)

ORDER_FIELDS = [field.name for field in ORDER_SCHEMA.fields]
CLICK_FIELDS = [field.name for field in CLICK_SCHEMA.fields]
PAYMENT_FIELDS = [field.name for field in PAYMENT_SCHEMA.fields]
ERASURE_FIELDS = [field.name for field in ERASURE_SCHEMA.fields]

SUPPRESSION_REGISTRY = "eurostream.governance.suppression_registry"
# A left-outer stream join is the supported Databricks form of a dynamic
# anti-join (the logical LEFT ANTI JOIN). The very long suppression watermark
# is intentional: an erasure tombstone is permanent, so its join state must
# outlive any supported replay
# horizon. The event side uses a bounded lateness window to avoid unbounded
# event join state.
SUPPRESSION_WATERMARK = "36500 days"
SUPPRESSION_RANGE = "INTERVAL 100 YEARS"
EVENT_LATENESS = spark.conf.get("eurostream.event_lateness", "30 days")
AUTO_LOADER_ROOT = "/Volumes/eurostream/bronze/inbox/orders"
AUTO_LOADER_SCHEMA = "/Volumes/eurostream/bronze/_checkpoints/orders_files/schema"

# REPLACE USING is Beta on DBR 18.2+. The target tables are created by the
# Lakeflow pipeline, never by SQL/10_bronze_ddl.sql. The official contract
# appends rows that share both key and sequence, so this is an explicit,
# deterministic replacement contract—not a claim of exactly-once replay dedup.
# Source coordinates are a stable tie-breaker: Kafka partition/offset or Auto
# Loader file path/modification time distinguish separate source records. An
# exact replay with the same coordinates remains indistinguishable to the flow.
BRONZE_REPLACE_SEQUENCE = F.sha2(
    F.concat_ws(
        "|",
        F.coalesce(F.col("_source").cast("string"), F.lit("unknown")),
        F.coalesce(F.col("_kafka_partition").cast("string"), F.lit("")),
        F.coalesce(F.col("_kafka_offset").cast("string"), F.lit("")),
        F.coalesce(F.col("_file_modification_time").cast("long").cast("string"), F.lit("")),
        F.coalesce(F.col("_file_path").cast("string"), F.lit("")),
        F.coalesce(F.col("event_id").cast("string"), F.lit("")),
        F.coalesce(F.col("event_type").cast("string"), F.lit("")),
        F.coalesce(F.col("occurred_at").cast("string"), F.lit("")),
    ),
    256,
)
QUARANTINE_REPLACE_SEQUENCE = F.sha2(
    F.concat_ws(
        "|",
        F.coalesce(F.col("source_name").cast("string"), F.lit("unknown")),
        F.coalesce(F.col("source_partition").cast("string"), F.lit("")),
        F.coalesce(F.col("source_offset").cast("string"), F.lit("")),
        F.coalesce(F.col("file_path").cast("string"), F.lit("")),
        F.coalesce(F.col("event_id").cast("string"), F.lit("")),
        F.coalesce(F.col("customer_id").cast("string"), F.lit("")),
        F.coalesce(F.col("raw_payload").cast("string"), F.lit("")),
    ),
    256,
)

# %md
# ## Shared source and contract helpers


def _with_common(df: DataFrame) -> DataFrame:
    """Add the common lineage columns without changing event values."""
    if "schema_version" in df.columns:
        df = df.withColumn("schema_version", F.coalesce(F.col("schema_version"), F.lit(1)))
    if "_raw_payload" in df.columns:
        # Best-effort recovery for a syntactically corrupt payload. It lets a
        # malformed row with an attributable customer/event honor suppression
        # and the stable replacement key; rows without an attributable ID are
        # explicitly quarantined as unknown.
        if "customer_id" in df.columns:
            recovered_customer_id = F.nullif(
                F.regexp_extract(F.col("_raw_payload"), r'"customer_id"\s*:\s*"([^"]*)"', 1),
                F.lit(""),
            )
            df = df.withColumn(
                "customer_id", F.coalesce(F.col("customer_id"), recovered_customer_id)
            )
        if "event_id" in df.columns:
            recovered_event_id = F.nullif(
                F.regexp_extract(F.col("_raw_payload"), r'"event_id"\s*:\s*"([^"]*)"', 1),
                F.lit(""),
            )
            df = df.withColumn("event_id", F.coalesce(F.col("event_id"), recovered_event_id))
    if "_occurred_at" not in df.columns:
        df = df.withColumn("_occurred_at", F.timestamp_seconds(F.col("occurred_at")))
    if "_ingested_at" not in df.columns:
        df = df.withColumn("_ingested_at", F.current_timestamp())
    if "_source" not in df.columns:
        df = df.withColumn("_source", F.lit("unknown"))
    if "_kafka_ts" not in df.columns:
        df = df.withColumn("_kafka_ts", F.lit(None).cast("timestamp"))
    if "_kafka_partition" not in df.columns:
        df = df.withColumn("_kafka_partition", F.lit(None).cast("int"))
    if "_kafka_offset" not in df.columns:
        df = df.withColumn("_kafka_offset", F.lit(None).cast("long"))
    if "_file_path" not in df.columns:
        df = df.withColumn("_file_path", F.lit(None).cast("string"))
    if "_file_modification_time" not in df.columns:
        df = df.withColumn("_file_modification_time", F.lit(None).cast("timestamp"))
    return df


def _kafka_source(topic: str, schema: StructType, source_name: str) -> DataFrame:
    """Read one Kafka topic without discarding malformed records."""
    scope = spark.conf.get("eurostream.kafka.secret_scope", "eurostream")
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
        f"username='{dbutils.secrets.get(scope, 'kafka_username')}' "
        f"password='{dbutils.secrets.get(scope, 'kafka_password')}';"
    )
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", spark.conf.get("eurostream.kafka.bootstrap"))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
        .option("kafka.sasl.jaas.config", jaas)
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(
            F.col("value").cast("string").alias("_raw_payload"),
            F.col("timestamp").alias("_kafka_ts"),
            F.col("partition").cast("int").alias("_kafka_partition"),
            F.col("offset").cast("long").alias("_kafka_offset"),
        )
        .withColumn(
            "_parsed",
            F.from_json(
                F.col("_raw_payload"),
                schema,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ),
        )
        .select(
            "_raw_payload",
            "_kafka_ts",
            "_kafka_partition",
            "_kafka_offset",
            F.col("_parsed.*"),
        )
        .withColumn("_source", F.lit(source_name))
    )


def _file_source() -> DataFrame:
    """Read the canonical partner-file relation with an explicit schema."""
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", AUTO_LOADER_SCHEMA)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(ORDER_SCHEMA)
        .load(AUTO_LOADER_ROOT)
    )
    payload = F.to_json(F.struct(*[F.col(name) for name in ORDER_FIELDS]))
    return (
        raw.select(
            *[F.col(name) for name in ORDER_FIELDS],
            F.col("_rescued_data").alias("_corrupt_record"),
            F.col("_metadata.file_path").alias("_file_path"),
            F.col("_metadata.file_modification_time").alias("_file_modification_time"),
        )
        .withColumn(
            "_raw_payload",
            F.coalesce(F.col("_corrupt_record"), payload),
        )
        .withColumn("_source", F.lit("autoloader:orders_files"))
        .withColumn("_kafka_ts", F.lit(None).cast("timestamp"))
        .withColumn("_kafka_partition", F.lit(None).cast("int"))
        .withColumn("_kafka_offset", F.lit(None).cast("long"))
    )


def _contract_status(
    df: DataFrame,
    expected_event_type: str,
    required_fields: list[str],
    *,
    check_amount: bool = False,
    check_payment_status: bool = False,
) -> DataFrame:
    """Add a non-null malformed flag and a triage reason to a parsed source."""
    required_missing = F.lit(False)
    for field in required_fields:
        required_missing = required_missing | F.col(field).isNull()

    event_type_mismatch = F.coalesce(F.col("event_type") != F.lit(expected_event_type), F.lit(True))
    corrupt_record = F.coalesce(F.col("_corrupt_record").isNotNull(), F.lit(False))
    business_rule_failed = F.lit(False)
    if check_amount:
        business_rule_failed = business_rule_failed | F.coalesce(
            F.col("amount_eur") < F.lit(0), F.lit(True)
        )
    if check_payment_status:
        # `status` is optional in the event contract; a present value must be
        # one of the three producer states.
        business_rule_failed = business_rule_failed | (
            F.col("status").isNotNull()
            & F.coalesce(~F.col("status").isin("authorized", "declined", "captured"), F.lit(False))
        )
    if expected_event_type == "erasure_requested":
        business_rule_failed = business_rule_failed | (
            F.col("reason").isNotNull()
            & F.coalesce(F.col("reason") != F.lit("GDPR_ARTICLE_17"), F.lit(False))
        )

    malformed = (
        corrupt_record
        | F.coalesce(F.col("event_id").isNull(), F.lit(False))
        | event_type_mismatch
        | required_missing
        | business_rule_failed
        | F.coalesce(F.col("occurred_at").isNull(), F.lit(False))
        | F.coalesce(F.col("occurred_at") <= F.lit(0), F.lit(False))
        | F.coalesce(F.col("_occurred_at").isNull(), F.lit(False))
    )
    reason = (
        F.when(corrupt_record, F.lit("JSON_PARSE_ERROR"))
        .when(F.col("event_id").isNull(), F.lit("EVENT_ID_MISSING"))
        .when(event_type_mismatch, F.lit("EVENT_TYPE_MISMATCH"))
        .when(required_missing, F.lit("REQUIRED_FIELD_MISSING"))
        .when(business_rule_failed, F.lit("BUSINESS_RULE_FAILED"))
        .otherwise(F.lit("INVALID_OCCURRED_AT"))
    )
    return df.withColumn("_is_malformed", malformed).withColumn("_parse_error", reason)


def _not_suppressed(df: DataFrame) -> DataFrame:
    """Apply the durable, append-only suppression registry as a dynamic anti-join.

    Databricks supports watermarked left-outer stream joins, while a stream
    anti-join is not a portable stream-stream operation. The null-marker filter
    below is therefore the explicit anti-join. The registry has no legal expiry:
    its 100-year watermark is a practical state horizon, not a tombstone TTL.
    Rows with no customer_id cannot be attributed to a suppressed customer and
    are handled as explicit CUSTOMER_ID_UNKNOWN quarantine rows.
    """
    event_side = df.withColumn(
        "_suppression_event_time",
        F.coalesce(F.col("_occurred_at"), F.col("_ingested_at")),
    ).withWatermark("_suppression_event_time", EVENT_LATENESS)
    registry = (
        dp.read_stream(SUPPRESSION_REGISTRY)
        .where(F.col("customer_id").isNotNull())
        .select(
            F.col("customer_id").alias("_suppressed_customer_id"),
            F.timestamp_seconds(F.col("added_at")).alias("_suppression_time"),
        )
        .withWatermark("_suppression_time", SUPPRESSION_WATERMARK)
    )
    # The time-range predicate is required for a supported stream-stream outer
    # join. The 100-year range is the same practical horizon as the tombstone
    # watermark; it covers delayed/replayed events without a legal expiry.
    joined = event_side.join(
        registry,
        on=(
            (F.col("customer_id") == F.col("_suppressed_customer_id"))
            & (
                F.col("_suppression_event_time")
                >= F.col("_suppression_time") - F.expr(SUPPRESSION_RANGE)
            )
            & (
                F.col("_suppression_event_time")
                <= F.col("_suppression_time") + F.expr(SUPPRESSION_RANGE)
            )
        ),
        how="left_outer",
    )
    return joined.where(F.col("_suppressed_customer_id").isNull()).drop(
        "_suppressed_customer_id", "_suppression_time", "_suppression_event_time"
    )


def _valid_events(
    source: DataFrame,
    expected_event_type: str,
    required_fields: list[str],
    *,
    check_amount: bool = False,
    check_payment_status: bool = False,
) -> DataFrame:
    marked = _contract_status(
        _with_common(source),
        expected_event_type,
        required_fields,
        check_amount=check_amount,
        check_payment_status=check_payment_status,
    )
    return _not_suppressed(marked.where(~F.col("_is_malformed")))


def _project_event(df: DataFrame, fields: list[str]) -> DataFrame:
    """Project exactly the columns promised by the Bronze SQL contract."""
    return df.select(
        *[F.col(field) for field in fields],
        F.col("_occurred_at"),
        F.col("_ingested_at"),
        F.col("_source"),
        F.col("_kafka_ts"),
        F.col("_kafka_partition"),
        F.col("_kafka_offset"),
        F.col("_file_path"),
        F.col("_file_modification_time"),
    )


def _quarantine_projection(df: DataFrame) -> DataFrame:
    """Build the exclusive, deterministic quarantine relation."""
    record_identity = F.concat(
        F.lit("record|"),
        F.coalesce(F.col("_source").cast("string"), F.lit("")),
        F.lit("|"),
        F.coalesce(F.col("_kafka_partition").cast("string"), F.lit("")),
        F.lit("|"),
        F.coalesce(F.col("_kafka_offset").cast("string"), F.lit("")),
        F.lit("|"),
        F.coalesce(F.col("_file_path").cast("string"), F.lit("")),
        F.lit("|"),
        F.coalesce(F.col("_raw_payload").cast("string"), F.lit("")),
    )
    # A parsed event_id is the stable replacement-key identity even if a
    # producer retries at a different Kafka offset. Only records without an
    # event_id fall back
    # to source location/payload identity.
    identity = F.when(
        F.col("event_id").isNotNull(),
        F.concat(F.lit("event|"), F.col("event_id").cast("string")),
    ).otherwise(record_identity)
    return (
        df.withColumn("quarantine_id", F.sha2(identity, 256))
        .withColumn("source_name", F.col("_source"))
        .withColumn("source_partition", F.col("_kafka_partition"))
        .withColumn("source_offset", F.col("_kafka_offset"))
        .withColumn("file_path", F.col("_file_path"))
        .withColumn("raw_payload", F.coalesce(F.col("_raw_payload"), F.col("_corrupt_record")))
        .withColumn("quarantine_reason", F.col("_parse_error"))
        .withColumn(
            "suppression_status",
            F.when(F.col("customer_id").isNull(), F.lit("CUSTOMER_ID_UNKNOWN")).otherwise(
                F.lit("NOT_SUPPRESSED")
            ),
        )
        .select(
            "quarantine_id",
            "source_name",
            "source_partition",
            "source_offset",
            "file_path",
            "event_id",
            "customer_id",
            "raw_payload",
            "quarantine_reason",
            "suppression_status",
            F.col("_occurred_at").alias("occurred_at"),
            F.col("_ingested_at").alias("ingested_at"),
        )
    )


# %md
# ## `bronze.orders` and `bronze.orders_files`

ORDER_REQUIRED = [
    "event_id",
    "event_type",
    "occurred_at",
    "order_id",
    "customer_id",
    "email",
    "iban",
    "country",
    "amount_eur",
    "marketing_consent",
]


@dp.table(
    name="orders",
    schema=ORDER_OUTPUT_SCHEMA,
    partition_cols=["country"],
    replace_using=["event_id"],
    sequence_by=BRONZE_REPLACE_SEQUENCE,
    comment="Raw order events. Clear-text PII — internal only, Art.17-maskable.",
    table_properties={
        "quality": "bronze",
        "pii_classification": "restricted",
        "replace_using_key": "event_id",
        "replace_using_sequence": "BRONZE_REPLACE_SEQUENCE",
        "replace_using_duplicate_policy": "same_key_same_sequence_appends",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
@dp.expect_all(
    {
        "event_id_present": "event_id IS NOT NULL",
        "amount_non_negative": "amount_eur >= 0",
        "event_type_is_order": "event_type = 'order_placed'",
    }
)
def orders():
    return _project_event(
        _valid_events(
            _kafka_source("orders", ORDER_SCHEMA, "kafka:orders"),
            "order_placed",
            ORDER_REQUIRED,
            check_amount=True,
        ),
        ORDER_FIELDS,
    )


@dp.table(
    name="orders_files",
    schema=ORDER_OUTPUT_SCHEMA,
    partition_cols=["country"],
    replace_using=["event_id"],
    sequence_by=BRONZE_REPLACE_SEQUENCE,
    comment="Canonical file-dropped partner orders ingested via Auto Loader.",
    table_properties={
        "quality": "bronze",
        "pii_classification": "restricted",
        "replace_using_key": "event_id",
        "replace_using_sequence": "BRONZE_REPLACE_SEQUENCE",
        "replace_using_duplicate_policy": "same_key_same_sequence_appends",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_all(
    {
        "event_id_present": "event_id IS NOT NULL",
        "amount_non_negative": "amount_eur >= 0",
        "event_type_is_order": "event_type = 'order_placed'",
    }
)
def orders_files():
    return _project_event(
        _valid_events(
            _file_source(),
            "order_placed",
            ORDER_REQUIRED,
            check_amount=True,
        ),
        ORDER_FIELDS,
    )


# %md
# ## `bronze.clicks`

CLICK_REQUIRED = [
    "event_id",
    "event_type",
    "occurred_at",
    "click_id",
    "customer_id",
    "session_id",
    "ip_address",
    "page",
    "country",
]


@dp.table(
    name="clicks",
    schema=CLICK_OUTPUT_SCHEMA,
    partition_cols=["country"],
    replace_using=["event_id"],
    sequence_by=BRONZE_REPLACE_SEQUENCE,
    comment="Raw page-click events. ip_address is PII — Art.17-maskable.",
    table_properties={
        "quality": "bronze",
        "pii_classification": "restricted",
        "replace_using_key": "event_id",
        "replace_using_sequence": "BRONZE_REPLACE_SEQUENCE",
        "replace_using_duplicate_policy": "same_key_same_sequence_appends",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_all(
    {
        "event_id_present": "event_id IS NOT NULL",
        "event_type_is_click": "event_type = 'page_click'",
    }
)
def clicks():
    return _project_event(
        _valid_events(
            _kafka_source("clicks", CLICK_SCHEMA, "kafka:clicks"),
            "page_click",
            CLICK_REQUIRED,
        ),
        CLICK_FIELDS,
    )


# %md
# ## `bronze.payments`

PAYMENT_REQUIRED = [
    "event_id",
    "event_type",
    "occurred_at",
    "payment_id",
    "order_id",
    "customer_id",
    "iban",
    "amount_eur",
    "country",
    "merchant_country",
]


@dp.table(
    name="payments",
    schema=PAYMENT_OUTPUT_SCHEMA,
    partition_cols=["country"],
    replace_using=["event_id"],
    sequence_by=BRONZE_REPLACE_SEQUENCE,
    comment="Raw payment events. iban is PII — Art.17-maskable.",
    table_properties={
        "quality": "bronze",
        "pii_classification": "restricted",
        "replace_using_key": "event_id",
        "replace_using_sequence": "BRONZE_REPLACE_SEQUENCE",
        "replace_using_duplicate_policy": "same_key_same_sequence_appends",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_all(
    {
        "event_id_present": "event_id IS NOT NULL",
        "amount_non_negative": "amount_eur >= 0",
        "event_type_is_payment": "event_type = 'payment_processed'",
        "status_allowed": "status IS NULL OR status IN ('authorized', 'declined', 'captured')",
    }
)
def payments():
    return _project_event(
        _valid_events(
            _kafka_source("payments", PAYMENT_SCHEMA, "kafka:payments"),
            "payment_processed",
            PAYMENT_REQUIRED,
            check_amount=True,
            check_payment_status=True,
        ),
        PAYMENT_FIELDS,
    )


# %md
# ## `bronze.erasure_requests`

ERASURE_REQUIRED = [
    "event_id",
    "event_type",
    "occurred_at",
    "request_id",
    "customer_id",
]


@dp.table(
    name="erasure_requests",
    schema=ERASURE_OUTPUT_SCHEMA,
    replace_using=["event_id"],
    sequence_by=BRONZE_REPLACE_SEQUENCE,
    comment="GDPR Art. 17 DSAR intake. Source of truth for cascade triggers.",
    table_properties={
        "quality": "bronze",
        "pii_classification": "confidential",
        "replace_using_key": "event_id",
        "replace_using_sequence": "BRONZE_REPLACE_SEQUENCE",
        "replace_using_duplicate_policy": "same_key_same_sequence_appends",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_all(
    {
        "request_id_present": "request_id IS NOT NULL",
        "customer_id_present": "customer_id IS NOT NULL",
        "reason_is_art17": "reason IS NULL OR reason = 'GDPR_ARTICLE_17'",
    }
)
def erasure_requests():
    return _project_event(
        _valid_events(
            _kafka_source("erasure_requests", ERASURE_SCHEMA, "kafka:erasure_requests"),
            "erasure_requested",
            ERASURE_REQUIRED,
        ),
        ERASURE_FIELDS,
    )


# %md
# ## Exclusive malformed-input quarantine
#
# All source readers are parsed independently so a malformed row is classified
# using that source's contract. The union is projected only after the valid
# branch has been removed. Suppression is applied before this projection.


def _quarantine_sources() -> DataFrame:
    sources = [
        _contract_status(
            _with_common(_kafka_source("orders", ORDER_SCHEMA, "kafka:orders")),
            "order_placed",
            ORDER_REQUIRED,
            check_amount=True,
        ),
        _contract_status(
            _with_common(_kafka_source("clicks", CLICK_SCHEMA, "kafka:clicks")),
            "page_click",
            CLICK_REQUIRED,
        ),
        _contract_status(
            _with_common(_kafka_source("payments", PAYMENT_SCHEMA, "kafka:payments")),
            "payment_processed",
            PAYMENT_REQUIRED,
            check_amount=True,
            check_payment_status=True,
        ),
        _contract_status(
            _with_common(
                _kafka_source("erasure_requests", ERASURE_SCHEMA, "kafka:erasure_requests")
            ),
            "erasure_requested",
            ERASURE_REQUIRED,
        ),
        _contract_status(
            _with_common(_file_source()),
            "order_placed",
            ORDER_REQUIRED,
            check_amount=True,
        ),
    ]
    malformed = sources[0].where(F.col("_is_malformed"))
    for source in sources[1:]:
        malformed = malformed.unionByName(
            source.where(F.col("_is_malformed")), allowMissingColumns=True
        )
    return _not_suppressed(malformed).transform(_quarantine_projection)


@dp.table(
    name="ingest_quarantine",
    schema=QUARANTINE_SCHEMA,
    replace_using=["quarantine_id"],
    sequence_by=QUARANTINE_REPLACE_SEQUENCE,
    comment="Exclusive malformed-event quarantine; never a fallback event stream.",
    table_properties={
        "quality": "quarantine",
        "pii_classification": "restricted",
        "replace_using_key": "quarantine_id",
        "replace_using_sequence": "QUARANTINE_REPLACE_SEQUENCE",
        "replace_using_duplicate_policy": "same_key_same_sequence_appends",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect("quarantine_id_present", "quarantine_id IS NOT NULL")
def ingest_quarantine():
    return _quarantine_sources()

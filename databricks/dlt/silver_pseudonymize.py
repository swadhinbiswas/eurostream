# Databricks notebook source
# ruff: noqa: F821, N812
# %md
# # Silver — Cleansed & Pseudonymized
#
# Databricks-native Lakeflow source. It reads only Unity Catalog Delta
# relations and Unity Catalog governance tables.
#
# ## Contracts
# - Bronze `event_id` is the global source key. Silver is a full-refresh
#   materialized view over the current Delta snapshot, so replacement commits
#   from Bronze are visible on the next Lakeflow update. Window functions shape
#   that snapshot; they are not presented as a streaming idempotency proof.
# - Every input path is anti-joined to the durable suppression registry before
#   hashing or aggregation. A replay for an erased customer therefore cannot
#   recreate a Silver row on the next refresh.
# - Normal Silver tables contain hashes only. Invalid IBAN/PII rows are sent to
#   exclusive, restricted quarantine tables and are filtered out here.
# - Hash contract: `SHA-256(salt + ":" + value)` — the semantic reference
#   contract has no space after the colon.

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# This must be supplied through a Databricks secret-backed pipeline
# configuration. There is deliberately no production fallback secret.
PII_SALT = spark.conf.get("eurostream.pii.salt")
if not PII_SALT:
    raise ValueError(
        "eurostream.pii.salt must be supplied through the Databricks secret-backed "
        "pipeline configuration"
    )

ANONYMIZED = "<anonymized>"  # noqa: S105 - marker literal, not a secret
SUPPRESSION_REGISTRY = "eurostream.governance.suppression_registry"
BRONZE_ORDERS = "eurostream.bronze.orders"
BRONZE_ORDERS_FILES = "eurostream.bronze.orders_files"
BRONZE_PAYMENTS = "eurostream.bronze.payments"

IBAN_LENGTHS = {
    "DE": 22,
    "FR": 27,
    "NL": 18,
    "IE": 22,
    "DK": 18,
    "FI": 18,
    "SE": 24,
    "AT": 20,
    "BE": 16,
    "ES": 24,
    "IT": 27,
}

# %md
# ## Deterministic PII hashing


def hash_pii(col_name: str):
    """Hash exactly `salt + ':' + value` for cross-layer compatibility."""
    return F.sha2(F.concat(F.lit(PII_SALT), F.lit(":"), F.col(col_name)), 256)


# %md
# ## ISO 7064 Mod-97 IBAN validator


@F.udf(returnType="boolean")
def is_valid_iban(iban: str | None) -> bool:
    if not iban:
        return False
    compact = iban.replace(" ", "").replace("-", "").upper()
    country = compact[:2]
    expected_length = IBAN_LENGTHS.get(country)
    if (
        expected_length is None
        or len(compact) != expected_length
        or not compact.isascii()
        or not compact.isalnum()
        or not compact[2:4].isdigit()
    ):
        return False
    rearranged = compact[4:] + compact[:4]
    numeric = "".join(str(ord(char) - 55) if char.isalpha() else char for char in rearranged)
    return int(numeric) % 97 == 1


# %md
# ## Suppression and source contracts


def _not_suppressed(df: DataFrame) -> DataFrame:
    """Drop customers present in the current durable suppression snapshot.

    Silver is a batch materialized view in the Lakeflow bundle. A batch read is
    intentional: Bronze uses REPLACE USING, and reading an update-producing
    table as a plain Structured Streaming source would either reject change
    commits or silently skip them. The next Silver refresh reads the current
    Delta snapshot, including replacements and erasure tombstones. The
    resulting join is the batch equivalent of a LEFT ANTI JOIN.
    """
    registry = (
        dp.read(SUPPRESSION_REGISTRY).select("customer_id").where(F.col("customer_id").isNotNull())
    )
    return df.join(registry, on="customer_id", how="left_anti")


def _order_stream() -> DataFrame:
    """Union the canonical Kafka and partner-file order relations."""
    fields = [
        "event_id",
        "order_id",
        "customer_id",
        "email",
        "iban",
        "country",
        "amount_eur",
        "marketing_consent",
        "currency",
        "_occurred_at",
        "_ingested_at",
        "_source",
        "_kafka_offset",
    ]
    kafka_orders = dp.read(BRONZE_ORDERS).select(*fields)
    file_orders = dp.read(BRONZE_ORDERS_FILES).select(*fields)
    return kafka_orders.unionByName(file_orders)


def _dedup_event_id(df: DataFrame) -> DataFrame:
    """Shape a full-refresh snapshot; it is not the global source-key proof."""
    window = Window.partitionBy("event_id").orderBy(
        F.col("_occurred_at").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_kafka_offset").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == F.lit(1))
        .drop("_rn")
    )


def _live_pii(column: str):
    return (
        F.col(column).isNotNull()
        & (F.col(column) != F.lit(ANONYMIZED))
        & (F.length(F.trim(F.col(column))) > F.lit(0))
    )


def _valid_order_rows(df: DataFrame) -> DataFrame:
    """Return only rows safe to hash into normal Silver."""
    return df.withColumn("iban_valid", is_valid_iban(F.col("iban"))).where(
        F.col("iban_valid")
        & _live_pii("email")
        & _live_pii("iban")
        & F.col("event_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("country").isNotNull()
        & F.col("amount_eur").isNotNull()
        & (F.col("amount_eur") >= F.lit(0))
        & F.col("marketing_consent").isNotNull()
        & F.col("_occurred_at").isNotNull()
    )


def _valid_payment_rows(df: DataFrame) -> DataFrame:
    return df.withColumn("iban_valid", is_valid_iban(F.col("iban"))).where(
        F.col("iban_valid")
        & F.col("event_id").isNotNull()
        & F.col("payment_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("iban").isNotNull()
        & F.col("amount_eur").isNotNull()
        & (F.col("amount_eur") >= F.lit(0))
        & F.col("country").isNotNull()
        & F.col("merchant_country").isNotNull()
        & (F.col("status").isNull() | F.col("status").isin("authorized", "declined", "captured"))
        & F.col("_occurred_at").isNotNull()
    )


def _silver_timestamp() -> str:
    return "_silver_at"


# %md
# ## `silver.customers`


@dp.materialized_view(
    name="customers",
    comment="Cleansed customer dimension. PII salted-SHA-256 hashed.",
    table_properties={
        "quality": "silver",
        "pii_classification": "pseudonymized",
        "source_key": "customer_id",
        "refresh_mode": "full",
    },
)
@dp.expect_all(
    {
        "email_hashed_not_clear": "email_hash NOT LIKE '%@%'",
        "email_hash_format": "email_hash RLIKE '^[0-9a-f]{64}$'",
        "iban_hash_format": "iban_hash RLIKE '^[0-9a-f]{64}$'",
        "customer_id_present": "customer_id IS NOT NULL",
    }
)
def customers():
    all_orders = _not_suppressed(_order_stream()).where(
        F.col("event_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("marketing_consent").isNotNull()
    )
    source = _valid_order_rows(all_orders)
    latest_window = Window.partitionBy("customer_id").orderBy(
        F.col("_occurred_at").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )
    latest = (
        source.withColumn("_rn", F.row_number().over(latest_window))
        .where(F.col("_rn") == F.lit(1))
        .select(
            "customer_id",
            "country",
            F.col("email").alias("latest_email"),
            F.col("iban").alias("latest_iban"),
            F.col("marketing_consent").alias("latest_consent"),
        )
    )
    # Consent is fail-closed across every attributable order, including an
    # order whose IBAN is quarantined. Invalid PII must never silently opt a
    # customer back into marketing.
    consent = all_orders.groupBy("customer_id").agg(
        F.bool_and("marketing_consent").alias("consent_gate")
    )
    joined = latest.join(consent, on="customer_id", how="inner")
    consent_value = F.coalesce(F.col("consent_gate"), F.col("latest_consent"), F.lit(False))
    return joined.select(
        F.col("customer_id"),
        F.col("country"),
        hash_pii("latest_email").alias("email_hash"),
        hash_pii("latest_iban").alias("iban_hash"),
        consent_value.alias("marketing_consent"),
        F.current_timestamp().alias(_silver_timestamp()),
    )


# %md
# ## `silver.orders`


@dp.materialized_view(
    name="orders",
    comment="Full-refresh, pseudonymized orders. Invalid rows are quarantined.",
    table_properties={
        "quality": "silver",
        "pii_classification": "pseudonymized",
        "source_key": "event_id",
        "refresh_mode": "full",
    },
)
@dp.expect_all(
    {
        "email_hashed_not_clear": "email_hash NOT LIKE '%@%'",
        "iban_hash_format": "iban_hash RLIKE '^[0-9a-f]{64}$'",
        "iban_is_valid": "iban_valid = true",
        "amount_non_negative": "amount_eur >= 0",
    }
)
def orders():
    source = _valid_order_rows(_not_suppressed(_order_stream()))
    return _dedup_event_id(source).select(
        "event_id",
        "order_id",
        "customer_id",
        hash_pii("email").alias("email_hash"),
        hash_pii("iban").alias("iban_hash"),
        "iban_valid",
        "country",
        "amount_eur",
        "currency",
        "marketing_consent",
        "_occurred_at",
        F.current_timestamp().alias(_silver_timestamp()),
        "_source",
    )


# %md
# ## `silver.payments`


@dp.materialized_view(
    name="payments",
    comment="Deduplicated, pseudonymized payments with ISO 13616 validation.",
    table_properties={
        "quality": "silver",
        "pii_classification": "pseudonymized",
        "source_key": "event_id",
        "refresh_mode": "full",
    },
)
@dp.expect_all(
    {
        "iban_hash_format": "iban_hash RLIKE '^[0-9a-f]{64}$'",
        "iban_is_valid": "iban_valid = true",
        "amount_non_negative": "amount_eur >= 0",
        "status_allowed": "status IS NULL OR status IN ('authorized', 'declined', 'captured')",
    }
)
def payments():
    source = _valid_payment_rows(_not_suppressed(dp.read(BRONZE_PAYMENTS)))
    return _dedup_event_id(source).select(
        "event_id",
        "payment_id",
        "order_id",
        "customer_id",
        hash_pii("iban").alias("iban_hash"),
        "iban_valid",
        "amount_eur",
        "country",
        "merchant_country",
        "status",
        "_occurred_at",
        F.current_timestamp().alias(_silver_timestamp()),
        "_source",
    )


# %md
# ## Exclusive Silver quarantine tables


def _order_quarantine_id(df: DataFrame) -> DataFrame:
    record_identity = F.concat_ws(
        "|",
        F.col("event_id"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("iban"),
    )
    identity = F.when(
        F.col("event_id").isNotNull(),
        F.concat(F.lit("event|"), F.col("event_id").cast("string")),
    ).otherwise(record_identity)
    return df.withColumn("quarantine_id", F.sha2(identity, 256))


@dp.materialized_view(
    name="orders_quarantine",
    comment="Orders excluded from Silver for IBAN/PII contract failure.",
    table_properties={
        "quality": "quarantine",
        "pii_classification": "restricted",
        "source_key": "quarantine_id",
        "refresh_mode": "full",
    },
)
def orders_quarantine():
    source = _not_suppressed(_order_stream()).withColumn("iban_valid", is_valid_iban(F.col("iban")))
    invalid = source.where(
        ~F.col("iban_valid")
        | ~_live_pii("email")
        | ~_live_pii("iban")
        | F.col("event_id").isNull()
        | F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("country").isNull()
        | F.col("amount_eur").isNull()
        | (F.col("amount_eur") < F.lit(0))
        | F.col("marketing_consent").isNull()
        | F.col("_occurred_at").isNull()
        | F.coalesce(F.col("event_type") != F.lit("order_placed"), F.lit(True))
    )
    return _order_quarantine_id(invalid).select(
        "quarantine_id",
        "event_id",
        "order_id",
        "customer_id",
        "email",
        "iban",
        "country",
        "amount_eur",
        F.col("_occurred_at").alias("occurred_at"),
        F.col("_source").alias("source_name"),
        F.when(~F.col("iban_valid"), F.lit("IBAN_MOD97_FAILED"))
        .when(~_live_pii("email") | ~_live_pii("iban"), F.lit("PII_ANONYMIZED_OR_MISSING"))
        .otherwise(F.lit("ORDER_CONTRACT_FAILED"))
        .alias("quarantine_reason"),
        F.current_timestamp().alias("_quarantined_at"),
    )


@dp.materialized_view(
    name="payments_quarantine",
    comment="Payments excluded from Silver for IBAN/PII contract failure.",
    table_properties={
        "quality": "quarantine",
        "pii_classification": "restricted",
        "source_key": "quarantine_id",
        "refresh_mode": "full",
    },
)
def payments_quarantine():
    source = _not_suppressed(dp.read(BRONZE_PAYMENTS)).withColumn(
        "iban_valid", is_valid_iban(F.col("iban"))
    )
    invalid = source.where(
        ~F.col("iban_valid")
        | F.col("event_id").isNull()
        | F.col("payment_id").isNull()
        | F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("amount_eur").isNull()
        | (F.col("amount_eur") < F.lit(0))
        | F.col("country").isNull()
        | F.col("merchant_country").isNull()
        | F.col("_occurred_at").isNull()
        | (
            F.col("status").isNotNull()
            & F.coalesce(~F.col("status").isin("authorized", "declined", "captured"), F.lit(False))
        )
    )
    record_identity = F.concat_ws(
        "|", F.col("event_id"), F.col("payment_id"), F.col("customer_id"), F.col("iban")
    )
    identity = F.when(
        F.col("event_id").isNotNull(),
        F.concat(F.lit("event|"), F.col("event_id").cast("string")),
    ).otherwise(record_identity)
    return invalid.withColumn("quarantine_id", F.sha2(identity, 256)).select(
        "quarantine_id",
        "event_id",
        "payment_id",
        "order_id",
        "customer_id",
        "iban",
        "country",
        "merchant_country",
        "amount_eur",
        "status",
        F.col("_occurred_at").alias("occurred_at"),
        F.col("_source").alias("source_name"),
        F.when(~F.col("iban_valid"), F.lit("IBAN_MOD97_FAILED"))
        .otherwise(F.lit("PAYMENT_CONTRACT_FAILED"))
        .alias("quarantine_reason"),
        F.current_timestamp().alias("_quarantined_at"),
    )

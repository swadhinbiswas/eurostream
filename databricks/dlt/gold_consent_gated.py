# Databricks notebook source
# ruff: noqa: F821, N812
# %md
# # Gold — Curated & Consent-Gated Aggregates
#
# Databricks-native Lakeflow materialized views over Unity Catalog Delta
# tables. The source pipeline is the only production runtime dependency.
#
# Gold is deliberately free of clear-text PII. `consents_marketing` and
# `marketing_consent` are projected from the same fail-closed expression, and
# the fraud rollup exposes both `last_alert_at` and the UI-compatible
# `last_alert` alias.

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SUPPRESSION_REGISTRY = "eurostream.governance.suppression_registry"


def _not_suppressed(df: DataFrame) -> DataFrame:
    """Apply the current durable suppression snapshot to batch Gold views."""
    registry = (
        dp.read(SUPPRESSION_REGISTRY).select("customer_id").where(F.col("customer_id").isNotNull())
    )
    return df.join(registry, on="customer_id", how="left_anti")


# %md
# ## `gold.customer_360`


@dp.materialized_view(
    name="customer_360",
    comment="Curated customer profile. Unique customer_id, consent-gated marketing.",
    table_properties={
        "quality": "gold",
        "pii_classification": "pseudonymized",
        "gdpr_consent_gate": "consents_marketing == marketing_consent",
    },
)
@dp.expect_all(
    {
        "customer_id_present": "customer_id IS NOT NULL",
        "consent_mirror": "consents_marketing = marketing_consent",
    }
)
def customer_360():
    customers = _not_suppressed(dp.read("silver.customers")).select(
        "customer_id",
        "country",
        "email_hash",
        F.col("marketing_consent").alias("customer_consent"),
    )
    facts = _not_suppressed(dp.read("silver.orders"))
    aggregates = facts.groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.coalesce(F.sum("amount_eur"), F.lit(0.0)).alias("lifetime_spend_eur"),
        F.min("_occurred_at").alias("first_order_at"),
        F.max("_occurred_at").alias("last_order_at"),
        F.bool_and("marketing_consent").alias("order_consent_gate"),
        F.first("country", ignorenulls=True).alias("fact_country"),
        F.first("email_hash", ignorenulls=True).alias("fact_email_hash"),
    )
    joined = customers.join(aggregates, on="customer_id", how="left")
    consent = F.coalesce(
        F.col("order_consent_gate"),
        F.col("customer_consent"),
        F.lit(False),
    )
    return joined.select(
        F.col("customer_id"),
        F.coalesce(F.col("fact_country"), F.col("country")).alias("country"),
        F.coalesce(F.col("fact_email_hash"), F.col("email_hash")).alias("email_hash"),
        F.coalesce(F.col("order_count"), F.lit(0).cast("long")).alias("order_count"),
        F.coalesce(F.col("lifetime_spend_eur"), F.lit(0.0)).alias("lifetime_spend_eur"),
        F.col("first_order_at"),
        F.col("last_order_at"),
        consent.alias("consents_marketing"),
        # Keep the legacy/UI field exactly equal to the canonical consent gate.
        consent.alias("marketing_consent"),
    )


# %md
# ## `gold.order_facts`


@dp.materialized_view(
    name="order_facts",
    comment="Order fact table. Unique order_id, FK to customer_360.",
    table_properties={
        "quality": "gold",
        "pii_classification": "pseudonymized",
        "gdpr_consent_gate": "consents_marketing",
    },
)
@dp.expect_all(
    {
        "order_id_present": "order_id IS NOT NULL",
        "amount_non_negative": "amount_eur >= 0",
        "consent_present": "consents_marketing IS NOT NULL",
    }
)
def order_facts():
    orders = _not_suppressed(dp.read("silver.orders"))
    customers = _not_suppressed(dp.read("customer_360")).select("customer_id", "consents_marketing")
    window = Window.partitionBy("order_id").orderBy(
        F.col("_occurred_at").desc_nulls_last(),
        F.col("event_id").desc_nulls_last(),
    )
    return (
        orders.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == F.lit(1))
        .drop("_rn")
        .join(customers, on="customer_id", how="inner")
        .select(
            "order_id",
            "customer_id",
            "email_hash",
            "iban_hash",
            "country",
            "amount_eur",
            "currency",
            F.col("_occurred_at").alias("occurred_at"),
            "consents_marketing",
        )
    )


# %md
# ## `gold.fraud_summary`


@dp.materialized_view(
    name="fraud_summary",
    comment="Per-customer fraud alert rollup. Suppressed customers are excluded.",
    table_properties={
        "quality": "gold",
        "pii_classification": "pseudonymized",
    },
)
@dp.expect("customer_id_present", "customer_id IS NOT NULL")
def fraud_summary():
    alerts = _not_suppressed(dp.read("bronze.fraud_alerts")).where(F.col("alert_id").isNotNull())
    # The sink is globally keyed and MERGE-idempotent; this second drop also
    # protects a materialized-view refresh from repeated source versions.
    alerts = alerts.dropDuplicates(["alert_id"])
    return (
        alerts.groupBy("customer_id")
        .agg(
            F.count("*").alias("alert_count"),
            F.countDistinct(F.when(F.col("rule") == "VELOCITY", F.col("alert_id"))).alias(
                "velocity_alerts"
            ),
            F.countDistinct(F.when(F.col("rule") == "AMOUNT_ZSCORE", F.col("alert_id"))).alias(
                "zscore_alerts"
            ),
            F.countDistinct(F.when(F.col("rule") == "GEO_MISMATCH", F.col("alert_id"))).alias(
                "geo_alerts"
            ),
            F.max("score").alias("max_score"),
            F.max("alerted_at").alias("last_alert_at"),
        )
        .select(
            "customer_id",
            "alert_count",
            "velocity_alerts",
            "zscore_alerts",
            "geo_alerts",
            "max_score",
            "last_alert_at",
            F.col("last_alert_at").alias("last_alert"),
        )
    )

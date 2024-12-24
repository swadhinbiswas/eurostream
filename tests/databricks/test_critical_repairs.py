from __future__ import annotations

import ast
import hashlib
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATABRICKS_ROOT = REPO_ROOT / "databricks"


def _source(relative: str) -> str:
    return (DATABRICKS_ROOT / relative).read_text(encoding="utf-8")


def _active_sql(relative: str) -> str:
    """Return SQL after removing line and block comments."""
    source = _source(relative)
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    return "\n".join(
        line for line in source.splitlines() if line.strip() and not line.lstrip().startswith("--")
    )


def test_catalog_and_bronze_reference_contract_are_present() -> None:
    catalogs = _source("sql/00_catalogs.sql")
    bronze = _source("sql/10_bronze_ddl.sql")
    assert "CREATE SCHEMA IF NOT EXISTS lake" in catalogs
    assert "eurostream.lake.exports" in catalogs
    assert "CREATE TABLE" not in catalogs
    assert "orders_files" in bronze
    assert "ingest_quarantine" in bronze
    assert "target table must be created by Lakeflow" in bronze
    assert "CREATE TABLE IF NOT EXISTS fraud_alerts" not in bronze


def test_pii_hash_fixture_uses_the_local_contract() -> None:
    salt = "test-salt"
    value = "person@example.eu"
    expected = hashlib.sha256(f"{salt}:{value}".encode()).hexdigest()
    source = _source("dlt/silver_pseudonymize.py")
    assert len(expected) == 64
    assert 'F.lit(":")' in source
    assert 'F.lit(": ")' not in source


def test_pipeline_owned_targets_are_not_bootstrap_created() -> None:
    create_target = re.compile(
        r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|STREAMING\s+TABLE|MATERIALIZED\s+VIEW|FLOW)\b",
        flags=re.IGNORECASE,
    )
    for relative in ("sql/10_bronze_ddl.sql", "sql/20_silver_ddl.sql", "sql/30_gold_ddl.sql"):
        active = _active_sql(relative)
        assert not active.strip(), relative
        assert not create_target.search(active), relative
        assert not re.search(r"\bUSE\s+(?:CATALOG|SCHEMA)\b", active, re.IGNORECASE), relative

    bronze = _source("sql/10_bronze_ddl.sql")
    assert "REPLACE USING" in bronze
    assert "flows-replace-using" in bronze
    assert "DBR 18.2" in bronze
    normalized_bronze = " ".join(bronze.lower().split())
    assert "same key" in normalized_bronze
    assert "same sequence" in normalized_bronze
    assert "appended" in normalized_bronze


def test_dlt_sources_use_current_api_and_explicit_replace_contracts() -> None:
    for relative in (
        "dlt/bronze_ingest.py",
        "dlt/silver_pseudonymize.py",
        "dlt/gold_consent_gated.py",
    ):
        source = _source(relative)
        assert "from pyspark import pipelines as dp" in source
        assert "import dlt" not in source

    bronze = _source("dlt/bronze_ingest.py")
    silver = _source("dlt/silver_pseudonymize.py")
    assert 'replace_using=["event_id"]' in bronze
    assert "sequence_by=BRONZE_REPLACE_SEQUENCE" in bronze
    assert 'replace_using=["quarantine_id"]' in bronze
    assert "sequence_by=QUARANTINE_REPLACE_SEQUENCE" in bronze
    assert 'sequence_by="_ingested_at"' not in bronze
    assert 'format("kafka")' in bronze
    assert 'format("cloudFiles")' in bronze
    assert "@dp.materialized_view" in silver
    assert "dp.read(BRONZE_PAYMENTS)" in silver
    assert 'F.lit(":")' in silver
    assert 'F.lit(": ")' not in silver
    assert "ingest_quarantine" in bronze
    assert "INTERVAL 100 YEARS" in bronze
    assert bronze.count('"delta.enableChangeDataFeed": "true"') == 6
    assert "orders_files" in silver
    fraud = _source("notebooks/03_fraud_streaming.py")
    shaded = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
    assert shaded in fraud
    assert shaded in bronze
    assert '"org.apache.kafka.common.security.scram.ScramLoginModule' not in fraud
    assert '"org.apache.kafka.common.security.scram.ScramLoginModule' not in bronze


def test_governance_uses_group_based_current_uc_syntax() -> None:
    source = _source("sql/50_grants_and_tags.sql").lower()
    assert "creates no pipeline-owned target tables" in source
    assert "create governed tag" in source
    assert "to group" in source
    assert "is_account_group_member" in source
    assert "alter column" in source and "set mask" in source
    assert "alter streaming table" in source
    assert "alter materialized view" in source
    assert "set row filter" in source
    assert "create masking policy" not in source
    assert "create row filter policy" not in source
    assert "current_user()" not in source
    assert "eurostream_dpo_operators" in source
    assert "erasure_impact_counts" in source
    assert "showcase_table_counts" in source


def test_fraud_job_bootstraps_its_sink_and_keeps_merges_idempotent() -> None:
    source = _source("notebooks/03_fraud_streaming.py")
    assert "ALERT_SINK_SCHEMA" in source
    assert "CREATE TABLE IF NOT EXISTS" in source
    assert "ensure_alert_sink()" in source
    assert "bootstrap_only" in source
    assert source.index("\nensure_alert_sink()") < source.index("\nscope = spark.conf.get")
    assert "spark.table(table)" in source
    assert "F.sha2(" in source
    assert "whenMatchedUpdate" in source
    assert "whenNotMatchedInsert" in source
    assert "spark.table(SUPPRESSION_TABLE)" in source
    assert "current_suppression_snapshot" in source
    assert "payments_for_state" in source
    assert "readStream.table(SUPPRESSION_TABLE)" not in source
    assert "SUPPRESSION_WATERMARK" not in source
    assert "SUPPRESSION_RANGE" not in source
    assert "dropDuplicatesWithinWatermark" not in source
    assert "F.window(" not in source
    assert "velocity =" not in source
    assert "geo =" not in source
    assert "zscore_stream" not in source
    assert source.count("flatMapGroupsWithState") == 1
    assert source.count("groupByKey") == 1
    assert "F.agg(" not in source
    assert "deque(" in source
    assert "maxlen=VELOCITY_MAX_EVENTS" in source
    assert "seen_event_ids[-EVENT_ID_STATE_N:]" in source
    assert "amounts[-ZSCORE_STATE_N:]" in source
    assert "GroupStateTimeout.ProcessingTimeTimeout" in source
    assert "setTimeoutTimestamp" in source
    assert "uuid()" not in source
    assert 'saveAsTable("bronze.fraud_alerts")' not in source


def test_assigned_databricks_files_have_no_local_runtime_imports() -> None:
    forbidden = {"duckdb", "turso", "fastapi", "eurostream"}
    paths = [
        DATABRICKS_ROOT / "dlt/bronze_ingest.py",
        DATABRICKS_ROOT / "dlt/silver_pseudonymize.py",
        DATABRICKS_ROOT / "dlt/gold_consent_gated.py",
        DATABRICKS_ROOT / "notebooks/03_fraud_streaming.py",
    ]
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
        assert not imports.intersection(forbidden), path

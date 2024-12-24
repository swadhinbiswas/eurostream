# Databricks notebook source
# ruff: noqa: E402, F821, N812
# %md
# # Governed Pseudonymized Lake Export
# #
# Rebuilds every approved Silver and Gold table under the existing Unity
# Catalog Volume:
# `lake.exports` → `/Volumes/eurostream/lake/exports`.
# Bronze and all restricted Silver quarantine tables are never exportable.
# Gold still contains stable customer IDs and deterministic hashes, so this is a
# restricted pseudonymized export, not anonymous or automatically public data.
# Each table is staged, schema/count checked, and then replaces its destination;
# stale local table directories and files are removed before the snapshot is
# declared complete. Silver and Gold are read-only Lakeflow MATERIALIZED VIEWs;
# this notebook never mutates or vacuums them. Callers must synchronously refresh
# both MV pipelines and pass `silver_gold_refresh_run_id` before this task runs.

# COMMAND ----------

dbutils.widgets.text("sync_huggingface", "false")
dbutils.widgets.text("silver_gold_refresh_run_id", "")
SYNC_HF = dbutils.widgets.get("sync_huggingface").strip().lower() == "true"
REFRESH_RUN_ID = dbutils.widgets.get("silver_gold_refresh_run_id").strip()

# COMMAND ----------

import time
from datetime import UTC, datetime

from pyspark.sql import functions as F

# COMMAND ----------

LAKE_ROOT = "/Volumes/eurostream/lake/exports"
STAGING_ROOT = f"{LAKE_ROOT}/.staging"

EXPORTS = {
    "silver/orders": "silver.orders",
    "silver/payments": "silver.payments",
    "silver/customers": "silver.customers",
    "gold/customer_360": "gold.customer_360",
    "gold/order_facts": "gold.order_facts",
    "gold/fraud_summary": "gold.fraud_summary",
}
EXPORT_TABLES_BY_LAYER = {
    "silver": frozenset(path.split("/", 1)[1] for path in EXPORTS if path.startswith("silver/")),
    "gold": frozenset(path.split("/", 1)[1] for path in EXPORTS if path.startswith("gold/")),
}


def _remove(path: str) -> None:
    """Remove a managed export path; `dbutils.fs.rm` is idempotent when absent."""
    dbutils.fs.rm(path, recurse=True)


def _validate_export_manifest() -> None:
    """Reject any attempt to export restricted source tables or paths."""
    for relative_path, table in EXPORTS.items():
        if not relative_path.startswith(("silver/", "gold/")):
            raise PermissionError(f"Only Silver/Gold export paths are allowed: {relative_path}")
        if not table.startswith(("silver.", "gold.")) or table.rsplit(".", 1)[-1].endswith(
            "_quarantine"
        ):
            raise PermissionError(f"Restricted table export blocked: {table}")


def _write_staged_table(relative_path: str, table: str) -> tuple[int, str]:
    source = spark.read.table(table)
    destination = f"{STAGING_ROOT}/{relative_path}"
    (
        source.repartition(F.col("customer_id"))
        .write.mode("overwrite")
        .option("maxRecordsPerFile", 500_000)
        .parquet(destination)
    )
    exported = spark.read.parquet(destination)
    exported_count = exported.count()
    source_count = source.count()
    if exported_count != source_count:
        raise RuntimeError(
            f"Staged export count mismatch for {table}: source={source_count}, "
            f"staged={exported_count}"
        )
    if exported.schema != source.schema:
        raise RuntimeError(f"Staged export schema mismatch for {table}")
    return exported_count, destination


def _remove_stale_local_paths() -> None:
    """Remove local paths not declared by the governed export manifest."""
    for entry in dbutils.fs.ls(LAKE_ROOT):
        if entry.name not in {"silver", "gold"}:
            _remove(entry.path)

    for layer, expected_tables in EXPORT_TABLES_BY_LAYER.items():
        layer_root = f"{LAKE_ROOT}/{layer}"
        for entry in dbutils.fs.ls(layer_root):
            if entry.name not in expected_tables:
                _remove(entry.path)


def verify_governed_export() -> dict[str, dict[str, int]]:
    """Verify every current Volume table against its source table."""
    verification: dict[str, dict[str, int]] = {}
    for relative_path, table in EXPORTS.items():
        source = spark.read.table(table)
        exported = spark.read.parquet(f"{LAKE_ROOT}/{relative_path}")
        if exported.schema != source.schema:
            raise RuntimeError(f"Published export schema mismatch for {table}")
        source_count = source.count()
        exported_count = exported.count()
        if exported_count != source_count:
            raise RuntimeError(
                f"Published export count mismatch for {table}: source={source_count}, "
                f"exported={exported_count}"
            )
        verification[relative_path] = {"rows": exported_count}
    return verification


def rewrite_governed_export() -> dict[str, dict[str, int]]:
    """Stage every table, replace declared folders, then verify all outputs.

    Publishing is folder-by-folder and is not a cross-table filesystem
    transaction. A mid-publish failure is failed closed and must be rerun before
    the export is accepted as evidence.
    """
    _validate_export_manifest()
    dbutils.fs.mkdirs(LAKE_ROOT)
    _remove(STAGING_ROOT)
    staged: list[tuple[str, str, str]] = []

    try:
        dbutils.fs.mkdirs(STAGING_ROOT)
        for relative_path, table in EXPORTS.items():
            row_count, staged_path = _write_staged_table(relative_path, table)
            staged.append((relative_path, table, staged_path))
            print(f"staged {table:28s} ({row_count:,} rows)")

        dbutils.fs.mkdirs(f"{LAKE_ROOT}/silver")
        dbutils.fs.mkdirs(f"{LAKE_ROOT}/gold")
        for relative_path, _table, staged_path in staged:
            destination = f"{LAKE_ROOT}/{relative_path}"
            _remove(destination)
            dbutils.fs.mv(staged_path, destination)
    finally:
        # Staging is never part of a published snapshot. Cleanup failure is fatal
        # because a hidden partial copy must not be mistaken for evidence.
        _remove(STAGING_ROOT)

    _remove_stale_local_paths()
    verification = verify_governed_export()
    print(f"governed export complete: {LAKE_ROOT}")
    print(f"silver_gold_refresh_run_id={REFRESH_RUN_ID}")
    return verification


# COMMAND ----------

if not REFRESH_RUN_ID:
    raise RuntimeError(
        "silver_gold_refresh_run_id is required; refresh the Lakeflow-owned Silver and "
        "Gold MATERIALIZED VIEWs before export"
    )
if len(REFRESH_RUN_ID) > 512 or any(ord(character) < 32 for character in REFRESH_RUN_ID):
    raise ValueError("silver_gold_refresh_run_id is invalid")

export_started = time.monotonic()
export_verification = rewrite_governed_export()
for relative_path, details in export_verification.items():
    print(f"verified {relative_path:28s} ({details['rows']:,} rows)")

dbutils.jobs.taskValues.set(
    key="export_row_count",
    value=str(sum(details["rows"] for details in export_verification.values())),
)
dbutils.jobs.taskValues.set(key="silver_gold_refresh_run_id", value=REFRESH_RUN_ID)
dbutils.jobs.taskValues.set(
    key="export_duration_seconds",
    value=f"{time.monotonic() - export_started:.3f}",
)

# %md
# ## Optional Hugging Face current-tree synchronization
# `delete_patterns=["*"]` removes stale files from the new remote tree in the
# same commit as the upload. Hugging Face Git/LFS/Xet commit history is not
# erased by this operation, and this notebook makes no commit-history deletion
# claim.

# COMMAND ----------

HF_SYNC_STATUS = "skipped"
if SYNC_HF:
    try:
        from huggingface_hub import HfApi

        token = dbutils.secrets.get("eurostream", "hf_token")
        repo_id = spark.conf.get("eurostream.hf.repo", "swadhinbiswas/eustream")
        if not repo_id.strip():
            raise ValueError("eurostream.hf.repo must not be empty")

        api = HfApi(token=token)
        api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
        remote_patterns = [f"{relative_path}/*.parquet" for relative_path in EXPORTS]
        api.upload_folder(
            folder_path=LAKE_ROOT,
            path_in_repo="",
            repo_id=repo_id,
            repo_type="dataset",
            allow_patterns=remote_patterns,
            delete_patterns=["*"],
            commit_message="Synchronize governed EuroStream pseudonymized export",
        )
        HF_SYNC_STATUS = "completed"
        print(f"synchronized current remote tree → hf.co/datasets/{repo_id}")
    except Exception as exc:
        # The Databricks Volume is the production system of record. Remote sync is
        # explicitly optional, but its failure remains visible in task metadata.
        HF_SYNC_STATUS = f"failed_optional:{type(exc).__name__}"
        print(f"optional Hugging Face sync failed: {type(exc).__name__}")
    print(
        "Remote commit history was not deleted; prior Git/LFS/Xet revisions may remain "
        "reachable and require a separate provider-approved eradication process."
    )
else:
    print("Hugging Face sync skipped (sync_huggingface=false)")

dbutils.jobs.taskValues.set(key="huggingface_sync_status", value=HF_SYNC_STATUS)

completed_at = datetime.now(UTC).isoformat()
print(f"export_completed_at={completed_at}")
dbutils.notebook.exit("ok")

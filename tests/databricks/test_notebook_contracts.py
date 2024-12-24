from __future__ import annotations

import ast
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS = REPO_ROOT / "databricks" / "notebooks"
SQL = REPO_ROOT / "databricks" / "sql"


def _load_functions(
    relative_path: str, names: set[str], namespace: dict[str, object]
) -> SimpleNamespace:
    tree = ast.parse((REPO_ROOT / relative_path).read_text(encoding="utf-8"))
    functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
    ]
    found = {node.name for node in functions}
    assert found == names
    module = ast.Module(body=functions, type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, relative_path, "exec"), namespace)  # noqa: S102
    return SimpleNamespace(**namespace)


def test_erasure_text_and_hash_helpers() -> None:
    helpers = _load_functions(
        "databricks/notebooks/02_article17_erasure.py",
        {
            "positive_int",
            "required_text",
            "validate_replay_identity",
            "confirmation_hash",
            "error_detail",
        },
        {"hashlib": hashlib},
    )

    assert helpers.positive_int("60", "internal_slo_seconds") == 60
    with pytest.raises(ValueError, match="greater than zero"):
        helpers.positive_int("0", "internal_slo_seconds")
    with pytest.raises(ValueError, match="must be an integer"):
        helpers.positive_int("sixty", "internal_slo_seconds")
    assert helpers.required_text("  request-1  ", "request_id") == "request-1"
    with pytest.raises(ValueError, match="required"):
        helpers.required_text(" ", "request_id")
    with pytest.raises(ValueError, match="control characters"):
        helpers.required_text("request\n1", "request_id")
    existing = {
        "customer_id": "customer-1",
        "ticket_id": "ticket-1",
        "idempotency_key": "replay-1",
    }
    helpers.validate_replay_identity(
        existing,
        customer_id="customer-1",
        ticket_id="ticket-1",
        idempotency_key="replay-1",
    )
    with pytest.raises(ValueError, match="different customer_id"):
        helpers.validate_replay_identity(
            existing,
            customer_id="customer-2",
            ticket_id="ticket-1",
            idempotency_key="replay-1",
        )
    with pytest.raises(ValueError, match="different ticket_id"):
        helpers.validate_replay_identity(
            existing,
            customer_id="customer-1",
            ticket_id="ticket-2",
            idempotency_key="replay-1",
        )
    with pytest.raises(ValueError, match="different idempotency_key"):
        helpers.validate_replay_identity(
            existing,
            customer_id="customer-1",
            ticket_id="ticket-1",
            idempotency_key="replay-2",
        )
    expected = hashlib.sha256(b"request-1:customer-1").hexdigest()[:16]
    assert helpers.confirmation_hash("request-1", "customer-1") == expected
    assert helpers.error_detail(ValueError("boom")) == "ValueError: boom"


def test_benchmark_percentile_helper() -> None:
    helpers = _load_functions(
        "databricks/notebooks/05_benchmark_erasure.py",
        {"percentile"},
        {},
    )

    assert helpers.percentile([1.0, 2.0, 3.0, 4.0], 0.5) == pytest.approx(2.5)
    assert helpers.percentile([1.0, 2.0, 3.0, 4.0], 0.95) == pytest.approx(3.85)
    with pytest.raises(ValueError, match="at least one"):
        helpers.percentile([], 0.5)
    with pytest.raises(ValueError, match="between zero and one"):
        helpers.percentile([1.0], 1.1)


def test_notebooks_keep_databricks_as_the_runtime_boundary() -> None:
    paths = [
        NOTEBOOKS / "01_quality_gates.py",
        NOTEBOOKS / "02_article17_erasure.py",
        NOTEBOOKS / "04_export_lake.py",
        NOTEBOOKS / "05_benchmark_erasure.py",
    ]
    forbidden_runtime_dependencies = (
        "src/eurostream",
        "duckdb",
        "turso",
        "fastapi",
        "render",
        "github actions",
        "github_actions",
    )
    for path in paths:
        source = path.read_text(encoding="utf-8").lower()
        assert "spark." in source
        assert "delta" in source or "dbutils" in source
        for dependency in forbidden_runtime_dependencies:
            assert dependency not in source


def test_quality_and_erasure_safety_contracts() -> None:
    quality = (NOTEBOOKS / "01_quality_gates.py").read_text(encoding="utf-8")
    erasure = (NOTEBOOKS / "02_article17_erasure.py").read_text(encoding="utf-8")
    benchmark = (NOTEBOOKS / "05_benchmark_erasure.py").read_text(encoding="utf-8")

    assert "LIMIT 50" not in quality
    assert "required table is empty" in quality
    assert "spark.sql(f" not in erasure
    assert "WHERE customer_id = '" not in erasure
    assert ":customer_id" in erasure
    assert "silver.orders_quarantine" in erasure
    for table in (
        "bronze.orders_files",
        "bronze.ingest_quarantine",
        "silver.payments_quarantine",
    ):
        assert table in erasure
    for table in ("gold.customer_360", "gold.order_facts", "gold.fraud_summary"):
        assert table in erasure
    assert "bronze.fraud_alerts" in erasure
    assert "governance.suppression_registry" in erasure
    assert "logical_verified" in erasure
    assert "physical_verified" in erasure
    assert "export_verified" in erasure
    assert "internal SLO; not a statutory claim" in erasure
    assert "_resolve_verify_command" in erasure
    assert "_find_command_by_idempotency" in erasure
    for field in ("ticket_id", "idempotency_key", "requested_by", "reason"):
        assert f'"{field}"' in erasure
    assert 'dbutils.widgets.text("ticket_id", "")' in erasure
    assert 'dbutils.widgets.text("idempotency_key", "")' in erasure
    assert "WORKFLOW_SLO_WIDGET" in erasure
    assert 'getattr(dbutils.context, "jobRunId", "")' in erasure
    assert "DeltaTable.forName" in benchmark
    for table in (
        "bronze.orders_files",
        "bronze.ingest_quarantine",
    ):
        assert table in benchmark
    assert 'dbutils.widgets.text("sla_seconds", "60")' in benchmark
    assert "USE CATALOG eurostream" in quality
    assert "USE CATALOG eurostream" in erasure
    assert "USE CATALOG eurostream" in benchmark
    assert "INSERT INTO" not in benchmark
    assert "cleanup_iteration(customer_id)" in benchmark


def test_lakeflow_materialized_view_ownership_boundary() -> None:
    quality = (NOTEBOOKS / "01_quality_gates.py").read_text(encoding="utf-8")
    erasure = (NOTEBOOKS / "02_article17_erasure.py").read_text(encoding="utf-8")
    export = (NOTEBOOKS / "04_export_lake.py").read_text(encoding="utf-8")
    benchmark = (NOTEBOOKS / "05_benchmark_erasure.py").read_text(encoding="utf-8")

    for source in (quality, erasure, export, benchmark):
        for unsupported in (
            "DELETE FROM silver.",
            "DELETE FROM gold.",
            "UPDATE silver.",
            "UPDATE gold.",
            "VACUUM silver.",
            "VACUUM gold.",
            "REORG TABLE silver.",
            "REORG TABLE gold.",
        ):
            assert unsupported not in source

    assert 'dbutils.widgets.text("mode", "prepare")' in erasure
    assert 'dbutils.widgets.text("mode", "execute")' not in erasure
    assert 'dbutils.widgets.text("silver_gold_refresh_run_id", "")' in erasure
    assert "def run_prepare()" in erasure
    assert "def verify_silver_gold_refresh" in erasure
    assert "silver_gold_refresh_required" in erasure
    assert "synchronously refresh the silver and gold lakeflow pipelines" in erasure.lower()

    finish_source = erasure[erasure.index("def _finish_after_refresh") :]
    assert finish_source.index("verify_silver_gold_refresh") < finish_source.index(
        "rewrite_governed_export"
    )

    physical_source = erasure[erasure.index("PHYSICAL_CLEANUP_SQL") :]
    physical_source = physical_source[: physical_source.index("# COMMAND ----------")]
    assert "bronze.fraud_alerts" in physical_source
    assert "bronze.orders" not in physical_source
    assert "silver." not in physical_source
    assert "gold." not in physical_source

    assert "silver_gold_refresh_run_id" in export
    assert "MATERIALIZED VIEW" in export
    assert "silver." not in benchmark
    assert "gold." not in benchmark
    assert "lake.exports" not in benchmark


def test_export_and_durable_state_contracts() -> None:
    export = (NOTEBOOKS / "04_export_lake.py").read_text(encoding="utf-8")
    state_ddl = (SQL / "60_erasure_command_state.sql").read_text(encoding="utf-8")

    assert 'LAKE_ROOT = "/Volumes/eurostream/lake/exports"' in export
    assert "dbutils.fs.rm" in export
    assert "delete_patterns" in export
    assert "history was not deleted" in export
    for status in ("queued", "running", "completed", "failed"):
        assert status in state_ddl
    for verification in ("logical_verified", "physical_verified", "export_verified"):
        assert verification in state_ddl
    for field in (
        "actor",
        "requested_by",
        "reason",
        "ticket_id",
        "idempotency_key",
        "silver_gold_refresh_run_id",
        "blocked_layer",
        "next_action",
        "internal_slo_state",
    ):
        assert field in state_ddl

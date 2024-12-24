from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAB = ROOT / "databricks" / "dab"


def _source(relative: str) -> str:
    return (DAB / relative).read_text(encoding="utf-8")


def test_pipeline_bundle_uses_triggered_current_serverless_targets() -> None:
    source = _source("resources/pipelines.yml")
    assert source.count("channel: CURRENT") == 3
    assert source.count("serverless: true") == 3
    assert "pipeline_type: BATCH" not in source
    assert "dependencies:" not in source
    assert "replace_using" in _source("../dlt/bronze_ingest.py")


def test_erasure_bundle_has_refresh_barrier_and_scoped_physical_cleanup() -> None:
    jobs = _source("resources/jobs.yml")
    article_job = jobs.split("eurostream_art17_erasure:", 1)[1].split(
        "eurostream_erasure_benchmark:", 1
    )[0]
    assert "verify_refresh_barrier" in article_job
    assert article_job.index("verify_refresh_barrier") < article_job.index("export_lake")
    assert "notebooks/02_article17_erasure.py" not in jobs
    assert 'sync_huggingface: "false"' in jobs
    assert "silver_gold_refresh_run_id" in jobs
    assert "sql/fraud_sink.sql" not in jobs

    final_stage = _source("notebooks/erasure_physical_cleanup.py")
    assert '"eurostream.bronze.fraud_alerts": ("absent", ())' in final_stage
    assert "eurostream.silver" not in final_stage.split("PHYSICAL_TABLES =", 1)[1].split("}", 1)[0]
    assert "eurostream.gold" not in final_stage.split("PHYSICAL_TABLES =", 1)[1].split("}", 1)[0]
    assert "REORG TABLE silver" not in final_stage
    assert "VACUUM silver" not in final_stage


def test_bundle_runtime_groups_and_app_resources_are_explicit() -> None:
    bundle = _source("databricks.yml")
    jobs = _source("resources/jobs.yml")
    app = _source("resources/app.yml")
    assert "pipeline_run_as_group" in bundle
    assert "job_run_as_group" in bundle
    assert "fraud_run_as_group" in bundle
    assert "erasure_run_as_group" in bundle
    assert "auto:latest-lts" not in bundle
    assert "eurostream_dpo_operators" in app
    assert "eurostream_platform_admins" in app
    assert "permission: CAN_MANAGE_RUN" in app
    assert "permission: CAN_USE" in app
    assert "permission: READ" in app
    assert "eurostream_fraud_continuous" in jobs
    assert "eurostream.kafka.bootstrap:" in jobs
    assert "spark.eurostream.kafka.bootstrap:" not in jobs
    assert "data_security_mode: SINGLE_USER" in jobs


def test_dab_stage_sources_are_synced_and_failure_safe() -> None:
    bundle = _source("databricks.yml")
    assert "dab/notebooks/*.py" in bundle
    assert "erasure_suppression_mask.py" in _source("resources/jobs.yml")
    assert "erasure_refresh_verify.py" in _source("resources/jobs.yml")
    assert "erasure_failure_record.py" in _source("resources/jobs.yml")
    failure = _source("notebooks/erasure_failure_record.py")
    assert "status = 'failed'" in failure
    assert "data_mutation" in failure


def test_uc_policy_uses_current_privilege_names() -> None:
    grants = _source("../sql/50_grants_and_tags.sql")
    assert "GRANT USE CATALOG" in grants
    assert "GRANT USE SCHEMA" in grants
    assert "GRANT USAGE" not in grants
    assert "GRANT CREATE STREAMING TABLE" not in grants
    assert "MODIFY ON TABLE eurostream.silver" not in grants
    assert "MODIFY ON TABLE eurostream.gold" not in grants

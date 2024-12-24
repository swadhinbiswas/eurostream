# EuroStream optional Declarative Automation Bundle

This directory is an **optional** automation layer for the Databricks-native
showcase.  The primary operating path remains the manually created and rehearsed
Databricks UI resources.  Deploy this bundle only after the UI path has been
validated, and review the generated resource IDs and permissions in the target
workspace.

The bundle is intentionally split into:

- `databricks.yml` — target/workspace variables, source sync, and deployment
  identity.
- `resources/pipelines.yml` — three Serverless Lakeflow pipelines.
- `resources/jobs.yml` — the continuous Bronze wrapper, fraud stream,
  medallion, quality, export, erasure, benchmark, and four-hour DAG. The
  four-hour parent is split into `eurostream_medallion_refresh` plus quality and
  export children to preserve explicit task boundaries.
- `resources/uc.yml` — namespace-only Unity Catalog resources and narrow schema
  grants.  It does **not** define medallion target tables.
- `resources/security.yml` — secret-scope ACLs only; it contains no secret
  values.
- `resources/app.yml` — an optional App resource that binds the existing App's
  SQL warehouse, erasure Job, and preview secret by the keys already used in
  `app/app.yaml`.
- `notebooks/erasure_suppression_mask.py` — DAB-local stage that masks Bronze
  and records suppression without touching Silver/Gold.
- `notebooks/erasure_physical_cleanup.py` — DAB-local final stage that verifies
  current views/export and cleans only the job-owned fraud sink.
- `notebooks/erasure_failure_record.py` — conditional on-failure state recorder.
- `sql/fraud_sink.sql` — an optional reviewed manual bootstrap artifact; the
  fraud notebook is the Job-owned bootstrap boundary.

## Important table-ownership boundary

`sql/10_bronze_ddl.sql`, `sql/20_silver_ddl.sql`, and `sql/30_gold_ddl.sql` are
**schema contracts/review artifacts for this optional Bundle, not bootstrap
scripts**.  Do not run them before the first pipeline update and do not make a
Bundle deployment depend on them.

The source files attached by `resources/pipelines.yml` own their targets:

- `dlt/bronze_ingest.py` declares the streaming Bronze tables with
  `replace_using` and `sequence_by`.
- `dlt/silver_pseudonymize.py` and `dlt/gold_consent_gated.py` declare their
  downstream datasets.

REPLACE USING is a Beta Lakeflow feature with strict requirements: the source
must be streaming, the target must be created inside the pipeline, and the
runtime must be DBR 18.2 or newer.  The pipelines therefore use the current
channel, Serverless compute, and the source declarations above; the workspace
must confirm that the selected `CURRENT` runtime is at least 18.2 before the
first update.  The Bundle deliberately does not pin a stale DBR or add a
competing `CREATE TABLE` resource.

A live preflight must also review the checked-in `dlt/bronze_ingest.py` source
contract. Its suppression helper uses a left-outer stream-stream join inside a
REPLACE USING query with the documented watermark and time-range condition.
The target runtime must still prove that source plan before the first Bronze
update. This Bundle does not mask an unsupported plan by pre-creating a table
or selecting a different flow. If a previous rehearsal already created a
REPLACE USING target, reconcile it through a separately reviewed migration;
this Bundle does not silently adopt or destroy it.

The only table bootstrap added here is `eurostream.bronze.fraud_alerts`, which
is owned by the fraud Job because `03_fraud_streaming.py` writes to it. The
notebook's `ensure_alert_sink()` runs before Kafka access and is the DAB Job's
idempotent bootstrap/schema-validation boundary. The fraud Job exposes the
notebook's `bootstrap_only` parameter for an explicit one-shot bootstrap. The
optional `sql/fraud_sink.sql` artifact is retained for a reviewed manual
preflight, but the Bundle does not require a SQL warehouse permission or a
separate bootstrap task. The medallion Job assumes the reviewed sink exists;
create it by running the fraud notebook bootstrap before the first Gold run.

## Article 17 orchestration boundary

`eurostream_art17_erasure` does **not** run the existing
`notebooks/02_article17_erasure.py` in a monolithic `execute` mode. The checked-in
notebook now exposes ownership-safe `prepare` and `verify_only` modes, but the
Bundle keeps the stage boundaries explicit so a failed refresh cannot be
mistaken for completion:

```text
suppression_mask
  -> refresh_silver (pipeline task)
  -> refresh_gold (pipeline task)
  -> verify_refresh_barrier (read-only all-layer check)
  -> export_lake (run eurostream_export_lake with sync_huggingface=false)
  -> verify_export_cleanup (verify current views/export; clean fraud sink only)
```

The first task performs only the durable suppression tombstone, Bronze PII
masking, restricted Bronze quarantine cleanup, and standalone fraud-row
cleanup. Silver and Gold are explicitly full-refreshed by their owning Lakeflow
pipeline tasks (`full_refresh: true`). The barrier task runs before export and
fails if any target remains. The final physical task never issues `DELETE`,
`REORG`, or `VACUUM` against pipeline-owned Bronze, Silver, or Gold; it verifies
current rows and limits physical cleanup to the job-owned fraud sink. The
`02_article17_erasure.py` notebook is excluded from Bundle sync so it cannot be
selected accidentally.

The parent has `max_concurrent_runs: 1`, zero retries for the mutating/refresh
stages, an `on_failure` email, and an `on_failure_record` task guarded by
`run_if: AT_LEAST_ONE_FAILED`.  A
failed stage prevents all later success-path tasks; the failure recorder only
updates governance state and does not touch showcase data (the exact task key
is retained in the parent run/logs).  A deferred
physical run is recorded as failed evidence, not as a completed erasure.  The
Bundle does not pause other Jobs automatically: pause the continuous Bronze,
fraud, export, and competing pipeline writers before the suppression/physical
window, and verify quiescence in the workspace.

## Prerequisites and variables

The UI-created catalog/schemas, Volumes, governance tables, serverless SQL
warehouse, account groups, and secret values remain workspace prerequisites.
The bundle's schema resource does not create the catalog or any medallion
table.  It can establish the namespace-level `CREATE TABLE` grants needed for
in-pipeline publication and the fraud preflight.  Apply the reviewed
Unity Catalog table/Volume grants and policies after the first in-pipeline
publication; the `pipeline_run_as_group`, `job_run_as_group`,
`fraud_run_as_group`, and `erasure_run_as_group` identities must have the
reviewed task-specific table-level
`MODIFY`/Volume permissions required by the already-reviewed SQL policy.
Do not use `sql/10_bronze_ddl.sql`, `sql/20_silver_ddl.sql`, or
`sql/30_gold_ddl.sql` to fill that ordering gap.

The source files and the DAB-local fraud sink SQL currently use the reviewed
`eurostream` catalog and `bronze`/`silver`/`gold`/`governance` schema names.
Keep the defaults aligned with those source contracts; changing the catalog or
layer-schema variables requires a coordinated source change outside this
DAB-only repair.

Set environment-specific values with Databricks CLI `--var` flags or
`BUNDLE_VAR_*` environment variables.  Required values intentionally have no
committed defaults:

- `dev_workspace_host`, `prod_workspace_host`
- `dev_kafka_bootstrap`, `prod_kafka_bootstrap`
- `fraud_node_type_id` (a node available in the target cloud/workspace)
- `prod_deployer_application_id` (application ID, not a human display name)
- `on_call_email` (approved notification recipient; no value is committed)
- `sql_warehouse_id` if the warehouse is not named `eurostream-warehouse`

For example, after authenticating with the Databricks CLI:

```bash
cd databricks/dab
databricks bundle validate --target dev \
  --var="dev_workspace_host=https://<dev-workspace-host>" \
  --var="dev_kafka_bootstrap=<host:port>" \
  --var="fraud_node_type_id=<workspace-node-type>"
```

The production target additionally needs
`--var="prod_workspace_host=..."` and
`--var="prod_deployer_application_id=<application-id>"`.  Do not put secret
values, PATs, client secrets, or service-principal IDs in this repository.

## Secret handling

The runtime scope is named by `runtime_secret_scope` (default `eurostream`).
Populate these keys through the approved secret-management process, without
putting their values in YAML:

- `pii_salt` — read by the Silver pipeline through a Spark secret reference
  (`{{secrets/<scope>/pii_salt}}`); no salt is stored in Bundle configuration.
- `kafka_username` and `kafka_password` — read by the Bronze and fraud
  runtimes.
- `hf_token` — read only when the export Job is explicitly run with
  `sync_huggingface=true`.
- `EUROSTREAM_PREVIEW_SECRET` — bound to the App as `preview-secret` with
  `READ` permission.

`resources/security.yml` manages only the scope and ACLs.  The App resource
binding, rather than a broad grant, gives the App service principal access to
the single erasure Job (`CAN_MANAGE_RUN`), the SQL warehouse (`CAN_USE`), and
the preview key (`READ`).  DPO operators receive App `CAN_USE`; platform
administrators receive App `CAN_MANAGE`.

## Job behavior

- `eurostream_bronze_continuous` is a paused-by-default continuous Job
  wrapping the triggered Bronze pipeline.  Resume it from the Jobs UI only
  after the first update succeeds.
- `eurostream_fraud_stream` uses classic Jobs compute with the checked-in
  `18.2.x-scala2.13` baseline, because the current notebook uses a time-based
  Structured Streaming trigger and `awaitTermination()`. Serverless Jobs do not
  support that trigger. Supply a workspace-available node type, validate the
  runtime/state/checkpoint branch, and start the Job manually; the notebook
  bootstraps and validates the fraud sink before reading Kafka.
- `eurostream_medallion_refresh` sequences Silver and Gold and does not launch a
  second Bronze update. It assumes the reviewed fraud sink already exists; run
  the fraud notebook's explicit `bootstrap_only=true` preflight first.
- Quality, export, erasure, verification, benchmark, and the four-hour
  orchestrator use Serverless where supported.  The Article 17 graph has zero
  retries on its mutating/refresh stages; a failed stage is recorded and must be
  resumed with the same request identity rather than silently bypassed.
- The App submits only `eurostream_art17_erasure`; the DAB-local final stage
  performs current-view/export verification and state completion after the
  Silver/Gold refreshes. The barrier is fail-closed and runs before export. The
  physical cleanup confirmation remains an explicit runtime parameter and
  defaults to false; only the job-owned fraud sink is physically cleaned.

## App deployment

`databricks bundle deploy` creates/updates the App definition but does not
start it (`lifecycle.started: false`).  Review the App resources, then start it
from the Apps UI or with the current Databricks CLI.  The App source is synced
from the existing `app/` directory; generated `node_modules`, `dist`, and
coverage output are excluded.

## Validation status

No live bundle deployment or `databricks bundle validate` result is claimed
by this README.  The local environment used for this repair did not have the
Databricks CLI installed, so only YAML parsing, reference/path checks, the
current official Databricks bundle JSON schema, and DAB-local notebook
AST/lint checks were used for static review.  A live rehearsal must still
verify workspace/runtime availability, secret ACLs,
UC ownership/grants, the REPLACE USING source-plan restrictions, the fraud sink
bootstrap, the new Article 17 stage notebooks/barrier/conditional failure task,
the Serverless support/permissions for the explicitly confirmed fraud-sink
`REORG`/`VACUUM` operations, continuous Job resume, pipeline
`run_as.group_name` availability (private preview in the current API), the
reviewed physical-maintenance privilege requirements, and the App resource
bindings.

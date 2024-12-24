-- Durable, idempotent state for Databricks Workflows erasure commands.
-- Apply after 40_governance_ddl.sql. If an earlier preview of this table already
-- exists, add/backfill ticket_id, idempotency_key, requested_by, reason, and the
-- Silver/Gold refresh handoff fields before deploying the matching notebook.
-- The notebook MERGEs on command_id or
-- the App idempotency_key and refuses to mark a command completed unless logical,
-- physical, and governed export verification all pass.

USE CATALOG eurostream;
USE SCHEMA governance;

CREATE TABLE IF NOT EXISTS erasure_command_state (
  command_id                    STRING    NOT NULL COMMENT 'Stable command key; currently request_id',
  request_id                    STRING    NOT NULL,
  ticket_id                     STRING    NOT NULL COMMENT 'Support ticket; manual runs use manual',
  idempotency_key               STRING    NOT NULL COMMENT 'App replay key; manual runs default to request_id',
  customer_id                   STRING    NOT NULL,
  actor                         STRING    NOT NULL COMMENT 'Databricks principal or workflow actor',
  requested_by                  STRING    NOT NULL COMMENT 'Human or service request initiator',
  reason                        STRING    NOT NULL,
  workflow_run_id               STRING    COMMENT 'Databricks Workflows run/job identifier when available',
  silver_gold_refresh_run_id    STRING    COMMENT 'Synchronous downstream Lakeflow refresh run identifier',
  command_type                  STRING    NOT NULL DEFAULT 'article_17_erasure',
  status                        STRING    NOT NULL CHECK (status IN ('queued','running','completed','failed')),
  requested_at                  TIMESTAMP NOT NULL,
  started_at                    TIMESTAMP,
  completed_at                  TIMESTAMP,
  updated_at                    TIMESTAMP NOT NULL,
  attempt_count                 INT       NOT NULL CHECK (attempt_count > 0),
  internal_slo_seconds          BIGINT    NOT NULL CHECK (internal_slo_seconds > 0),
  internal_slo_state            STRING    NOT NULL CHECK (internal_slo_state IN ('pending','met','breached')),
  slo_deadline_at               TIMESTAMP,
  latency_seconds               DOUBLE,
  logical_verified              BOOLEAN,
  physical_verified             BOOLEAN,
  export_verified               BOOLEAN,
  logical_verification_detail   STRING    COMMENT 'JSON evidence for logical table checks',
  physical_verification_detail  STRING    COMMENT 'JSON evidence; job-owned fraud sink only, never Bronze/MV physical claim',
  export_verification_detail    STRING    COMMENT 'JSON evidence for lake.exports schema/count/content checks',
  layers_completed              STRING    NOT NULL DEFAULT '',
  blocked_layer                 STRING    COMMENT 'Required downstream action while status remains running',
  next_action                   STRING    COMMENT 'Workflow handoff instruction for operators or dependent tasks',
  failed_layer                  STRING,
  failure_message               STRING,
  confirmation_hash             STRING    NOT NULL COMMENT 'sha256(request_id:customer_id)[0:16]',
  export_root                   STRING    NOT NULL DEFAULT '/Volumes/eurostream/lake/exports',
  CHECK (
    status NOT IN ('queued', 'running')
    OR completed_at IS NULL
  ),
  CHECK (
    status <> 'completed'
    OR (
      completed_at IS NOT NULL
      AND internal_slo_state IN ('met', 'breached')
      AND logical_verified = TRUE
      AND physical_verified = TRUE
      AND export_verified = TRUE
      AND silver_gold_refresh_run_id IS NOT NULL
      AND blocked_layer IS NULL
      AND next_action IS NULL
      AND failed_layer IS NULL
    )
  )
)
USING DELTA
COMMENT 'Fail-closed, retryable Article 17 erasure command state and verification evidence.'
TBLPROPERTIES (
  'pii_classification' = 'confidential',
  'gdpr_layer' = 'audit',
  'delta.enableChangeDataFeed' = 'true'
);

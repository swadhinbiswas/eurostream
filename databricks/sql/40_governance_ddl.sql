-- Governance DDL — suppression registry, erasure audit log, quality results
-- These three tables power the compliance evidence chain.

USE CATALOG eurostream;
USE SCHEMA governance;

-- Layer 1 of the cascade: the durable suppression set. Every stream
-- anti-joins against this table before scoring/transformation, so an erased
-- customer can never re-enter downstream state (incl. replayed Kafka events).
CREATE TABLE IF NOT EXISTS suppression_registry (
  customer_id   STRING   NOT NULL,
  added_at      DOUBLE   NOT NULL COMMENT 'Unix epoch seconds of suppression'
)
USING DELTA
COMMENT 'Art.17 suppression registry — anti-join key for all streams.'
TBLPROPERTIES ('pii_classification' = 'confidential', 'gdpr_layer' = '1');

-- Tamper-evident audit log.
-- confirmation_hash = sha256(f"{request_id}:{customer_id}")[0:16]
-- (byte-for-byte identical to ErasureService._confirmation_hash).
CREATE TABLE IF NOT EXISTS erasure_audit_log (
  request_id        STRING   NOT NULL,
  customer_id       STRING   NOT NULL,
  requested_at      DOUBLE   NOT NULL,
  completed_at      DOUBLE   NOT NULL,
  layers_touched    STRING   NOT NULL COMMENT 'Comma-separated layer names',
  status            STRING   NOT NULL DEFAULT 'completed',
  confirmation_hash STRING   NOT NULL
)
USING DELTA
COMMENT 'Cryptographic audit trail for every Art.17 execution.'
TBLPROPERTIES ('pii_classification' = 'confidential', 'gdpr_layer' = 'audit');

-- Data-quality results (written by notebooks/01_quality_gates.py).
CREATE TABLE IF NOT EXISTS quality_results (
  run_id      STRING   NOT NULL,
  check_name  STRING   NOT NULL,
  passed      BOOLEAN  NOT NULL,
  detail      STRING,
  checked_at  TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Six-gate DQ report — one row per assertion per run.'
TBLPROPERTIES ('pii_classification' = 'internal');

-- Machine-readable PII column manifest (mirrors governance/pii_manifest.json).
CREATE TABLE IF NOT EXISTS pii_manifest (
  schema_name   STRING  NOT NULL,
  table_name    STRING  NOT NULL,
  column_name   STRING  NOT NULL,
  pii_flag      STRING  NOT NULL COMMENT 'EMAIL | IBAN | IP_ADDRESS | NAME | PHONE | ADDRESS | COUNTRY | NONE',
  classification STRING NOT NULL COMMENT 'restricted | pseudonymized | de-identified | internal'
)
USING DELTA
COMMENT 'Machine-readable PII column manifest for lineage + masking policies.'
TBLPROPERTIES ('pii_classification' = 'confidential');

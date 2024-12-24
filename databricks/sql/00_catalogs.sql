-- EuroStream — Unity Catalog bootstrap (run once, as metastore admin)
-- Creates the namespace used by the Databricks-native medallion lake.
-- Residency: the metastore and managed storage inherit the approved
-- workspace/account region selected in chapter 01; these statements do not
-- assume a particular cloud or region.

-- Preconditions: an EU metastore already exists and is assigned to this
-- workspace. Verify with:
--   SHOW METERING USAGE;
--   SELECT * FROM system.billing.customer_usage; -- (billing shows EU metering)

CREATE CATALOG IF NOT EXISTS eurostream
  COMMENT = 'EuroStream — GDPR-native streaming & medallion lakehouse (EU)'
  PROPERTIES (
    'purpose' = 'eurostream-platform',
    'data_classification' = 'gdpr-relevant'
  );

USE CATALOG eurostream;

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT = 'Raw capture. Clear-text PII — restricted, internal only. Art.17-maskable.'
  PROPERTIES ('pii_zone' = 'restricted');

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT = 'Cleansed & pseudonymized (salted SHA-256). Full-refresh materialized views.'
  PROPERTIES ('pii_zone' = 'pseudonymized');

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT = 'Curated, consent-gated pseudonymized aggregates. Restricted export only.'
  PROPERTIES ('pii_zone' = 'pseudonymized');

CREATE SCHEMA IF NOT EXISTS governance
  COMMENT = 'Suppression registry, erasure audit log, quality results, PII manifest.'
  PROPERTIES ('pii_zone' = 'confidential');

-- The export notebooks and volume paths use /Volumes/eurostream/lake/....
-- A volume cannot be created until its parent schema exists.
CREATE SCHEMA IF NOT EXISTS lake
  COMMENT = 'Restricted governed export landing zone. No Bronze clear-text PII.'
  PROPERTIES ('pii_zone' = 'pseudonymized');

-- Volume for checkpoints, inbox, and the governed lake export. Use fully
-- qualified names so the script does not depend on the caller's current
-- schema. The `lake` object is a schema; `lake.exports` is its volume.
CREATE VOLUME IF NOT EXISTS eurostream.bronze._checkpoints
  COMMENT = 'Structured Streaming / Auto Loader checkpoints';

CREATE VOLUME IF NOT EXISTS eurostream.bronze.inbox
  COMMENT = 'Partner file-drop inbox (Auto Loader source)';

CREATE VOLUME IF NOT EXISTS eurostream.lake.exports
  COMMENT = 'Restricted pseudonymized Silver/Gold Parquet export (optional remote sync)';

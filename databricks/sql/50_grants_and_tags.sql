-- Grants, PII tags, masking, and row filtering for Unity Catalog.
-- This file creates no pipeline-owned target tables. Run it as the metastore/
-- table owner after 00, governance bootstrap, first Lakeflow publication, and
-- the standalone fraud notebook's idempotent fraud_alerts bootstrap. Identity
-- groups are provisioned in Databricks account IAM; they are intentionally not
-- created as workspace roles here.
--
-- Suggested order: 00 -> 40 -> 60 -> publish Bronze -> publish Silver -> run
-- 03_fraud_streaming.py with bootstrap_only=true -> publish Gold -> apply this file.
-- Run the bootstrap-only notebook as a metastore/catalog admin (or an identity
-- that already has CREATE TABLE on bronze); target grants below cannot make a
-- not-yet-created table bootstrappable.
--
-- The group names below are policy parameters. Supply the account-specific
-- names when applying the functions (or replace the three constants in the
-- policy bindings in one controlled deployment). No user email addresses are
-- embedded in a policy.

USE CATALOG eurostream;
USE SCHEMA governance;

-- ---------------------------------------------------------------------------
-- Account-level governed tags. CREATE GOVERNED TAG is intentionally a one-time
-- account bootstrap statement (it has no IF NOT EXISTS form); subsequent runs
-- may start at the grants section.
-- ---------------------------------------------------------------------------
CREATE GOVERNED TAG eurostream_pii_email VALUES ('true', 'restricted');
CREATE GOVERNED TAG eurostream_pii_iban VALUES ('true');
CREATE GOVERNED TAG eurostream_pii_ip VALUES ('true');
CREATE GOVERNED TAG eurostream_pii_customer VALUES ('true');
CREATE GOVERNED TAG eurostream_pii_country VALUES ('true');
CREATE GOVERNED TAG eurostream_pseudonymized VALUES ('true');

-- ---------------------------------------------------------------------------
-- Catalog/schema/table grants. Groups replace the old workspace-role model.
-- ---------------------------------------------------------------------------
GRANT USE CATALOG eurostream TO GROUP eurostream_platform_admin;
GRANT ALL PRIVILEGES ON CATALOG eurostream TO GROUP eurostream_platform_admin;

GRANT USE CATALOG eurostream TO GROUP eurostream_pii_engineer;
GRANT USE SCHEMA eurostream.bronze TO GROUP eurostream_pii_engineer;
GRANT USE SCHEMA eurostream.silver TO GROUP eurostream_pii_engineer;
GRANT USE SCHEMA eurostream.governance TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.bronze.orders TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.bronze.orders_files TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.bronze.clicks TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.bronze.payments TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.bronze.erasure_requests TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.bronze.ingest_quarantine TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.bronze.fraud_alerts TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.silver.orders_quarantine TO GROUP eurostream_pii_engineer;
GRANT SELECT ON TABLE eurostream.silver.payments_quarantine TO GROUP eurostream_pii_engineer;

-- Analysts can read only pseudonymized governed relations and non-PII
-- governance evidence. They never receive Bronze SELECT.
GRANT USE CATALOG eurostream TO GROUP eurostream_analyst;
GRANT USE SCHEMA eurostream.silver TO GROUP eurostream_analyst;
GRANT SELECT ON TABLE eurostream.silver.customers TO GROUP eurostream_analyst;
GRANT SELECT ON TABLE eurostream.silver.orders TO GROUP eurostream_analyst;
GRANT SELECT ON TABLE eurostream.silver.payments TO GROUP eurostream_analyst;
GRANT USE SCHEMA eurostream.gold TO GROUP eurostream_analyst;
GRANT SELECT ON TABLE eurostream.gold.customer_360 TO GROUP eurostream_analyst;
GRANT SELECT ON TABLE eurostream.gold.order_facts TO GROUP eurostream_analyst;
GRANT SELECT ON TABLE eurostream.gold.fraud_summary TO GROUP eurostream_analyst;
GRANT USE SCHEMA eurostream.governance TO GROUP eurostream_analyst;
GRANT SELECT ON TABLE eurostream.governance.quality_results TO GROUP eurostream_analyst;

-- External consumers receive Gold only. Bronze is never an export source.
GRANT USE CATALOG eurostream TO GROUP eurostream_external_reader;
GRANT USE SCHEMA eurostream.gold TO GROUP eurostream_external_reader;
GRANT SELECT ON TABLE eurostream.gold.customer_360 TO GROUP eurostream_external_reader;
GRANT SELECT ON TABLE eurostream.gold.order_facts TO GROUP eurostream_external_reader;
GRANT SELECT ON TABLE eurostream.gold.fraud_summary TO GROUP eurostream_external_reader;

-- Count-only governance views keep the custom App off Silver and restricted
-- quarantine tables. They expose a customer identifier and aggregate counts,
-- never raw email, IBAN, IP address, payload, or PII hash columns.
CREATE OR REPLACE VIEW eurostream.governance.erasure_impact_counts (
  table_name STRING,
  customer_id STRING,
  row_count BIGINT
)
COMMENT 'Count-only Article 17 impact surface for authorized App operators.'
AS
SELECT 'silver.customers', customer_id, count(*)
FROM eurostream.silver.customers
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT 'silver.orders', customer_id, count(*)
FROM eurostream.silver.orders
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT 'silver.payments', customer_id, count(*)
FROM eurostream.silver.payments
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT 'silver.orders_quarantine', customer_id, count(*)
FROM eurostream.silver.orders_quarantine
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT 'silver.payments_quarantine', customer_id, count(*)
FROM eurostream.silver.payments_quarantine
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT 'gold.customer_360', customer_id, count(*)
FROM eurostream.gold.customer_360
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT 'gold.order_facts', customer_id, count(*)
FROM eurostream.gold.order_facts
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT 'gold.fraud_summary', customer_id, count(*)
FROM eurostream.gold.fraud_summary
WHERE customer_id IS NOT NULL
GROUP BY customer_id;

CREATE OR REPLACE VIEW eurostream.governance.showcase_table_counts (
  table_name STRING,
  row_count BIGINT
)
COMMENT 'Count-only inventory for the portfolio App.'
AS
SELECT 'silver.customers', count(*) FROM eurostream.silver.customers
UNION ALL SELECT 'silver.orders', count(*) FROM eurostream.silver.orders
UNION ALL SELECT 'silver.payments', count(*) FROM eurostream.silver.payments
UNION ALL SELECT 'gold.customer_360', count(*) FROM eurostream.gold.customer_360
UNION ALL SELECT 'gold.order_facts', count(*) FROM eurostream.gold.order_facts
UNION ALL SELECT 'gold.fraud_summary', count(*) FROM eurostream.gold.fraud_summary;

-- DPO/operator users are the only human group expected to open the custom App.
-- OBO SQL therefore receives governed Gold/governance access but no direct
-- Silver or quarantine table privilege.
GRANT USE CATALOG eurostream TO GROUP eurostream_dpo_operators;
GRANT USE SCHEMA eurostream.gold TO GROUP eurostream_dpo_operators;
GRANT SELECT ON TABLE eurostream.gold.customer_360 TO GROUP eurostream_dpo_operators;
GRANT SELECT ON TABLE eurostream.gold.fraud_summary TO GROUP eurostream_dpo_operators;
GRANT USE SCHEMA eurostream.governance TO GROUP eurostream_dpo_operators;
GRANT SELECT ON TABLE eurostream.governance.quality_results TO GROUP eurostream_dpo_operators;
GRANT SELECT ON TABLE eurostream.governance.suppression_registry TO GROUP eurostream_dpo_operators;
GRANT SELECT ON TABLE eurostream.governance.erasure_command_state TO GROUP eurostream_dpo_operators;
GRANT SELECT ON VIEW eurostream.governance.erasure_impact_counts TO GROUP eurostream_dpo_operators;
GRANT SELECT ON VIEW eurostream.governance.showcase_table_counts TO GROUP eurostream_dpo_operators;

-- The Lakeflow run-as identity is a member of this group and must be able to
-- read the append-only suppression registry even though it has no Bronze
-- human grant.
GRANT USE CATALOG eurostream TO GROUP eurostream_pipeline_service;
GRANT USE SCHEMA eurostream.governance TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.governance.suppression_registry
  TO GROUP eurostream_pipeline_service;
GRANT USE SCHEMA eurostream.bronze TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.bronze.orders TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.bronze.orders_files TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.bronze.payments TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.bronze.erasure_requests TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.bronze.clicks TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.bronze.ingest_quarantine TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.bronze.fraud_alerts TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.silver.customers TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.silver.orders TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.silver.payments TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.silver.orders_quarantine TO GROUP eurostream_pipeline_service;
GRANT SELECT ON TABLE eurostream.silver.payments_quarantine TO GROUP eurostream_pipeline_service;
GRANT CREATE TABLE ON SCHEMA eurostream.bronze TO GROUP eurostream_pipeline_service;
GRANT USE SCHEMA eurostream.silver TO GROUP eurostream_pipeline_service;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA eurostream.silver TO GROUP eurostream_pipeline_service;
GRANT USE SCHEMA eurostream.gold TO GROUP eurostream_pipeline_service;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA eurostream.gold TO GROUP eurostream_pipeline_service;
GRANT MODIFY ON TABLE eurostream.bronze.orders TO GROUP eurostream_pipeline_service;
GRANT MODIFY ON TABLE eurostream.bronze.orders_files TO GROUP eurostream_pipeline_service;
GRANT MODIFY ON TABLE eurostream.bronze.clicks TO GROUP eurostream_pipeline_service;
GRANT MODIFY ON TABLE eurostream.bronze.payments TO GROUP eurostream_pipeline_service;
GRANT MODIFY ON TABLE eurostream.bronze.erasure_requests TO GROUP eurostream_pipeline_service;
GRANT MODIFY ON TABLE eurostream.bronze.ingest_quarantine TO GROUP eurostream_pipeline_service;
-- Silver/Gold are pipeline-owned materialized views; publication refreshes
-- them through their owning definitions, not through direct table MODIFY.
GRANT READ VOLUME ON VOLUME eurostream.bronze.inbox TO GROUP eurostream_pipeline_service;
GRANT WRITE VOLUME ON VOLUME eurostream.bronze.inbox TO GROUP eurostream_pipeline_service;
GRANT READ VOLUME ON VOLUME eurostream.bronze._checkpoints TO GROUP eurostream_pipeline_service;
GRANT WRITE VOLUME ON VOLUME eurostream.bronze._checkpoints TO GROUP eurostream_pipeline_service;

-- Bounded quality/export Job identity. Pipeline tasks continue to execute as
-- eurostream_pipeline_service; this identity has no Bronze raw-event access.
GRANT USE CATALOG eurostream TO GROUP eurostream_job_runner;
GRANT USE SCHEMA eurostream.bronze TO GROUP eurostream_job_runner;
GRANT CREATE TABLE ON SCHEMA eurostream.bronze TO GROUP eurostream_job_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.bronze.fraud_alerts
  TO GROUP eurostream_job_runner;
GRANT USE SCHEMA eurostream.silver TO GROUP eurostream_job_runner;
GRANT USE SCHEMA eurostream.gold TO GROUP eurostream_job_runner;
GRANT USE SCHEMA eurostream.governance TO GROUP eurostream_job_runner;
GRANT USE SCHEMA eurostream.lake TO GROUP eurostream_job_runner;
GRANT SELECT ON TABLE eurostream.silver.customers TO GROUP eurostream_job_runner;
GRANT SELECT ON TABLE eurostream.silver.orders TO GROUP eurostream_job_runner;
GRANT SELECT ON TABLE eurostream.silver.payments TO GROUP eurostream_job_runner;
GRANT SELECT ON TABLE eurostream.gold.customer_360 TO GROUP eurostream_job_runner;
GRANT SELECT ON TABLE eurostream.gold.order_facts TO GROUP eurostream_job_runner;
GRANT SELECT ON TABLE eurostream.gold.fraud_summary TO GROUP eurostream_job_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.governance.quality_results
  TO GROUP eurostream_job_runner;
GRANT READ VOLUME ON VOLUME eurostream.lake.exports TO GROUP eurostream_job_runner;
GRANT WRITE VOLUME ON VOLUME eurostream.lake.exports TO GROUP eurostream_job_runner;

-- Continuous fraud Job: source credentials, current suppression snapshot,
-- fraud sink, and checkpoint only. No Bronze event or Silver/Gold access.
GRANT USE CATALOG eurostream TO GROUP eurostream_fraud_runner;
GRANT USE SCHEMA eurostream.bronze TO GROUP eurostream_fraud_runner;
GRANT CREATE TABLE ON SCHEMA eurostream.bronze TO GROUP eurostream_fraud_runner;
GRANT USE SCHEMA eurostream.governance TO GROUP eurostream_fraud_runner;
GRANT SELECT ON TABLE eurostream.governance.suppression_registry
  TO GROUP eurostream_fraud_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.bronze.fraud_alerts
  TO GROUP eurostream_fraud_runner;
GRANT READ VOLUME ON VOLUME eurostream.bronze._checkpoints TO GROUP eurostream_fraud_runner;
GRANT WRITE VOLUME ON VOLUME eurostream.bronze._checkpoints TO GROUP eurostream_fraud_runner;

-- Dedicated erasure runner. It may mutate only direct command targets and the
-- job-owned fraud sink. Silver/Gold are SELECT-only refresh barriers here.
GRANT USE CATALOG eurostream TO GROUP eurostream_erasure_runner;
GRANT USE SCHEMA eurostream.bronze TO GROUP eurostream_erasure_runner;
GRANT USE SCHEMA eurostream.silver TO GROUP eurostream_erasure_runner;
GRANT USE SCHEMA eurostream.gold TO GROUP eurostream_erasure_runner;
GRANT USE SCHEMA eurostream.governance TO GROUP eurostream_erasure_runner;
GRANT USE SCHEMA eurostream.lake TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.governance.suppression_registry
  TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.governance.erasure_command_state
  TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.governance.erasure_audit_log
  TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.bronze.orders TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.bronze.orders_files
  TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.bronze.clicks TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.bronze.payments TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.bronze.ingest_quarantine
  TO GROUP eurostream_erasure_runner;
GRANT SELECT, MODIFY ON TABLE eurostream.bronze.fraud_alerts
  TO GROUP eurostream_erasure_runner;
GRANT SELECT ON TABLE eurostream.silver.customers TO GROUP eurostream_erasure_runner;
GRANT SELECT ON TABLE eurostream.silver.orders TO GROUP eurostream_erasure_runner;
GRANT SELECT ON TABLE eurostream.silver.payments TO GROUP eurostream_erasure_runner;
GRANT SELECT ON TABLE eurostream.silver.orders_quarantine TO GROUP eurostream_erasure_runner;
GRANT SELECT ON TABLE eurostream.silver.payments_quarantine TO GROUP eurostream_erasure_runner;
GRANT SELECT ON TABLE eurostream.gold.customer_360 TO GROUP eurostream_erasure_runner;
GRANT SELECT ON TABLE eurostream.gold.order_facts TO GROUP eurostream_erasure_runner;
GRANT SELECT ON TABLE eurostream.gold.fraud_summary TO GROUP eurostream_erasure_runner;
GRANT READ VOLUME ON VOLUME eurostream.lake.exports TO GROUP eurostream_erasure_runner;

-- ---------------------------------------------------------------------------
-- Governed column tags. SET TAGS is the current ALTER COLUMN form; it is
-- separate from the value mask so classification and access control remain
-- independently auditable. Pipeline-created Bronze datasets use ALTER
-- STREAMING TABLE, Silver/Gold datasets use ALTER MATERIALIZED VIEW, and the
-- standalone fraud table remains an ordinary ALTER TABLE.
-- ---------------------------------------------------------------------------
ALTER STREAMING TABLE eurostream.bronze.orders ALTER COLUMN email
  SET TAGS (eurostream_pii_email = 'true');
ALTER STREAMING TABLE eurostream.bronze.orders ALTER COLUMN iban
  SET TAGS (eurostream_pii_iban = 'true');
ALTER STREAMING TABLE eurostream.bronze.orders ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');
ALTER STREAMING TABLE eurostream.bronze.orders ALTER COLUMN country
  SET TAGS (eurostream_pii_country = 'true');

ALTER STREAMING TABLE eurostream.bronze.orders_files ALTER COLUMN email
  SET TAGS (eurostream_pii_email = 'true');
ALTER STREAMING TABLE eurostream.bronze.orders_files ALTER COLUMN iban
  SET TAGS (eurostream_pii_iban = 'true');
ALTER STREAMING TABLE eurostream.bronze.orders_files ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');
ALTER STREAMING TABLE eurostream.bronze.orders_files ALTER COLUMN country
  SET TAGS (eurostream_pii_country = 'true');

ALTER STREAMING TABLE eurostream.bronze.clicks ALTER COLUMN ip_address
  SET TAGS (eurostream_pii_ip = 'true');
ALTER STREAMING TABLE eurostream.bronze.clicks ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');
ALTER STREAMING TABLE eurostream.bronze.clicks ALTER COLUMN country
  SET TAGS (eurostream_pii_country = 'true');

ALTER STREAMING TABLE eurostream.bronze.payments ALTER COLUMN iban
  SET TAGS (eurostream_pii_iban = 'true');
ALTER STREAMING TABLE eurostream.bronze.payments ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');
ALTER STREAMING TABLE eurostream.bronze.payments ALTER COLUMN country
  SET TAGS (eurostream_pii_country = 'true');
ALTER STREAMING TABLE eurostream.bronze.payments ALTER COLUMN merchant_country
  SET TAGS (eurostream_pii_country = 'true');

ALTER STREAMING TABLE eurostream.bronze.erasure_requests ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');

ALTER STREAMING TABLE eurostream.bronze.ingest_quarantine ALTER COLUMN raw_payload
  SET TAGS (eurostream_pii_email = 'restricted');
ALTER STREAMING TABLE eurostream.bronze.ingest_quarantine ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');

ALTER TABLE eurostream.bronze.fraud_alerts ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');

ALTER MATERIALIZED VIEW eurostream.silver.customers ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.customers ALTER COLUMN email_hash
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.customers ALTER COLUMN iban_hash
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.orders ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.orders ALTER COLUMN email_hash
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.orders ALTER COLUMN iban_hash
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.payments ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.payments ALTER COLUMN iban_hash
  SET TAGS (eurostream_pseudonymized = 'true');

ALTER MATERIALIZED VIEW eurostream.silver.orders_quarantine ALTER COLUMN email
  SET TAGS (eurostream_pii_email = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.orders_quarantine ALTER COLUMN iban
  SET TAGS (eurostream_pii_iban = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.orders_quarantine ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.payments_quarantine ALTER COLUMN iban
  SET TAGS (eurostream_pii_iban = 'true');
ALTER MATERIALIZED VIEW eurostream.silver.payments_quarantine ALTER COLUMN customer_id
  SET TAGS (eurostream_pii_customer = 'true');

ALTER MATERIALIZED VIEW eurostream.gold.customer_360 ALTER COLUMN customer_id
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.gold.customer_360 ALTER COLUMN email_hash
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.gold.order_facts ALTER COLUMN customer_id
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.gold.order_facts ALTER COLUMN email_hash
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.gold.order_facts ALTER COLUMN iban_hash
  SET TAGS (eurostream_pseudonymized = 'true');
ALTER MATERIALIZED VIEW eurostream.gold.fraud_summary ALTER COLUMN customer_id
  SET TAGS (eurostream_pseudonymized = 'true');

-- ---------------------------------------------------------------------------
-- Current UC masking/row-filter functions. The extra arguments are bound as
-- constant USING COLUMNS values, so group names are policy parameters rather
-- than hard-coded user identities. The pipeline service group is an explicit
-- exemption for writers; add its service principal to that account group.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION eurostream.governance.pii_clear_text_mask(
  val STRING,
  pii_group STRING,
  admin_group STRING,
  pipeline_group STRING,
  erasure_group STRING
)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member(pii_group)
    OR is_account_group_member(admin_group)
    OR is_account_group_member(pipeline_group)
    OR is_account_group_member(erasure_group)
  THEN val
  ELSE '<masked>'
END;

CREATE OR REPLACE FUNCTION eurostream.governance.bronze_restricted_row_filter(
  pii_group STRING,
  admin_group STRING,
  pipeline_group STRING,
  erasure_group STRING
)
RETURNS BOOLEAN
RETURN is_account_group_member(pii_group)
  OR is_account_group_member(admin_group)
  OR is_account_group_member(pipeline_group)
  OR is_account_group_member(erasure_group);

GRANT EXECUTE ON FUNCTION eurostream.governance.pii_clear_text_mask(
  STRING, STRING, STRING, STRING, STRING
) TO GROUP eurostream_platform_admin;
GRANT EXECUTE ON FUNCTION eurostream.governance.pii_clear_text_mask(
  STRING, STRING, STRING, STRING, STRING
) TO GROUP eurostream_pii_engineer;
GRANT EXECUTE ON FUNCTION eurostream.governance.pii_clear_text_mask(
  STRING, STRING, STRING, STRING, STRING
) TO GROUP eurostream_analyst;
GRANT EXECUTE ON FUNCTION eurostream.governance.pii_clear_text_mask(
  STRING, STRING, STRING, STRING, STRING
) TO GROUP eurostream_external_reader;
GRANT EXECUTE ON FUNCTION eurostream.governance.pii_clear_text_mask(
  STRING, STRING, STRING, STRING, STRING
) TO GROUP eurostream_pipeline_service;
GRANT EXECUTE ON FUNCTION eurostream.governance.pii_clear_text_mask(
  STRING, STRING, STRING, STRING, STRING
) TO GROUP eurostream_erasure_runner;
GRANT EXECUTE ON FUNCTION eurostream.governance.bronze_restricted_row_filter(
  STRING, STRING, STRING, STRING
) TO GROUP eurostream_platform_admin;
GRANT EXECUTE ON FUNCTION eurostream.governance.bronze_restricted_row_filter(
  STRING, STRING, STRING, STRING
) TO GROUP eurostream_pii_engineer;
GRANT EXECUTE ON FUNCTION eurostream.governance.bronze_restricted_row_filter(
  STRING, STRING, STRING, STRING
) TO GROUP eurostream_analyst;
GRANT EXECUTE ON FUNCTION eurostream.governance.bronze_restricted_row_filter(
  STRING, STRING, STRING, STRING
) TO GROUP eurostream_external_reader;
GRANT EXECUTE ON FUNCTION eurostream.governance.bronze_restricted_row_filter(
  STRING, STRING, STRING, STRING
) TO GROUP eurostream_pipeline_service;
GRANT EXECUTE ON FUNCTION eurostream.governance.bronze_restricted_row_filter(
  STRING, STRING, STRING, STRING
) TO GROUP eurostream_erasure_runner;

-- Clear-text columns are masked at read time. The same function is used for
-- the canonical file table and both quarantine payloads.
ALTER STREAMING TABLE eurostream.bronze.orders ALTER COLUMN email SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.orders ALTER COLUMN iban SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.orders_files ALTER COLUMN email SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.orders_files ALTER COLUMN iban SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.payments ALTER COLUMN iban SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.clicks ALTER COLUMN ip_address SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.erasure_requests ALTER COLUMN customer_id SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.ingest_quarantine ALTER COLUMN raw_payload SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER MATERIALIZED VIEW eurostream.silver.orders_quarantine ALTER COLUMN email SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER MATERIALIZED VIEW eurostream.silver.orders_quarantine ALTER COLUMN iban SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER MATERIALIZED VIEW eurostream.silver.payments_quarantine ALTER COLUMN iban SET MASK
  eurostream.governance.pii_clear_text_mask
  USING COLUMNS ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');

-- Row filters are defence in depth for any accidentally broad Bronze grant.
-- The pipeline and erasure service groups must be members of the third/fourth
-- parameters.
ALTER STREAMING TABLE eurostream.bronze.orders SET ROW FILTER
  eurostream.governance.bronze_restricted_row_filter
  ON ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.orders_files SET ROW FILTER
  eurostream.governance.bronze_restricted_row_filter
  ON ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.clicks SET ROW FILTER
  eurostream.governance.bronze_restricted_row_filter
  ON ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.payments SET ROW FILTER
  eurostream.governance.bronze_restricted_row_filter
  ON ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.erasure_requests SET ROW FILTER
  eurostream.governance.bronze_restricted_row_filter
  ON ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER STREAMING TABLE eurostream.bronze.ingest_quarantine SET ROW FILTER
  eurostream.governance.bronze_restricted_row_filter
  ON ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
ALTER TABLE eurostream.bronze.fraud_alerts SET ROW FILTER
  eurostream.governance.bronze_restricted_row_filter
  ON ('eurostream_pii_engineer', 'eurostream_platform_admin', 'eurostream_pipeline_service', 'eurostream_erasure_runner');
-- ---------------------------------------------------------------------------
-- Export boundary: only Silver/Gold are exportable. Bronze, quarantine, and
-- the raw inbox/checkpoint volumes are intentionally not granted to readers.
-- ---------------------------------------------------------------------------
-- Example external location (configure in the account, not in this pipeline):
-- CREATE EXTERNAL LOCATION IF NOT EXISTS eurostream_lake_eu
--   URL 'abfss://<container>@<storage-account>.dfs.core.windows.net/'
--   WITH (CREDENTIAL `eurostream_storage_cred`);

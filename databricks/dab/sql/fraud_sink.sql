-- DAB-local bootstrap owned by the fraud Job.
--
-- This is intentionally the only table bootstrap in the optional Bundle.  It
-- does not create the Bronze/Silver/Gold pipeline targets; those relations are
-- created by the Lakeflow pipeline definitions on first update.  The fraud
-- notebook MERGEs into this table, so the fraud Job must be able to create it
-- before it starts the streaming query.

CREATE TABLE IF NOT EXISTS eurostream.bronze.fraud_alerts (
  alert_id          STRING    NOT NULL COMMENT 'Deterministic global alert key',
  customer_id       STRING    NOT NULL,
  rule              STRING    NOT NULL CHECK (rule IN ('VELOCITY', 'AMOUNT_ZSCORE', 'GEO_MISMATCH')),
  score             DOUBLE    NOT NULL,
  detail            STRING,
  alerted_at        TIMESTAMP NOT NULL,
  window_start      TIMESTAMP,
  window_end        TIMESTAMP,
  _created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (rule)
COMMENT 'Idempotent fraud alerts; created and owned by the fraud Lakeflow Job.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pii_classification' = 'confidential',
  'idempotency_key' = 'alert_id'
);

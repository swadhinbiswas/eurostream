# EuroStream architecture

## Overview

EuroStream is a Lambda architecture — a speed path for real-time fraud scoring
and a batch path for accuracy — sharing a durable event log. Every component
implements the same interface as its production counterpart (Kafka, Spark
Structured Streaming, Snowflake, Airflow), so swapping to managed services is a
wiring change, not a rewrite.

The key insight: the event log is the single source of truth. Both paths read
from it, but neither depends on the other's state. The speed path gives you
low-latency alerts. The batch path gives you a deduplicated, consent-aware,
quality-gated warehouse. The erasure service fans out through the log to every
layer.

## Topology

```
 Source systems (simulated, pure Python: order service · clickstream · payments)
      │  orders · clicks · payments  (JSON events, schema_version)
      ▼
 EventBus  (durable append-only log — SQLite locally, Kafka on deploy)
      │
      ├─────────────► STREAM PATH (speed)                       BATCH PATH (accuracy) ◄─
      │              FraudScorer: windowed rules                │
      │              · VELOCITY (>N payments / window)          │ Bronze: raw append
      │              · AMOUNT_ZSCORE (deviation from mean)      │   │
      │              · GEO_MISMATCH (billing vs merchant)       │   ▼
      │              │                                          │ PII gate: classify +
      │              ▼                                          │ manifest (fail on drift)
      │        fraud_alerts (topic) ──────────► Bronze          │   ▼
      │                                                  Silver: dedup, typed, PII-hashed
      │                                                        Gold: customer_360 · order_facts · fraud_summary
      │                                                        (consent-aware marketing view)
      ▼
 ErasureService (GDPR Art. 17): tombstone ─► suppress stream ─► anonymize Bronze ─► delete Silver/Gold ─► audit log
```

## Components

| Component | File | What it does |
|-----------|------|-------------|
| Config | `src/eurostream/config.py` | Pydantic Settings, env-driven (`EUROSTREAM_*` prefix), `.env` support |
| Event models | `src/eurostream/models.py` | Typed Pydantic contracts with `schema_version` for forward compatibility |
| Schema contracts | `src/eurostream/contracts.py` | Registry that snapshots current schemas; CI checks drift against a committed baseline |
| Event bus | `src/eurostream/bus/` | `Producer`/`Consumer` ABC with SQLite and Kafka implementations |
| Producers | `src/eurostream/producers.py` | Simulated EU source systems (orders, clicks, payments) using Faker |
| Fraud scoring | `src/eurostream/streaming.py` | `FraudScorer` with rolling windows; `FraudStreamProcessor` bridges the bus to the scorer |
| Warehouse | `src/eurostream/warehouse.py` | DuckDB medallion: Bronze → Silver → Gold, plus governance schema |
| PII governance | `src/eurostream/governance/pii.py` | Pure-Python classifier with ISO 13616 mod-97 IBAN validation |
| Erasure | `src/eurostream/governance/erasure.py` | GDPR Art. 17 cascade: tombstone → suppress → anonymize → delete → audit |
| Data quality | `src/eurostream/quality.py` | Gate engine: uniqueness, referential integrity, PII-not-clear, consent gating |
| Orchestration | `src/eurostream/orchestration.py` | Python DAG executor with task dependencies and timing (Airflow-shaped, zero scheduler) |
| API | `src/eurostream/api.py` | FastAPI: erasure endpoint, health, metrics, governance queries |
| Metrics | `src/eurostream/metrics.py` | Prometheus-shaped counters and histograms written to JSONL |
| CLI | `src/eurostream/cli.py` | Typer CLI: `demo`, `produce`, `stream`, `transform`, `erase`, `contracts` |

## Medallion layers

The warehouse follows the medallion architecture (Bronze → Silver → Gold), each
schema with a distinct PII policy:

### Bronze (raw capture)

Append-only rows exactly as ingested from the event bus. PII is present in
clear text — this is intentional. Bronze is the raw capture layer, and its
contents are anonymized on erasure. It's the audit trail for what actually
happened.

Tables: `bronze.orders`, `bronze.clicks`, `bronze.payments`, `bronze.fraud_alerts`

### Silver (typed, deduped, PII-hashed)

Built from Bronze by the medallion DAG:

- **Customers**: deduplicated by `customer_id`, PII hashed (salted SHA-256),
  first/last seen timestamps, consent flag. This is the last layer that
  "knows" PII at all, and only in hashed form.
- **Orders**: deduplicated by `order_id` using `row_number()` over natural key
- **Payments**: deduplicated by `payment_id`, same approach

Tables: `silver.customers`, `silver.orders`, `silver.payments`

### Gold (business aggregates)

Built from Silver. Consent-aware — the marketing view is gated on
`marketing_consent`:

- **customer_360**: total orders, total spend, average order value, fraud flag,
  consent status
- **order_facts**: individual orders with customer linkage
- **fraud_summary**: aggregated fraud alerts by customer and rule

Tables: `gold.customer_360`, `gold.order_facts`, `gold.fraud_summary`

### Governance schema

Audit and metadata tables:

- `governance.erasure_audit_log`: tamper-evident records of every erasure execution
- `governance.pii_manifest`: machine-readable column-level PII classification
- `governance.data_quality_runs`: history of quality gate results

## Speed path vs. batch path

This is the Lambda split, and it's worth understanding why both exist:

**Speed path** (streaming fraud scoring):
- Consumes the `payments` topic in real time
- Maintains per-customer rolling windows (configurable size, default 300s)
- Fires alerts when velocity, amount, or geo rules trigger
- Alerts land on the `fraud_alerts` topic and get ingested into Bronze
- This is why the streaming path exists — you can't wait for a batch job to
  catch a fraud burst

**Batch path** (medallion warehouse):
- Re-reads the durable event log from `earliest`
- Builds Bronze (raw), scans for PII, builds Silver (typed, deduped, hashed),
  builds Gold (aggregated)
- Runs a data quality gate that fails the DAG if anything's wrong
- This is the source of truth for reporting — the batch path trades latency
  for accuracy and governance

The two paths share the event log but not state. A bad transform in the batch
path doesn't affect the speed path, and vice versa. The batch path is always
replayable from the log.

## Data flow through the erasure cascade

When a customer exercises their right to erasure (GDPR Art. 17):

```
erasure_requested (tombstone on bus)
        │
        ▼
suppression_registry (add customer_id → streaming stops)
        │
        ▼
Bronze anonymize (email, iban, ip_address → <anonymized>)
        │
        ▼
Silver DELETE (customers, orders, payments)
        │
        ▼
Gold DELETE (customer_360, order_facts, fraud_summary)
        │
        ▼
audit_log (SHA-256 confirmation hash, layers touched, timing)
```

Everything is idempotent — running the same erasure twice produces the same end
state. If the worker dies mid-cascade, the uncommitted offset replays the
tombstone on restart and the cascade re-runs.

## Deployment targets

### Local (zero containers)

```bash
uv run eurostream demo
```

SQLite for the bus, DuckDB for the warehouse, in-process fraud scoring. This is
the primary development mode — everything runs in a single Python process.

### Docker

```bash
docker build -t eurostream .
docker compose up
```

Runs the FastAPI image with a health check. The container uses SQLite locally
by default; swap to Kafka via environment variables for deployment.

### Cloud

Terraform (`infra/main.tf`) provisions AWS resources in `eu-central-1`:

- S3 bucket (versioned, AES-256 encrypted) for the object lake
- IAM role for pipeline workers (GetObject, PutObject, DeleteObject, ListBucket)
- MSK (Kafka) cluster with TLS encryption, 3 broker nodes

The application code doesn't change — the bus adapter and warehouse connection
swap via config. Same topics, same consumer groups, same record shape.

## SLOs

Defined in config, measured in the demo output:

| SLO | Default | How it's measured |
|-----|---------|------------------|
| Fraud alert latency | within one `fraud_window_seconds` window (300s) | Alerts surface as the scorer processes the payments topic |
| Erasure SLA | 60s (`EUROSTREAM_ERASURE_SLA_SECONDS`) | `erasure_latency` metric; `erasure_sla_breach` counter if exceeded |
| Data quality | all checks pass | DAG aborts if any gate fails |

## Error handling

The system is designed for recoverability:

- **Event log is append-only**: consumers track committed offsets, so a bad
  transform is undone by re-running from a clean Silver/Gold
- **Erasure is idempotent**: same request → same outcome, every time
- **Tombstone-on-bus pattern**: if the erasure worker crashes between
  "tombstone published" and "cascade complete", the uncommitted offset replays
  on restart
- **PII gate fails fast**: unregistered PII columns abort the DAG before they
  propagate to Silver/Gold
- **Schema contracts in CI**: breaking drift is caught before merge, not in
  production

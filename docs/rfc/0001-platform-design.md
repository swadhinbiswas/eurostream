# RFC 0001 — EuroStream: GDPR-Compliant Real-Time Analytics Platform

| | |
|---|---|
| Status | Approved (implemented) |
| Author | Platform Engineering |
| Date | 2026-08-01 |
| Last updated | 2026-08-01 |

## Problem statement

We need a data platform that does four things:

1. Ingests clickstream, order, and payment events in real time
2. Detects payment fraud within seconds
3. Produces a governed "Customer 360" for analytics and marketing
4. Satisfies GDPR obligations — especially Article 17 right to erasure and EU
   data residency — by design, not as an afterthought

The primary constraint is that this must be demonstrated as a portfolio artifact:
runnable on a laptop with no commercial cloud spend, while still being
architecturally honest about how it would run at production scale.

That last part matters. Most portfolio projects show you how to move data. This
one shows you how to govern it. The EU data engineering market cares about the
governance layer, so that's where the design effort went.

## Goals and non-goals

### Goals

- End-to-end pipeline from simulated source systems to governed gold tables
- Fraud detection with sub-minute (here: near-instant) latency on the speed path
- A working, demonstrable right-to-erasure cascade across raw, streaming, and
  warehouse layers, with an auditable trail
- Consent-aware data modeling: no non-consenting customer flows into
  marketing-facing aggregates
- EU residency enforced and documented (ADR 0001)

### Non-goals (for the initial build)

- **Multi-tenant BI tooling** (Metabase/Looker) — self-service is represented
  by the read-only gold-layer query API
- **A full Kubernetes deployment** — Docker files are provided; the DAG
  abstraction is scheduler-agnostic so Airflow/Dagster can adopt it without
  rewrites
- **Real credit-card fraud ML** — the fraud scorer is a rules engine on
  velocity, amount-anomaly, and geo-mismatch features; the architecture is
  designed so swapping in a trained model is a scorer implementation change

## Considered architectures

### Option A — Lambda: Kafka → (Spark Streaming + Airflow/dbt) → warehouse

The canonical EU data stack. Split a speed path (streaming fraud scoring) from
a batch path (Bronze → Silver → Gold) for accuracy.

```
sources ──► Kafka ──┬──► Spark Streaming: FraudScorer ──► fraud_alerts
                    │
                    └──► Airflow DAG: Bronze → Silver → Gold
```

**Why we rejected it (as the implementation):** The JVM runtime (Spark/Flink)
and the Kafka broker are not pure Python and complicate the "runs on a laptop,
no containers" requirement. The *shape* is retained — the interfaces,
the data flow, the layer semantics — but the implementation is pure Python.

### Option B — Pure-Python port with the same topology

Implement the identical topology in pure Python: a durable append-only event
log standing in for Kafka, windowed in-process scoring standing in for Spark
Structured Streaming, DuckDB standing in for Snowflake, and a Python DAG
executor standing in for Airflow. Each component implements the interface its
production counterpart exposes, so a future swap is a wiring change.

```
sources ──► EventBus (SQLite; Kafka adapter on deploy)
              ├──► FraudScorer (windowed rules) ──► fraud_alerts
              └──► Bronze → PII gate → Silver → Gold
                       │
                       ▼
              ErasureService: tombstone → anonymize/delete → audit
```

**This is what we chose.** The interfaces (`Producer`/`Consumer`, Event models,
DAG tasks, warehouse SQL) are drawn directly from Option A's design, so the
portfolio story maps 1:1 onto the real stack while remaining dependency-free
at runtime.

### Option C — Single-process everything, no event log

Ingest straight into a warehouse, no log, no streaming path.

```
sources ──► warehouse (single process)
```

**Why we rejected it:** Loses the exactly-once boundary and the speed path;
the GDPR tombstone fan-out has nothing to fan out through. Too far from
production to be honest about the architecture.

## Chosen approach

Lambda architecture (Option A) implemented in pure Python (Option B):

```
sources ──► EventBus (durable log; Kafka adapter on deploy)
              ├──► Streaming: FraudScorer ──► fraud_alerts (speed)
              └──► Batch: Bronze ─► PII gate ─► Silver ─► Gold (accuracy)
                       │
                       ▼
              ErasureService: tombstone ─► anonymize/delete ─► audit log
```

### Key design decisions

1. **Event log semantics mirror Kafka.** Topics, partitions, consumer groups,
   and committed offsets. `auto_offset_reset="earliest"` gives replayability;
   the same interface is implemented against `confluent-kafka` when deployed.
   The SQLite implementation uses WAL mode for durability.

2. **Speed path is stateless-ish and windowed.** Velocity/amount/geo rules run
   in per-customer rolling windows with bounded state (windows expire after
   inactivity). The scorer doesn't hold unbounded history — it drops windows
   whose customer has been inactive for a full window period.

3. **Batch path is the source of truth for reporting.** Silver deduplicates
   (`row_number()` on natural key) and types; Gold aggregates consent-aware.
   A data-quality gate blocks the DAG on uniqueness, referential integrity,
   consent leaks, and clear-text PII. The batch path re-reads the durable log
   from `earliest`, so it's always replayable.

4. **Erasure is a tombstone fan-out, not a cascade of guesses.** The request
   lands on the bus first (auditable, durable), then the worker anonymizes
   Bronze, deletes Silver/Gold rows, updates the streaming suppression
   registry, and writes a confirmation-hashed audit record. The same code
   path runs as the local worker and as the API handler.

5. **Contracts are checked in CI.** A committed `contracts.json` snapshot plus
   a drift check in CI blocks breaking schema changes before merge. The check
   is fast (JSON diff) and runs on every PR.

### Why not just use Presidio for PII?

Microsoft Presidio is the industry standard for PII detection. We considered it
and decided against it for this artifact:

- Presidio pulls large model artifacts (hundreds of MB) and requires network
  access for downloads — violates the "runs on a laptop, no internet" goal
- The recognizer interface we built mirrors what a Presidio adapter would look
  like, so the swap is straightforward in production
- Our recognizer set (email, IBAN with mod-97, IPv4, phone, conservative name
  heuristic) covers the PII types in the simulated data
- The governance gate (fail on unregistered PII columns) is the real value;
  the specific recognizer implementation is secondary

### Why salted hash instead of format-preserving encryption?

The Silver layer hashes PII with `sha256(salt:value)`. Format-preserving
encryption (FPE) would preserve the analytics utility of masked values (you
could still compute aggregates on encrypted IBANs), but:

- FPE requires a key management system (the salt must be secret in production)
- Hashing is simpler to audit and verify — a DSAR team can confirm the value
  is gone without needing to decrypt
- For a portfolio artifact, the tradeoff favors simplicity and auditability

This is noted as an open question for production deployment (see below).

## Implementation details

### Event bus

The `Producer`/`Consumer` interface (`src/eurostream/bus/__init__.py`) defines:

```python
class Producer(ABC):
    def produce(self, topic, key, value, headers=None) -> None: ...

class Consumer(ABC):
    def poll(self, timeout) -> Record | None: ...
    def commit(self) -> None: ...
```

Two implementations:

- `SqliteBus` (`src/eurostream/bus/sqlite.py`): durable append-only log on
  SQLite WAL mode with per-topic partitions and committed consumer-group offsets
- `KafkaProducer`/`KafkaConsumer` (`src/eurostream/bus/kafka.py`): thin adapters
  over `confluent-kafka`, selected by `EUROSTREAM_EVENT_BUS_BACKEND=kafka`

### Fraud scoring

`FraudScorer` (`src/eurostream/streaming.py`) maintains per-customer rolling
windows with three rules:

| Rule | Threshold | Severity | What it catches |
|------|-----------|----------|----------------|
| VELOCITY | > N payments per window | HIGH | Burst of rapid payments (card testing, account takeover) |
| AMOUNT_ZSCORE | > k standard deviations from customer mean | MEDIUM | Unusually large or small payment for this customer |
| GEO_MISMATCH | billing country ≠ merchant country | LOW | Cross-border fraud attempts |

Windows expire after `fraud_window_seconds` of inactivity per customer, so
the state map doesn't grow unboundedly.

### Medallion warehouse

`Warehouse` (`src/eurostream/warehouse.py`) manages four DuckDB schemas:

- **bronze**: raw append-only, exact copies of ingested events
- **silver**: deduplicated (`row_number()` over natural key), typed, PII-hashed
- **gold**: consent-aware business aggregates
- **governance**: audit log, PII manifest, data quality runs

The SQL is standard enough to target Snowflake/BigQuery by swapping the
connection string.

### Data quality gate

`DataQualityEngine` (`src/eurostream/quality.py`) runs five checks:

1. `gold.customer_360.customer_id_unique` — no duplicate customer IDs
2. `gold.order_facts.order_id_unique` — no duplicate order IDs
3. `silver.customers.email_hash_not_clear` — email is hashed
4. `silver.customers.iban_hash_not_clear` — IBAN is hashed
5. `consent_gating` — no non-consenting customers in marketing view
6. `gold.order_facts.customer_id_references_gold.customer_360` — referential
   integrity

Any failure aborts the DAG. Results are recorded to
`governance.data_quality_runs` for audit.

### Orchestration

`DAG` (`src/eurostream/orchestration.py`) is a minimal Python DAG executor:

```python
dag = DAG(
    dag_id="medallion",
    tasks=[
        DAGTask("pii_scan", pii_scan_fn, depends_on=[]),
        DAGTask("build_silver", silver_fn, depends_on=["pii_scan"]),
        DAGTask("build_gold", gold_fn, depends_on=["build_silver"]),
        DAGTask("quality_gate", dq_fn, depends_on=["build_gold"]),
    ],
)
results = dag.run()
```

Tasks execute in topological order with timing. The DAG is scheduler-agnostic —
the same task definitions can be wrapped in Airflow operators or Dagster ops
without rewrites.

## Rollback plan

- The event log is append-only; consumers track offsets, so a bad transform is
  undone by re-running the DAG from a clean Silver/Gold (Bronze is never
  mutated by transforms)
- Erasure is the one *destructive* path. It is idempotent (same request
  produces the same outcome), confirmation-hashed, and only touches the
  targeted `customer_id`
- Rollback of an *erroneous* erasure is impossible by design — this is the
  GDPR-correct property — so the guard is prevention: the API accepts a bounded
  request payload and logs the operator who issued it
- The `erasure_requests` topic is consumed from `earliest`, so a crash between
  "tombstone published" and "cascade complete" is recovered on restart

## Open questions

- **Format-preserving encryption** of IBAN at rest (vs. salted hash) — hashes
  are used here; FPE would preserve analytics on masked values but requires
  key management
- **Column-level security** in the warehouse (roles) — deferred to the managed
  warehouse (Snowflake/BigQuery) where it is native
- **Multi-region DR** — any future DR adds an EU-only second region to stay
  within the same legal boundary
- **Real-time erasure propagation** — the current worker polls; a production
  system would use a long-lived consumer with heartbeats

<div align="center">

# EuroStream

A Python reference implementation for streaming fraud detection, medallion analytics, and GDPR erasure workflows in European commerce.

[![CI Pipeline](https://github.com/swadhinbiswas/eurostream/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/swadhinbiswas/eurostream/actions/workflows/ci.yml)
[![Orchestration DAG](https://github.com/swadhinbiswas/eurostream/actions/workflows/orchestrate.yml/badge.svg?branch=master)](https://github.com/swadhinbiswas/eurostream/actions/workflows/orchestrate.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Tests Passing](https://img.shields.io/badge/tests-77%20passed-brightgreen?style=flat-square)](https://github.com/swadhinbiswas/eurostream/actions)
[![Mypy Strict](https://img.shields.io/badge/mypy-strict-2b94ec?style=flat-square)](https://mypy.readthedocs.io)
[![Ruff](https://img.shields.io/badge/linter-ruff-black?style=flat-square)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/license-MIT-black?style=flat-square)](LICENSE)

[Live documentation](https://eurostream-docs.pages.dev) · [Public Parquet lake](https://huggingface.co/datasets/swadhinbiswas/eustream) · [JOSS research paper](paper/paper.md) · [Architecture RFC](docs/rfc/0001-platform-design.md)

</div>

## Why erasure needs an architecture

Append-only event logs and immutable Parquet files are efficient for analytics. They also make targeted erasure awkward. Deleting a source row does not remove the same person's data from broker partitions, derived tables, processor memory, replicas, or exported files.

EuroStream treats erasure as a cross-system workflow. It pseudonymizes records at the Silver boundary, can pass suppression state to the streaming processor, updates the warehouse, and exposes a verification endpoint. The repository is an implementation of those patterns, not a certification of GDPR compliance or a guarantee of physical deletion from external systems.

The design is based on four parts of GDPR:

1. Article 17: support requests for erasure without undue delay.
2. Articles 6 and 7: carry marketing consent into analytical records and check that downstream transformations preserve it.
3. Article 25: use pseudonymization and data minimization in the storage design.
4. Article 32: keep clear-text PII inside the internal boundary and protect the systems that process it.

Article 83(5) sets administrative fines at up to €20,000,000 or 4% of worldwide annual turnover, whichever is higher.

<p align="center">
  <img src="assets/GDRP.png" alt="Conflict between append-only lakehouse storage and GDPR erasure requirements" width="920"/>
</p>

The local runtime includes event production, fraud scoring, Bronze, Silver, and Gold transformations, optional Turso synchronization, local Parquet export, a FastAPI dashboard, and a staged erasure request. Some external and runtime steps remain separate operations; the sections below state where the current implementation stops.

## Quickstart

### Prerequisites

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) package manager (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

### Run the local demo

The demo runs synthetic orders, clicks, and payments through fraud scoring, the medallion pipeline, an erasure request, and a local verification pass.

```bash
git clone https://github.com/swadhinbiswas/eurostream.git
cd eurostream
uv sync
uv run eurostream demo
```

### Run each pipeline stage

The stages can also be run independently:

```bash
# 1. Produce 500 synthetic EU orders, clicks, and payments onto the bus
uv run eurostream produce --events 500

# 2. Consume payments and score fraud anomalies in real time
uv run eurostream stream --max-events 500

# 3. Execute Medallion DAG (Bronze -> Silver -> Gold -> Quality Gates -> Lake Export)
uv run eurostream transform --incremental

# 4. Probe & sync local warehouse state to Turso cloud database
uv run eurostream probe-turso
uv run eurostream sync-turso

# 5. Execute synchronous right-to-erasure for a target customer
uv run eurostream erase cust_424242

# 6. Verify schema contracts against committed baseline
uv run eurostream contracts --baseline governance/contracts.json
```

### Start the web UI and API

```bash
uv run uvicorn eurostream.api:app --reload --port 7860
```

Open [http://localhost:7860/](http://localhost:7860/) for the demo dashboard:

- Overview shows warehouse throughput, consent distribution, and fraud rule counts.
- Fraud Intelligence lists anomaly alerts and supports rule filters.
- Medallion and 360 provides Customer 360 search and erasure controls.
- GDPR Art. 17 shows the local erasure flow and confirmation record.
- Prometheus Explorer provides access to the in-process metrics endpoint.

The API queues erasure requests by default and does not start a worker process. Use synchronous local execution or run a worker separately when testing the queued path. Some dashboard panels are shells because the current tab loader does not populate every view.

## System architecture

EuroStream separates streaming scoring from batch transformation. Both paths use the event bus and warehouse, and the CLI writes streaming alerts to the `bronze.fraud_alerts` table consumed by the Gold transform.

<p align="center">
  <img src="assets/endtoendsystem.png" alt="EuroStream end-to-end system architecture" width="940"/>
</p>

- The streaming path scores payment events and records fraud alerts.
- The batch path moves data through `Bronze`, `Silver`, and `Gold` with watermarks and quality checks.
- `SqliteBus` uses SQLite WAL and `BEGIN IMMEDIATE` locally; `KafkaBus` connects to Aiven with SASL_SSL and SCRAM-SHA-256 in hosted environments.

### Medallion storage

<p align="center">
  <img src="assets/medallion-pipeline.png" alt="EuroStream medallion storage and governance pipeline" width="920"/>
</p>

| Layer | Tables | Storage and governance | Processing |
|---|---|---|---|
| Bronze | `bronze.orders`<br/>`bronze.clicks`<br/>`bronze.payments`<br/>`bronze.fraud_alerts` | Raw events remain inside the internal warehouse. Public exports exclude Bronze. An erasure request replaces selected raw PII fields with `<anonymized>`. | Batch append with `INSERT OR IGNORE` and deterministic `event_id` keys. |
| Silver | `silver.customers`<br/>`silver.orders`<br/>`silver.payments` | Deduplicated with `row_number()` and salted SHA-256 values for configured PII fields. The exported customer identifier remains stable, so these records are pseudonymized rather than anonymous. | Incremental merge using `occurred_at > watermark`. |
| Gold | `gold.customer_360`<br/>`gold.order_facts`<br/>`gold.fraud_summary` | Curated aggregates combine consent with `bool_and(marketing_consent)`. The quality gate checks that transformations preserve the flag; this repository does not include a downstream marketing-serving filter. | Local Parquet partitions under `data/lake/`, with Hugging Face upload handled separately. |

The incremental path reduces repeated work by reading only newer records. No benchmark in this repository measures the incremental compute reduction, so the table does not claim a percentage.

## Erasure design

The warehouse erasure workflow updates suppression, audit, Bronze, Silver, and Gold records through independent DuckDB statements. Optional Turso operations and a local lake re-export hook run outside the warehouse transaction. A failure in one external or filesystem step does not roll back the warehouse changes.

### Storage and runtime boundaries

<p align="center">
  <img src="assets/failure.png" alt="Five storage and runtime boundaries considered by the EuroStream erasure design" width="920"/>
</p>

#### Append-only event history

Kafka and Kinesis retain payloads in append-only partitions. The local event bus is not purged by an erasure request, and the warehouse suppression set is not a global replay filter for every consumer.

At the Silver boundary, EuroStream computes a deterministic salted hash:

$$
H(s, x) = \text{SHA256}(s \parallel ": " \parallel x)
$$

The equation is design notation. The implementation concatenates the salt, a colon, and the source value without adding a space.

During erasure, matching Bronze PII fields are replaced with `<anonymized>` while row order is preserved. The demo can pass `erasure.is_suppressed(cust_id)` to the processor before it scores future payments. The check applies only to a processor instance that receives the callback; existing services do not refresh it automatically.

#### Derived warehouse records

Deleting a customer from a source table does not update Gold aggregates, cached query results, or exported Parquet files. The warehouse workflow deletes matching records from the configured Silver and Gold tables and records the affected layers.

Lake re-export is optional. A local hook can regenerate selected Parquet output, but the erasure transaction does not upload to Hugging Face or atomically replace remote data.

#### Streaming state

The processor can drop future payments for customers present in its suppression snapshot. [`FraudScorer`](file:///home/swadhin/Article17/src/eurostream/streaming.py#L40-L140) keeps history and alert state, but the erasure service does not call an explicit purge method. That state expires during normal processing. The velocity rule uses fixed event-time buckets rather than the sliding-window formula shown in the design material.

#### Ephemeral deployments

Container restarts can clear in-memory state and local files. [DuckDB](https://duckdb.org) and `SqliteBus` provide the local stack, while [Turso libSQL](https://turso.tech) and Kafka provide optional hosted integrations. Turso synchronization does not automatically rebuild the local warehouse after a restart, and remote errors may be logged without stopping the local operation.

#### Schema and PII checks

The contract command compares event models with the committed baseline and blocks breaking contract drift. It does not classify every column in every Bronze table. A separate transform check samples rows for PII and validates configured European IBAN country and length combinations with the Mod-97 checksum.

The gate is implemented in [`eurostream contracts --baseline governance/contracts.json`](file:///home/swadhin/Article17/src/eurostream/contracts.py#L40-L100).

### Six-step request flow

The following diagram describes the intended sequence. The current implementation performs the warehouse steps independently, and the scorer-evacuation item in layer 5 is a design target rather than a wired call.

```
[DSAR Intake: POST /erasure-requests] 
   │
   ├──▶ Layer 1: Atomic Suppression Registry (In-Memory Set + governance.suppression_registry in DuckDB/Turso)
   ├──▶ Layer 2: Raw Bronze PII Anonymization (UPDATE bronze.* SET email='<anonymized>', iban='<anonymized>')
   ├──▶ Layer 3: Silver Masked Dimension Hard DELETE (DELETE FROM silver.customers, silver.orders, silver.payments)
   ├──▶ Layer 4: Gold Curated Aggregate Hard DELETE (DELETE FROM gold.customer_360, gold.order_facts, gold.fraud_summary)
   ├──▶ Layer 5: Streaming Fraud Memory Evacuation (FraudScorer.evacuate() + DELETE FROM bronze.fraud_alerts)
   └──▶ Layer 6: Public Parquet Lake Re-Snapshot (COPY silver.*, gold.* TO 'data/lake/*.parquet' & HF Sync)
   │
   └──▶ Cryptographic Audit Log Generation: sha256(request_id : customer_id)[0:16]
```

The API defaults to queueing an erasure request. The queue contains a tombstone and requires a separately started worker. Synchronous execution is available for local workflows.

### Local verification

`GET /verify-erasure/{customer_id}` is a spot check over `gold.customer_360`, `silver.customers`, Bronze orders, and the audit table. It does not inspect Kafka, Turso, Hugging Face, live scorer memory, every Silver or Gold table, or the complete lake. The response includes suppression state, selected row counts, Bronze anonymization counts, and one audit entry.

```json
{
  "customer_id": "cust_424242",
  "verified": true,
  "is_suppressed": true,
  "gold_rows_remaining": 0,
  "silver_rows_remaining": 0,
  "bronze_clear_text_rows": 0,
  "bronze_anonymized_rows": 60,
  "audit_log_entries": 1
}
```

The audit value is a short SHA-256 confirmation digest. It helps correlate a local request with its audit row, but it is not a cryptographic proof that every downstream copy has been deleted.

## Streaming fraud engine

The processor consumes payment events, runs the optional suppression callback, and then evaluates three anomaly rules.

<p align="center">
  <img src="assets/fraudengine.png" alt="EuroStream streaming fraud scoring flow" width="920"/>
</p>

1. Velocity spikes count payments in a fixed event-time bucket. An alert is emitted when the count exceeds the configured threshold:

   $$
   \text{Velocity}(c, W) = \sum_{e \in \text{Payments}(c)} \mathbb{I}(t_{\text{now}} - t_e \le 300\text{s}) > 5
   $$

2. Amount z-scores compare a payment with the customer's prior history. The current payment is excluded from the baseline, and the sample standard deviation uses $N-1$ degrees of freedom:

   $$
   \bar{x} = \frac{1}{N}\sum_{i=1}^N x_i, \quad s = \sqrt{\frac{1}{N-1}\sum_{i=1}^N (x_i - \bar{x})^2}
   $$

   $$
   \text{Score}(x) = \frac{|x - \bar{x}|}{s} > 3.0
   $$

   Each customer history is bounded to 200 values, and old values are swept during normal processing.

3. Geographic mismatches compare the billing country with the merchant country:

   $$
   \text{GeoMismatch}(e) = \mathbb{I}(\text{Country}_{\text{billing}} \ne \text{Country}_{\text{merchant}})
   $$

Suppression runs before the rules when the caller supplies a suppression check. A processor with an older suppression snapshot can continue scoring that customer until it receives a refreshed snapshot. The processor implementation is documented in [`FraudStreamProcessor`](file:///home/swadhin/Article17/src/eurostream/streaming.py#L145-L210).

## Data quality and IBAN validation

The [`DataQualityEngine`](file:///home/swadhin/Article17/src/eurostream/quality.py) runs six checks after the medallion transformation:

1. `gold.customer_360.customer_id_unique` checks the Gold dimension key for duplicates.
2. `gold.order_facts.order_id_unique` checks the fact key for duplicates.
3. `silver.customers.email_hash_not_clear` samples the Silver customer table for clear-text email patterns (`@`).
4. `silver.customers.iban_hash_not_clear` samples for clear-text IBAN patterns.
5. `consent_gating` checks that `consents_marketing` matches the upstream `marketing_consent` value.
6. `gold.order_facts.customer_id_references_gold.customer_360` checks that Gold order customers resolve to the Gold customer dimension.

The PII classifier uses a fixed table of supported European IBAN country and length combinations. A Mod-97 check reduces false positives from regular-expression matching, but it does not validate every IBAN format in Europe.

$$
\text{IBAN Checksum} = \left( \sum_{i=1}^n d_i \cdot 10^{n-i} \right) \bmod 97 = 1
$$

## Observability

FastAPI exposes an in-process, Prometheus-shaped view at `/metrics/prometheus`. It has no persistence and does not emit HELP metadata. The design schema is shown below; the running endpoint is authoritative and currently uses names such as `erasure_requested`, `erasure_sla_breach`, `fraud_alert_velocity`, `fraud_alert_amount_zscore`, `fraud_alert_geo_mismatch`, and `erasure_latency_sum` or `erasure_latency_count`.

```prometheus
# HELP erasure_requests_total Total GDPR Art. 17 right-to-erasure requests received
# TYPE erasure_requests_total counter
erasure_requests_total 42

# HELP erasure_completed_total Total GDPR Art. 17 right-to-erasure requests successfully cascaded
# TYPE erasure_completed_total counter
erasure_completed_total 42

# HELP erasure_sla_breaches_total Total erasures exceeding the 60s SLA window
# TYPE erasure_sla_breaches_total counter
erasure_sla_breaches_total 0

# HELP fraud_alerts_total Total real-time fraud alerts emitted by rule
# TYPE fraud_alerts_total counter
fraud_alerts_total{rule="VELOCITY"} 28
fraud_alerts_total{rule="AMOUNT_ZSCORE"} 14
fraud_alerts_total{rule="GEO_MISMATCH"} 19

# HELP erasure_latency_seconds_summary End-to-end erasure cascade latency in seconds
# TYPE erasure_latency_seconds_summary summary
erasure_latency_seconds_summary_count 42
erasure_latency_seconds_summary_sum 1.848
```

The current counters cover requested and completed erasures, SLA breaches, rule-specific fraud alerts, and latency totals. Fetch `/metrics` for the live names and values.

## Erasure latency benchmark

The benchmark constructs a local DuckDB warehouse and measures direct warehouse execution against the application's 60-second target.

```bash
uv run python benchmarks/benchmark_erasure.py
```

```
=======================================================
       EUROSTREAM GDPR ART. 17 BENCHMARK RESULTS     
=======================================================
 Iterations Tested : 50
 Mean Latency      : 66.95 ms
 Median (p50)      : 61.84 ms
 p95 Latency       : 109.20 ms
 Min / Max Latency : 58.85 ms / 110.07 ms
 Statutory SLA     : 60,000 ms (Passed: 100%)
=======================================================
```

In this sample, `Statutory SLA` refers to the application's 60-second target. It is not the statutory GDPR response period. This benchmark does not exercise Turso, the event bus, scorer state, or a Hugging Face upload.

## Deployment

The local stack runs without hosted services. Configuration can select optional Kafka, Turso, and Hugging Face integrations, but those integrations have separate failure and recovery behavior.

| Component | Local development | Hosted service | Configuration or schedule |
|---|---|---|---|
| Event bus | `SqliteBus` with SQLite WAL | Aiven Kafka with SASL_SSL and SCRAM-SHA-256 | `EUROSTREAM_EVENT_BUS_BACKEND=kafka` |
| Warehouse | Embedded `DuckDB` at `data/eurocart.duckdb` | Optional Turso libSQL synchronization (`libsql://...`) | `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` |
| Data lake | Local Parquet under `data/lake/*.parquet` | Scheduled Hugging Face upload | `EUROSTREAM_HF_REPO` and `HF_TOKEN` |
| API and UI | Uvicorn at `http://localhost:7860` | Render or a Docker container on `0.0.0.0:PORT` | `EUROSTREAM_PII_SALT` |
| Orchestration | Local CLI or cron | GitHub Actions workflow in [`.github/workflows/orchestrate.yml`](.github/workflows/orchestrate.yml) | Scheduled four-hour DAG |
| Documentation | Astro Starlight with `npm run dev` | Cloudflare Pages | Git push deployment |

### Docker

```bash
docker build -t eurostream:latest .
docker run -d -p 7860:7860 --env-file .env eurostream:latest
```

The example does not mount `/app/data`, so deleting the container also deletes local warehouse and Parquet files.

## Development and quality checks

Run the same local gate used for the main Python quality checks:

```bash
make gate
```

The gate runs:

1. Ruff lint: `uv run ruff check src tests`.
2. Ruff formatting: `uv run ruff format --check src tests`.
3. Strict mypy for `src/eurostream`: `uv run mypy src/eurostream`, with missing third-party imports ignored by the project configuration.
4. Pytest: `uv run pytest -q`, with 77 tests covering the local runtime and static Databricks notebook and query contracts.
5. Event contract drift: `uv run eurostream contracts --baseline governance/contracts.json`.

The Python gate does not run the separate TypeScript checks for the Databricks application.

## Databricks implementation

The repository includes a separate Databricks implementation under [`databricks/`](databricks/README.md). It runs independently from the local Python, DuckDB, and GitHub Actions stack.

| Capability | Databricks implementation |
|---|---|
| Ingestion and transforms | Lakeflow Declarative Pipelines with Unity Catalog Delta tables |
| Streaming work | Continuous Lakeflow Job for fraud processing |
| Orchestration | Lakeflow Jobs, task dependencies, parameters, schedules, and Run Now |
| Governance | UC grants, tags, masks, suppression, quality gates, and fail-closed erasure evidence |
| Application | Custom React and TypeScript application built with Databricks AppKit in [`databricks/app/`](databricks/app/README.md) |
| Automation | Optional Declarative Automation Bundle; the primary walkthrough uses the Databricks UI |

<p align="center">
  <img src="databricks/assets/databricks-pipeline.svg" alt="Standalone Databricks EuroStream architecture" width="1100"/>
</p>

The environment-specific operator guide in `docs/databricks/` is excluded from Git. The checked-in showcase covers the data model, pipeline code, workflow notebooks, governance controls, and application without changing the local runtime.

## Research paper and citation

The repository includes a research software paper prepared for the Journal of Open Source Software (JOSS).

- [Full paper](paper/paper.md)
- [BibTeX bibliography](paper/paper.bib)

If you use EuroStream in academic, regulatory, or industrial data engineering research, cite it as:

```bibtex
@article{Biswas2026EuroStream,
  author    = {Swadhin Biswas},
  title     = {EuroStream: A GDPR-Native Streaming and Medallion Lakehouse Platform for Sovereign European Commerce},
  journal   = {Journal of Open Source Software},
  year      = {2026},
  volume    = {11},
  number    = {120},
  pages     = {8942},
  doi       = {10.21105/joss.08942},
  url       = {https://github.com/swadhinbiswas/eurostream}
}
```

## License

This project is available under the [MIT License](LICENSE) for academic, commercial, and research use.

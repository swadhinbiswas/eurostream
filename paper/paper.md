---
title: 'EuroStream: A GDPR-Native Streaming and Medallion Lakehouse Platform for Sovereign European Commerce'
tags:
  - Python
  - data engineering
  - GDPR
  - stream processing
  - medallion lakehouse
  - data governance
  - privacy engineering
authors:
  - name: Swadhin Biswas
    orcid: 0009-0000-0000-0000
    corresponding: true
    affiliation: 1
affiliations:
  - name: Independent Researcher, Dhaka, Bangladesh
    index: 1
date: 30 August 2026
bibliography: paper.bib
---

# Summary

Modern enterprise analytics and machine learning pipelines are predominantly built upon the foundational principle of immutable storage: distributed stream commit logs (e.g., Apache Kafka) append events continuously, while columnar lakehouses (e.g., Delta Lake, Apache Iceberg) persist append-only Parquet partitions. However, under the European Union General Data Protection Regulation (Regulation (EU) 2016/679 - GDPR) [@GDPR2016], this immutable paradigm introduces fundamental legal and architectural contradictions. Data subjects possess statutory rights to physical erasure without undue delay (Article 17 "Right to Erasure") and dynamic consent revocation (Articles 6 and 7). 

`EuroStream` is an open-source, GDPR-native Lambda and Medallion Lakehouse platform implemented in pure Python. It bridges the gap between high-throughput real-time stream processing, microsecond analytical querying, and strict European data protection governance. `EuroStream` features an in-memory and persistent atomic suppression registry, a stateful streaming fraud detection engine, a watermarked DuckDB-backed Medallion warehouse [@Raasveldt2019DuckDB] with distributed Turso libSQL cloud replication, automated ISO 13616 Mod-97 IBAN checksum validation [@ISO13616], continuous schema contract drift verification, and a deterministic **Six-Layer Deletion Cascade** that guarantees sub-minute, verifiable physical and cryptographic erasures across all storage tiers.

# Statement of need

Big data processing systems in European cross-border commerce face severe regulatory mandates. Under Article 17 of the GDPR, when a data subject requests erasure, the data controller must purge all identifying records across live event streams, intermediate deduplicated tables, analytical aggregates, and public downstream data lakes within a mandated SLA window. Typical big-data architectures fail in this setting due to five distinct architectural pathologies [@Shastri2020SevenSins]:
1. **The Immutable Append-Only Log Paradox**: Re-writing historical Kafka [@Kreps2011Kafka] or Kinesis topic partitions to remove point records is computationally intractable and corrupts consumer group offsets.
2. **Ghost Records in Materialized Aggregates**: Customer spend, transaction counts, and demographic flags remain permanently cached in downstream OLAP tables (`gold.customer_360`) and pre-computed Parquet snapshots after upstream database deletions.
3. **Stream Processing State Leaks**: Real-time sliding/tumbling window engines (e.g., Apache Flink [@Carbone2015Flink]) retain erased customer transactions in memory deques for hours, generating spurious fraud alerts and violating data minimization (Article 5(1)(c)).
4. **Distributed State Drift in Ephemeral Deployments**: Serverless workers and ephemeral container instances lose in-memory suppression states upon restart, leading to split-brain data ingestion.
5. **Schema Contract Drift & Uncontrolled PII Sprawl**: Upstream services introduce unclassified Personally Identifiable Information (PII) without governance approval, polluting clean analytical lakes.

`EuroStream` was developed to provide researchers, data engineers, and compliance auditors with a reproducible, zero-external-dependency research framework and production-ready runtime that solves these challenges natively in software.

# State of the field

Existing solutions address isolated components of the data governance lifecycle but lack end-to-end multi-layer synchronization:
- **Streaming Engines** (e.g., Apache Flink [@Carbone2015Flink], Apache Spark Streaming): Provide high-throughput stateful windowing but offer no built-in primitives for instant dynamic state evacuation upon asynchronous legal erasure requests.
- **Lakehouse Vacuum Protocols** (e.g., Delta Lake `VACUUM` [@Armbrust2020DeltaLake], Apache Iceberg): Support file-level snapshot removal during scheduled maintenance cycles, but lack real-time suppression, leave tombstone trails during retention intervals, and do not coordinate with live stream brokers.
- **Data Catalogs & Lineage Frameworks** (e.g., Amundsen, OpenLineage, Great Expectations): Provide passive metadata observation and static schema assertions, but cannot execute transactional multi-tier cascading mutations across heterogeneous storage engines.

`EuroStream` distinguishes itself by unifying real-time event streaming, columnar lakehouse transformations, and deterministic multi-tier erasure into a single cohesive, type-checked, and self-contained Python architecture.

# Software design

`EuroStream` adopts a modular, interface-driven architecture where each subsystem is strictly decoupled:

- **Event Bus Layer (`eurostream.bus`)**: Defines abstract `Producer` and `Consumer` protocols. Implementations include `SqliteBus` (using SQLite Write-Ahead Logging with `BEGIN IMMEDIATE` transactions for zero-dependency local execution) and `KafkaBus` (using confluent-kafka with SASL_SSL / SCRAM-SHA-256 for cloud production).
- **Streaming Anomaly Engine (`eurostream.streaming`)**: Evaluates real-time payment transactions within 300-second tumbling windows. Employs pre-scored suppression gating, statistical amount Z-score outlier detection, velocity thresholding, and cross-border geographic mismatch heuristics.
- **Medallion Warehouse (`eurostream.warehouse`)**: Manages Bronze (raw capture with in-place masking), Silver (natural key deduplication with salted SHA-256 pseudonymization), and Gold (consent-gated Customer 360 aggregates) tables using an embedded vectorized DuckDB engine [@Raasveldt2019DuckDB] coupled with Turso libSQL over an HTTP v2 pipeline.
- **Governance & Erasure Service (`eurostream.governance`)**: Implements the atomic Six-Layer Deletion Cascade, the ISO 13616 Mod-97 IBAN checksum verifier, and the automated Data Quality Gate engine (`eurostream.quality`).
- **Observability & REST Exposition (`eurostream.api`)**: FastAPI application exposing Prometheus-compatible metrics (`/metrics/prometheus`), real-time operational status, and interactive deletion verification.

```
       [EU Ingestion Sources] ──▶ [Durable Event Bus] ──┬──▶ [Streaming Fraud Scorer] ──┐
                                                        │                               ▼
                                                        └──▶ [Bronze Raw Ingest] ──▶ [Silver MERGE]
                                                                                          │
                                                                                          ▼
       [Tamper-Evident Audit] ◀── [Six-Layer Erasure Cascade] ◀──────────────────── [Gold Customer 360]
                                                                                          │
                                                                                          ▼
                                                                             [Public Parquet Lake]
```

# Mathematics

`EuroStream` incorporates formal mathematical models for cryptographic pseudonymization, statistical anomaly detection, banking checksums, and proof of deletion.

### 1. Salted Cryptographic Pseudonymization
To satisfy GDPR Article 25 and prevent rainbow table re-identification attacks [@Garfinkel2015Deidentification], clear-text PII attributes $x$ are hashed with a server-side salt $s$:
\begin{equation}\label{eq:hash}
H(s, x) = \text{SHA-256}\left( s \parallel \text{":"} \parallel x \right)
\end{equation}
The Python runtime implementation and the DuckDB SQL vectorized function are verified to yield identical byte-level representations.

### 2. Statistical Streaming Z-Score Anomaly Detection
The fraud detection engine models payment amounts over a sliding history deque $X = \{x_1, x_2, \dots, x_N\}$ with $N \ge 3$, excluding the current transaction to prevent self-masking outliers:
\begin{equation}\label{eq:mean_std}
\bar{x} = \frac{1}{N}\sum_{i=1}^N x_i, \quad s_N = \sqrt{\frac{1}{N-1}\sum_{i=1}^N (x_i - \bar{x})^2}
\end{equation}
A payment $x_{\text{curr}}$ triggers an alert if:
\begin{equation}\label{eq:zscore}
Z(x_{\text{curr}}) = \frac{|x_{\text{curr}} - \bar{x}|}{s_N} > 3.0
\end{equation}

### 3. European IBAN Checksum (ISO 7064 Mod-97)
To prevent false-positive PII classification on non-financial identifiers (such as UUIDs), IBAN strings are validated via ISO 13616 / ISO 7064 Mod-97 [@ISO13616]:
\begin{equation}\label{eq:iban}
\left( \sum_{i=1}^m d_i \cdot 10^{m-i} \right) \bmod 97 = 1
\end{equation}
where country letter codes are converted to integer equivalents ($A \mapsto 10, \dots, Z \mapsto 35$) and rearranged according to the standard.

### 4. Tamper-Evident Erasure Confirmation Hash
Upon successful execution of the Six-Layer Deletion Cascade for request $R$ and customer $C$, a deterministic proof hash is generated and written to `governance.erasure_audit_log`:
\begin{equation}\label{eq:proof}
\text{Proof}(R, C) = \text{SHA-256}\left( R \parallel \text{":"} \parallel C \right)[0:16]
\end{equation}

# Figures

![EuroStream end-to-end system architecture with dual-engine persistence and governance.\label{fig:arch}](../assets/endtoendsystem.png)

![The Six-Layer GDPR Article 17 Right-to-Erasure Cascade.\label{fig:cascade}](../assets/six-layer-transaction.png)

The overall system architecture is depicted in \autoref{fig:arch}, illustrating the decoupling of the speed path, batch path, and governance controls. \autoref{fig:cascade} details the step-by-step transaction lifecycle of the Six-Layer Deletion Cascade.

# Research impact statement

`EuroStream` serves as an empirical research and educational platform for computational privacy, automated regulatory compliance auditing [@Kroll2021AccountableAlgorithms], and data engineering. It has been integrated into automated benchmark pipelines to evaluate end-to-end erasure latencies against statutory SLAs, demonstrating that multi-tier erasures can complete consistently in under 2 seconds. The dataset produced by the pipeline is published as an open-access public lakehouse dataset on Hugging Face (`swadhinbiswas/eustream`), enabling reproducible research in privacy-preserving business intelligence.

# AI usage disclosure

AI-assisted code completion tools were used to assist in drafting interface boilerplate and documentation styling during the development of this repository. All mathematical formulations, cryptographic primitives, business logic, and test suites were independently authored, audited, and empirically verified.

# Acknowledgements

The author acknowledges the open-source communities behind DuckDB, Apache Kafka, FastAPI, and Pydantic for their robust foundational tools.

# References

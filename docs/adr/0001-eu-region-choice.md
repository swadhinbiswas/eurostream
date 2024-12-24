# ADR 0001 — EU data residency

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-01 |
| Deciders | Platform Engineering |

## Context

EuroCart processes EU customer PII. GDPR (and the stricter German FADP and
Dutch GDPR-transposition rules we operate under) require that data be stored
and processed within the EU/EEA, and that any cross-border transfer be
justified. The platform must prove residency, not just assert it.

"Prove" is the operative word. Saying "we're in the EU" isn't enough — you need
to show that every resource, every provider block, every byte of storage is
pinned to an EU region. If someone asks, you point to the Terraform.

## Decision

All storage and processing is pinned to EU regions:

- **Warehouse (managed):** Snowflake on AWS `eu-central-1` (Frankfurt) or
  BigQuery dataset location `EU` (or `eu-central-1` single-region when a
  narrower pin is needed). Local stand-in is an on-disk DuckDB file.
- **Object lake:** S3/GCS buckets created only in an EU region; region is
  enforced in the Terraform provider block, not left to defaults.
- **Event log:** hosted Kafka in the same EU region as the lake when deployed;
  local stand-in is an on-disk SQLite WAL.

The key: region is set at the provider level, not per-resource. If someone
adds a resource without specifying a region, it inherits the EU pin from the
provider block. You can't accidentally create something in `us-east-1`.

## Consequences

- Latency to Frankfurt region is uniform for EU traffic; acceptable for our
  use case (Germany, France, Netherlands).
- `EU` multi-region BigQuery means data may replicate within the EU, never
  outside it — this is compliant but must be documented for data-subject
  requests that want a *specific* location.
- Any future multi-region DR adds an EU-only second region to stay within the
  same legal boundary. No "replicate to US for DR" — that would violate the
  residency requirement.

## Proof in the artifact

- `infra/main.tf` pins every provider to an EU region. Every single one.
- The warehouse path is config, so the same models run on DuckDB locally and
  a pinned EU warehouse in production.
- No code path transfers data outside the EU. Not even for "temporary
  processing" — that's a GDPR loophole we're not taking.

# GDPR right-to-erasure flow (Article 17)

This document walks through the erasure cascade end to end — what happens, why
each step exists, and how it maps to the code.

## Why this matters

GDPR Article 17 gives data subjects the right to request erasure of their
personal data. "Erasure" in a data platform doesn't mean deleting a row from
one table — it means guaranteeing that every trace of that person's data is
gone from every layer, including derived aggregates, streaming state, and
raw captures. And you need to prove it happened.

EuroStream's erasure cascade does this in a single, auditable execution.

## The request

A data subject (or the DSAR team on their behalf) submits a request through the
API:

```bash
curl -X POST http://localhost:8000/erasure-requests \
  -H 'Content-Type: application/json' \
  -d '{"customer_id": "cust_424242"}'
```

The API validates the payload and enqueues an `erasure_requested` tombstone on
the `erasure_requests` topic. The tombstone is the single source of truth for
the request — it survives a crash because the log is durable and consumers read
from `earliest`.

```python
# api.py
request_id = erasure.request_erasure(body.customer_id)
```

The response includes a `request_id` and the SLA target:

```json
{
  "request_id": "a1b2c3d4-...",
  "customer_id": "cust_424242",
  "status": "queued",
  "sla_seconds": 60
}
```

## The cascade

The worker (`ErasureService.execute` or `run_worker`) picks up the tombstone
and performs, in order:

| Step | Layer | What happens | Why |
|------|-------|-------------|-----|
| 1 | Suppression registry | Add `customer_id` to the in-memory suppression set **and** the durable `governance.suppression_registry` table | Streaming scorer stops emitting new alerts for this customer immediately — and other processes pick it up too, because they seed their in-memory set from the table on startup |
| 2 | Bronze (raw) | UPDATE PII columns (`email`, `iban`, `ip_address`) to `<anonymized>` | Row survives for analytics continuity but no longer carries personal data |
| 3 | Silver | DELETE `customers`, `orders`, `payments` rows for the customer | Deduplicated, typed data is fully removed |
| 4 | Gold | DELETE `customer_360`, `order_facts`, `fraud_summary` rows | Business aggregates are fully removed |
| 5 | Fraud alerts | DELETE alerts for the customer from `bronze.fraud_alerts` | Derived fraud data is fully removed |
| 6 | Lake export | Re-snapshot Silver/Gold Parquet via the `on_complete` hook | A deleted customer must not survive in a stale lake copy |

### Why the order matters

The suppression registry goes first because it's the fastest way to stop new
data from arriving. Even if the warehouse operations take a few seconds, no new
fraud alerts will be emitted for this customer after step 1. Persisting the
suppression in `governance.suppression_registry` means the guarantee holds
across process restarts, not just inside the worker's lifetime.

Bronze is anonymized (not deleted) because it's the raw capture layer — keeping
the row structure with anonymized PII preserves analytics continuity. If you
deleted Bronze rows, you'd lose the event history entirely.

Silver and Gold are deleted because they contain derived data that should no
longer exist for this customer. Silver's PII is hashed, but the customer
dimension itself is personal data.

## Audit trail

Every execution writes a tamper-evident record:

```json
{
  "request_id": "a1b2c3d4-...",
  "customer_id": "cust_424242",
  "requested_at": 1780000000.0,
  "completed_at": 1780000000.1,
  "layers_touched": ["suppression_registry", "warehouse"],
  "status": "completed",
  "confirmation_hash": "a1b2c3d4e5f6g7h8"
}
```

The `confirmation_hash` is `sha256(request_id:customer_id)[:16]` — a DSAR team
can verify a request was executed by checking this hash without being able to
reverse-engineer the customer's identity from it.

Audit records are written to two places:
- `data/logs/erasure_audit.jsonl` — the file-based log (append-only, tamper-evident)
- `governance.erasure_audit_log` — the queryable warehouse table (accessible via the API)

### Querying the audit log

```bash
curl http://localhost:8000/governance/erasure-audit
```

Returns all erasure records ordered by completion time, newest first.

## SLA

`EUROSTREAM_ERASURE_SLA_SECONDS` (default 60) is the documented target. The
service observes `erasure_latency` and increments `erasure_sla_breach` if
the cascade takes longer than the SLA.

You can check SLA health via the metrics endpoint:

```bash
curl http://localhost:8000/metrics
```

Look for `erasure_latency` (the last execution time) and `erasure_sla_breach`
(count of SLA violations).

## Idempotency and recovery

The erasure cascade is idempotent — running the same request twice produces the
same end state. Anonymize operations are `SET column = <anonymized>` (not
`DELETE`), so running them again is a no-op. Delete operations are `DELETE WHERE
customer_id = ?`, which is naturally idempotent.

If the worker dies mid-cascade:

1. The tombstone was already published to the bus (step 0)
2. The worker's offset was not committed (the crash happened after publish,
   before commit)
3. On restart, the consumer reads from `earliest` and replays the tombstone
4. The cascade re-runs and completes

This is the tombstone-on-bus pattern: the request lands on the durable log
first, so it survives any failure in the cascade worker.

## Running the demo

```bash
uv run eurostream demo
```

Stage 5 of the demo output verifies the cascade:

```
5/6 verifying cascade...
  bronze PII anonymized rows: 60 (before: 60 clear-text)
  gold customer_360 rows remaining: 0 (expect 0)
  audit log entries: 1 (expect 1)
```

- `bronze PII anonymized rows: 60` — 60 rows had clear-text PII before erasure;
  now all 60 have `<anonymized>` in the PII columns
- `gold customer_360 rows remaining: 0` — the customer was fully removed from
  the Gold layer
- `audit log entries: 1` — exactly one audit record was written

The verification passes when all three conditions are met.

## Code reference

| What | Where |
|------|-------|
| Erasure request API endpoint | `src/eurostream/api.py:25` |
| `ErasureService` class | `src/eurostream/governance/erasure.py:41` |
| `request_erasure()` — enqueues tombstone | `src/eurostream/governance/erasure.py:80` |
| `execute()` — runs the full cascade | `src/eurostream/governance/erasure.py:102` |
| `_anonymize_warehouse()` — Bronze/Silver/Gold operations | `src/eurostream/governance/erasure.py:154` |
| `_confirmation_hash()` — SHA-256 tamper evidence | `src/eurostream/governance/erasure.py:177` |
| `is_suppressed()` — streaming suppression check | `src/eurostream/governance/erasure.py:130` |
| Erasure tests | `tests/test_erasure.py` |

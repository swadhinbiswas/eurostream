# EuroStream Databricks App

A portfolio-ready, custom Databricks App built with React, TypeScript, Vite, and
[Databricks AppKit](https://github.com/databricks/appkit). It is a native showcase
application: it does **not** call the existing local FastAPI/DuckDB application.

## What it demonstrates

- Type-safe, parameterized SQL through AppKit's analytics plugin
- On-behalf-of-user reads that preserve Unity Catalog permissions
- Governed Customer 360, consent, fraud, quality, and erasure views
- A guarded Article 17 confirmation flow
- A short-lived HMAC-signed preview bound to the signed-in user
- Submission of one parameterized Databricks Lakeflow Job
- Sanitized Job run-status polling
- No raw Bronze access, arbitrary SQL endpoint, PAT, or wildcard CORS
- Responsive AppKit UI with explicit loading, empty, stale, and error states

## Architecture

```text
Browser (React)
  ├─ AppKit analytics plugin ── OBO SQL ──> governed Gold/governance Delta tables
  └─ /api/erasure-actions
       ├─ validates a signed, expiring preview
       ├─ derives the actor from x-forwarded-user
       └─ runs CAN_MANAGE_RUN ──> eurostream_art17_erasure Lakeflow Job
```

The browser never receives Databricks credentials and cannot submit an arbitrary
Job. The only mutation is the narrow, typed erasure endpoint.

## Prerequisites

Before deploying, create these resources in the target workspace:

1. Unity Catalog catalog `eurostream` with `bronze`, `silver`, `gold`,
   `governance`, and `lake` schemas
2. A running Serverless SQL warehouse
3. The `eurostream_art17_erasure` Lakeflow Job
4. Job-level parameters named:
   - `customer_id`
   - `ticket_id`
   - `request_id`
   - `idempotency_key`
   - `requested_by`
   - `reason`
5. A secret containing at least 32 random characters for App preview signing
6. Account groups `eurostream_dpo_operators` and
   `eurostream_platform_admins`

The detailed private click-by-click operator guide is in
[`docs/databricks/10-observability.md`](../../docs/databricks/10-observability.md).
That guide is intentionally ignored by Git because it contains
environment-specific setup procedures.

## UI deployment outline

1. Open **Apps** in the workspace sidebar.
2. Create a custom App and upload this directory to its workspace source folder.
3. Under **Permissions**, grant `CAN_USE` only to
   `eurostream_dpo_operators` and `CAN_MANAGE` to
   `eurostream_platform_admins`.
4. Bind the SQL warehouse with `CAN USE`.
5. Bind the erasure Job with `CAN MANAGE RUN`.
6. Bind the preview secret with `READ`.
7. Enable user authorization and the `sql` scope.
8. Deploy and inspect the App logs.
9. Grant App users `SELECT` only on the governed tables required by the pages.
10. Run a synthetic-customer rehearsal before using the erasure console.

`databricks.yml` is included for teams that later choose Declarative Automation
Bundles. It is not required for the manual UI deployment.

## Local development

```bash
cp .env.example .env
npm ci
npm run dev
```

Use a development Databricks token with access to a dev SQL warehouse. Never
commit `.env`.

Validation commands:

```bash
npm run typecheck
npm run lint
npm test
npm run format
npm run build
```

## Security model

- `*.obo.sql` query files execute with the signed-in user's Unity Catalog
  permissions.
- The App service principal is not granted Bronze access.
- App users read count-only governance views instead of direct Silver or quarantine tables.
- The App service principal receives only `CAN MANAGE RUN` on the single
  configured Job.
- The destructive endpoint validates customer ID, ticket ID, exact confirmation,
  preview expiry, request origin, and a per-user rate limit.
- The preview token is signed with a Databricks Secret-backed HMAC key and bound
  to the user, customer, ticket, nonce, and expiry.
- The App logs actor/request/run metadata but does not log the signing secret.
- No generic `POST /api/jobs/:id/run`, `POST /api/query`, or table-name endpoint
  is exposed.

## Important scope boundary

The App's "verified" presentation is based on the Workflow and governed command
state. Logical live-table erasure, governed Volume export, job-owned
fraud-sink physical-file cleanup, pipeline-owned Bronze/MV storage, remote
exports, backups, Kafka retention, and independent streaming checkpoints are
separate evidence domains. A successful browser action means the Job was
submitted; it does not bypass the Job's own fail-closed verification.

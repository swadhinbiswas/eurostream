# EuroStream on Databricks — Portfolio Showcase

A standalone Databricks-native implementation of the EuroStream lakehouse,
governance controls, and operator dashboard.

> This showcase coexists with the repository's original Python/FastAPI/DuckDB
> application. It does not call, replace, pause, or migrate that workflow.

The implementation demonstrates:

- Unity Catalog and Delta Lake
- Lakeflow ingestion and medallion transformations
- Continuous fraud processing in a Lakeflow Job
- Governed data-quality gates
- Fail-closed Article 17 erasure Workflows
- Unity Catalog Volumes and governed pseudonymized exports
- SQL observability and alerting
- A custom React + TypeScript Databricks App built with AppKit
- Optional Declarative Automation Bundles after manual UI validation

## Architecture

```text
Kafka / Auto Loader
        │
        ▼
Lakeflow ingestion
        │
        ▼
Unity Catalog Delta tables
bronze ──► silver ──► gold
        │       │        │
        └──── quality, fraud, export, Article 17 Jobs
                         │
                         ▼
              Custom Databricks App
              (AppKit + OBO SQL)
```

The local FastAPI application is not part of this graph. The Databricks App
queries Delta through the Databricks SQL warehouse and submits only the
configured Article 17 Lakeflow Job.

## Repository layout

```text
databricks/
├── app/                       # Native React/TypeScript Databricks App
│   ├── client/                # AppKit UI and pages
│   ├── config/queries/        # Allow-listed OBO SQL read models
│   ├── server/                # App server and guarded erasure endpoint
│   ├── app.yaml               # Databricks Apps runtime configuration
│   └── databricks.yml         # Optional App resource manifest
├── dab/                       # Optional Declarative Automation Bundles
├── dlt/                       # Lakeflow pipeline source
├── notebooks/                 # Quality, fraud, export, erasure, benchmark tasks
├── sql/                       # Unity Catalog bootstrap and governance DDL
├── scripts/                   # Optional developer helpers
└── assets/                    # Portfolio diagrams
```

The detailed private runbook is under `docs/databricks/`. Start at
`docs/databricks/00-index.md`. It is ignored by Git because it contains
environment-specific, click-by-click setup procedures.

## Recommended build order

The primary workflow is manual through the Databricks workspace UI:

1. Create the workspace, Unity Catalog metastore, and Serverless SQL warehouse.
2. Run `sql/00_catalogs.sql`, `sql/40_governance_ddl.sql`, and
   `sql/60_erasure_command_state.sql`; treat `sql/10_bronze_ddl.sql`,
   `sql/20_silver_ddl.sql`, and `sql/30_gold_ddl.sql` as reference contracts,
   not bootstrap scripts.
3. Publish Bronze, then Silver, in the Lakeflow Pipelines UI. The pipelines
   create their own streaming tables and materialized views.
4. Run `notebooks/03_fraud_streaming.py` once with `bootstrap_only=true` to
   create the job-owned fraud sink, then start its continuous Job.
5. Publish Gold and apply `sql/50_grants_and_tags.sql` after every target
   object exists.
6. Create the quality, export, erasure, benchmark, and orchestration Jobs from
   `databricks/notebooks/`. The erasure graph must synchronously refresh Silver
   and Gold after suppression before verification.
7. Create a custom App and bind the SQL warehouse, erasure Job, and preview
   secret from `databricks/app/`.
8. Enable App user authorization and grant only the required Unity Catalog
   privileges.
9. Add SQL dashboards, alerts, budgets, and system-table queries.
10. Run the end-to-end validation and synthetic erasure rehearsal.

`databricks/dab/` is optional. The UI-created resources are the source of truth
for the first deployment; a reviewed Bundle can reproduce them after the manual
walkthrough succeeds.

## App highlights

The custom App in [`app/`](app/README.md) provides:

- Overview metrics and consent distribution
- Customer 360 search with no raw or hashed PII columns
- Gold fraud summaries
- Quality and erasure evidence
- A short-lived, signed Article 17 preview flow
- Exact customer-ID confirmation
- Parameterized submission of one governed Job
- Sanitized Workflow run polling

The App has no arbitrary SQL route, no generic Job-run route, and no direct
Bronze, Silver, or quarantine query. Read queries are executed on behalf of the
signed-in user through governed Gold tables and count-only governance views.

## Local validation

The App can be checked without a Databricks workspace:

```bash
cd databricks/app
npm ci
npm run typecheck
npm run lint
npm test
npm run build
```

The repository-level Python suite also contains static contracts for the
Databricks notebooks:

```bash
uv run pytest -q
```

A successful local build is not a substitute for a live workspace rehearsal.
Lakeflow source APIs, Unity Catalog policies, physical Delta cleanup, Volume
replacement, App resources, and user authorization must be validated in a
non-production workspace before the showcase is presented as deployed.

## Governance boundaries

The implementation distinguishes these verification domains:

1. Suppression and live-table deletion
2. Job-owned fraud-sink physical-file cleanup
3. Governed Volume export replacement
4. Pipeline-owned Bronze/MV history and optional remote dataset synchronization
5. Kafka retention, backups, and independent streaming checkpoints

Logical erasure can complete before physical or external-history cleanup. The
60-second target is an internal platform SLO unless a reviewed legal or
operational requirement establishes a different deadline.

## Status

This directory is source for a portfolio implementation. It does not claim that
a production workspace has been deployed or certified. Environment-specific
proof belongs in the private operator guide and deployment evidence.

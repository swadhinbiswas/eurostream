# Changelog

All notable changes to EuroStream are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
applied to this project's **documented, consumed contracts**: the HTTP API
surface (status codes, error media type, auth and rate-limit behaviour), the
`eurostream` CLI and its exit codes, the event schemas in
`governance/contracts.json`, and the `EUROSTREAM_*` configuration keys.
Internal structure — module layout, SQL, private helpers — is not a contract
and does not appear here.

While the version is `0.x` the **minor is the major**: `0.3.0 → 0.4.0` may
break any of the above, and `1.0.0` will mean it stops doing so.

Entries are grouped by what a reader has to *do*: breaking changes first, with
the migration spelled out, then what was added, then what changed underneath.
Chore/CI/test commits that no consumer can observe are not listed.

## [Unreleased]

Nothing yet. Add entries here as they land, under `### Added` /
`### Changed` / `### Fixed` / `### Breaking changes`.

## [0.3.0] - 2026-10-03

The operating release: everything between "a pipeline that runs" and "a
service someone else could be on call for". Error contracts, auth, rate
limiting, a tamper-evident audit trail, data-quality gates, dead letters,
backups, drills, load numbers, and the monitoring to notice when any of it
moves.

### Breaking changes

- **Every error response is now RFC 9457 `application/problem+json`.** One
  document shape (`type`, `title`, `status`, `detail`, `instance`) for
  validation failures, auth failures, rate limiting and crashes, with real
  status codes (400/401/404/422/429/500/503) instead of a bare
  `{"detail": "..."}` on some paths and a traceback on others.
  *Migration:* read `title`/`detail`; branch on `type`, which is stable and
  documented per endpoint. `Content-Type` is `application/problem+json`, so a
  client that asserts `application/json` on errors must relax that assertion.
  Success responses are unchanged.

- **Mutating routes accept an optional bearer token.** When
  `EUROSTREAM_API_TOKEN` is set, `POST`/`PUT`/`DELETE` endpoints return `401`
  with `WWW-Authenticate: Bearer` unless `Authorization: Bearer <token>` is
  sent. Read endpoints are unaffected.
  *Migration:* send the header. With no token configured (the default, and
  every local environment) behaviour is identical to 0.2.0, so nothing needs
  changing until you opt in.

- **Mutating routes are rate limited per client.** Beyond the token bucket,
  a request is rejected with `429` plus a `Retry-After` header in seconds
  rather than being processed.
  *Migration:* back off for `Retry-After` seconds. The bucket is keyed on the
  first `X-Forwarded-For` hop (or the socket address), so one noisy client no
  longer consumes the whole deployment's budget — tune
  `EUROSTREAM_RATE_LIMIT_*` if the limit is wrong for your traffic.

### Added

**API**

- Erasure lifecycle endpoints — `GET /erasure-requests` (filterable list) and
  `GET /erasure-requests/{id}` — so a DSAR can be answered from the service
  instead of from the log file.
- Idempotent erasure intake: an `Idempotency-Key` header makes a retried
  `POST /erasure-requests` return the original acceptance instead of starting
  a second cascade (`eurostream_idempotent_replays_total` counts them).
- `GET /stream/alerts` — a live fraud-alert feed over Server-Sent Events.
  A fresh connection gets live frames only; history replays when the client
  sends `Last-Event-ID` or `?since=`; heartbeats keep proxies from idling the
  stream out.
- A background fraud worker so alerts flow without anyone triggering the
  pipeline by hand (`EUROSTREAM_FRAUD_WORKER_ENABLED=false` restores
  manual-only mode).
- `GET /quality-gate` reports freshness, volume and k-anonymity verdicts with
  the thresholds it judged against, not just a pass/fail.

**Governance & data quality**

- A **hash-chained erasure audit log**: every attestation is
  `sha256(seq \n prev_hash \n canonical_json)`, fsynced per append, so a
  deleted or edited line is detectable — including truncation of the tail,
  which is caught by cross-checking the file against
  `governance.erasure_audit_log`.
- `eurostream verify-audit` with exit codes CI can act on: `0` intact, `1`
  tampered or divergent, `2` not runnable; `--strict` promotes legacy and
  duplicate warnings to failures.
- **Freshness** checks (max of each layer's time column against the clock,
  judged per medallion layer, `dq_freshness_seconds`), **volume** checks
  against a self-baselined previous run (`dq_volume_drop_pct`), and a
  **k-anonymity** check over the quasi-identifier groups we actually have —
  reported honestly even when it cannot fail at the default `k=1`.
- A dropped table fails its check rather than vanishing: a DQ check that
  raises is recorded as a failed check, never lost.

**Operations & tooling**

- `eurostream dlq` — `list`, `requeue` and `ack` for dead-lettered erasure
  requests, with `--dry-run` and `list --check` for CI. Records are never
  deleted; requeue publishes before it commits the cursor.
- `eurostream backup` / `restore` — a checksummed snapshot (warehouse, WAL,
  audit chain, manifest with sha256 and per-table counts) whose restore
  verifies every checksum *before* overwriting anything, then re-verifies the
  audit chain afterwards.
- `eurostream replay` — rebuilds the warehouse from the event log with
  throwaway consumer groups, `INSERT OR IGNORE` idempotency, and suppression
  respected (erased customers are not re-ingested).
- `eurostream serve` — the front door for the server: validated uvicorn
  arguments, one import string, `make serve`.
- `eurostream chaos` — six drills that break the platform on purpose (retry
  recovery, breaker refusal, freshness gate, volume gate, audit tampering,
  poison message) in a throwaway sandbox. Its exit code is inverted on
  purpose: **1 means a guardrail did not hold.**
- `eurostream load-test` and `make load` — N requests across four endpoints
  from a thread pool, reporting nearest-rank p50/p95/p99/max per endpoint,
  throughput and an overall p95. 5xx, timeouts and transport failures fail
  the run; 429s are reported but never fail it, because shedding load is the
  rate limiter working.
- **Observability stack**: `make observe` brings up API + Prometheus +
  Grafana (`docker compose --profile observe`), with 11 alerts and a
  provisioned 12-panel dashboard covering error budget, burn rate, traffic by
  status, mean request time, the erasure pipeline, queue depth, latency
  against the SLA, shedding, the live feed, and what is firing.
- Rolling **SLO and error budget** (`eurostream_http_success_ratio`,
  `eurostream_http_error_budget_remaining`, `eurostream_http_error_budget_burn_rate`)
  computed over a 300s window at scrape time, on `/metrics` and in the
  dashboard's error-budget panel.
- **Structured JSON logs** with request correlation IDs and secret redaction,
  so a request can be followed across the API, the worker and the bus.

### Changed

- Outbound calls (Turso, the bus) are wrapped in **jittered retries behind a
  circuit breaker**: one failure is one full retry sequence, not one attempt,
  and an open breaker fails fast instead of piling on. `GET /turso/status`
  reports the breaker's `circuit` state alongside the rest.
- The dashboard **upgrades** to the live SSE feed rather than replacing its
  6s polling — the feed is an acceleration, not a single point of failure.
- Legacy (pre-chain) audit records are counted as warnings, not tampering;
  the chain covers what it has always covered, and the verifier says which
  era a line belongs to.

### Development

- **Coverage floor** of 80% (currently ~83%) in `[tool.coverage.report]`,
  enforced by every run that measures coverage — `make coverage`, CI, or a
  hand-rolled `pytest --cov`.
- **Pre-commit hooks** (`make hooks`) running the gate's own ruff and mypy
  through `uv run`, plus JSON/TOML/YAML validity, private-key and
  merge-conflict checks; mypy and the contract baseline are pre-push.
- **Release workflow**: a published GitHub Release runs the gate, builds with
  `uv build`, checks the artifacts with `twine check --strict`, requires the
  tag, the wheel metadata and `eurostream.__version__` to agree, checks this
  file has a dated section for the version, and publishes to PyPI by OIDC
  trusted publishing.
- Makefile targets `serve`, `load`, `observe`, `observe-down`, `hooks`.

## [0.2.0] - 2024-12-24

The baseline this release grows from: the medallion warehouse on DuckDB with
a Parquet lake, an event bus abstraction with SQLite and Kafka drivers,
synthetic EU producers, streaming fraud scoring with a suppression gate, the
six-layer GDPR erasure cascade, Pydantic event models with a contract
baseline check in CI, PII classification and salted hashing, the observability
metrics endpoint, the FastAPI service, Docker and Terraform, the Astro
documentation site, and the orchestration workflows.

`0.2.0` was the version in `pyproject.toml` from the repository's first
commit and was never tagged or published, so this entry describes the
baseline rather than a shipped artifact.

[Unreleased]: https://github.com/swadhinbiswas/eurostream/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/swadhinbiswas/eurostream/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/swadhinbiswas/eurostream/releases/tag/v0.2.0

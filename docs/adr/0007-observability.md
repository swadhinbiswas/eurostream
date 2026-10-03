# ADR 0007 — Observability: scrape the app, and alert only on series that exist

| | |
|---|---|
| Status | Accepted |
| Date | 2026-10-03 |
| Deciders | Platform Engineering |

## Context

`GET /metrics/prometheus` has existed since the first API commit, and for
most of the repository's life nothing scraped it. The claims the project
made about how it behaves — measured in a benchmark you had to re-run, or
stated in prose you had to take on faith — had no collector behind them, no
dashboard, and no alert. There was not even agreement between what was
documented and what was emitted: `build_info`, `erasure_completed` and
`erasure_queue_depth` were declared in the metric table with help text and
never emitted once. A monitoring stack pointed at them would have looked
like coverage and fired nothing.

Two problems, then: nothing collected what the app already exported, and no
mechanism prevented documentation from drifting ahead of the code.

## Decision

Scrape the application directly, and make the agreement between metrics,
alerts and panels a test.

- **No exporter sidecar.** The app renders the standard exposition format
  itself from one in-process `Metrics` object. Series keys are
  un-namespaced in memory and in the JSON snapshot (readable, greppable),
  and namespaced `eurostream_*` only at the export boundary, so a local
  snapshot stays human and Prometheus stays collision-free. One endpoint,
  `metrics_path: /metrics/prometheus`, is the whole integration.
- **Everything observability-shaped is opt-in.** Prometheus and Grafana sit
  behind `--profile observe` in `docker-compose.yml`, so `docker compose up`
  is still one container. The stack costs nothing to people who only want
  the API, and `make observe` starts api + Prometheus + Grafana together.
- **An alert or a panel must name a series that exists.**
  `tests/test_observability.py` parses `rules.yml` and the dashboard JSON,
  extracts every `eurostream_*` identifier, and checks it against the metric
  table; a second test scans the source for emitted metric literals and
  fails if one has no help text or if a declared metric is never emitted.
  That second test exists because of a real find: `build_info`,
  `erasure_completed` and `erasure_queue_depth` were all declared with help
  text and emitted nowhere — three metrics a dashboard could have been built
  on top of and would have been empty — and they are wired up now, with the
  test keeping them that way.
- **Latency is a mean, and it says so.** `http_request_duration_seconds` is
  exported as a summary — count, sum, max — with no buckets, so the
  dashboard plots `increase(_sum)/increase(_count)` over a window and titles
  it "mean". A quantile from a summary without buckets would be invented,
  not measured, and an invented number in a latency panel is worse than no
  panel. Histogram buckets are the upgrade path if a real latency SLO is
  ever wanted; until then the honest number is the one that can be computed.
- **Scrape faster than the budget moves.** The SLO and error budget are
  computed at scrape time over a rolling 300s window; scraping every 15s
  (10s for the EuroStream job) means the budget an operator reads is never
  already stale when they look at it.
- **429 is not a failure.** Rate-limited requests get their own panel and an
  `info` alert after ten minutes of continuous shedding. The token bucket
  working is not an incident; silent 429s would be the dishonest version of
  the same metric, so they are counted, shown, and never allowed to fail a
  run.
- **Eleven alerts across four groups** (availability, GDPR erasure, ingest,
  live feed), each with severity, a summary and a runbook line. Deliberately
  absent: an alert on `events_produced`, which only the demo's produce
  endpoint increments — in a real deployment events arrive on the bus, and
  that alert would fire in every environment where the metric is honest.
- The dashboard **upgrades** the dashboard page's 6s polling rather than
  replacing it: the live feed is an acceleration, not a single point of
  failure.

## Consequences

- Metric names are now a contract with a test: renaming or removing one
  fails the suite with the name of the alert or panel it broke, instead of
  shipping a dashboard of empty graphs.
- Help text is complete for everything the source emits, so a Grafana tooltip
  or an alert annotation is never blank.
- Latency claims are limited by the metric's resolution. Any statement about
  p95 HTTP latency requires adding histogram buckets first — an explicit,
  deliberate upgrade rather than a quiet approximation.
- The stack is reproducible from the repo: `make observe` is the whole
  setup, the datasource, dashboards and rules are provisioned from files
  rather than imported by hand, and the host ports are overridable for the
  machines where 3000 is already taken.
- **Known limit:** the test validates names, structure and wiring — targets,
  profile gating, mounted files, datasource uid — but not PromQL syntax; a
  malformed expression would pass the suite. The rules were loaded into a
  real Prometheus during the work (four groups, eleven rules, zero unhealthy
  and a healthy target), and `promtool` in CI would close the remaining gap.
- Alerts ship with runbooks because an alert without one is a notification,
  not an answer.

## Alternatives considered

- **A Prometheus Exporter exporter sidecar (statsd/exporter):** rejected —
  the app already renders the exposition format; a sidecar would add a
  container, a port and a second place for a metric name to be wrong.
- **Grafana with a JSON dashboard imported by hand:** rejected — it works
  once and is unreviewable afterwards; provisioned files are diffable and
  testable like the rest of the repository.
- **Plotting a quantile from the existing summary:** rejected — it would be
  a plausible-looking number derived from data that cannot support it.
- **Alerting on request rate dropping to zero:** rejected as a general rule —
  this service legitimately goes quiet between runs, and an alert that fires
  on an idle demo teaches people to ignore alerts.

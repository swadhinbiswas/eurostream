# Observability

Prometheus + Grafana for EuroStream, wired to the metrics the app already
exposes — no exporter sidecar, no agent, one scrape endpoint.

```bash
make observe          # docker compose --profile observe up -d
make observe-down     # docker compose --profile observe down
```

| Service    | URL                        | Credentials                       |
| ---------- | -------------------------- | --------------------------------- |
| API        | http://localhost:7860/docs | —                                 |
| Prometheus | http://localhost:9090      | —                                 |
| Grafana    | http://localhost:3000      | `admin` / `eurostream` (override with `GRAFANA_ADMIN_PASSWORD`) |

Host ports are overridable when something else already owns one — `3000` in
particular is a busy port on developer machines:

```bash
GRAFANA_PORT=3001 PROMETHEUS_PORT=9091 make observe
```

The **EuroStream** dashboard is provisioned into the *EuroStream* folder: SLO
and error budget, request rate by status, mean request time, the erasure
pipeline (requested / completed / failed), queue depth, erasure latency
against the SLA, rate-limited and malformed traffic, the live SSE feed, and
whatever is firing.

## What is here

| File                                            | Purpose                                                  |
| ----------------------------------------------- | -------------------------------------------------------- |
| `prometheus.yml`                                | Scrape `/metrics/prometheus` every 10s, load the rules.  |
| `rules.yml`                                     | 9 alerts: availability, error budget, GDPR erasure, ingest, live feed. |
| `grafana/provisioning/datasources/prometheus.yml` | Points Grafana at the `prometheus` service.            |
| `grafana/provisioning/dashboards/dashboards.yml`  | Loads the dashboard from disk and rescans every 30s.   |
| `grafana/dashboards/eurostream.json`            | The dashboard itself.                                    |

## Rules that keep this honest

**Every query names a series that exists.** An alert pointing at a metric
which never appears looks like coverage and fires nothing — worse than no
alert. `tests/test_observability.py` parses `rules.yml` and the dashboard
JSON, extracts every `eurostream_*` identifier, and checks it against the
metric table in `eurostream.metrics`; it also checks the scrape target
matches the compose service. Rename or remove a metric and the test suite
tells you which alert broke.

**Latency is a mean, not a quantile.** `http_request_duration_seconds` is
exported as a summary (count/sum/max, no buckets), so the dashboard plots
`increase(_sum)/increase(_count)` and says so. A quantile from a summary
without buckets would be invented, not measured. Adding histogram buckets
would be the upgrade path if a real latency SLO is ever wanted.

**429s are not failures.** Rate-limited requests appear on their own panel
and only page after ten minutes of continuous shedding — the token bucket
working is not an incident.

## Adding a metric

1. Emit it in `eurostream.metrics` and add a line to `_HELP`.
2. Reference it in `rules.yml` or the dashboard.
3. Run `make gate` — the contract test fails if step 2 names something
   step 1 does not produce.

For local metrics without this stack, `GET /metrics` (JSON) and
`GET /metrics/prometheus` (exposition format) work against any running
server, and `eurostream load-test` will give that server something to
measure.

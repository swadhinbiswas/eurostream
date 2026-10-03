"""The observability stack must describe the app that actually runs.

Two classes of drift are caught here. The first is metric drift: an alert
or a panel naming a series that no longer exists looks like coverage and
fires nothing, which is worse than having no alert at all — so every
``eurostream_*`` identifier in ``rules.yml`` and the dashboard is checked
against the metric table. The second is emission drift: a metric declared
in ``_HELP`` and never incremented (``build_info``, ``erasure_completed``
and ``erasure_queue_depth`` were all in that state) shows up as a gap
between the metrics the source emits and the ones the docs advertise.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from eurostream import __version__
from eurostream.bus.sqlite import open_bus
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import _HELP, NAMESPACE, Metrics, _exported_name
from eurostream.models import ErasureRequested
from eurostream.warehouse import Warehouse

ROOT = Path(__file__).resolve().parent.parent
OBSERVABILITY = ROOT / "observability"
COMPOSE = ROOT / "docker-compose.yml"

_METRIC_REF = re.compile(rf"\b{NAMESPACE}_[a-z0-9_]+")
#: ``metrics.incr("name")`` anywhere in the source — but not ``incr(f"...")``,
#: whose name only exists at runtime.
_EMITTED = re.compile(r'\.(?:incr|set_gauge|observe)\(\s*(?<!f)"([a-z0-9_]+)"')


def _known_metrics() -> set[str]:
    """Every name the exposition format can produce, from the metric table."""
    names: set[str] = set()
    for key in _HELP:
        names.add(_exported_name(key, "counter"))
        names.add(_exported_name(key, "gauge"))
        names.add(f"{_exported_name(key, 'summary')}_sum")
        names.add(f"{_exported_name(key, 'summary')}_count")
    return names


def _metrics_in(*expressions: str) -> set[str]:
    found: set[str] = set()
    for expression in expressions:
        found.update(_METRIC_REF.findall(expression))
    return found


def _rules() -> list[dict]:
    return yaml.safe_load((OBSERVABILITY / "rules.yml").read_text())["groups"]


def _dashboard() -> dict:
    return json.loads((OBSERVABILITY / "grafana/dashboards/eurostream.json").read_text())


def _expressions(payload: dict) -> list[str]:
    return [
        target["expr"]
        for panel in payload["panels"]
        for target in panel.get("targets", [])
        if "expr" in target
    ]


# --------------------------------------------------------------- metric contract


def test_every_declared_metric_has_help_text_and_every_emitted_one_is_declared():
    declared = set(_HELP)
    emitted: set[str] = set()
    for path in (ROOT / "src" / "eurostream").rglob("*.py"):
        emitted.update(_EMITTED.findall(path.read_text()))

    # Nothing emitted goes out without documentation...
    missing = emitted - declared
    assert not missing, f"metrics emitted without HELP text: {sorted(missing)}"
    # ...and nothing is documented that nobody emits. The three names that
    # failed this before they were wired up are the reason it exists.
    orphaned = {
        "build_info",
        "erasure_completed",
        "erasure_queue_depth",
    } - emitted
    assert not orphaned, f"declared but never emitted: {sorted(orphaned)}"


def test_build_info_and_up_are_exported():
    text = Metrics().render_prometheus()
    assert f'{NAMESPACE}_build_info{{version="{__version__}"}} 1' in text
    assert f"{NAMESPACE}_up 1" in text


# ------------------------------------------------------------- prometheus config


def test_scrape_config_points_at_the_compose_api_service():
    config = yaml.safe_load((OBSERVABILITY / "prometheus.yml").read_text())
    compose = yaml.safe_load(COMPOSE.read_text())

    assert config["rule_files"] == ["/etc/prometheus/rules.yml"]
    scrape = config["scrape_configs"][0]
    assert scrape["job_name"] == "eurostream"
    assert scrape["metrics_path"] == "/metrics/prometheus"
    # Scraping faster than the 300s SLO window would show a stale budget.
    assert scrape["scrape_interval"] == "10s"

    host, port = scrape["static_configs"][0]["targets"][0].split(":")
    assert host in compose["services"], "scrape target is not a compose service"
    # Host ports may be overridden with an env var, so compare the container
    # side of each mapping: Prometheus must reach this port inside the network.
    ports = compose["services"][host].get("ports", [])
    assert any(entry.split(":")[-1] == port for entry in ports), (
        f"container port {port} is not published by {host}: {ports}"
    )


def test_prometheus_and_grafana_are_behind_the_observe_profile():
    compose = yaml.safe_load(COMPOSE.read_text())
    services = compose["services"]

    for name in ("prometheus", "grafana"):
        assert services[name]["profiles"] == ["observe"], f"{name} must be opt-in"
    # The API stays unconditional: `docker compose up` must still be one
    # container, or the quickstart in the README breaks.
    assert "profiles" not in services["api"]

    mounts = " ".join(services["prometheus"]["volumes"])
    assert "./observability/prometheus.yml" in mounts
    assert "./observability/rules.yml" in mounts
    grafana_mounts = " ".join(services["grafana"]["volumes"])
    assert "./observability/grafana/provisioning" in grafana_mounts
    assert "./observability/grafana/dashboards" in grafana_mounts

    for volume in ("prometheus-data", "grafana-data"):
        assert volume in compose["volumes"]


def test_mounted_config_files_exist():
    compose = yaml.safe_load(COMPOSE.read_text())
    for service in ("prometheus", "grafana"):
        for mount in compose["services"][service]["volumes"]:
            source = mount.split(":")[0]
            if source.startswith("./"):
                assert (ROOT / source).exists(), f"{source} is mounted but missing"


# ------------------------------------------------------------- alerts & dashboard


def test_every_alert_names_a_metric_that_exists():
    known = _known_metrics()
    referenced: set[str] = set()
    alerts = 0
    for group in _rules():
        assert group["rules"], f"group {group['name']} has no rules"
        for rule in group["rules"]:
            alerts += 1
            assert rule["labels"]["severity"] in {"critical", "warning", "info"}
            assert rule["annotations"]["summary"], rule["alert"]
            referenced |= _metrics_in(rule["expr"])

    assert alerts >= 8, "the stack should ship a real alert set"
    unknown = referenced - known
    assert not unknown, f"alerts reference metrics that do not exist: {sorted(unknown)}"
    # The alerts a reader would expect from this project's promises.
    names = {rule["alert"] for group in _rules() for rule in group["rules"]}
    assert "EurostreamDown" in names
    assert "ErrorBudgetBurningTooFast" in names
    assert "ErasureSlaBreached" in names


def test_every_dashboard_panel_query_names_a_metric_that_exists():
    dashboard = _dashboard()
    known = _known_metrics()

    assert dashboard["uid"] and dashboard["title"]
    assert dashboard["panels"], "an empty dashboard is a worse artefact than none"
    for panel in dashboard["panels"]:
        assert panel["title"], "a panel nobody can name cannot be read"
        assert panel["gridPos"]["w"] > 0 and panel["gridPos"]["h"] > 0
        assert panel["datasource"]["uid"] == "prometheus", panel["title"]
        assert panel.get("targets"), panel["title"]

    unknown = _metrics_in(*_expressions(dashboard)) - known
    assert not unknown, f"panels reference metrics that do not exist: {sorted(unknown)}"


def test_dashboard_uid_matches_the_provisioned_datasource():
    datasource = yaml.safe_load(
        (OBSERVABILITY / "grafana/provisioning/datasources/prometheus.yml").read_text()
    )["datasources"][0]
    provider = yaml.safe_load(
        (OBSERVABILITY / "grafana/provisioning/dashboards/dashboards.yml").read_text()
    )["providers"][0]

    assert datasource["url"] == "http://prometheus:9090"
    assert datasource["uid"] == "prometheus"
    uids = {panel["datasource"]["uid"] for panel in _dashboard()["panels"]}
    assert uids == {datasource["uid"]}, "a panel pointing at an unprovisioned datasource"

    # The dashboard file is the one the provider loads from disk.
    assert provider["options"]["path"] == "/var/lib/grafana/dashboards"
    assert (OBSERVABILITY / "grafana/dashboards/eurostream.json").exists()


# --------------------------------------------------------------- the metrics, live


def _service(settings, tmp_path, metrics: Metrics) -> ErasureService:
    bus = open_bus(tmp_path / "events.db")
    warehouse = Warehouse(tmp_path / "warehouse.duckdb")
    return ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "obs-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
        sla_seconds=60,
    )


def test_erasure_completed_counts_a_finished_cascade(settings, tmp_path):
    metrics = Metrics()
    service = _service(settings, tmp_path, metrics)

    audit = service.execute(
        ErasureRequested(
            event_id="obs-1",
            occurred_at=0.0,
            request_id="obs-1",
            customer_id="cust_obs",
        )
    )
    assert audit.status == "completed"

    counters = metrics.snapshot()["counters"]
    assert counters.get("erasure_completed") == 1
    assert counters.get("erasure_failed") is None
    assert f"{NAMESPACE}_erasure_completed_total 1" in metrics.render_prometheus()


def test_queue_depth_gauge_rises_at_intake_and_falls_on_completion(settings, tmp_path):
    metrics = Metrics()
    service = _service(settings, tmp_path, metrics)

    request_id = service.request_erasure("cust_depth")
    assert metrics.snapshot()["gauges"].get("erasure_queue_depth") == 1.0

    service.execute(
        ErasureRequested(
            event_id=request_id,
            occurred_at=0.0,
            request_id=request_id,
            customer_id="cust_depth",
        )
    )
    # Published after the pop, so an emptied queue reads 0 rather than
    # sticking at its high-water mark.
    assert metrics.snapshot()["gauges"]["erasure_queue_depth"] == 0.0
    assert f"{NAMESPACE}_erasure_queue_depth 0" in metrics.render_prometheus()

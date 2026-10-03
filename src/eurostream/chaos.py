"""Deliberate-failure drills: break something on purpose, then report
whether a guardrail caught it.

Each scenario runs in its own directory under a fresh tempdir, so
``eurostream chaos`` never opens the operator's warehouse, audit log or
event log. These are not load tests and not chaos on a live system: they
are executable evidence that the protections this project claims — jittered
retries, the circuit breaker, the freshness and volume gates, the hash
chained audit log, the dead-letter queue — actually fire when the thing
they guard against happens.
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import httpx

from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings
from eurostream.governance.audit_chain import AuditChain, verify_audit_log
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.producers import EventGenerator
from eurostream.quality import DataQualityEngine
from eurostream.turso import TursoClient
from eurostream.warehouse import Warehouse


@dataclass(frozen=True)
class ScenarioResult:
    """One drill: what was broken, and whether a guardrail caught it."""

    name: str
    ok: bool
    detail: str


#: A scenario takes its sandbox directory and answers (held?, what happened).
ScenarioFn = Callable[[Path], tuple[bool, str]]


def _as_int(value: object) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _ok_execute() -> dict[str, object]:
    """A Turso pipeline response for a write that affected one row."""
    return {"results": [{"type": "ok", "response": {"result": {"affected_row_count": 1}}}]}


def _settings(root: Path) -> Settings:
    """Settings that live entirely inside ``root``: the operator's paths
    are never read from, never written to, and never even opened."""
    data = root / "data"
    return Settings(
        data_dir=data,
        warehouse_path=data / "warehouse.duckdb",
        lake_root=root / "lake",
        audit_log_path=data / "logs" / "audit.jsonl",
        metrics_path=data / "logs" / "metrics.jsonl",
        pii_manifest_path=root / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
    )


def _seed(warehouse: Warehouse, settings: Settings, customers: int = 4) -> None:
    generator = EventGenerator(settings)
    for i in range(customers):
        warehouse.append_order(generator.order(customer_id=f"cust_{i}", consent=True))
    warehouse.build_silver()
    warehouse.build_gold()


# ------------------------------------------------------------- scenarios


def retry_recovers(root: Path) -> tuple[bool, str]:
    """A flaky dependency: two failures, then health — the write must succeed."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, json={"error": "unavailable"})
        return httpx.Response(200, json=_ok_execute())

    client = TursoClient(
        "libsql://chaos.turso.io",
        "token",
        transport=httpx.MockTransport(handler),
        retry_attempts=3,
        retry_base_delay=0.01,
    )
    try:
        affected = client.execute("INSERT INTO bronze.orders VALUES (1)")
        status = client.status()
    finally:
        client.close()

    retries = _as_int(status.get("retries"))
    state = str(status.get("state"))
    held = affected == 1 and retries >= 2 and state == "closed"
    return (
        held,
        f"transport failed twice, write succeeded on HTTP call {calls['n']} "
        f"(retries={retries}, circuit={state})",
    )


def breaker_refuses(root: Path) -> tuple[bool, str]:
    """A dead dependency: the breaker must open and refuse to try again."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, json={"error": "boom"})

    client = TursoClient(
        "libsql://chaos.turso.io",
        "token",
        transport=httpx.MockTransport(handler),
        retry_attempts=1,
        failure_threshold=1,
        recovery_seconds=60,
    )
    refused = False
    try:
        try:
            client.execute("INSERT INTO bronze.orders VALUES (1)")
        except Exception:  # noqa: BLE001, S110 - the first failure is expected
            pass
        after_first = calls["n"]
        try:
            client.execute("INSERT INTO bronze.orders VALUES (1)")
        except Exception as exc:  # noqa: BLE001 - refusal is the expected outcome
            refused = type(exc).__name__ == "CircuitOpenError"
        # Fast-fail: the refusal must not have cost an HTTP round trip.
        held = refused and calls["n"] == after_first
        state = str(client.status().get("state"))
    finally:
        client.close()

    return (
        held and state == "open",
        f"circuit opened after {after_first} failed call(s); the next write was "
        f"refused without an HTTP request (state={state})",
    )


def freshness_gate(root: Path) -> tuple[bool, str]:
    """A stalled ingest: the freshness gate must fail the stale layer."""
    settings = _settings(root)
    warehouse = Warehouse(settings.warehouse_path)
    try:
        generator = EventGenerator(settings)
        for i in range(4):
            warehouse.append_order(generator.order(customer_id=f"cust_{i}", consent=True))
        # The source stopped three hours ago.
        warehouse.conn.execute("UPDATE bronze.orders SET occurred_at = ?", (time.time() - 7200,))
        warehouse.build_silver()
        warehouse.build_gold()

        report = DataQualityEngine(warehouse, freshness_seconds=3600).run_all()
        failing = [
            result.check_name
            for result in report.results
            if not result.passed and result.check_name.startswith("freshness.")
        ]
        held = bool(failing) and not report.all_passed
    finally:
        warehouse.close()
    return held, f"freshness failed {len(failing)} layer(s): {', '.join(failing) or 'none'}"


def volume_gate(root: Path) -> tuple[bool, str]:
    """A transform that wipes a table: the volume gate must notice."""
    settings = _settings(root)
    warehouse = Warehouse(settings.warehouse_path)
    try:
        _seed(warehouse, settings)
        DataQualityEngine(warehouse).run_all()  # record the baseline counts

        warehouse.conn.execute(
            "DELETE FROM bronze.orders WHERE event_id IN "
            "(SELECT event_id FROM bronze.orders LIMIT 3)"
        )
        report = DataQualityEngine(warehouse, volume_drop_pct=50.0).run_all()
        volume = [r for r in report.results if not r.passed and r.check_name.startswith("volume.")]
        detail = "; ".join(f"{r.check_name}: {r.detail}" for r in volume)
        held = bool(volume)
    finally:
        warehouse.close()
    return held, detail or "no volume check failed"


def audit_tamper(root: Path) -> tuple[bool, str]:
    """An edited audit record: the hash chain must refuse it."""
    settings = _settings(root)
    path = settings.audit_log_path
    path.parent.mkdir(parents=True, exist_ok=True)
    chain = AuditChain(path)
    chain.link(
        {
            "request_id": "chaos-1",
            "customer_id": "cust_chaos",
            "requested_at": time.time(),
            "completed_at": time.time(),
            "layers_touched": ["warehouse"],
            "status": "completed",
        }
    )
    chain.link(
        {
            "request_id": "chaos-2",
            "customer_id": "cust_other",
            "requested_at": time.time(),
            "completed_at": time.time(),
            "layers_touched": ["warehouse"],
            "status": "completed",
        }
    )

    # Rewrite history: one customer swapped for another, hash untouched.
    lines = path.read_text(encoding="utf-8").splitlines()
    lines[0] = lines[0].replace("cust_chaos", "cust_someone_else")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    verification = verify_audit_log(path)
    held = not verification.ok and bool(verification.errors)
    return (
        held,
        f"edited record 1 of {len(lines)}; verifier reported {len(verification.errors)} error(s)",
    )


def poison_message(root: Path) -> tuple[bool, str]:
    """An unparseable erasure request: dead-letter it, keep the worker alive."""
    settings = _settings(root)
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    bus = open_bus(settings.data_dir / "events.db")
    warehouse = Warehouse(settings.warehouse_path)
    metrics = Metrics(settings.metrics_path)
    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "chaos", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
        sla_seconds=60,
    )
    bus.produce("erasure_requests", key="poison", value="{this is not json")

    stop = threading.Event()
    worker = threading.Thread(
        target=erasure.run_worker,
        kwargs={"poll_timeout": 0.05, "stop_event": stop},
        daemon=True,
    )
    worker.start()
    dead = 0
    try:
        deadline = time.time() + 15
        while time.time() < deadline:
            # A fresh throwaway group every probe: it reads the log from the
            # front and never commits, so nothing else's cursor moves.
            probe = bus.consumer(
                "erasure_requests_dlq",
                f"chaos-probe-{uuid4()}",
                auto_offset_reset="earliest",
            )
            dead = 0
            while probe.poll(0.0) is not None:
                dead += 1
            if dead:
                break
            time.sleep(0.05)
        alive = worker.is_alive()
    finally:
        stop.set()
        worker.join(timeout=5)
        warehouse.close()
        bus.close()

    return (
        dead >= 1 and alive,
        f"poison request dead-lettered ({dead} in the DLQ); worker still running"
        if alive
        else f"poison request dead-lettered ({dead} in the DLQ) but the worker died",
    )


#: Ordered by how a failure actually arrives: first the remote dependency
#: goes wrong, then the data goes wrong, then the evidence goes wrong.
SCENARIOS: dict[str, ScenarioFn] = {
    "retry-recovers": retry_recovers,
    "breaker-refuses": breaker_refuses,
    "freshness-gate": freshness_gate,
    "volume-gate": volume_gate,
    "audit-tamper": audit_tamper,
    "poison-message": poison_message,
}


def new_sandbox() -> Path:
    """A fresh temp directory the drills may ruin."""
    return Path(tempfile.mkdtemp(prefix="eurostream-chaos-"))


def discard_sandbox(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def run_scenarios(sandbox: Path, names: Sequence[str] | None = None) -> list[ScenarioResult]:
    """Run ``names`` (default: all) inside ``sandbox``.

    A scenario that raises is reported as a failed drill — a guardrail that
    could not be exercised is not a guardrail that held."""
    selected = list(names) if names else list(SCENARIOS)
    results: list[ScenarioResult] = []
    for name in selected:
        scenario = SCENARIOS[name]
        root = sandbox / name
        root.mkdir(parents=True, exist_ok=True)
        try:
            held, detail = scenario(root)
        except Exception as exc:  # noqa: BLE001 - a crashed drill is a failed drill
            results.append(
                ScenarioResult(name, False, f"scenario crashed: {type(exc).__name__}: {exc}")
            )
            continue
        results.append(ScenarioResult(name, bool(held), str(detail)))
    return results


__all__ = [
    "SCENARIOS",
    "ScenarioResult",
    "discard_sandbox",
    "new_sandbox",
    "run_scenarios",
]

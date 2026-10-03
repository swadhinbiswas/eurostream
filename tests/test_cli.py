from __future__ import annotations

import hashlib
import json
import json as _json
import threading
import time
from uuid import uuid4

from typer.testing import CliRunner

from eurostream.bus.sqlite import open_bus
from eurostream.cli import DLQ_TOPIC, ERASURE_TOPIC, app
from eurostream.config import get_settings
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.models import ErasureRequested
from eurostream.portal import build_portal_html
from eurostream.producers import EventGenerator
from eurostream.warehouse import Warehouse

runner = CliRunner()


def test_cli_contracts(tmp_path):
    out = tmp_path / "contracts.json"
    res = runner.invoke(app, ["contracts", "--out", str(out)])
    assert res.exit_code == 0
    assert out.exists()


def test_cli_produce_and_transform(tmp_path, monkeypatch):
    monkeypatch.setenv("EUROSTREAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EUROSTREAM_WAREHOUSE_PATH", str(tmp_path / "data" / "warehouse.duckdb"))
    monkeypatch.setenv("EUROSTREAM_LAKE_ROOT", str(tmp_path / "data" / "lake"))
    monkeypatch.setenv("EUROSTREAM_AUDIT_LOG_PATH", str(tmp_path / "data" / "audit.jsonl"))
    monkeypatch.setenv("EUROSTREAM_METRICS_PATH", str(tmp_path / "data" / "metrics.jsonl"))

    # 1. Produce
    res_produce = runner.invoke(app, ["produce", "--events", "5"])
    assert res_produce.exit_code == 0
    assert "produced 5 events" in res_produce.stdout

    # 2. Stream
    res_stream = runner.invoke(app, ["stream", "--max-events", "5"])
    assert res_stream.exit_code == 0

    # 3. Transform
    res_transform = runner.invoke(app, ["transform", "--incremental"])
    assert res_transform.exit_code == 0
    assert "quality_gate: ok" in res_transform.stdout


def test_portal_html():
    html = build_portal_html()
    assert "<!DOCTYPE html>" in html
    assert "EuroStream" in html


def _point_cli_at(tmp_path, monkeypatch):
    monkeypatch.setenv("EUROSTREAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EUROSTREAM_WAREHOUSE_PATH", str(tmp_path / "data" / "warehouse.duckdb"))
    monkeypatch.setenv("EUROSTREAM_LAKE_ROOT", str(tmp_path / "data" / "lake"))
    monkeypatch.setenv("EUROSTREAM_AUDIT_LOG_PATH", str(tmp_path / "data" / "audit.jsonl"))
    monkeypatch.setenv("EUROSTREAM_METRICS_PATH", str(tmp_path / "data" / "metrics.jsonl"))
    return tmp_path / "data" / "audit.jsonl"


def test_cli_verify_audit_passes_after_a_real_erasure(tmp_path, monkeypatch):
    audit_path = _point_cli_at(tmp_path, monkeypatch)

    erased = runner.invoke(app, ["erase", "cust_999999"])
    assert erased.exit_code == 0, erased.output

    res = runner.invoke(app, ["verify-audit"])
    assert res.exit_code == 0, res.output
    assert "audit chain is intact" in res.output
    assert str(audit_path) in res.output
    assert "1 chained" in res.output


def test_cli_verify_audit_json_is_machine_readable(tmp_path, monkeypatch):
    _point_cli_at(tmp_path, monkeypatch)
    assert runner.invoke(app, ["erase", "cust_123123"]).exit_code == 0

    res = runner.invoke(app, ["verify-audit", "--json"])
    assert res.exit_code == 0, res.output
    payload = _json.loads(res.stdout)
    assert payload["ok"] is True
    assert payload["errors"] == []
    assert payload["entries"] == 1
    assert payload["hashed"] == 1
    assert payload["last_seq"] == 1
    assert payload["chain"]["seq"] == 1
    assert payload["tip"] == payload["chain"]["tip"]


def test_cli_verify_audit_fails_on_an_edited_record(tmp_path, monkeypatch):
    audit_path = _point_cli_at(tmp_path, monkeypatch)
    assert runner.invoke(app, ["erase", "cust_555555"]).exit_code == 0

    lines = audit_path.read_text().splitlines()
    record = _json.loads(lines[0])
    record["status"] = "failed"  # rewrite history
    audit_path.write_text(_json.dumps(record) + "\n")

    res = runner.invoke(app, ["verify-audit"])
    assert res.exit_code == 1
    assert "BROKEN" in res.output
    assert "hash mismatch" in res.output

    json_res = runner.invoke(app, ["verify-audit", "--json"])
    assert json_res.exit_code == 1
    payload = _json.loads(json_res.stdout)
    assert payload["ok"] is False
    assert any("hash mismatch" in e for e in payload["errors"])


def test_cli_verify_audit_missing_file_exits_one(tmp_path, monkeypatch):
    _point_cli_at(tmp_path, monkeypatch)
    res = runner.invoke(app, ["verify-audit", "--path", str(tmp_path / "never-written.jsonl")])
    assert res.exit_code == 1
    assert "does not exist" in res.output


def test_cli_verify_audit_strict_rejects_unhashed_records(tmp_path, monkeypatch):
    audit_path = _point_cli_at(tmp_path, monkeypatch)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        _json.dumps({"request_id": "old-1", "customer_id": "cust_old", "status": "completed"})
        + "\n"
    )

    lenient = runner.invoke(app, ["verify-audit", "--no-cross-check"])
    assert lenient.exit_code == 0, lenient.output
    assert "predate the hash chain" in lenient.output
    assert "legacy" in lenient.output

    strict = runner.invoke(app, ["verify-audit", "--no-cross-check", "--strict"])
    assert strict.exit_code == 1
    assert "strict mode" in strict.output


def test_cli_verify_audit_can_skip_the_warehouse_copy(tmp_path, monkeypatch):
    _point_cli_at(tmp_path, monkeypatch)
    assert runner.invoke(app, ["erase", "cust_777777"]).exit_code == 0

    res = runner.invoke(app, ["verify-audit", "--no-cross-check"])
    assert res.exit_code == 0, res.output
    assert "scope: file only" in res.output
    assert "cross_checked" not in res.output  # human mode stays human


def _topic_count(bus, topic: str) -> int:
    """Count records in a topic from a throwaway group that never commits."""
    consumer = bus.consumer(topic, f"probe-{uuid4()}", auto_offset_reset="earliest")
    count = 0
    while consumer.poll(0.0) is not None:
        count += 1
    return count


def _park_dead_letter(tmp_path, monkeypatch):
    """Produce a request the worker cannot parse and run the worker on it, so
    the dead letter arrives over the real path instead of being hand-written."""
    _point_cli_at(tmp_path, monkeypatch)
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    bus = open_bus(settings.data_dir / "events.db")
    warehouse = Warehouse(settings.warehouse_path)
    metrics = Metrics(settings.metrics_path)
    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "dlq-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    bus.produce("erasure_requests", key="bad", value="{this is not json")

    stop = threading.Event()
    worker = threading.Thread(
        target=erasure.run_worker,
        kwargs={"poll_timeout": 0.05, "stop_event": stop},
        daemon=True,
    )
    worker.start()
    try:
        deadline = time.time() + 15
        while time.time() < deadline:
            if _topic_count(bus, DLQ_TOPIC) >= 1:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("worker never dead-lettered the malformed request")
    finally:
        stop.set()
        worker.join(timeout=5)
        warehouse.close()
        bus.close()


def test_cli_dlq_list_shows_the_unhandled_letters(tmp_path, monkeypatch):
    _park_dead_letter(tmp_path, monkeypatch)

    res = runner.invoke(app, ["dlq", "list"])
    assert res.exit_code == 0, res.output
    assert "execution_failed" in res.output
    assert "unhandled dead letter" in res.output
    assert "queue is clear" not in res.output

    # A monitor needs an exit code, not a text match.
    assert runner.invoke(app, ["dlq", "list", "--check"]).exit_code == 1

    payload = json.loads(runner.invoke(app, ["dlq", "list", "--json"]).stdout)
    assert payload["count"] == 1
    letter = payload["records"][0]
    assert letter["offset"] == 0
    assert letter["reason"] == "execution_failed"
    assert letter["request_id"] == ""
    assert str(letter["parse_error"]).startswith("JSONDecodeError")
    assert "not json" in str(letter["value"])
    # Reading never consumes: the same letter is still there afterwards.
    assert json.loads(runner.invoke(app, ["dlq", "list", "--json"]).stdout)["count"] == 1


def test_cli_dlq_requeue_republishes_and_advances_the_cursor(tmp_path, monkeypatch):
    _park_dead_letter(tmp_path, monkeypatch)

    dry = runner.invoke(app, ["dlq", "requeue", "--dry-run"])
    assert dry.exit_code == 0, dry.output
    assert "[dry-run] 1 record(s) left untouched" in dry.output
    # Nothing moved and nothing was published.
    assert runner.invoke(app, ["dlq", "list", "--check"]).exit_code == 1

    res = runner.invoke(app, ["dlq", "requeue"])
    assert res.exit_code == 0, res.output
    assert "republished 1 dead letter(s) to erasure_requests" in res.output

    # The cursor moved: nothing is unhandled any more...
    listed = runner.invoke(app, ["dlq", "list"])
    assert "queue is clear" in listed.output
    assert runner.invoke(app, ["dlq", "list", "--check"]).exit_code == 0

    # ...and the request is back on the intake topic, tagged with where it
    # came from, ready for the worker to try again.
    settings = get_settings()
    bus = open_bus(settings.data_dir / "events.db")
    assert _topic_count(bus, ERASURE_TOPIC) == 2  # original + requeue
    replayed = bus.consumer(ERASURE_TOPIC, f"check-{uuid4()}", auto_offset_reset="earliest")
    seen = []
    while (record := replayed.poll(0.0)) is not None:
        seen.append(record)
    bus.close()
    assert seen[-1].headers["requeued_from"] == "dlq"
    assert seen[-1].headers["dlq_offset"] == "0"
    assert seen[-1].value == "{this is not json"


def test_cli_dlq_ack_drops_without_republishing(tmp_path, monkeypatch):
    _park_dead_letter(tmp_path, monkeypatch)

    dry = runner.invoke(app, ["dlq", "ack", "--dry-run"])
    assert dry.exit_code == 0
    assert "[dry-run] 1 record(s) left untouched" in dry.output
    assert runner.invoke(app, ["dlq", "list", "--check"]).exit_code == 1

    res = runner.invoke(app, ["dlq", "ack"])
    assert res.exit_code == 0, res.output
    assert "acknowledged 1 dead letter(s)" in res.output
    assert "queue is clear" in runner.invoke(app, ["dlq", "list"]).output

    settings = get_settings()
    bus = open_bus(settings.data_dir / "events.db")
    assert _topic_count(bus, ERASURE_TOPIC) == 1  # never republished
    assert _topic_count(bus, DLQ_TOPIC) == 1  # kept for audit
    bus.close()


def test_cli_dlq_on_an_empty_queue(tmp_path, monkeypatch):
    _point_cli_at(tmp_path, monkeypatch)

    listed = runner.invoke(app, ["dlq", "list"])
    assert listed.exit_code == 0, listed.output
    assert "queue is clear" in listed.output
    assert runner.invoke(app, ["dlq", "list", "--check"]).exit_code == 0

    assert runner.invoke(app, ["dlq", "requeue"]).exit_code == 0
    assert "nothing to requeue" in runner.invoke(app, ["dlq", "requeue"]).output
    assert "nothing to acknowledge" in runner.invoke(app, ["dlq", "ack"]).output

    help_out = runner.invoke(app, ["dlq", "--help"])
    assert help_out.exit_code == 0
    for command in ("list", "requeue", "ack"):
        assert command in help_out.output


# ------------------------------------------------------- backup / restore


def _seed_backup_state(tmp_path, monkeypatch) -> None:
    """Populate the warehouse and the audit log for a backup, closing every
    handle so the CLI that later restores owns the file outright."""
    _point_cli_at(tmp_path, monkeypatch)
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.audit_log_path.parent.mkdir(parents=True, exist_ok=True)

    bus = open_bus(settings.data_dir / "events.db")
    warehouse = Warehouse(settings.warehouse_path)
    warehouse.append_order(EventGenerator(settings).order(customer_id="cust_seed", consent=True))
    service = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "seed", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=Metrics(),
        sla_seconds=60,
    )
    service.execute(
        ErasureRequested(
            event_id="req-seed",
            occurred_at=time.time(),
            request_id="req-seed",
            customer_id="cust_seed",
        )
    )
    warehouse.close()
    bus.close()


def test_cli_backup_writes_a_checksummed_snapshot(tmp_path, monkeypatch):
    _seed_backup_state(tmp_path, monkeypatch)
    dest = tmp_path / "snap"

    res = runner.invoke(app, ["backup", "--out", str(dest)])
    assert res.exit_code == 0, res.output
    assert "backup written to" in res.output

    manifest = json.loads((dest / "manifest.json").read_text())
    assert manifest["format"] == 1
    assert {"warehouse.duckdb", "erasure_audit.jsonl"} <= set(manifest["files"])

    # The recorded digests are the digests of the bytes that were written.
    digest = hashlib.sha256((dest / "warehouse.duckdb").read_bytes()).hexdigest()
    assert manifest["files"]["warehouse.duckdb"]["sha256"] == digest
    audit_digest = hashlib.sha256((dest / "erasure_audit.jsonl").read_bytes()).hexdigest()
    assert manifest["files"]["erasure_audit.jsonl"]["sha256"] == audit_digest

    # Row counts and the chain tip are the evidence used to verify a restore.
    assert manifest["tables"]["governance.erasure_audit_log"] == 1
    assert manifest["tables"]["governance.suppression_registry"] == 1
    assert manifest["tables"]["bronze.orders"] == 1  # anonymised, not deleted
    assert manifest["audit"] == {
        "records": 1,
        "seq": 1,
        "tip": manifest["audit"]["tip"],
    }
    assert manifest["audit"]["tip"]
    assert str(dest / "manifest.json") in res.output

    # A snapshot is never written over another one.
    again = runner.invoke(app, ["backup", "--out", str(dest)])
    assert again.exit_code == 1
    assert "refusing to overwrite an existing backup" in again.output


def test_cli_restore_round_trips_the_warehouse_and_the_audit_chain(tmp_path, monkeypatch):
    _seed_backup_state(tmp_path, monkeypatch)
    settings = get_settings()
    dest = tmp_path / "snap"
    assert runner.invoke(app, ["backup", "--out", str(dest)]).exit_code == 0
    manifest = json.loads((dest / "manifest.json").read_text())

    # Destroy the live data: rows gone, audit trail replaced with a lie.
    warehouse = Warehouse(settings.warehouse_path)
    warehouse.conn.execute("DELETE FROM bronze.orders")
    warehouse.conn.execute("DELETE FROM governance.suppression_registry")
    warehouse.conn.execute("DELETE FROM governance.erasure_audit_log")
    warehouse.close()
    settings.audit_log_path.write_text('{"request_id": "forged"}\n')

    res = runner.invoke(app, ["restore", str(dest), "--force"])
    assert res.exit_code == 0, res.output
    assert f"verified {len(manifest['files'])} file checksum(s)" in res.output
    assert "row counts match the manifest and the audit chain is intact" in res.output

    warehouse = Warehouse(settings.warehouse_path)
    assert warehouse.scalar("SELECT count(*) FROM bronze.orders", local_only=True) == 1
    assert (
        warehouse.scalar("SELECT count(*) FROM governance.suppression_registry", local_only=True)
        == 1
    )
    warehouse.close()

    # Both copies agree again, and the chain verifies against them.
    verified = runner.invoke(app, ["verify-audit"])
    assert verified.exit_code == 0, verified.output
    assert "intact" in verified.output.lower()
    assert "forged" not in settings.audit_log_path.read_text()


def test_cli_restore_refuses_to_overwrite_without_force(tmp_path, monkeypatch):
    _seed_backup_state(tmp_path, monkeypatch)
    dest = tmp_path / "snap"
    assert runner.invoke(app, ["backup", "--out", str(dest)]).exit_code == 0

    res = runner.invoke(app, ["restore", str(dest)])
    assert res.exit_code == 1
    assert "refusing to overwrite existing data" in res.output
    assert "--force" in res.output


def test_cli_restore_refuses_a_corrupt_snapshot_before_touching_live_data(tmp_path, monkeypatch):
    _seed_backup_state(tmp_path, monkeypatch)
    settings = get_settings()
    dest = tmp_path / "snap"
    assert runner.invoke(app, ["backup", "--out", str(dest)]).exit_code == 0

    # Corrupt one byte of the snapshot's warehouse copy.
    target = dest / "warehouse.duckdb"
    blob = bytearray(target.read_bytes())
    blob[len(blob) // 2] ^= 0xFF
    target.write_bytes(bytes(blob))

    # Mark the live data so we can prove it was left alone.
    settings.audit_log_path.write_text(settings.audit_log_path.read_text() + '{"forged": 1}\n')

    res = runner.invoke(app, ["restore", str(dest), "--force"])
    assert res.exit_code == 1
    assert "checksum mismatch" in res.output
    assert "nothing was restored" in res.output
    assert settings.audit_log_path.read_text().count("\n") == 2


def test_cli_restore_rejects_an_unusable_snapshot(tmp_path, monkeypatch):
    _point_cli_at(tmp_path, monkeypatch)

    empty = tmp_path / "empty"
    empty.mkdir()
    missing = runner.invoke(app, ["restore", str(empty)])
    assert missing.exit_code == 1
    assert "not a EuroStream backup" in missing.output

    # A manifest is untrusted input: names stay inside the snapshot.
    _seed_backup_state(tmp_path, monkeypatch)
    dest = tmp_path / "snap"
    assert runner.invoke(app, ["backup", "--out", str(dest)]).exit_code == 0
    manifest = json.loads((dest / "manifest.json").read_text())
    manifest["files"]["../../evil.txt"] = {"sha256": "0" * 64, "bytes": 1}
    (dest / "manifest.json").write_text(json.dumps(manifest))

    traversal = runner.invoke(app, ["restore", str(dest), "--force"])
    assert traversal.exit_code == 1
    assert "refusing to restore unknown file" in traversal.output
    assert not (dest.parent.parent / "evil.txt").exists()

    manifest = json.loads((dest / "manifest.json").read_text())
    del manifest["files"]["../../evil.txt"]
    manifest["format"] = 99
    (dest / "manifest.json").write_text(json.dumps(manifest))
    future = runner.invoke(app, ["restore", str(dest), "--force"])
    assert future.exit_code == 1
    assert "unsupported backup format" in future.output

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eurostream.api import create_app
from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings
from eurostream.governance.audit_chain import (
    GENESIS,
    AuditChain,
    normalize_audit,
    record_hash,
    verify_audit_log,
)
from eurostream.governance.erasure import ErasureService
from eurostream.metrics import Metrics
from eurostream.warehouse import Warehouse


def _payload(request_id: str, customer_id: str = "cust_1") -> dict[str, object]:
    return {
        "request_id": request_id,
        "customer_id": customer_id,
        "requested_at": 1700000000.0,
        "completed_at": 1700000001.5,
        "layers_touched": ["bronze", "silver"],
        "status": "completed",
        "confirmation_hash": "abc123def456",
    }


def _write(path: Path, *records: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


def _chain(path: Path, count: int, start: int = 1) -> list[dict[str, object]]:
    chain = AuditChain(path)
    return [chain.link(_payload(f"req-{n}")) for n in range(start, start + count)]


# ------------------------------------------------------------------ writing


def test_linking_records_a_chain(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    chain = AuditChain(path)
    first = chain.link(_payload("req-1"))
    second = chain.link(_payload("req-2"))

    assert first["seq"] == 1
    assert first["prev_hash"] == GENESIS
    assert second["seq"] == 2
    assert second["prev_hash"] == first["hash"]
    assert chain.seq == 2
    assert chain.tip == second["hash"]
    assert Path(path).read_text().count("\n") == 2


def test_hash_covers_content_not_field_order(tmp_path: Path) -> None:
    payload = _payload("req-1")
    forwards = record_hash(1, GENESIS, payload)
    backwards = record_hash(1, GENESIS, dict(reversed(list(payload.items()))))
    assert forwards == backwards
    # ...and it must actually depend on the content.
    assert record_hash(1, GENESIS, _payload("req-1", "cust_2")) != forwards
    assert record_hash(2, GENESIS, payload) != forwards
    assert record_hash(1, "deadbeef", payload) != forwards


def test_chain_resumes_where_the_file_left_off(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    _chain(path, 2)

    resumed = AuditChain(path)
    assert resumed.seq == 2
    third = resumed.link(_payload("req-3"))
    assert third["seq"] == 3

    assert verify_audit_log(path).ok


def test_a_failed_write_rolls_the_tip_back(tmp_path: Path) -> None:
    # Path is a directory: mkdir succeeds, open("a") does not.
    chain = AuditChain(tmp_path)
    before_seq, before_tip = chain.seq, chain.tip

    with pytest.raises(IsADirectoryError):
        chain.link(_payload("req-1"))

    # Chaining the next record onto an unwritten one would leave a gap the
    # verifier would blame on the wrong record.
    assert chain.seq == before_seq
    assert chain.tip == before_tip


def test_empty_and_missing_files_start_at_genesis(tmp_path: Path) -> None:
    missing = AuditChain(tmp_path / "nope.jsonl")
    assert missing.seq == 0
    assert missing.tip == GENESIS

    empty = tmp_path / "empty.jsonl"
    empty.touch()
    assert AuditChain(empty).seq == 0


# ---------------------------------------------------------------- verifying


def test_verify_accepts_a_clean_chain(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    records = _chain(path, 3)

    result = verify_audit_log(path)
    assert result.ok
    assert result.chained
    assert result.entries == 3
    assert result.hashed == 3
    assert result.legacy == 0
    assert result.last_seq == 3
    assert result.tip == records[-1]["hash"]
    assert result.errors == []
    assert result.to_dict()["ok"] is True


def test_verify_detects_an_edited_record(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    _chain(path, 3)
    lines = path.read_text().splitlines()
    edited = json.loads(lines[1])
    edited["customer_id"] = "someone_else"  # the classic log edit
    lines[1] = json.dumps(edited)
    path.write_text("\n".join(lines) + "\n")

    result = verify_audit_log(path)
    assert not result.ok
    assert any("line 2" in e and "hash mismatch" in e for e in result.errors)


def test_verify_detects_a_removed_record(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    _chain(path, 3)
    lines = path.read_text().splitlines()
    del lines[1]
    path.write_text("\n".join(lines) + "\n")

    result = verify_audit_log(path)
    assert not result.ok
    assert any("prev_hash does not follow" in e for e in result.errors)
    assert result.entries == 2
    assert result.last_seq == 3  # the tail still claims seq 3


def test_verify_detects_reordered_records(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    _chain(path, 3)
    lines = path.read_text().splitlines()
    lines[0], lines[1] = lines[1], lines[0]
    path.write_text("\n".join(lines) + "\n")

    result = verify_audit_log(path)
    assert not result.ok
    assert any("does not follow" in e for e in result.errors)


def test_verify_reports_a_malformed_line(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    _chain(path, 2)
    with path.open("a", encoding="utf-8") as fh:
        fh.write("{not json at all\n")

    result = verify_audit_log(path)
    assert not result.ok
    assert any("line 3" in e and "not valid JSON" in e for e in result.errors)
    assert result.hashed == 2  # the good records are still judged


def test_verify_tolerates_records_written_before_the_chain(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    _write(path, _payload("legacy-1"), _payload("legacy-2"))
    chain = AuditChain(path)
    chain.link(_payload("req-1"))

    result = verify_audit_log(path)
    assert result.ok  # a legacy record is a gap in evidence, not tampering
    assert result.legacy == 2
    assert result.hashed == 1
    assert not result.chained
    assert any("predate the hash chain" in w for w in result.warnings)


def test_verify_flags_duplicate_request_ids(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    _chain(path, 2, start=1)
    chain = AuditChain(path)
    chain.link(_payload("req-1"))  # a replay of the first DSAR

    result = verify_audit_log(path)
    assert result.ok
    assert result.duplicates == ["req-1"]
    assert any("duplicate request_id" in w for w in result.warnings)


def test_verify_missing_file_is_an_error(tmp_path: Path) -> None:
    result = verify_audit_log(tmp_path / "absent.jsonl")
    assert not result.ok
    assert result.errors == ["audit log does not exist"]


def test_verify_empty_file_is_harmless(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    path.touch()
    result = verify_audit_log(path)
    assert result.ok
    assert result.entries == 0
    assert result.errors == []
    assert result.warnings == []


# --------------------------------------------------------------- cross-check


def _row(record: dict[str, object]) -> dict[str, object]:
    """The warehouse's view: layers as a comma string, no chain fields."""
    return {
        "request_id": record["request_id"],
        "customer_id": record["customer_id"],
        "requested_at": record["requested_at"],
        "completed_at": record["completed_at"],
        "layers_touched": ",".join(record["layers_touched"]),  # type: ignore[arg-type]
        "status": record["status"],
        "confirmation_hash": record["confirmation_hash"],
    }


def test_crosscheck_passes_when_both_copies_agree(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    records = _chain(path, 2)

    result = verify_audit_log(path, expected=[_row(r) for r in records])
    assert result.ok
    assert result.cross_checked
    assert result.errors == []


def test_crosscheck_catches_a_record_missing_from_the_file(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    records = _chain(path, 2)
    truncated = records[1:]  # the tail was dropped from the file
    path.write_text(json.dumps(truncated[0]) + "\n")

    result = verify_audit_log(path, expected=[_row(r) for r in records])
    assert not result.ok
    assert any("audit log does not" in e for e in result.errors)


def test_crosscheck_catches_a_record_missing_from_the_warehouse(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    records = _chain(path, 2)

    result = verify_audit_log(path, expected=[_row(records[0])])
    assert not result.ok
    assert any("warehouse does not" in e for e in result.errors)


def test_crosscheck_catches_a_single_changed_field(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    records = _chain(path, 1)
    row = _row(records[0])
    row["status"] = "failed"

    result = verify_audit_log(path, expected=[row])
    assert not result.ok
    assert any("on 'status'" in e for e in result.errors)


def test_normalize_accepts_both_layer_encodings() -> None:
    assert normalize_audit({"layers_touched": "bronze,silver"})["layers_touched"] == [
        "bronze",
        "silver",
    ]
    assert normalize_audit({"layers_touched": ["bronze", "silver"]})["layers_touched"] == [
        "bronze",
        "silver",
    ]
    assert normalize_audit({"layers_touched": ""})["layers_touched"] == []
    assert normalize_audit({})["requested_at"] == 0.0


# ------------------------------------------------------------------ the API


def _app(tmp_path: Path):
    settings = Settings(
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "eurocart.duckdb",
        audit_log_path=tmp_path / "data" / "logs" / "audit.jsonl",
        metrics_path=tmp_path / "data" / "logs" / "metrics.jsonl",
        pii_manifest_path=tmp_path / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
    )
    bus = open_bus(tmp_path / "events.db")
    warehouse = Warehouse(tmp_path / "eurocart.duckdb")
    metrics = Metrics(tmp_path / "metrics.jsonl")
    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "chain-test", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
    )
    app = create_app(erasure, metrics, settings, warehouse, bus, start_worker=False)
    return app, bus, warehouse, settings


def test_endpoint_verifies_a_freshly_written_attestation(tmp_path: Path) -> None:
    app, bus, wh, settings = _app(tmp_path)
    client = TestClient(app)

    assert client.post(
        "/erasure-requests", json={"customer_id": "cust_verify", "sync": True}
    ).status_code in (200, 202)

    payload = client.get("/governance/erasure-audit/verify").json()
    assert payload["ok"] is True
    assert payload["errors"] == []
    assert payload["entries"] >= 1
    assert payload["hashed"] >= 1
    assert payload["cross_checked"] is True
    assert payload["chain"]["seq"] >= 1
    assert payload["chain"]["tip"]
    assert payload["tip"] == payload["chain"]["tip"]  # file and writer agree

    # The new route is discoverable.
    assert client.get("/api").json()["erasure_audit_verify"] == "/governance/erasure-audit/verify"
    bus.close()
    wh.close()


def test_endpoint_reports_tampering_without_a_500(tmp_path: Path) -> None:
    app, bus, wh, settings = _app(tmp_path)
    client = TestClient(app)
    assert client.post(
        "/erasure-requests", json={"customer_id": "cust_tamper", "sync": True}
    ).status_code in (200, 202)

    # Edit the record in place, exactly as someone covering their tracks would.
    lines = settings.audit_log_path.read_text().splitlines()
    edited = json.loads(lines[0])
    edited["customer_id"] = "cust_someone_else"
    settings.audit_log_path.write_text(json.dumps(edited) + "\n" + "\n".join(lines[1:]) + "\n")

    response = client.get("/governance/erasure-audit/verify")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is False
    assert any("hash mismatch" in e for e in payload["errors"])
    # and the cross-check notices the customer changed in one copy only
    assert any("customer_id" in e for e in payload["errors"])

    # Verification can be run without the warehouse comparison too.
    chain_only = client.get(
        "/governance/erasure-audit/verify", params={"cross_check": False}
    ).json()
    assert chain_only["cross_checked"] is False
    assert any("hash mismatch" in e for e in chain_only["errors"])
    bus.close()
    wh.close()

"""Tamper-evident erasure audit log: a hash chain over the JSONL trail.

Each appended record carries three extra fields:

``seq``
    1-based position in the chain.
``prev_hash``
    the ``hash`` of the record before it (``""`` for the first).
``hash``
    ``sha256(seq \\n prev_hash \\n canonical-json(payload))``.

Editing one line breaks its own hash; deleting or reordering a line breaks
the ``prev_hash`` link that follows it; and because the warehouse holds a
copy of every attestation, dropping records from the *tail* is caught by
cross-checking the file against ``governance.erasure_audit_log``. Nothing
here stops someone with write access to both copies — that needs an anchor
outside the system — but it does mean silent edits stop being silent.

Records written before this feature have no ``hash``; they are counted as
``legacy`` and skipped, and the chain is judged across the records that do
carry one.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: ``prev_hash`` of the first record in a file.
GENESIS = ""

#: Fields a verifier derives rather than trusts.
CHAIN_FIELDS = ("seq", "prev_hash", "hash")

#: Audit columns, in the order the warehouse stores them.
AUDIT_FIELDS = (
    "request_id",
    "customer_id",
    "requested_at",
    "completed_at",
    "layers_touched",
    "status",
    "confirmation_hash",
)


def canonical_payload(payload: Mapping[str, object]) -> str:
    """Deterministic JSON: sorted keys, no whitespace, stable across runs."""
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        ensure_ascii=False,
    )


def record_hash(seq: int, prev_hash: str, payload: Mapping[str, object]) -> str:
    """The chain hash for one record."""
    material = f"{seq}\n{prev_hash}\n{canonical_payload(payload)}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def payload_of(record: Mapping[str, object]) -> dict[str, object]:
    """The record without the chain fields — what the hash is computed over."""
    return {k: v for k, v in record.items() if k not in CHAIN_FIELDS}


class AuditChain:
    """Append-only writer that links every record to the one before it.

    Load and append share a lock, so two threads cannot both build on the
    same tip and leave a forked chain. A failed write rolls the in-memory
    tip back: chaining onto a record that never landed would make the next
    verification fail for the wrong reason.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._seq = 0
        self._tip = GENESIS
        self._loaded = False

    @property
    def seq(self) -> int:
        with self._lock:
            self._load_locked()
            return self._seq

    @property
    def tip(self) -> str:
        with self._lock:
            self._load_locked()
            return self._tip

    def link(self, payload: Mapping[str, object]) -> dict[str, object]:
        """Chain ``payload``, append it to the file, return the full record."""
        with self._lock:
            self._load_locked()
            prev_seq, prev_tip = self._seq, self._tip
            seq = prev_seq + 1
            digest = record_hash(seq, prev_tip, payload)
            record: dict[str, object] = {
                "seq": seq,
                "prev_hash": prev_tip,
                **payload,
                "hash": digest,
            }
            try:
                self._append_locked(record)
            except Exception:
                self._seq, self._tip = prev_seq, prev_tip
                raise
            self._seq, self._tip = seq, digest
            return record

    def _load_locked(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self._path.exists():
            return
        for record in read_records(self._path):
            if isinstance(record.get("seq"), int) and isinstance(record.get("hash"), str):
                self._seq = int(record["seq"])
                self._tip = str(record["hash"])

    def _append_locked(self, record: Mapping[str, object]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, default=str) + "\n"
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
            # An audit record that only reached the page cache can still be
            # lost with the machine; fsync is cheap at DSAR volume.
            os.fsync(fh.fileno())


def read_records(path: Path) -> list[dict[str, Any]]:
    """Every parseable JSON object in the file, in order."""
    records: list[dict[str, Any]] = []
    try:
        with Path(path).open(encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    records.append(parsed)
    except OSError as exc:
        # Unreadable (or a directory): treat as no chain state rather than
        # failing the caller that only wants to append.
        logger.debug("audit chain could not read %s: %s", path, exc)
        return []
    return records


def normalize_audit(record: Mapping[str, object]) -> dict[str, object]:
    """Same shape from either copy: file record or warehouse row.

    The warehouse keeps ``layers_touched`` as a comma string, the file as a
    list; timestamps must compare as floats, not as ``1699999999.1`` vs
    ``"1699999999.1"``.
    """
    layers = record.get("layers_touched", [])
    if isinstance(layers, str):
        layers = [part for part in layers.split(",") if part]
    elif not isinstance(layers, list):
        layers = []

    def as_float(value: object) -> float:
        return float(value) if isinstance(value, (int, float)) else 0.0

    return {
        "customer_id": str(record.get("customer_id", "")),
        "status": str(record.get("status", "")),
        "confirmation_hash": str(record.get("confirmation_hash", "")),
        "layers_touched": list(layers),
        "requested_at": as_float(record.get("requested_at")),
        "completed_at": as_float(record.get("completed_at")),
    }


@dataclass
class AuditVerification:
    """Outcome of checking the chain (and, optionally, the warehouse copy)."""

    path: str
    entries: int = 0
    hashed: int = 0
    legacy: int = 0
    last_seq: int = 0
    tip: str = GENESIS
    duplicates: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    cross_checked: bool = False

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def chained(self) -> bool:
        """True when every record in the file is part of the chain."""
        return self.hashed > 0 and self.legacy == 0

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "chained": self.chained,
            "path": self.path,
            "entries": self.entries,
            "hashed": self.hashed,
            "legacy": self.legacy,
            "last_seq": self.last_seq,
            "tip": self.tip,
            "duplicates": self.duplicates,
            "errors": self.errors,
            "warnings": self.warnings,
            "cross_checked": self.cross_checked,
        }


def verify_audit_log(
    path: Path,
    *,
    expected: Iterable[Mapping[str, object]] | None = None,
) -> AuditVerification:
    """Verify the chain, and — when ``expected`` is given — the two copies.

    ``expected`` is the warehouse's view of the audit log. Every attestation
    it holds must be in the file with matching contents (catches a truncated
    or edited tail), and every file record must be there too (catches a
    record that never committed).
    """
    path = Path(path)
    result = AuditVerification(path=str(path))
    if not path.exists():
        result.errors.append("audit log does not exist")
        return result

    prev_hash = GENESIS
    prev_seq = 0
    seen_ids: dict[str, str] = {}
    file_records: dict[str, dict[str, Any]] = {}

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        result.errors.append(f"audit log could not be read: {exc}")
        return result

    for lineno, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        result.entries += 1
        try:
            record = json.loads(stripped)
        except json.JSONDecodeError as exc:
            result.errors.append(f"line {lineno}: not valid JSON ({exc})")
            continue
        if not isinstance(record, dict):
            result.errors.append(f"line {lineno}: expected a JSON object")
            continue

        request_id = record.get("request_id")
        if isinstance(request_id, str):
            if request_id in seen_ids:
                result.duplicates.append(request_id)
            seen_ids[request_id] = f"line {lineno}"
            file_records[request_id] = record

        if "hash" not in record:
            result.legacy += 1
            continue

        seq = record.get("seq")
        prev = record.get("prev_hash")
        stored = record.get("hash")
        if not isinstance(seq, int) or not isinstance(prev, str) or not isinstance(stored, str):
            result.errors.append(f"line {lineno}: chained record is missing seq/prev_hash/hash")
            continue

        recomputed = record_hash(seq, prev, payload_of(record))
        if recomputed != stored:
            result.errors.append(
                f"line {lineno}: hash mismatch — this record was modified after it was written"
            )
        if prev != prev_hash:
            result.errors.append(
                f"line {lineno}: prev_hash does not follow the previous chained record "
                "(something was removed, reordered, or inserted)"
            )
        if result.hashed and seq <= prev_seq:
            result.errors.append(
                f"line {lineno}: seq {seq} does not increase (previous record was {prev_seq})"
            )

        # Advance on what the file actually claims, so one bad record does
        # not cascade into a misleading error on every line after it.
        prev_hash, prev_seq = stored, seq
        result.hashed += 1
        result.last_seq = seq
        result.tip = stored

    if result.legacy:
        result.warnings.append(
            f"{result.legacy} record(s) predate the hash chain and cannot be verified"
        )
    if result.duplicates:
        result.warnings.append(
            "duplicate request_id(s): " + ", ".join(sorted(set(result.duplicates)))
        )
    if result.hashed == 0 and result.entries:
        result.warnings.append("no chained records: nothing in this file has been verified")

    if expected is not None:
        result.cross_checked = True
        _crosscheck(result, file_records, expected)

    return result


def _crosscheck(
    result: AuditVerification,
    file_records: Mapping[str, Mapping[str, object]],
    expected: Iterable[Mapping[str, object]],
) -> None:
    expected_by_id: dict[str, Mapping[str, object]] = {}
    for row in expected:
        request_id = row.get("request_id")
        if isinstance(request_id, str):
            expected_by_id[request_id] = row

    for request_id, row in expected_by_id.items():
        record = file_records.get(request_id)
        if record is None:
            result.errors.append(
                f"{request_id}: the warehouse has this attestation but the audit log does not"
            )
            continue
        wanted = normalize_audit(row)
        have = normalize_audit(record)
        for key, value in wanted.items():
            if have.get(key) != value:
                result.errors.append(
                    f"{request_id}: audit log disagrees with the warehouse on {key!r} "
                    f"(log={have.get(key)!r}, warehouse={value!r})"
                )

    for request_id in file_records:
        if request_id not in expected_by_id:
            result.errors.append(
                f"{request_id}: the audit log has this record but the warehouse does not"
            )

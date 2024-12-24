from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from pathlib import Path

from eurostream.config import PII_SALT
from eurostream.models import PIIFlag
from eurostream.types import Manifest, Row

# Re-exported for consumers that want the salt from the governance module;
# the canonical definition lives in eurostream.config (import-cycle safe).
__all__ = ["PII_SALT", "PIIClassifier", "classify_column", "classify_value", "hash_pii"]

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b")
_IPV4_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
_PHONE_RE = re.compile(r"\+?\d[\d\s().-]{7,}")
_IBAN_COUNTRY_PREFIX = ("DE", "FR", "NL", "IE", "DK", "FI", "SE", "AT", "BE", "ES", "IT")
_IBAN_LENGTHS = {
    "DE": 22,
    "FR": 27,
    "NL": 18,
    "IE": 22,
    "DK": 18,
    "FI": 18,
    "SE": 24,
    "AT": 20,
    "BE": 16,
    "ES": 24,
    "IT": 27,
}


def _looks_like_iban(value: str) -> bool:
    """Validates an IBAN by structure and mod-97 checksum (ISO 13616), which
    rejects look-alikes such as hex UUIDs or arbitrary country-prefixed codes."""
    compact = re.sub(r"[^A-Z0-9]", "", value.upper())
    country = compact[:2]
    expected = _IBAN_LENGTHS.get(country)
    if not expected or len(compact) != expected:
        return False
    if not compact[2:4].isdigit():
        return False
    rearranged = compact[4:] + compact[:4]
    numeric = "".join(str(ord(ch) - 55) if ch.isalpha() else ch for ch in rearranged)
    return int(numeric) % 97 == 1


def classify_value(value: str) -> list[str]:
    """Classify a single string value into PII entity types using recognizer
    rules. Pure-Python stand-in for a library like Presidio; keeps local
    runs dependency-free and auditable.

    NAME detection is deliberately conservative (title-cased two-word names)
    to avoid false positives on free text.
    """
    if not value:
        return []
    hits: set[str] = set()
    if _EMAIL_RE.fullmatch(value.strip()):
        hits.add(PIIFlag.EMAIL.value)
    if _looks_like_iban(value):
        hits.add(PIIFlag.IBAN.value)
    if _IPV4_RE.fullmatch(value.strip()):
        try:
            ipaddress.IPv4Address(value.strip())
            hits.add(PIIFlag.IP_ADDRESS.value)
        except ValueError:
            pass
    if _PHONE_RE.fullmatch(value.strip()) and "+" in value:
        hits.add(PIIFlag.PHONE.value)
    if (
        re.fullmatch(r"[A-Z][a-z]+ [A-Z][a-z]+", value.strip())
        and len(value.strip()) < 40
        and not hits
    ):
        hits.add(PIIFlag.NAME.value)
    return sorted(hits)


def classify_column(name: str, sample_values: list[str], threshold: float = 0.6) -> list[str]:
    """Classify a column by majority vote over its sampled values. A column
    only gets a tag when most of its samples agree, avoiding false positives
    on free-text columns."""
    counts: dict[str, int] = {}
    for v in sample_values:
        for tag in classify_value(v):
            counts[tag] = counts.get(tag, 0) + 1
    total = len(sample_values) or 1
    return sorted(tag for tag, n in counts.items() if n / total >= threshold)


class PIIClassifier:
    """Builds and enforces a machine-readable PII column manifest from
    sampled Bronze data. The governance gate fails the pipeline when a new
    column looks like PII but is not yet in the manifest."""

    def __init__(self, manifest_path: Path | None = None) -> None:
        self.manifest_path = manifest_path
        self._manifest: Manifest = {}

    def load(self) -> Manifest:
        if self.manifest_path and self.manifest_path.exists():
            self._manifest = json.loads(self.manifest_path.read_text())
        return self._manifest

    def save(self) -> None:
        if self.manifest_path:
            self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
            self.manifest_path.write_text(json.dumps(self._manifest, indent=2) + "\n")

    def build_from_rows(self, rows: list[Row]) -> Manifest:
        """Classify all columns of a set of rows (the Bronze table scan) and
        store the resulting manifest, keyed by table name -> column -> tags."""
        for row in rows:
            table = str(row.get("_table", "unknown"))
            columns = self._manifest.setdefault(table, {})
            for col, value in row.items():
                if col.startswith("_"):
                    continue
                if value is None:
                    continue
                tags = classify_value(str(value))
                if tags:
                    existing = columns.setdefault(col, [])
                    for t in tags:
                        if t not in existing:
                            existing.append(t)
        return self._manifest

    def detect_unregistered(self, rows: list[Row]) -> list[dict[str, object]]:
        """Returns column-level findings that look like PII but are not in
        the manifest. A non-empty result means the governance gate fails."""
        manifest = self.load()
        findings: list[dict[str, object]] = []
        for row in rows:
            table = str(row.get("_table", "unknown"))
            known = manifest.get(table, {})
            for col, value in row.items():
                if col.startswith("_"):
                    continue
                if value is None:
                    continue
                tags = classify_value(str(value))
                if tags and not set(known.get(col, [])).intersection(tags):
                    findings.append(
                        {
                            "table": table,
                            "column": col,
                            "detected": tags,
                            "registered": known.get(col, []),
                        }
                    )
        return findings


def hash_pii(value: str | None, salt: str = PII_SALT) -> str | None:
    """Irreversible, salted hash used to mask PII in Silver/Gold instead of
    leaking the raw value. Salt must live in a secret manager in production."""
    if value is None:
        return None
    return hashlib.sha256(f"{salt}:{value}".encode()).hexdigest()

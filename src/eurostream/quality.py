from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field

from eurostream.warehouse import Warehouse

_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$")


def _safe_identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"not a safe identifier: {name}")
    return name


@dataclass
class DQCheckResult:
    check_name: str
    passed: bool
    detail: str = ""


@dataclass
class DQReport:
    run_id: str
    results: list[DQCheckResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)


class DataQualityEngine:
    """The governance gate: uniqueness, referential integrity, PII-not-in-
    clear-text, and consent-gating checks. Results are recorded to the
    warehouse and fail the DAG when any check fails."""

    PII_FIELDS = {
        "silver.customers": ["email_hash", "iban_hash"],
    }
    HASH_RE = re.compile(r"^[0-9a-f]{64}$")

    def __init__(self, warehouse: Warehouse) -> None:
        self._warehouse = warehouse

    def run_all(self) -> DQReport:
        report = DQReport(run_id=str(uuid.uuid4()))
        report.results += self._check_uniqueness("gold.customer_360", "customer_id")
        report.results += self._check_uniqueness("gold.order_facts", "order_id")
        for table, columns in self.PII_FIELDS.items():
            report.results += self._check_pii_not_clear(table, columns)
        report.results += self._check_consent_gated()
        report.results += self._check_referential_integrity(
            "gold.order_facts", "customer_id", "gold.customer_360", "customer_id"
        )
        for result in report.results:
            self._warehouse.record_dq(
                report.run_id, result.check_name, result.passed, result.detail
            )
        return report

    def _check_uniqueness(self, table: str, column: str) -> list[DQCheckResult]:
        _safe_identifier(table)
        _safe_identifier(column)
        rows = self._warehouse.query(
            f"SELECT {column}, count(*) c FROM {table} GROUP BY {column} HAVING count(*) > 1"  # noqa: S608
        )
        ok = len(rows) == 0
        return [
            DQCheckResult(
                f"{table}.{column}_unique",
                ok,
                "" if ok else f"{len(rows)} duplicate values",
            )
        ]

    def _check_pii_not_clear(self, table: str, columns: list[str]) -> list[DQCheckResult]:
        results: list[DQCheckResult] = []
        schema, name = table.split(".")
        for col in columns:
            if not self._warehouse.table_exists(schema, name):
                continue
            _safe_identifier(col)
            sample = self._warehouse.query(
                f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL LIMIT 50"  # noqa: S608
            )
            leaked = any(
                (val := r[col]) is not None
                and ("@" in str(val) or not self.HASH_RE.match(str(val)))
                for r in sample
            )
            results.append(
                DQCheckResult(
                    f"{table}.{col}_not_clear",
                    not leaked,
                    "clear-text or non-hashed PII detected" if leaked else "",
                )
            )
        return results

    def _check_consent_gated(self) -> list[DQCheckResult]:
        """Consent mirror integrity: ``consents_marketing`` in the marketing
        view of customer_360 must equal the source ``marketing_consent`` flag
        for every row, so a customer who opted out can never be selected into
        a marketing segment — even if the Gold build is later changed."""
        rows = self._warehouse.query(
            "SELECT count(*) c FROM gold.customer_360 WHERE consents_marketing <> marketing_consent"
        )
        ok = rows[0]["c"] == 0
        return [
            DQCheckResult(
                "consent_gating",
                ok,
                f"{rows[0]['c']} customers where consents_marketing != marketing_consent",
            )
        ]

    def _check_referential_integrity(
        self, table: str, col: str, ref_table: str, ref_col: str
    ) -> list[DQCheckResult]:
        _safe_identifier(table)
        _safe_identifier(col)
        _safe_identifier(ref_table)
        _safe_identifier(ref_col)
        rows = self._warehouse.query(
            f"SELECT count(*) c FROM {table} t LEFT JOIN {ref_table} r "  # noqa: S608
            f"ON t.{col} = r.{ref_col} WHERE r.{ref_col} IS NULL"
        )
        ok = rows[0]["c"] == 0
        return [
            DQCheckResult(
                f"{table}.{col}_references_{ref_table}",
                ok,
                f"{rows[0]['c']} orphans",
            )
        ]

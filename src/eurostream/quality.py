from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from eurostream.warehouse import Warehouse

#: Detail prefix written by the volume check, so the next run can find the
#: count it recorded last time.
_VOLUME_PREFIX = "rows="

_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$")


def _safe_identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"not a safe identifier: {name}")
    return name


def _count(row: dict[str, object], key: str) -> int:
    """Read an aggregate column as int. Rows are ``dict[str, object]``, so the
    conversion is explicit rather than asserted away."""
    value = row.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


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
    clear-text, consent-gating and suppression enforcement. Results are
    recorded to the warehouse and fail the DAG when any check fails.

    Every verdict is computed with ``local_only=True``: a quality gate that
    answers from a cloud replica is not a gate on the data in front of it.
    """

    PII_FIELDS = {
        "silver.customers": ["email_hash", "iban_hash"],
    }
    HASH_RE = re.compile(r"^[0-9a-f]{64}$")
    #: The same pattern, as SQL source. Kept out of the f-string above so the
    #: ``{64}`` quantifier is not mistaken for a format field.
    HASH_SQL = "^[0-9a-f]{64}$"

    #: Tables that must contain no suppressed (erased) customer.
    SUPPRESSION_TABLES = (
        "silver.customers",
        "silver.orders",
        "silver.payments",
        "gold.customer_360",
        "gold.order_facts",
        "gold.fraud_summary",
    )

    #: Table -> column holding its newest event/user time. One entry per
    #: layer, so a stalled consumer in Silver or Gold is as visible as a
    #: stalled ingest in Bronze: freshness is asked of every layer, not just
    #: the front door.
    FRESHNESS_COLUMNS: dict[str, str] = {
        "bronze.orders": "occurred_at",
        "bronze.clicks": "occurred_at",
        "bronze.payments": "occurred_at",
        "silver.customers": "last_seen",
        "silver.orders": "occurred_at",
        "silver.payments": "occurred_at",
        "gold.customer_360": "last_seen",
        "gold.order_facts": "occurred_at",
        "gold.fraud_summary": "last_alert",
    }

    #: Tables whose row count is watched for an unexpected collapse — the
    #: "a transform silently wiped a table" failure mode.
    VOLUME_TABLES = tuple(FRESHNESS_COLUMNS)

    def __init__(
        self,
        warehouse: Warehouse,
        *,
        freshness_seconds: float = 3600.0,
        volume_drop_pct: float = 50.0,
    ) -> None:
        self._warehouse = warehouse
        #: A layer whose newest event is older than this is stalled, not
        #: quiet. Freshness is wall-clock by nature, so the limit is a knob.
        self._freshness_seconds = float(freshness_seconds)
        #: Row counts are only compared against the previous run's, so this
        #: is a *relative* limit: baseline, growth and small erasures pass,
        #: a table that lost more than half of itself does not.
        self._volume_drop_pct = float(volume_drop_pct)

    def run_all(self) -> DQReport:
        report = DQReport(run_id=str(uuid.uuid4()))
        groups: list[tuple[str, Callable[[], list[DQCheckResult]]]] = [
            (
                "gold.customer_360.customer_id_unique",
                lambda: self._check_uniqueness("gold.customer_360", "customer_id"),
            ),
            (
                "gold.order_facts.order_id_unique",
                lambda: self._check_uniqueness("gold.order_facts", "order_id"),
            ),
            (
                "pii_not_clear",
                lambda: sum(
                    (
                        self._check_pii_not_clear(table, columns)
                        for table, columns in self.PII_FIELDS.items()
                    ),
                    [],
                ),
            ),
            ("consent_gating", self._check_consent_gated),
            (
                "gold.order_facts.customer_id_references_gold.customer_360",
                lambda: self._check_referential_integrity(
                    "gold.order_facts", "customer_id", "gold.customer_360", "customer_id"
                ),
            ),
            ("suppression_enforced", self._check_suppression_enforced),
            ("freshness", self._check_freshness),
            ("volume", self._check_volume),
        ]
        for name, check in groups:
            try:
                report.results += check()
            except Exception as exc:  # noqa: BLE001 - a broken check is a failed check
                # A gate that raises on a missing table would take the whole
                # report down with it; record the failure and keep going.
                report.results.append(
                    DQCheckResult(name, False, f"check could not run: {type(exc).__name__}: {exc}")
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
            f"SELECT {column}, count(*) c FROM {table} GROUP BY {column} HAVING count(*) > 1",  # noqa: S608
            local_only=True,
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
        """Assert the whole column is hashed, not the first 50 rows of it.

        The previous version sampled with ``LIMIT 50``, so a single clear-text
        PII value at row 51 passed the gate. The verdict is now aggregated in
        SQL over every non-NULL value and reports how many rows leaked.
        """
        results: list[DQCheckResult] = []
        schema, name = table.split(".")
        for col in columns:
            if not self._warehouse.table_exists(schema, name):
                # The table this check protects is missing — that is a failure,
                # not a check to skip. Silently passing hides an unbuilt layer.
                results.append(
                    DQCheckResult(
                        f"{table}.{col}_not_clear",
                        False,
                        f"table {table} does not exist",
                    )
                )
                continue
            _safe_identifier(col)
            # f-string with HASH_SQL interpolated (not inlined) so the ``{64}``
            # quantifier is data, not a format field.
            sql = f"""
                SELECT count(*) AS total,
                       count(*) FILTER (
                           WHERE {col} IS NOT NULL
                             AND (strpos({col}::VARCHAR, '@') > 0
                                  OR NOT regexp_matches({col}::VARCHAR, '{self.HASH_SQL}'))
                       ) AS leaked
                FROM {table}
                """  # noqa: S608 - identifiers validated above
            row = self._warehouse.query(sql, local_only=True)[0]
            total = _count(row, "total")
            leaked = _count(row, "leaked")
            results.append(
                DQCheckResult(
                    f"{table}.{col}_not_clear",
                    leaked == 0,
                    f"{leaked} of {total} rows hold clear-text or non-hashed PII" if leaked else "",
                )
            )
        return results

    def _check_consent_gated(self) -> list[DQCheckResult]:
        """Consent mirror integrity: ``consents_marketing`` in the marketing
        view of customer_360 must equal the source ``marketing_consent`` flag
        for every row, so a customer who opted out can never be selected into
        a marketing segment — even if the Gold build is later changed.

        ``IS DISTINCT FROM`` rather than ``<>``: with ``<>`` a NULL in either
        column makes the predicate NULL, the row is never counted, and a
        customer whose consent became unknown passes the gate as if they had
        opted out safely.
        """
        rows = self._warehouse.query(
            "SELECT count(*) AS c FROM gold.customer_360 "
            "WHERE consents_marketing IS DISTINCT FROM marketing_consent "
            "   OR consents_marketing IS NULL",
            local_only=True,
        )
        mismatched = _count(rows[0], "c")
        ok = mismatched == 0
        return [
            DQCheckResult(
                "consent_gating",
                ok,
                f"{mismatched} customers where consents_marketing != marketing_consent (or is NULL)"
                if not ok
                else "",
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
            f"ON t.{col} = r.{ref_col} WHERE r.{ref_col} IS NULL",
            local_only=True,
        )
        orphans = _count(rows[0], "c")
        return [
            DQCheckResult(
                f"{table}.{col}_references_{ref_table}",
                orphans == 0,
                f"{orphans} orphans",
            )
        ]

    def _check_suppression_enforced(self) -> list[DQCheckResult]:
        """No suppressed (erased) customer may remain in Silver or Gold.

        This is the check that catches the resurrection bug: the deletion
        cascade tombstones a customer in ``governance.suppression_registry``,
        but any rebuild that forgets the anti-join puts them straight back.
        The gate must notice, not the Data Subject.
        """
        results: list[DQCheckResult] = []
        for table in self.SUPPRESSION_TABLES:
            _safe_identifier(table)
            rows = self._warehouse.query(
                f"SELECT count(*) AS c FROM {table} t "  # noqa: S608
                "JOIN governance.suppression_registry s "
                "ON t.customer_id = s.customer_id",
                local_only=True,
            )
            leaked = _count(rows[0], "c")
            results.append(
                DQCheckResult(
                    f"{table}_no_suppressed_customers",
                    leaked == 0,
                    f"{leaked} suppressed customers present — erasure did not hold"
                    if leaked
                    else "",
                )
            )
        return results

    # -------------------------------------------------- freshness / volume

    def _newest(self, table: str, column: str) -> float | None:
        """Newest timestamp in ``table``, or None when it holds no rows."""
        _safe_identifier(table)
        _safe_identifier(column)
        rows = self._warehouse.query(
            f"SELECT max({column}) AS newest FROM {table}",  # noqa: S608
            local_only=True,
        )
        value = rows[0].get("newest") if rows else None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return None
        return None

    def _check_freshness(self) -> list[DQCheckResult]:
        """Is every layer keeping up with its source?

        ``now - max(newest)`` per table, one check per layer: a stalled
        consumer in Gold looks exactly like a stalled ingest in Bronze, and
        both are worth failing the gate for. An empty table is not stale —
        there is nothing to be behind — so it passes with that said.
        """
        now = time.time()
        results: list[DQCheckResult] = []
        for table, column in self.FRESHNESS_COLUMNS.items():
            name = f"freshness.{table}"
            try:
                newest = self._newest(table, column)
            except Exception as exc:  # noqa: BLE001 - a broken check is a failed check
                results.append(
                    DQCheckResult(name, False, f"check could not run: {type(exc).__name__}: {exc}")
                )
                continue
            if newest is None:
                results.append(DQCheckResult(name, True, "empty — nothing to be stale"))
                continue
            lag = now - newest
            results.append(
                DQCheckResult(
                    name,
                    lag <= self._freshness_seconds,
                    f"newest event {lag:.0f}s ago (limit {self._freshness_seconds:.0f}s)",
                )
            )
        return results

    def _row_count(self, table: str) -> int:
        _safe_identifier(table)
        rows = self._warehouse.query(
            f"SELECT count(*) AS c FROM {table}",  # noqa: S608
            local_only=True,
        )
        return _count(rows[0], "c")

    def _previous_volume(self, check_name: str) -> int | None:
        """The count this check recorded the last time it ran (None = first run)."""
        rows = self._warehouse.query(
            "SELECT detail FROM governance.data_quality_runs "
            "WHERE check_name = ? ORDER BY checked_at DESC LIMIT 1",
            (check_name,),
            local_only=True,
        )
        if not rows:
            return None
        detail = rows[0].get("detail")
        match = re.search(re.escape(_VOLUME_PREFIX) + r"(\d+)", str(detail))
        return int(match.group(1)) if match else None

    def _check_volume(self) -> list[DQCheckResult]:
        """Did any table lose a large share of its rows since the last run?

        Row counts are only compared against what this check itself recorded
        before, which makes the check self-baselining: a fresh warehouse, a
        growing table and a DSAR that removed a customer all pass, while a
        transform that wiped half a table (or emptied it outright) fails with
        both numbers in the detail. There is no absolute expected count to
        keep in config, so the threshold never goes stale as data grows.
        """
        results: list[DQCheckResult] = []
        for table in self.VOLUME_TABLES:
            name = f"volume.{table}"
            try:
                current = self._row_count(table)
                previous = self._previous_volume(name)
            except Exception as exc:  # noqa: BLE001 - a broken check is a failed check
                results.append(
                    DQCheckResult(name, False, f"check could not run: {type(exc).__name__}: {exc}")
                )
                continue

            if previous is None:
                results.append(DQCheckResult(name, True, f"{_VOLUME_PREFIX}{current} (baseline)"))
                continue
            if previous == 0:
                results.append(
                    DQCheckResult(name, True, f"{_VOLUME_PREFIX}{current} (first rows, was 0)")
                )
                continue
            if current == 0:
                results.append(
                    DQCheckResult(
                        name,
                        False,
                        f"{_VOLUME_PREFIX}0 (was {previous}) — table was emptied",
                    )
                )
                continue

            change = (current - previous) / previous * 100.0
            if change < -self._volume_drop_pct:
                results.append(
                    DQCheckResult(
                        name,
                        False,
                        f"{_VOLUME_PREFIX}{current} (was {previous}, {change:.0f}%) — "
                        f"dropped more than {self._volume_drop_pct:.0f}% since the last run",
                    )
                )
                continue
            results.append(
                DQCheckResult(
                    name,
                    True,
                    f"{_VOLUME_PREFIX}{current} (was {previous}, {change:+.0f}%)",
                )
            )
        return results

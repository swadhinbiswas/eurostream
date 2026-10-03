from __future__ import annotations

import time

import pytest
from pydantic import ValidationError

from eurostream.config import Settings
from eurostream.producers import EventGenerator
from eurostream.quality import DataQualityEngine, DQCheckResult, DQReport
from eurostream.warehouse import Warehouse


def _build(warehouse: Warehouse, settings: Settings, customers: int = 6) -> None:
    gen = EventGenerator(settings)
    for i in range(customers):
        warehouse.append_order(gen.order(customer_id=f"cust_{i}", consent=True))
    warehouse.build_silver()
    warehouse.build_gold()


def _named(report: DQReport, prefix: str) -> dict[str, DQCheckResult]:
    return {r.check_name: r for r in report.results if r.check_name.startswith(prefix)}


def _newest_stale(table: str, warehouse: Warehouse, age_seconds: float) -> None:
    warehouse.conn.execute(
        f"UPDATE {table} SET occurred_at = ?",  # noqa: S608
        (time.time() - age_seconds,),
    )


# ------------------------------------------------------------- freshness


def test_freshness_covers_every_layer_and_passes_on_fresh_data(warehouse, settings):
    _build(warehouse, settings)
    report = DataQualityEngine(warehouse).run_all()

    assert report.all_passed, [r for r in report.results if not r.passed]
    freshness = _named(report, "freshness.")
    assert set(freshness) == {f"freshness.{table}" for table in DataQualityEngine.FRESHNESS_COLUMNS}
    # One per layer, all within the default one-hour limit.
    assert all(r.passed for r in freshness.values())
    assert any("limit 3600s" in str(r.detail) for r in freshness.values())


def test_freshness_skips_tables_that_hold_no_rows(warehouse):
    # No data at all: nothing can be stale, and the gate must not fail for it.
    report = DataQualityEngine(warehouse).run_all()
    freshness = _named(report, "freshness.")
    assert freshness and all(r.passed for r in freshness.values())
    assert all("empty" in str(r.detail) for r in freshness.values())
    assert report.all_passed


def test_freshness_judges_each_layer_on_its_own_clock(warehouse, settings):
    """Staleness is per layer: a stalled Gold build fails just like a stalled
    Bronze ingest, and the healthy layer in between is judged on its own
    timestamps rather than inheriting its neighbours' verdicts."""
    _build(warehouse, settings)
    _newest_stale("bronze.orders", warehouse, age_seconds=7200)
    warehouse.conn.execute("UPDATE gold.customer_360 SET last_seen = ?", (time.time() - 5400,))

    report = DataQualityEngine(warehouse, freshness_seconds=3600).run_all()
    freshness = _named(report, "freshness.")
    failing = {name for name, r in freshness.items() if not r.passed}
    assert failing == {"freshness.bronze.orders", "freshness.gold.customer_360"}

    detail = str(freshness["freshness.bronze.orders"].detail)
    assert "limit 3600s" in detail
    # Built before the edits: Silver is fresh and says so.
    assert freshness["freshness.silver.orders"].passed
    assert "ago" in str(freshness["freshness.silver.orders"].detail)
    assert not report.all_passed


def test_freshness_threshold_is_configurable(warehouse, settings):
    _build(warehouse, settings)
    report = DataQualityEngine(warehouse, freshness_seconds=0.000001).run_all()

    freshness = _named(report, "freshness.")
    non_empty = {
        f"freshness.{table}"
        for table in DataQualityEngine.FRESHNESS_COLUMNS
        if warehouse.query(f"SELECT count(*) AS c FROM {table}", local_only=True)[0]["c"]  # noqa: S608
    }
    assert non_empty, "expected the build to have produced rows"
    assert {name for name, r in freshness.items() if not r.passed} == non_empty
    assert not report.all_passed


# ---------------------------------------------------------------- volume


def test_volume_starts_from_a_baseline_not_a_guess(warehouse, settings):
    _build(warehouse, settings)
    report = DataQualityEngine(warehouse).run_all()

    volume = _named(report, "volume.")
    assert set(volume) == {f"volume.{table}" for table in DataQualityEngine.VOLUME_TABLES}
    orders = volume["volume.bronze.orders"]
    assert orders.passed
    assert "rows=6 (baseline)" in str(orders.detail)
    # A fresh warehouse has no history to fall back on, so nothing fails.
    assert all(r.passed for r in volume.values())


def test_volume_flags_a_table_that_lost_most_of_itself(warehouse, settings):
    _build(warehouse, settings)
    DataQualityEngine(warehouse).run_all()  # record the baseline

    warehouse.conn.execute(
        "DELETE FROM bronze.orders WHERE event_id IN (SELECT event_id FROM bronze.orders LIMIT 5)"
    )
    report = DataQualityEngine(warehouse).run_all()

    orders = _named(report, "volume.")["volume.bronze.orders"]
    assert not orders.passed
    assert "rows=1 (was 6" in str(orders.detail)
    assert "dropped more than 50%" in str(orders.detail)
    # Every other table is untouched and still passes.
    others = {n: r for n, r in _named(report, "volume.").items() if n != "volume.bronze.orders"}
    assert all(r.passed for r in others.values())


def test_volume_flags_a_table_that_was_emptied(warehouse, settings):
    _build(warehouse, settings)
    DataQualityEngine(warehouse).run_all()

    warehouse.conn.execute("DELETE FROM bronze.orders")
    report = DataQualityEngine(warehouse).run_all()

    orders = _named(report, "volume.")["volume.bronze.orders"]
    assert not orders.passed
    assert "table was emptied" in str(orders.detail)


def test_volume_allows_growth_and_a_small_shrink(warehouse, settings):
    _build(warehouse, settings)
    DataQualityEngine(warehouse).run_all()

    gen = EventGenerator(settings)
    for i in range(2):
        warehouse.append_order(gen.order(customer_id=f"cust_extra_{i}", consent=True))
    grown = DataQualityEngine(warehouse).run_all()
    check = _named(grown, "volume.")["volume.bronze.orders"]
    assert check.passed
    assert "rows=8 (was 6" in str(check.detail)

    # A DSAR that removes one of eight rows is normal operation, not loss.
    warehouse.conn.execute(
        "DELETE FROM bronze.orders WHERE event_id IN (SELECT event_id FROM bronze.orders LIMIT 1)"
    )
    shrunk = DataQualityEngine(warehouse).run_all()
    check = _named(shrunk, "volume.")["volume.bronze.orders"]
    assert check.passed
    assert "rows=7 (was 8" in str(check.detail)


def test_volume_drop_below_the_default_threshold_passes(warehouse, settings):
    _build(warehouse, settings)
    DataQualityEngine(warehouse).run_all()

    warehouse.conn.execute(
        "DELETE FROM bronze.orders WHERE event_id IN (SELECT event_id FROM bronze.orders LIMIT 2)"
    )  # -33%, inside the default 50% tolerance
    report = DataQualityEngine(warehouse).run_all()
    assert _named(report, "volume.")["volume.bronze.orders"].passed


def test_volume_drop_threshold_is_configurable(warehouse, settings):
    _build(warehouse, settings)
    DataQualityEngine(warehouse).run_all()

    warehouse.conn.execute(
        "DELETE FROM bronze.orders WHERE event_id IN (SELECT event_id FROM bronze.orders LIMIT 2)"
    )  # -33%: tolerated at 50%, fatal at 10%
    strict = DataQualityEngine(warehouse, volume_drop_pct=10.0).run_all()
    check = _named(strict, "volume.")["volume.bronze.orders"]
    assert not check.passed
    assert "dropped more than 10%" in str(check.detail)


def test_volume_check_survives_a_dropped_table(warehouse, settings):
    """A check that raises is recorded as a failed check, never lost — a
    missing table must not take the rest of the report down with it."""
    _build(warehouse, settings)
    warehouse.conn.execute("DROP TABLE bronze.clicks")

    report = DataQualityEngine(warehouse).run_all()
    clicks = _named(report, "volume.")["volume.bronze.clicks"]
    assert not clicks.passed
    assert "could not run" in str(clicks.detail)
    assert _named(report, "freshness.")["freshness.bronze.orders"].passed


# ------------------------------------------------------------- thresholds


def test_settings_expose_the_quality_thresholds() -> None:
    settings = Settings()
    assert settings.dq_freshness_seconds == 3600.0
    assert settings.dq_volume_drop_pct == 50.0

    with pytest.raises(ValidationError):
        Settings(dq_volume_drop_pct=150)
    with pytest.raises(ValidationError):
        Settings(dq_freshness_seconds=0)

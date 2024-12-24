from __future__ import annotations

import hashlib

import duckdb

from eurostream.governance.pii import PII_SALT
from eurostream.producers import EventGenerator
from eurostream.quality import DataQualityEngine
from eurostream.warehouse import Warehouse


def test_medallion_build_and_quality_gate(warehouse, settings, make_payment):
    gen = EventGenerator(settings)
    for i in range(5):
        warehouse.append_order(gen.order(customer_id=f"cust_{i}", consent=i % 2 == 0))
    for i in range(5):
        warehouse.append_payment(make_payment(customer_id=f"cust_{i}"))

    warehouse.build_silver()
    warehouse.build_gold()

    assert warehouse.count_rows("gold.customer_360") == 5
    assert warehouse.count_rows("gold.order_facts") == 5

    report = DataQualityEngine(warehouse).run_all()
    assert report.all_passed


def test_silver_hashes_match_hash_pii_scheme(warehouse, settings):
    """The SQL hash in build_silver and governance.hash_pii must agree byte
    for byte — one salted-hash scheme across the platform, not two."""
    gen = EventGenerator(settings)
    order = gen.order(customer_id="cust_hash", consent=True)
    warehouse.append_order(order)

    warehouse.build_silver(pii_salt=PII_SALT)
    row = warehouse.query(
        "SELECT email_hash, iban_hash FROM silver.customers WHERE customer_id='cust_hash'"
    )[0]
    assert row["email_hash"] == hashlib.sha256(f"{PII_SALT}:{order.email}".encode()).hexdigest()
    assert row["iban_hash"] == hashlib.sha256(f"{PII_SALT}:{order.iban}".encode()).hexdigest()


def test_export_lake_writes_deidentified_layers_only(warehouse, settings, tmp_path):
    gen = EventGenerator(settings)
    warehouse.append_order(gen.order(customer_id="cust_x", consent=True))
    warehouse.build_silver()
    warehouse.build_gold()

    lake_root = tmp_path / "lake"
    paths = warehouse.export_lake(lake_root)

    assert len(paths) == len(Warehouse.LAKE_EXPORT_TABLES)
    assert all(p.exists() and p.stat().st_size > 0 for p in paths)
    # Bronze never leaves the governed warehouse boundary.
    assert not any(p.relative_to(lake_root).parts[0] == "bronze" for p in paths)

    # The Parquet snapshot is queryable and matches warehouse row counts.
    c360 = next(p for p in paths if p.name == "customer_360.parquet")
    with duckdb.connect() as con:
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{c360}')").fetchone()[0]
    assert rows == warehouse.count_rows("gold.customer_360")


def test_consent_gate_fails_when_non_consenting_leaks_into_marketing(warehouse, settings):
    gen = EventGenerator(settings)
    warehouse.append_order(gen.order(customer_id="cust_no_consent", consent=False))
    warehouse.build_silver()
    warehouse.build_gold()
    report = DataQualityEngine(warehouse).run_all()
    consent = [r for r in report.results if r.check_name == "consent_gating"]
    assert consent and consent[0].passed

    # Simulate a Gold-build regression: an opt-out customer gets flagged as
    # marketing. The gate must catch it — this is what the check protects.
    warehouse.conn.execute(
        "UPDATE gold.customer_360 SET consents_marketing = TRUE WHERE marketing_consent = FALSE"
    )
    report = DataQualityEngine(warehouse).run_all()
    consent = [r for r in report.results if r.check_name == "consent_gating"]
    assert consent and not consent[0].passed


def test_consent_gate_catches_null_consent(warehouse, settings):
    """`<>` silently skips NULL rows; `IS DISTINCT FROM` must not."""
    gen = EventGenerator(settings)
    warehouse.append_order(gen.order(customer_id="cust_null", consent=True))
    warehouse.build_silver()
    warehouse.build_gold()
    warehouse.conn.execute("UPDATE gold.customer_360 SET consents_marketing = NULL")
    report = DataQualityEngine(warehouse).run_all()
    consent = [r for r in report.results if r.check_name == "consent_gating"]
    assert consent and not consent[0].passed


def test_pii_gate_checks_every_row_not_a_sample(warehouse, settings):
    """The old gate sampled `LIMIT 50`, so clear-text PII at row 51 passed."""
    gen = EventGenerator(settings)
    for i in range(60):
        warehouse.append_order(gen.order(customer_id=f"cust_{i}", consent=True))
    warehouse.build_silver()
    warehouse.build_gold()

    # Inject one leaked value well past row 50.
    warehouse.conn.execute(
        "UPDATE silver.customers SET email_hash = 'leaked@example.com' "
        "WHERE customer_id = 'cust_59'"
    )
    report = DataQualityEngine(warehouse).run_all()
    pii = [r for r in report.results if r.check_name == "silver.customers.email_hash_not_clear"]
    assert pii and not pii[0].passed
    assert "1 of 60" in pii[0].detail


def test_pii_gate_fails_when_the_protected_table_is_missing(warehouse):
    """A missing table is a failure, not a check to silently skip."""
    warehouse.conn.execute("DROP TABLE silver.customers")
    report = DataQualityEngine(warehouse).run_all()
    pii = [r for r in report.results if r.check_name == "silver.customers.email_hash_not_clear"]
    assert pii and not pii[0].passed
    assert "does not exist" in pii[0].detail


def test_suppression_gate_catches_resurrection(warehouse, settings):
    """The gate must fail when an erased customer is still in Silver/Gold."""
    gen = EventGenerator(settings)
    warehouse.append_order(gen.order(customer_id="cust_gone", consent=True))
    warehouse.add_suppressed("cust_gone")
    warehouse.build_silver()
    warehouse.build_gold()

    report = DataQualityEngine(warehouse).run_all()
    assert report.all_passed, "rebuild filters must keep the suppressed customer out"

    # Simulate a rebuild that dropped the anti-join.
    warehouse.conn.execute(
        "INSERT INTO silver.customers "
        "SELECT 'cust_gone', 'h', 'h', NULL, 'DE', TRUE, 0.0, 0.0, FALSE "
        "WHERE NOT EXISTS (SELECT 1 FROM silver.customers WHERE customer_id='cust_gone')"
    )
    warehouse.conn.execute(
        "INSERT INTO gold.customer_360 "
        "SELECT 'cust_gone', 1, 1.0, 1.0, TRUE, FALSE, 0, TRUE "
        "WHERE NOT EXISTS (SELECT 1 FROM gold.customer_360 WHERE customer_id='cust_gone')"
    )
    report = DataQualityEngine(warehouse).run_all()
    assert not report.all_passed
    failures = [r for r in report.results if not r.passed]
    assert any("silver.customers_no_suppressed_customers" == r.check_name for r in failures)
    assert any("gold.customer_360_no_suppressed_customers" == r.check_name for r in failures)

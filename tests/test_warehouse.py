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

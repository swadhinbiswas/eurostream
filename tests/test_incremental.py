from __future__ import annotations

from eurostream.producers import EventGenerator


def test_incremental_silver_and_gold(warehouse, settings, tmp_path):
    gen = EventGenerator(settings)
    # First batch
    for i in range(3):
        warehouse.append_order(gen.order(customer_id=f"cust_{i}", consent=True))
    warehouse.build_silver()
    warehouse.build_gold()
    assert warehouse.count_rows("silver.customers") == 3
    assert warehouse.count_rows("gold.customer_360") == 3

    # Second batch incremental
    for i in range(3, 5):
        warehouse.append_order(gen.order(customer_id=f"cust_{i}", consent=True))
    stats_s = warehouse.build_silver_incremental()
    stats_g = warehouse.build_gold_incremental()
    assert stats_s["customers"] == 2
    assert warehouse.count_rows("silver.customers") == 5
    assert warehouse.count_rows("gold.customer_360") == 5
    assert stats_g["customers"] == 2


def test_incremental_noop_when_no_new_data(warehouse, settings):
    gen = EventGenerator(settings)
    warehouse.append_order(gen.order(customer_id="cust_1", consent=True))
    warehouse.build_silver()
    warehouse.build_gold()
    # No new data -> incremental does nothing
    assert warehouse.build_silver_incremental()["customers"] == 0
    assert warehouse.build_gold_incremental()["customers"] == 0


def test_watermark_persistence(warehouse):
    warehouse.set_watermark("silver", 123.45)
    assert warehouse.get_watermark("silver") == 123.45
    assert warehouse.get_watermark("unknown") == 0.0

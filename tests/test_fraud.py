from __future__ import annotations

from eurostream.streaming import FraudScorer, FraudStreamProcessor


def test_velocity_rule_fires(settings, metrics, make_payment):
    scorer = FraudScorer(settings, metrics, velocity_threshold=3, window_seconds=60)
    alerts = []
    for _ in range(4):
        alerts += scorer.score(make_payment())
    assert any(a.rule == "VELOCITY" for a in alerts)


def test_geo_mismatch_rule_fires(settings, metrics, make_payment):
    scorer = FraudScorer(settings, metrics, window_seconds=60)
    alerts = scorer.score(make_payment(country="DE", merchant_country="NL"))
    assert any(a.rule == "GEO_MISMATCH" for a in alerts)


def test_geo_mismatch_fires_once_per_window(settings, metrics, make_payment):
    """Repeated mismatches in the same customer window alert once — the rule
    signals a pattern change, not every payment."""
    scorer = FraudScorer(settings, metrics, window_seconds=60)
    first = scorer.score(make_payment(country="DE", merchant_country="NL"))
    second = scorer.score(make_payment(country="DE", merchant_country="NL"))
    assert sum(a.rule == "GEO_MISMATCH" for a in first + second) == 1


def test_processor_skips_suppressed_customers(settings, metrics, bus, make_payment):
    """The erasure suppression registry gates the streaming path: payments
    from a suppressed customer are consumed but never scored."""
    payment = make_payment()
    bus.produce("payments", key=payment.event_id, value=payment.model_dump_json())
    processor = FraudStreamProcessor(
        consumer=bus.consumer("payments", "fraud-stream", auto_offset_reset="earliest"),
        scorer=FraudScorer(settings, metrics),
        metrics=metrics,
        suppression_check=lambda cid: cid == payment.customer_id,
    )
    emitted = processor.run(max_events=1)
    assert emitted == []
    counters = metrics.snapshot()["counters"]
    assert counters.get("suppressed_for_erasure") == 1


def test_amount_zscore_rule_fires(settings, metrics, make_payment):
    scorer = FraudScorer(settings, metrics, amount_zscore=2.0, window_seconds=60)
    for i in range(5):
        scorer.score(make_payment(amount=100.0 + i))
    alerts = scorer.score(make_payment(amount=1000.0))
    assert any(a.rule == "AMOUNT_ZSCORE" for a in alerts)


def test_no_alert_on_normal_flow(settings, metrics, make_payment):
    scorer = FraudScorer(settings, metrics, velocity_threshold=10, window_seconds=60)
    alerts = scorer.score(make_payment(country="DE", merchant_country="DE", amount=50.0))
    assert alerts == []


def test_expire_cleans_state(settings, metrics, make_payment):
    scorer = FraudScorer(settings, metrics, window_seconds=60)
    payment = make_payment()
    scorer.score(payment)
    assert len(scorer._windows) == 1
    scorer.expire(now=payment.occurred_at + 120)
    assert len(scorer._windows) == 0

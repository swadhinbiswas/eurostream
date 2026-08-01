from __future__ import annotations

from eurostream.config import Settings
from eurostream.lineage import LineageEmitter
from eurostream.metrics import Metrics
from eurostream.streaming import FraudScorer


def test_lineage_emit_and_persist(tmp_path):
    path = tmp_path / "lineage.jsonl"
    emitter = LineageEmitter(path)
    emitter.start("build_silver", inputs=["bronze.orders"], outputs=["silver.customers"])
    evt = emitter.complete("build_silver", extra={"customers": 2})
    assert evt is not None
    assert evt.job == "build_silver"
    assert path.exists()
    assert "silver.customers" in path.read_text()


def test_fraud_scorer_checkpoint_restore(tmp_path, settings):
    settings = Settings()
    m = Metrics()
    scorer = FraudScorer(settings, m, velocity_threshold=5, window_seconds=60)
    from eurostream.producers import EventGenerator

    gen = EventGenerator(settings)
    pay = gen.payment(customer_id="cust_1", country="DE", merchant_country="NL", amount=100)
    scorer.score(pay)
    ckpt = tmp_path / "ckpt.json"
    scorer.checkpoint(ckpt)
    assert ckpt.exists()
    # Restore into new scorer
    scorer2 = FraudScorer(settings, m, velocity_threshold=5, window_seconds=60)
    scorer2.restore(ckpt)
    assert "cust_1" in scorer2._windows
    assert scorer2._windows["cust_1"].count == 1

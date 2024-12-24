from __future__ import annotations

import collections
import json
import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from eurostream.bus import Consumer, Producer, Record
from eurostream.config import Settings
from eurostream.metrics import Metrics
from eurostream.models import PaymentProcessed


@dataclass
class FraudAlert:
    customer_id: str
    rule: str
    detail: str
    severity: str
    alert_ts: float
    window_start: float
    window_end: float
    features: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "customer_id": self.customer_id,
            "rule": self.rule,
            "detail": self.detail,
            "severity": self.severity,
            "alert_ts": self.alert_ts,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "features": self.features,
        }


@dataclass
class WindowStats:
    window_start: int = 0
    count: int = 0
    geo_mismatch: bool = False


class FraudScorer:
    """Rolling window fraud-risk features over the payments stream.

    Pure-Python port of the Spark Structured Streaming velocity/geo/amount
    rules so the whole pipeline runs with zero JVM dependency. Rules:

    - VELOCITY: > threshold payments per customer within a window
    - AMOUNT_ZSCORE: single payment amount deviates > k std from customer mean
    - GEO_MISMATCH: billing country != merchant country
    """

    def __init__(
        self,
        settings: Settings,
        metrics: Metrics,
        velocity_threshold: int | None = None,
        window_seconds: int | None = None,
        amount_zscore: float | None = None,
    ) -> None:
        self._velocity_threshold = velocity_threshold or settings.fraud_velocity_threshold
        self._window_seconds = window_seconds or settings.fraud_window_seconds
        self._amount_zscore = amount_zscore or settings.fraud_amount_zscore
        self._metrics = metrics
        self._windows: dict[str, WindowStats] = {}
        self._last_seen: dict[str, float] = {}
        self._history_by_customer: dict[str, collections.deque[float]] = {}

    def _window_key(self, ts: float) -> tuple[int, int]:
        start = int(ts // self._window_seconds) * self._window_seconds
        return start, start + self._window_seconds

    def score(self, payment: PaymentProcessed) -> list[FraudAlert]:
        ts = payment.occurred_at
        wstart, wend = self._window_key(ts)
        stats = self._windows.get(payment.customer_id)
        if stats is None or stats.window_start != wstart:
            stats = WindowStats(window_start=wstart)
            self._windows[payment.customer_id] = stats
        stats.count += 1
        self._last_seen[payment.customer_id] = ts

        # Use history *before* current payment for z-score (no data leakage).
        history = self._history_by_customer.setdefault(
            payment.customer_id, collections.deque(maxlen=200)
        )
        amounts_before = list(history)

        alerts: list[FraudAlert] = []
        if stats.count == self._velocity_threshold + 1:
            alerts.append(
                FraudAlert(
                    customer_id=payment.customer_id,
                    rule="VELOCITY",
                    detail=f"{stats.count} payments in {self._window_seconds}s window",
                    severity="HIGH",
                    alert_ts=time.time(),
                    window_start=float(wstart),
                    window_end=float(wend),
                    features={"payment_count": float(stats.count)},
                )
            )

        if len(amounts_before) >= 3:
            mean = sum(amounts_before) / len(amounts_before)
            # sample variance (N-1) for small histories to avoid under-estimation
            denom = len(amounts_before) - 1 if len(amounts_before) > 1 else 1
            variance = sum((a - mean) ** 2 for a in amounts_before) / denom
            std = math.sqrt(variance)
            if std > 0 and abs(payment.amount_eur - mean) / std > self._amount_zscore:
                alerts.append(
                    FraudAlert(
                        customer_id=payment.customer_id,
                        rule="AMOUNT_ZSCORE",
                        detail=f"amount {payment.amount_eur:.2f} deviates {abs(payment.amount_eur - mean) / std:.1f} std",
                        severity="MEDIUM",
                        alert_ts=time.time(),
                        window_start=float(wstart),
                        window_end=float(wend),
                        features={"zscore": (payment.amount_eur - mean) / std},
                    )
                )

        # Update history *after* scoring so current payment doesn't leak into its own baseline.
        history.append(payment.amount_eur)

        if payment.country != payment.merchant_country:
            if not stats.geo_mismatch:
                alerts.append(
                    FraudAlert(
                        customer_id=payment.customer_id,
                        rule="GEO_MISMATCH",
                        detail=f"billing {payment.country} vs merchant {payment.merchant_country}",
                        severity="LOW",
                        alert_ts=time.time(),
                        window_start=float(wstart),
                        window_end=float(wend),
                    )
                )
            stats.geo_mismatch = True

        for alert in alerts:
            self._metrics.incr(f"fraud_alert_{alert.rule.lower()}")
        return alerts

    def expire(self, now: float) -> None:
        """Drop windows whose customer has been inactive for a full window
        so the state map does not grow unboundedly."""
        horizon = now - self._window_seconds
        for cid in list(self._windows.keys()):
            if self._last_seen.get(cid, 0.0) < horizon:
                del self._windows[cid]
                self._last_seen.pop(cid, None)
                self._history_by_customer.pop(cid, None)

    def checkpoint(self, path: Path) -> None:
        """Persist state like Flink checkpointing — enables exactly-once recovery."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "windows": {
                cid: {"window_start": ws.window_start, "count": ws.count, "geo": ws.geo_mismatch}
                for cid, ws in self._windows.items()
            },
            "last_seen": self._last_seen,
            "history": {cid: list(dq) for cid, dq in self._history_by_customer.items()},
        }
        path.write_text(json.dumps(data))

    def restore(self, path: Path) -> None:
        path = Path(path)
        if not path.exists():
            return
        data = json.loads(path.read_text())
        self._windows = {
            cid: WindowStats(
                window_start=v["window_start"], count=v["count"], geo_mismatch=v["geo"]
            )
            for cid, v in data.get("windows", {}).items()
        }
        self._last_seen = data.get("last_seen", {})
        self._history_by_customer = {
            cid: collections.deque(v, maxlen=200) for cid, v in data.get("history", {}).items()
        }


class FraudStreamProcessor:
    """Bridges the event bus into the FraudScorer and forwards alerts to an
    output topic or callback. This is the pure-Python streaming consumer."""

    def __init__(
        self,
        consumer: Consumer,
        scorer: FraudScorer,
        metrics: Metrics,
        output_producer: Producer | None = None,
        alert_topic: str = "fraud_alerts",
        on_alert: Callable[[FraudAlert], None] | None = None,
        suppression_check: Callable[[str], bool] | None = None,
    ) -> None:
        self._consumer = consumer
        self._scorer = scorer
        self._metrics = metrics
        self._producer = output_producer
        self._topic = alert_topic
        self._on_alert = on_alert
        self._suppression_check = suppression_check or (lambda cid: False)
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    def run(
        self,
        poll_timeout: float = 0.2,
        max_events: int | None = None,
        idle_stop: int = 5,
    ) -> list[FraudAlert]:
        emitted: list[FraudAlert] = []
        processed = 0
        idle = 0
        while not self._stopped:
            if max_events is not None and processed >= max_events:
                break
            record: Record | None = self._consumer.poll(timeout=poll_timeout)
            if record is None:
                idle += 1
                if idle >= idle_stop:
                    break
                time.sleep(0.05)
                continue
            idle = 0
            processed += 1
            # Periodic state expiry so long-running workers don't leak memory.
            if processed % 50 == 0:
                self._scorer.expire(time.time())
            try:
                payload = record.json_value()
                if payload.get("event_type") != "payment_processed":
                    self._consumer.commit()
                    continue
                payment = PaymentProcessed(**payload)
            except Exception:
                self._metrics.incr("malformed_events")
                self._consumer.commit()
                continue
            if self._suppression_check(payment.customer_id):
                self._metrics.incr("suppressed_for_erasure")
                self._consumer.commit()
                continue
            alerts = self._scorer.score(payment)
            for alert in alerts:
                emitted.append(alert)
                if self._producer is not None:
                    self._producer.produce(
                        self._topic,
                        key=alert.customer_id,
                        value=json.dumps(alert.to_dict()),
                        headers={"rule": alert.rule},
                    )
                if self._on_alert is not None:
                    self._on_alert(alert)
            self._consumer.commit()
        return emitted

from __future__ import annotations

import random
import time
import uuid

from faker import Faker

from eurostream.bus import Producer
from eurostream.config import Settings
from eurostream.models import (
    ErasureRequested,
    OrderPlaced,
    PageClick,
    PaymentProcessed,
)

_TOPICS = {
    "order_placed": "orders",
    "page_click": "clicks",
    "payment_processed": "payments",
    "erasure_requested": "erasure_requests",
}


def topic_for(event_type: str) -> str:
    try:
        return _TOPICS[event_type]
    except KeyError:
        raise ValueError(f"unknown event_type: {event_type}") from None


class EventGenerator:
    """Simulates the three source systems (order service, clickstream,
    payments API) with realistic EU data: IBANs, country codes, GDPR consent
    flags, and IP addresses."""

    def __init__(self, settings: Settings) -> None:
        self._fake = Faker(["de_DE", "fr_FR", "nl_NL"])
        self._settings = settings
        self._t0 = time.time()

    @staticmethod
    def _ts() -> float:
        return time.time()

    def order(self, customer_id: str | None = None, consent: bool | None = None) -> OrderPlaced:
        now = self._ts()
        return OrderPlaced(
            event_id=str(uuid.uuid4()),
            occurred_at=now,
            order_id=str(uuid.uuid4()),
            customer_id=customer_id or f"cust_{random.randint(1, 5000)}",
            email=self._fake.email(),
            iban=self._fake.iban(),
            country=self._fake.country_code(),
            amount_eur=round(random.uniform(5, 500), 2),
            marketing_consent=consent if consent is not None else self._settings.consent_default,
        )

    def click(self, customer_id: str | None = None) -> PageClick:
        now = self._ts()
        return PageClick(
            event_id=str(uuid.uuid4()),
            occurred_at=now,
            click_id=str(uuid.uuid4()),
            customer_id=customer_id or f"cust_{random.randint(1, 5000)}",
            session_id=str(uuid.uuid4()),
            ip_address=self._fake.ipv4(),
            page=random.choice(["/home", "/cart", "/checkout", "/product/42", "/login"]),
            country=self._fake.country_code(),
        )

    def payment(
        self,
        customer_id: str | None = None,
        country: str | None = None,
        merchant_country: str | None = None,
        amount: float | None = None,
    ) -> PaymentProcessed:
        now = self._ts()
        return PaymentProcessed(
            event_id=str(uuid.uuid4()),
            occurred_at=now,
            payment_id=str(uuid.uuid4()),
            order_id=str(uuid.uuid4()),
            customer_id=customer_id or f"cust_{random.randint(1, 5000)}",
            iban=self._fake.iban(),
            amount_eur=round(amount or random.uniform(5, 500), 2),
            country=country or self._fake.country_code(),
            merchant_country=merchant_country or self._fake.country_code(),
        )

    def erasure(self, customer_id: str) -> ErasureRequested:
        return ErasureRequested(
            event_id=str(uuid.uuid4()),
            occurred_at=self._ts(),
            request_id=str(uuid.uuid4()),
            customer_id=customer_id,
        )


class OrderProducer:
    def __init__(self, producer: Producer, settings: Settings) -> None:
        self._p = producer
        self._gen = EventGenerator(settings)

    def emit(self, customer_id: str | None = None) -> str:
        evt = self._gen.order(customer_id=customer_id)
        self._p.produce(
            topic_for("order_placed"),
            key=evt.customer_id,
            value=evt.model_dump_json(),
            headers={"schema_version": str(evt.schema_version), "event_type": evt.event_type},
        )
        return evt.customer_id


class ClickProducer:
    def __init__(self, producer: Producer, settings: Settings) -> None:
        self._p = producer
        self._gen = EventGenerator(settings)

    def emit(self, customer_id: str | None = None) -> str:
        evt = self._gen.click(customer_id=customer_id)
        self._p.produce(
            topic_for("page_click"),
            key=evt.customer_id,
            value=evt.model_dump_json(),
            headers={"schema_version": str(evt.schema_version), "event_type": evt.event_type},
        )
        return evt.customer_id


class PaymentProducer:
    def __init__(self, producer: Producer, settings: Settings) -> None:
        self._p = producer
        self._gen = EventGenerator(settings)

    def emit(
        self,
        customer_id: str | None = None,
        country: str | None = None,
        merchant_country: str | None = None,
        amount: float | None = None,
    ) -> str:
        evt = self._gen.payment(
            customer_id=customer_id,
            country=country,
            merchant_country=merchant_country,
            amount=amount,
        )
        self._p.produce(
            topic_for("payment_processed"),
            key=evt.customer_id,
            value=evt.model_dump_json(),
            headers={"schema_version": str(evt.schema_version), "event_type": evt.event_type},
        )
        return evt.customer_id

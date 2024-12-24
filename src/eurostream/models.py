from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

EventType: TypeAlias = Literal[
    "order_placed", "page_click", "payment_processed", "erasure_requested"
]


class PIIFlag(StrEnum):
    EMAIL = "EMAIL"
    IBAN = "IBAN"
    IP_ADDRESS = "IP_ADDRESS"
    NAME = "NAME"
    PHONE = "PHONE"
    ADDRESS = "ADDRESS"
    COUNTRY = "COUNTRY"
    NONE = "NONE"


class Event(BaseModel):
    """Base event contract.

    Every event carries a schema_version. Consumers must tolerate
    forward-compatible changes (new optional fields). Breaking changes are
    rejected at the contract registry, not discovered downstream.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    schema_version: int = 1
    event_id: str
    event_type: EventType
    occurred_at: float = Field(description="Unix epoch seconds, producer clock")

    def partition_key(self) -> str:
        return self.event_id


class OrderPlaced(Event):
    event_type: Literal["order_placed"] = "order_placed"
    order_id: str
    customer_id: str
    email: str
    iban: str
    country: str
    amount_eur: float = Field(ge=0.0)
    marketing_consent: bool
    currency: str = "EUR"


class PageClick(Event):
    event_type: Literal["page_click"] = "page_click"
    click_id: str
    customer_id: str
    session_id: str
    ip_address: str
    page: str
    country: str


class PaymentProcessed(Event):
    event_type: Literal["payment_processed"] = "payment_processed"
    payment_id: str
    order_id: str
    customer_id: str
    iban: str
    amount_eur: float = Field(ge=0.0)
    country: str
    merchant_country: str
    status: Literal["authorized", "declined", "captured"] = "authorized"


class ErasureRequested(Event):
    event_type: Literal["erasure_requested"] = "erasure_requested"
    request_id: str
    customer_id: str
    reason: str = "GDPR_ARTICLE_17"
    requested_by: str = "dsar@eurocart.eu"


EVENT_MODELS: dict[str, type[Event]] = {
    "order_placed": OrderPlaced,
    "page_click": PageClick,
    "payment_processed": PaymentProcessed,
    "erasure_requested": ErasureRequested,
}


def build_event(model: type[Event], **values: Any) -> Event:
    return model(**values)

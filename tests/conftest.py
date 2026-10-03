from __future__ import annotations

from pathlib import Path

import pytest

from eurostream.bus.sqlite import open_bus
from eurostream.config import Settings, get_settings
from eurostream.metrics import Metrics
from eurostream.models import PaymentProcessed
from eurostream.producers import EventGenerator
from eurostream.warehouse import Warehouse


@pytest.fixture(autouse=True)
def _isolated_settings_cache():
    """`get_settings()` is lru_cached for the process; without this, the first
    test to run fixes every later test's paths (and an env-based CLI test
    would silently operate on the repository's ./data instead of tmp_path)."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "eurocart.duckdb",
        audit_log_path=tmp_path / "data" / "logs" / "audit.jsonl",
        metrics_path=tmp_path / "data" / "logs" / "metrics.jsonl",
        pii_manifest_path=tmp_path / "governance" / "pii_manifest.json",
        event_bus_backend="sqlite",
    )


@pytest.fixture
def bus(tmp_path: Path):
    b = open_bus(tmp_path / "events.db")
    yield b
    b.close()


@pytest.fixture
def warehouse(tmp_path: Path):
    w = Warehouse(tmp_path / "eurocart.duckdb")
    yield w
    w.close()


@pytest.fixture
def metrics(tmp_path: Path):
    return Metrics(tmp_path / "metrics.jsonl")


@pytest.fixture
def make_payment():
    def _make(
        customer_id: str = "cust_1",
        country: str = "DE",
        merchant_country: str = "DE",
        amount: float = 100.0,
    ) -> PaymentProcessed:
        return EventGenerator(Settings()).payment(
            customer_id=customer_id,
            country=country,
            merchant_country=merchant_country,
            amount=amount,
        )

    return _make

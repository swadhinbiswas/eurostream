from __future__ import annotations

from pathlib import Path

from eurostream.contracts import ContractRegistry
from eurostream.models import Event

BASELINE = Path(__file__).resolve().parents[1] / "governance" / "contracts.json"


class OldOrderPlaced(Event):
    event_type: str = "order_placed"
    order_id: str


def test_removing_required_field_is_a_violation():
    baseline = ContractRegistry().snapshot()
    violations = ContractRegistry(models={"order_placed": OldOrderPlaced}).check_against(baseline)
    assert any("customer_id" in str(v) for v in violations)


def test_adding_optional_field_is_non_breaking():
    baseline = ContractRegistry().snapshot()
    violations = ContractRegistry().check_against(baseline)
    assert violations == []


def test_committed_baseline_matches_current_models():
    """The committed contracts.json must be a fresh snapshot of the models.
    Fails when an event model changes without regenerating the baseline
    (uv run eurostream contracts --out governance/contracts.json)."""
    registry = ContractRegistry()
    violations = registry.check_against(registry.load(BASELINE))
    assert violations == []

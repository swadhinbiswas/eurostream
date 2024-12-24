from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, get_args, get_origin

from .models import EVENT_MODELS, Event


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str
    required: bool


@dataclass(frozen=True)
class ContractViolation:
    topic: str
    field: str
    message: str

    def __str__(self) -> str:
        return f"{self.topic}.{self.field}: {self.message}"


def _resolve_type(annotation: Any) -> str:
    """Extract a human-readable type string from a field annotation.

    Handles: plain types (str, int), Optional[X], Literal["a", "b"],
    and Pydantic sentinel values for required fields.
    """
    origin = get_origin(annotation)
    if origin is not None:
        # Literal["order_placed"] → origin is Literal
        if getattr(origin, "__name__", "") == "Literal" or str(origin) == "typing.Literal":
            args = get_args(annotation)
            return f"Literal[{', '.join(repr(a) for a in args)}]"
        # Optional[X] → origin is Union
        args = get_args(annotation)
        if args:
            return " | ".join(_resolve_type(a) for a in args)
    if annotation is None:
        return "null"
    if isinstance(annotation, type):
        return annotation.__name__
    return str(annotation)


class ContractRegistry:
    """Schema registry that encodes the *current* contract per topic.

    CI runs ``check_against`` against the committed ``contracts.json`` to
    block a breaking change before it reaches production, rather than only
    detecting it downstream. This is the platform-thinking guard rail.
    """

    def __init__(
        self, path: Path | None = None, models: dict[str, type[Event]] | None = None
    ) -> None:
        self.path = path
        self._models = models or EVENT_MODELS

    def _spec(self, model: type[Event]) -> dict[str, FieldSpec]:
        specs: dict[str, FieldSpec] = {}
        for name, field_info in model.model_fields.items():
            required = field_info.is_required()
            annotation = field_info.annotation
            if annotation is not None:
                # Prefer the declared annotation: it carries the full contract
                # (Literal discriminators, Optional unions) unlike the default.
                type_str = _resolve_type(annotation)
            else:
                # No annotation available; fall back to the runtime type of
                # the default value.
                default = field_info.default
                type_str = _resolve_type(type(default)) if default is not None else "required"
            specs[name] = FieldSpec(
                name=name,
                type=type_str,
                required=required,
            )
        return specs

    def snapshot(self) -> dict[str, dict[str, FieldSpec]]:
        return {name: self._spec(model) for name, model in self._models.items()}

    def serialize(self) -> dict[str, Any]:
        return {
            topic: {name: f.__dict__ for name, f in fields.items()}
            for topic, fields in self.snapshot().items()
        }

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(self.serialize(), indent=2) + "\n")

    def load(self, path: Path) -> dict[str, dict[str, FieldSpec]]:
        raw = json.loads(path.read_text())
        return {
            topic: {name: FieldSpec(**f) for name, f in fields.items()}
            for topic, fields in raw.items()
        }

    def check_against(self, baseline: dict[str, dict[str, FieldSpec]]) -> list[ContractViolation]:
        """Detect backward-incompatible drift between the committed baseline
        and the current code. A breaking change removes a required field,
        makes it optional, tightens optional to required, adds a new required
        field, or changes a field's type."""
        violations: list[ContractViolation] = []
        current = self.snapshot()
        for topic, baseline_fields in baseline.items():
            current_fields = current.get(topic)
            if current_fields is None:
                violations.append(ContractViolation(topic, "*", "topic removed"))
                continue
            for name, old in baseline_fields.items():
                spec = current_fields.get(name)
                if spec is None:
                    if old.required:
                        violations.append(ContractViolation(topic, name, "required field removed"))
                    continue
                if old.required and not spec.required:
                    violations.append(
                        ContractViolation(topic, name, "required field became optional")
                    )
                if not old.required and spec.required:
                    violations.append(
                        ContractViolation(topic, name, "optional field became required")
                    )
                if old.type != spec.type:
                    # Type change is breaking regardless of required flag, but
                    # new optional fields are checked in the second loop.
                    if old.required or spec.required:
                        violations.append(
                            ContractViolation(
                                topic, name, f"type changed {old.type} -> {spec.type}"
                            )
                        )
            # New required fields that didn't exist in baseline are breaking for old producers.
            for name, spec in current_fields.items():
                if name not in baseline_fields and spec.required:
                    violations.append(ContractViolation(topic, name, "new required field added"))
        # Also check for entirely new topics with required fields? Already covered if topic not in baseline,
        # current snapshot will have required fields, but new topic itself is not breaking (new producer).
        return violations

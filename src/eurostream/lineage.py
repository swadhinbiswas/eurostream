from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class LineageEvent:
    run_id: str
    job: str
    inputs: list[str]
    outputs: list[str]
    started_at: float
    ended_at: float | None = None
    status: str = "running"
    extra: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class LineageEmitter:
    """Minimal OpenLineage/Marquez-style emitter.

    Every DAG task emits a LineageEvent with inputs/outputs. Locally it
    appends JSONL to a file; in production swap `path` for an HTTP
    endpoint (Marquez `POST /api/v1/lineage`).
    """

    def __init__(self, path: Path | None = None) -> None:
        self.path = path
        self._active: dict[str, LineageEvent] = {}

    def start(
        self, job: str, inputs: list[str], outputs: list[str], run_id: str | None = None
    ) -> str:
        rid = run_id or str(uuid.uuid4())
        evt = LineageEvent(
            run_id=rid,
            job=job,
            inputs=inputs,
            outputs=outputs,
            started_at=time.time(),
        )
        self._active[job] = evt
        return rid

    def complete(
        self, job: str, status: str = "completed", extra: dict[str, Any] | None = None
    ) -> LineageEvent | None:
        evt = self._active.pop(job, None)
        if evt is None:
            return None
        evt.ended_at = time.time()
        evt.status = status
        if extra:
            evt.extra.update(extra)
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a") as fh:
                fh.write(json.dumps(evt.to_dict()) + "\n")
        return evt

    def fail(self, job: str, error: str) -> LineageEvent | None:
        return self.complete(job, status="failed", extra={"error": error})

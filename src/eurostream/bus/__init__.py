from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Record:
    topic: str
    key: str
    value: str
    offset: int
    partition: int = 0
    timestamp: float = 0.0
    headers: dict[str, str] = field(default_factory=dict)

    def json_value(self) -> dict[str, Any]:
        import json

        return dict(json.loads(self.value))


@dataclass
class RecordMetadata:
    topic: str
    offset: int
    partition: int = 0


class Producer(abc.ABC):
    @abc.abstractmethod
    def produce(
        self, topic: str, key: str, value: str, headers: dict[str, str] | None = None
    ) -> RecordMetadata: ...


class Consumer(abc.ABC):
    """A position-tracking consumer. ``poll`` returns the next record or None
    when the log is currently empty. Consumer groups checkpoint their own
    offsets, so restarts resume from the last committed position."""

    @abc.abstractmethod
    def poll(self, timeout: float = 0.0) -> Record | None: ...

    @abc.abstractmethod
    def commit(self) -> None: ...

    @abc.abstractmethod
    def close(self) -> None: ...

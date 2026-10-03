"""At-most-once intake for a side-effecting endpoint.

``POST /erasure-requests`` deletes data. A client that retries after a
timeout must not open a second DSAR, and a client that retries with the same
key must get the same answer back. This store keeps the first completed
response per key, refuses a key reused for a different customer, and lets a
failed attempt release the key so the retry can proceed.

The registry is process-local by design: this platform runs as a single API
process (worker included). A multi-replica deployment would move these keys
into the warehouse — the interface is deliberately small so that swap is one
constructor argument.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

#: Key shape: URL/UUID-ish, bounded so a header cannot become a memory leak.
IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9_.:\-]{8,128}$"


@dataclass
class StoredResponse:
    """The response a key produced the first time it was accepted."""

    key: str
    customer_id: str
    request_id: str
    status_code: int
    body: dict[str, object]
    created_at: float = field(default_factory=time.time)


class IdempotencyConflictError(Exception):
    """The key is already bound to a different customer or is in flight."""


class IdempotencyStore:
    """Bounded, TTL'd map of key -> first completed response."""

    def __init__(
        self,
        ttl_seconds: float = 24 * 3600,
        max_entries: int = 10_000,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._ttl = float(ttl_seconds)
        self._max = int(max_entries)
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: dict[str, StoredResponse] = {}
        self._in_flight: dict[str, str] = {}
        self._replays = 0

    @property
    def replays(self) -> int:
        with self._lock:
            return self._replays

    def lookup(self, key: str, customer_id: str) -> StoredResponse | None:
        """Return the stored response, count it as a replay, or raise.

        Raises :class:`IdempotencyConflictError` when the key belongs to another
        customer (a reused key across DSARs is a client bug worth failing
        loudly for) or when an identical request is still running.
        """
        with self._lock:
            self._prune_locked()
            if key in self._in_flight:
                if self._in_flight[key] != customer_id:
                    raise IdempotencyConflictError(
                        f"key {key!r} is already in flight for another customer"
                    )
                raise IdempotencyConflictError(f"key {key!r} is still being processed")
            stored = self._entries.get(key)
            if stored is None:
                return None
            if stored.customer_id != customer_id:
                raise IdempotencyConflictError(
                    f"key {key!r} was first used for customer {stored.customer_id!r}"
                )
            self._replays += 1
            return stored

    def begin(self, key: str, customer_id: str) -> StoredResponse | None:
        """Claim ``key`` for this attempt.

        Returns the stored response when the key already completed (the call
        is a replay), otherwise ``None`` and the key is marked in flight —
        the caller now owns it and must call :meth:`complete` or :meth:`fail`.
        """
        replay = self.lookup(key, customer_id)
        if replay is not None:
            return replay
        with self._lock:
            if key in self._in_flight:
                raise IdempotencyConflictError(f"key {key!r} is already in flight")
            self._in_flight[key] = customer_id
        return None

    def complete(
        self,
        key: str,
        *,
        customer_id: str,
        request_id: str,
        status_code: int,
        body: dict[str, object],
    ) -> None:
        with self._lock:
            self._in_flight.pop(key, None)
            self._entries[key] = StoredResponse(
                key=key,
                customer_id=customer_id,
                request_id=request_id,
                status_code=status_code,
                body=body,
                created_at=self._clock(),
            )
            # Bound the map: evict the oldest completed key rather than grow.
            while len(self._entries) > self._max:
                oldest = min(self._entries, key=lambda k: self._entries[k].created_at)
                self._entries.pop(oldest, None)

    def fail(self, key: str) -> None:
        """Release an in-flight key so a retry can claim it."""
        with self._lock:
            self._in_flight.pop(key, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def _prune_locked(self) -> None:
        cutoff = self._clock() - self._ttl
        expired = [key for key, entry in self._entries.items() if entry.created_at < cutoff]
        for key in expired:
            self._entries.pop(key, None)

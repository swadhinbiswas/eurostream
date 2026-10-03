"""Retry with backoff, and a circuit breaker for the calls that hang.

Two failure modes, two answers:

* **Transient** (a 503, a dropped socket) — retry with exponential backoff
  and jitter. Jitter is not decoration: without it every client that failed
  together retries together and the dependency gets a second, synchronised
  stampede exactly when it is struggling.
* **Persistent** (the endpoint is down) — stop calling. A circuit breaker
  fails fast instead of parking a worker thread on a 15 second timeout per
  request, and lets one probe through after a cooldown so recovery is
  noticed without a restart.

Both primitives take their clock and sleep function as arguments, which is
what makes the failure paths testable without actually waiting.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

#: HTTP statuses worth retrying: timeouts, ask-later, and server errors.
RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class CircuitOpenError(RuntimeError):
    """The breaker is open: the call was refused, not attempted."""


class RetryPolicy:
    """Delays for ``attempts - 1`` sleeps: exponential, jittered.

    ``jitter=1.0`` is full jitter (uniform over ``[0, ceiling]``), which
    spreads a fleet of retrying clients; ``jitter=0`` is plain exponential
    and exists mostly so a test can assert the ceiling.
    """

    def __init__(
        self,
        attempts: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 2.0,
        jitter: float = 1.0,
    ) -> None:
        if attempts < 1:
            raise ValueError("attempts must be >= 1")
        if base_delay < 0 or max_delay < 0:
            raise ValueError("delays must be >= 0")
        if not 0.0 <= jitter <= 1.0:
            raise ValueError("jitter must be between 0 and 1")
        self.attempts = int(attempts)
        self.base_delay = float(base_delay)
        self.max_delay = float(max_delay)
        self.jitter = float(jitter)

    def delays(self, rng: Callable[[], float] = random.random) -> list[float]:
        """One delay per retry, in order."""
        out: list[float] = []
        for attempt in range(self.attempts - 1):
            ceiling = min(self.max_delay, self.base_delay * (2**attempt))
            out.append(ceiling * (1.0 - self.jitter * rng()))
        return out

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"RetryPolicy(attempts={self.attempts}, base_delay={self.base_delay}, "
            f"max_delay={self.max_delay}, jitter={self.jitter})"
        )


def call_with_retry(
    fn: Callable[[], T],
    *,
    policy: RetryPolicy | None = None,
    should_retry: Callable[[Exception], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    on_retry: Callable[[int, Exception, float], None] | None = None,
    rng: Callable[[], float] = random.random,
) -> T:
    """Call ``fn``, retrying transient failures.

    ``should_retry`` decides whether an exception is worth another attempt;
    when it is omitted every exception is retried, which is only right for a
    call whose failure modes are all transient. The last exception is always
    re-raised with its traceback intact — a retry must not turn a useful
    error into ``None``.
    """
    policy = policy or RetryPolicy()
    delays = policy.delays(rng)
    last: Exception | None = None
    for attempt in range(policy.attempts):
        try:
            return fn()
        except Exception as exc:
            last = exc
            if attempt >= len(delays):
                break
            if should_retry is not None and not should_retry(exc):
                raise
            delay = delays[attempt]
            if on_retry is not None:
                on_retry(attempt + 1, exc, delay)
            if delay > 0:
                sleep(delay)
    if last is None:  # pragma: no cover - loop always runs at least once
        raise RuntimeError("retry loop finished without a result or an exception")
    raise last


def http_status_is_retryable(exc: Exception) -> bool:
    """Whether an ``httpx`` error represents a transient failure."""
    import httpx

    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_STATUSES
    # Transport errors (connect/read/write/pool timeouts, TLS, DNS) and the
    # plain socket errors some layers surface them as.
    if isinstance(exc, (httpx.TransportError, ConnectionError, TimeoutError, OSError)):
        return True
    return False


class CircuitBreaker:
    """closed -> open -> half-open -> closed.

    While open, :meth:`call` raises :class:`CircuitOpenError` without
    touching the dependency. After ``recovery_seconds`` one call is allowed
    through (half-open): success closes the circuit, failure reopens it for
    another cooldown.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        name: str = "dependency",
        *,
        failure_threshold: int = 5,
        recovery_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_seconds < 0:
            raise ValueError("recovery_seconds must be >= 0")
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_seconds = float(recovery_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._state = self.CLOSED
        self._failures = 0
        self._opened_at: float | None = None
        self._probe_in_flight = False
        self._trips = 0

    @property
    def state(self) -> str:
        with self._lock:
            self._maybe_half_open_locked()
            return self._state

    @property
    def failures(self) -> int:
        with self._lock:
            return self._failures

    @property
    def trips(self) -> int:
        """How many times the circuit has opened (monotonic)."""
        with self._lock:
            return self._trips

    @property
    def retry_after(self) -> float:
        """Seconds until the next probe is allowed (0 unless open)."""
        with self._lock:
            return self._retry_after_locked()

    def call(self, fn: Callable[[], T]) -> T:
        """Run ``fn`` under the breaker, or refuse it fast."""
        with self._lock:
            self._maybe_half_open_locked()
            if self._state == self.OPEN:
                raise CircuitOpenError(
                    f"{self.name} circuit is open; probe allowed in "
                    f"{self._retry_after_locked():.2f}s"
                )
            if self._state == self.HALF_OPEN:
                if self._probe_in_flight:
                    raise CircuitOpenError(
                        f"{self.name} circuit is half-open; a probe is already running"
                    )
                self._probe_in_flight = True
        try:
            value = fn()
        except Exception:
            self._on_failure()
            raise
        self._on_success()
        return value

    def stats(self) -> dict[str, object]:
        with self._lock:
            self._maybe_half_open_locked()
            return {
                "name": self.name,
                "state": self._state,
                "failures": self._failures,
                "trips": self._trips,
                "failure_threshold": self.failure_threshold,
                "recovery_seconds": self.recovery_seconds,
                "retry_after": self._retry_after_locked(),
            }

    # ---- internals ----

    def _retry_after_locked(self) -> float:
        if self._state != self.OPEN or self._opened_at is None:
            return 0.0
        return max(0.0, (self._opened_at + self.recovery_seconds) - self._clock())

    def _maybe_half_open_locked(self) -> None:
        if self._state == self.OPEN and self._retry_after_locked() <= 0.0:
            self._state = self.HALF_OPEN
            self._probe_in_flight = False

    def _on_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = self.CLOSED
            self._probe_in_flight = False
            self._opened_at = None

    def _on_failure(self) -> None:
        with self._lock:
            self._failures += 1
            should_open = self._state == self.HALF_OPEN or self._failures >= self.failure_threshold
            if should_open:
                if self._state != self.OPEN:
                    self._trips += 1
                self._state = self.OPEN
                self._opened_at = self._clock()
                self._probe_in_flight = False
            # Below the threshold the circuit stays closed and keeps its
            # failure count: the next success resets it to zero.

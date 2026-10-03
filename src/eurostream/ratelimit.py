"""Token-bucket rate limiting for the routes that change things.

Reads are cheap and safe to hammer; ``POST /erase/{customer_id}`` is neither.
The limiter is per client and per bucket, refilled continuously from a
monotonic clock (so a burst costs nothing after a second of quiet, and a
clock adjustment cannot hand out free tokens).

Buckets are LRU-bounded: an unbounded ``key -> bucket`` map is a memory
exhaustion vector when the key is derived from anything a caller controls.
"""

from __future__ import annotations

import math
import threading
import time
from collections import OrderedDict
from collections.abc import Callable


class TokenBucket:
    """Continuous-refill bucket. ``rate`` tokens/second up to ``burst``."""

    def __init__(
        self,
        rate: float,
        burst: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        tokens: float | None = None,
    ) -> None:
        if rate < 0:
            raise ValueError("rate must be >= 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self.rate = float(rate)
        self.burst = float(burst)
        self._clock = clock
        self._tokens = self.burst if tokens is None else min(float(tokens), self.burst)
        self._updated = clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._updated = now

    def allow(self, cost: float = 1.0) -> tuple[bool, float]:
        """Try to spend ``cost`` tokens.

        Returns ``(allowed, retry_after_seconds)`` — ``retry_after`` is how
        long until the requested cost is affordable again, and is 0.0 when
        the call was allowed.
        """
        self._refill()
        if self._tokens >= cost:
            self._tokens -= cost
            return True, 0.0
        missing = cost - self._tokens
        wait = math.ceil(missing / self.rate) if self.rate > 0 else float("inf")
        return False, wait

    @property
    def tokens(self) -> float:
        self._refill()
        return self._tokens


class RateLimiter:
    """Per-key buckets with LRU eviction.

    ``rate <= 0`` disables limiting entirely — an operator can turn it off
    without touching call sites.
    """

    def __init__(
        self,
        rate: float,
        burst: float,
        *,
        max_buckets: int = 1024,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_buckets < 1:
            raise ValueError("max_buckets must be >= 1")
        self.rate = float(rate)
        self.burst = float(burst)
        self._max = int(max_buckets)
        self._clock = clock
        self._buckets: OrderedDict[str, TokenBucket] = OrderedDict()
        self._rejected = 0
        # Endpoints run on worker threads: bucket access must be serialised.
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self.rate > 0

    @property
    def rejected(self) -> int:
        return self._rejected

    def check(self, key: str, cost: float = 1.0) -> tuple[bool, float]:
        """Spend ``cost`` tokens from ``key``'s bucket."""
        if not self.enabled:
            return True, 0.0
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self.rate, self.burst, clock=self._clock)
                self._buckets[key] = bucket
                # LRU: a flood of fresh keys evicts the least recently used
                # bucket instead of growing the map.
                while len(self._buckets) > self._max:
                    self._buckets.popitem(last=False)
            else:
                self._buckets.move_to_end(key)
            allowed, retry_after = bucket.allow(cost)
            if not allowed:
                self._rejected += 1
            return allowed, retry_after

    def keys(self) -> int:
        """Number of tracked client buckets (bounded by ``max_buckets``)."""
        with self._lock:
            return len(self._buckets)

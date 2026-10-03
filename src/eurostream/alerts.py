"""In-process fan-out of fraud alerts for the live Server-Sent feed.

The scoring path is synchronous (a request triggers it, the response carries
the alerts). The dashboard does not want a response — it wants a firehose.
:class:`AlertBroker` is the seam between them:

* every published alert gets a monotonic ``seq`` and lands in a bounded ring
  buffer, so a browser that reconnects can ask "everything after event 12"
  with ``Last-Event-ID`` and resume without a gap;
* subscribers each get their own queue — one stalled consumer (a tab on a
  dead connection) cannot stall the publisher, and a queue that is full for
  too long drops *that* consumer's copy and counts it rather than growing
  without bound.

Everything here is process-local: the feed is a view of what this process
saw, which is exactly what a single-process reference platform should expose.
"""

from __future__ import annotations

import json
import queue
import threading
from collections import deque
from collections.abc import Iterable

#: Per-subscriber queue depth. A subscriber this far behind is not a slow
#: consumer, it is a dead one.
SUBSCRIBER_QUEUE_SIZE = 256

AlertPayload = dict[str, object]


class AlertBroker:
    """Thread-safe publish/subscribe with a replayable ring buffer."""

    def __init__(self, buffer_size: int = 512) -> None:
        if buffer_size < 1:
            raise ValueError("buffer_size must be >= 1")
        self._lock = threading.Lock()
        self._buffer: deque[tuple[int, AlertPayload]] = deque(maxlen=buffer_size)
        self._subscribers: set[queue.Queue[tuple[int, AlertPayload]]] = set()
        self._seq = 0
        self._published = 0
        self._dropped = 0

    # ---- publishing ----

    def publish(self, payload: AlertPayload) -> int:
        """Record ``payload``; returns the event id clients resume from."""
        with self._lock:
            self._seq += 1
            event = (self._seq, payload)
            self._published += 1
            self._buffer.append(event)
            for subscriber in self._subscribers:
                try:
                    subscriber.put_nowait(event)
                except queue.Full:
                    # Count it and move on: the publisher must never block on
                    # a consumer that stopped reading.
                    self._dropped += 1
        return self._seq

    # ---- subscribing ----

    def subscribe(
        self,
    ) -> tuple[queue.Queue[tuple[int, AlertPayload]], list[tuple[int, AlertPayload]]]:
        """Register a subscriber and return (its queue, current buffer).

        Both happen under one lock, so an event published after this call is
        in the queue and one published before is in the snapshot — there is
        no window where an alert is in neither.
        """
        with self._lock:
            subscriber: queue.Queue[tuple[int, AlertPayload]] = queue.Queue(
                maxsize=SUBSCRIBER_QUEUE_SIZE
            )
            self._subscribers.add(subscriber)
            return subscriber, list(self._buffer)

    def unsubscribe(self, subscriber: queue.Queue[tuple[int, AlertPayload]]) -> None:
        with self._lock:
            self._subscribers.discard(subscriber)

    def replay(self, after: int) -> list[tuple[int, AlertPayload]]:
        """Buffered events with ``seq > after`` (what Last-Event-ID asks for)."""
        with self._lock:
            return [(seq, payload) for seq, payload in self._buffer if seq > after]

    # ---- introspection ----

    @property
    def published(self) -> int:
        with self._lock:
            return self._published

    @property
    def dropped(self) -> int:
        with self._lock:
            return self._dropped

    @property
    def subscribers(self) -> int:
        with self._lock:
            return len(self._subscribers)

    @property
    def buffered(self) -> int:
        with self._lock:
            return len(self._buffer)

    @property
    def last_seq(self) -> int:
        with self._lock:
            return self._seq

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "published": self._published,
                "dropped": self._dropped,
                "subscribers": len(self._subscribers),
                "buffered": len(self._buffer),
                "last_seq": self._seq,
            }


def format_event(seq: int, payload: AlertPayload) -> str:
    """One SSE frame: ``id`` for resume, ``event`` for filtering, ``data`` JSON."""
    data = json.dumps(payload, default=str, separators=(",", ":"))
    return f"id: {seq}\nevent: alert\ndata: {data}\n\n"


def parse_last_event_id(value: str | None) -> int:
    """Last-Event-ID is whatever the client sent; junk means 'start over'."""
    if not value:
        return 0
    try:
        return max(0, int(value.strip()))
    except ValueError:
        return 0


def drain(
    subscriber: queue.Queue[tuple[int, AlertPayload]], up_to_seq: int
) -> Iterable[tuple[int, AlertPayload]]:
    """Yield queued events newer than ``up_to_seq``, discarding the rest."""
    while True:
        try:
            seq, payload = subscriber.get_nowait()
        except queue.Empty:
            return
        if seq > up_to_seq:
            yield seq, payload

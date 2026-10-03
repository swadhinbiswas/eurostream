# ADR 0004 — Jittered retries behind a circuit breaker

| | |
|---|---|
| Status | Accepted |
| Date | 2026-10-03 |
| Deciders | Platform Engineering |

## Context

The platform calls things that are not this process: the Turso libSQL HTTP
endpoint, the event bus, and anything else behind a URL. Those calls fail in
two ways that look similar and are not.

A **transient** failure (a 503, a socket dropped mid-write, a 429) wants a
second attempt. A **persistent** failure — the endpoint is down, the DNS
record moved, the credentials expired — wants no attempts at all: every
in-flight request parks on a 15 second timeout, the worker pool fills with
threads waiting for an answer that is not coming, and the dependency gets
pounded by a recovery stampede of clients that all failed at the same moment
and will all retry at the same moment.

Retrying harder makes the second case worse. Doing nothing makes the first
case user-visible. And the two failures compose: with retries alone, one
outage multiplies into `requests × attempts` timeouts.

## Decision

Two primitives in `eurostream.resilience`, composed in one order: **the
circuit breaker wraps the retry**, so a single failure of a call *is* one
failure of a call — one full retry sequence, not one attempt. The opposite
order would count three attempts as three failures and open the breaker on
the first blip, or (with the breaker inside) never observe a failure at all
because the retry already hid it.

- **Retry with exponential backoff and full jitter** — the delay is uniform
  over `[0, ceiling]` where the ceiling doubles per attempt (`base_delay`,
  capped at `max_delay`). Jitter is not decoration: without it, every client
  that failed together retries together, and the dependency receives a
  second, synchronised wave exactly when it is struggling. Defaults in the
  Turso client: 3 attempts, 0.2s base, ceiling `base × 8`.
- **Only statuses worth retrying are retried** — `RETRYABLE_STATUSES` is
  {408, 425, 429, 500, 502, 503, 504}. A 401 or a 422 is an answer, not an
  outage; retrying it wastes the caller's time and the server's capacity.
- **A three-state breaker**: `closed` → after `failure_threshold` consecutive
  failed calls → `open` (calls raise `CircuitOpenError` without touching the
  network) → after `recovery_seconds` → `half-open`, which admits one probe;
  success closes it, failure re-opens it for another cooldown. Defaults: 5
  failures, 30s recovery.
- **Both primitives take their clock and sleep function as arguments.** The
  failure paths are then testable without the test actually waiting — which
  is the only way a suite exercises "opens after five failures, recovers
  after thirty seconds" in milliseconds.
- **The state is observable**: `GET /turso/status` reports a `circuit` block
  (`state`, `failures`, `trips`, `retry_after`). A breaker nobody can see is
  a breaker that turns a slow dependency into a mysterious one.

## Consequences

- A dead dependency costs at most `failure_threshold` × (one retry budget)
  before requests fail fast with a clear error, instead of `failure_threshold`
  × timeout × pool size.
- Recovery is discovered by one probe per cooldown rather than by a
  stampede, and does not require restarting the process.
- A genuinely transient single failure is invisible to the breaker (one call,
  one sequence, still succeeds) — the breaker reacts to *persistence*, not to
  noise.
- Half-open admits exactly one probe; the rest of the traffic still fails
  fast. Deliberate: a breaker that lets everything through the moment the
  cooldown expires is a stampede with extra steps.
- The cost is an extra layer around every remote call and a `CircuitOpenError`
  that callers must be willing to see. Callers already handle exceptions from
  a failed call; this one arrives faster.
- Tests: `tests/test_resilience.py` drives the clock, so the open/recover/
  re-open transitions are asserted rather than assumed.

## Alternatives considered

- **Retry alone:** rejected — it converts a 15s timeout into 45s and never
  stops. Availability falls as the outage continues.
- **Breaker alone:** rejected — one dropped packet becomes a user-visible
  error and the breaker opens on noise.
- **Fixed backoff (no jitter):** rejected — synchronised retries are the
  thundering herd, which is the failure mode we were trying to avoid.
- **A library (`tenacity`, `backoff`):** rejected for now. The ordering rule
  (breaker around retry, with the retry budget folded into one failure) is the
  whole design; delegating it to a decorator stack hides exactly the part a
  reviewer needs to check, and pulls a dependency into a path that must work
  offline. Revisit if the policy grows (hedged requests, deadlines propagated
  per call).

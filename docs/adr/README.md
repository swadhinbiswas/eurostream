# Architecture decision records

Short, dated records of decisions that are expensive to reverse. One file
each, numbered forever: a decision that turns out wrong is superseded by a
new ADR that points at it, never by editing the old one — the point of the
record is that it still says what we thought at the time.

| #   | Decision                                                                             | Status     | Date       |
| --- | ------------------------------------------------------------------------------------ | ---------- | ---------- |
| [0001](0001-eu-region-choice.md)     | EU data residency: `eu-central-1` (Frankfurt), chosen for GDPR scope and latency      | Accepted | 2026-08-01 |
| [0002](0002-event-bus.md)            | Event bus: durable SQLite WAL locally, Kafka in production, behind one interface       | Accepted | 2026-08-01 |
| [0003](0003-pii-classification.md)   | PII classification: pure-Python recognizers + a manifest gate, not Presidio            | Accepted | 2026-08-01 |
| [0004](0004-resilience.md)           | Resilience: full-jitter retries behind a circuit breaker, breaker wrapping the retry   | Accepted | 2026-10-03 |
| [0005](0005-hash-chained-audit.md)   | Erasure audit: hash chain over the JSONL trail, cross-checked against the warehouse    | Accepted | 2026-10-03 |
| [0006](0006-data-quality-gates.md)    | Data-quality gates: self-baselined volume, per-layer freshness, measure-then-enforce k | Accepted | 2026-10-03 |
| [0007](0007-observability.md)          | Observability: scrape the app directly; alerts and panels must name series that exist | Accepted | 2026-10-03 |

## When to write one

Write an ADR when all three hold:

1. the decision is expensive to reverse (it shapes a schema, a public
   contract, or a dependency),
2. there is a real alternative being given up, and
3. six months from now the context will not be obvious from the code.

Adding a field to a settings model is none of these. Choosing what the
evidence for a GDPR erasure consists of is all three.

## Format

`# ADR <number> — <decision>`, then a table with Status and Date, then four
sections: **Context** (what was true, and what made this hard), **Decision**
(what we do, in the imperative, with the specifics a reviewer needs),
**Consequences** (including the limits we accepted — a known hole stated
plainly is worth more than an implied guarantee), and **Alternatives
considered** (why the obvious other options lost).

Number the next one `0008` and add it to the table above in the same commit.

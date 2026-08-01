# ADR 0002 — Event bus: durable SQLite log with a Kafka-compatible interface

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-01 |
| Deciders | Platform Engineering |

## Context

The platform needs an append-only event log with topics, consumer groups, and
replayability. Production deployments use Kafka (or Redpanda). The artifact
must run locally with zero containers and zero JVM. Confluent-kafka clients
are a native dependency that is not always available on minimal runtimes.

The tension is real: Kafka is the right production choice, but requiring a
broker for local development defeats the "runs on a laptop" goal. And the
GDPR erasure path depends on durable, replayable logs — an in-memory bus
doesn't cut it because tombstones need to survive crashes.

## Decision

Define a `Producer`/`Consumer` interface. Two implementations:

1. **`SqliteBus`** — a durable, append-only log on SQLite (WAL mode) with
   per-topic partitions, committed consumer-group offsets, and
   `auto_offset_reset` semantics. Used as the default backend.
2. **`KafkaProducer`/`KafkaConsumer`** — thin adapters over `confluent-kafka`
   selected at deploy time by configuration (`EUROSTREAM_EVENT_BUS_BACKEND`).

Application code depends only on the interface. The interface is the contract;
the implementation is a detail.

### Why SQLite specifically?

SQLite in WAL mode gives you:
- Durability (WAL survives crashes)
- Append-only semantics (no UPDATE/DELETE on the log itself)
- Consumer-group offset tracking (committed per partition)
- Zero configuration (no broker, no ports, no config files)
- Cross-platform (runs everywhere Python runs)

It doesn't shard or replicate. That's fine — it's the local stand-in. The
interface guarantees the semantics that matter (ordering per partition, offset
replay), and the Kafka adapter handles the rest in production.

## Consequences

- Local dev and CI are dependency-free and fast (no broker, no JVM, no Docker)
- The production path is a config change, not a rewrite: same topics, same
  consumer groups, same record shape
- SQLite is single-node; it does not shard or replicate. Acceptable for a
  portfolio artifact, and the interface abstracts this away.

## Alternatives considered

- **`confluent-kafka` everywhere + a local broker container:** rejected, needs
  Docker and a broker image. Violates the zero-container goal.
- **In-memory bus only:** rejected, loses durability and replay semantics that
  the GDPR tombstone path depends on. A tombstone that disappears on crash
  is not a tombstone.
- **RocksDB/LMDB:** rejected, adds native dependencies. SQLite is already
  in Python's standard library.

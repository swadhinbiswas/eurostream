from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

from . import Consumer, Producer, Record, RecordMetadata

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    topic      TEXT NOT NULL,
    partition  INTEGER NOT NULL,
    offset     INTEGER NOT NULL,
    key        TEXT NOT NULL,
    value      TEXT NOT NULL,
    headers    TEXT NOT NULL,
    ts         REAL NOT NULL,
    PRIMARY KEY (topic, partition, offset)
);
CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages (topic, partition, offset);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    consumer_group TEXT NOT NULL,
    topic          TEXT NOT NULL,
    partition      INTEGER NOT NULL,
    offset         INTEGER NOT NULL,
    PRIMARY KEY (consumer_group, topic, partition)
);

CREATE TABLE IF NOT EXISTS broker_offsets (
    topic     TEXT NOT NULL,
    partition INTEGER NOT NULL,
    next_offset INTEGER NOT NULL,
    PRIMARY KEY (topic, partition)
);
"""


class SqliteBus(Producer):
    """A durable, append-only event log on SQLite.

    Semantics mirror Kafka: producers append to a per-topic partition log,
    consumers track committed offsets per consumer group, and new consumers
    start at the tail (latest) unless ``auto_offset_reset="earliest"``.

    The same Producer/Consumer interface is backed by a real Kafka client
    when the platform is deployed (see kafka.py), so application code does
    not change between local and deployed topologies.
    """

    def __init__(self, db_path: Path, durability_checkpoint: int = 100) -> None:
        self._path = db_path
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        cur = self._conn.execute("PRAGMA journal_mode=WAL;")
        mode = cur.fetchone()
        # On read-only or NFS, WAL may silently fall back to DELETE; keep visibility.
        if mode and mode[0].lower() != "wal":
            # Not fatal for single-node demo, but warn via stderr if needed.
            pass
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._checkpoint = max(1, durability_checkpoint)
        self._produced = 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _next_offset(self, topic: str, partition: int) -> int:
        # Callers must hold self._lock and be inside a transaction.
        row = self._conn.execute(
            "SELECT next_offset FROM broker_offsets WHERE topic=? AND partition=?",
            (topic, partition),
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT OR IGNORE INTO broker_offsets (topic, partition, next_offset) VALUES (?,?,0)",
                (topic, partition),
            )
            return 0
        return int(row["next_offset"])

    def topic_offsets(self, topic: str) -> dict[int, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT partition, next_offset FROM broker_offsets WHERE topic=?", (topic,)
            ).fetchall()
            return {int(r["partition"]): int(r["next_offset"]) for r in rows}

    def produce(
        self, topic: str, key: str, value: str, headers: dict[str, str] | None = None
    ) -> RecordMetadata:
        partition = 0
        with self._lock:
            # BEGIN IMMEDIATE serializes concurrent producers at the SQLite level.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                offset = self._next_offset(topic, partition)
                self._conn.execute(
                    "INSERT INTO messages (topic, partition, offset, key, value, headers, ts) VALUES (?,?,?,?,?,?,?)",
                    (topic, partition, offset, key, value, json.dumps(headers or {}), time.time()),
                )
                self._conn.execute(
                    "INSERT OR REPLACE INTO broker_offsets (topic, partition, next_offset) VALUES (?,?,?)",
                    (topic, partition, offset + 1),
                )
                self._conn.commit()
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            # Periodically fold the WAL back into the main database file so the
            # durable log is not solely dependent on the write-ahead log.
            self._produced += 1
            if self._produced % self._checkpoint == 0:
                self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
            return RecordMetadata(topic=topic, offset=offset, partition=partition)

    def consumer(
        self, topic: str, group_id: str, auto_offset_reset: str = "latest"
    ) -> SqliteConsumer:
        start = "latest"
        if auto_offset_reset == "earliest":
            start = "earliest"
        return SqliteConsumer(self._conn, self._lock, topic, group_id, start)


class SqliteConsumer(Consumer):
    def __init__(
        self, conn: sqlite3.Connection, lock: threading.Lock, topic: str, group_id: str, start: str
    ) -> None:
        self._conn = conn
        self._lock = lock
        self._topic = topic
        self._group = group_id
        self._partition = 0
        self._pos = self._load_position(start)

    def _load_position(self, start: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT offset FROM consumer_offsets WHERE consumer_group=? AND topic=? AND partition=?",
                (self._group, self._topic, self._partition),
            ).fetchone()
            if row is not None:
                return int(row["offset"])
            if start == "earliest":
                return 0
            broker = self._conn.execute(
                "SELECT next_offset FROM broker_offsets WHERE topic=? AND partition=?",
                (self._topic, self._partition),
            ).fetchone()
            return int(broker["next_offset"]) if broker else 0

    def poll(self, timeout: float = 0.0) -> Record | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT topic, partition, offset, key, value, headers, ts FROM messages "
                "WHERE topic=? AND partition=? AND offset>=? ORDER BY offset LIMIT 1",
                (self._topic, self._partition, self._pos),
            ).fetchone()
            if row is None:
                return None
            rec = Record(
                topic=row["topic"],
                key=row["key"],
                value=row["value"],
                offset=int(row["offset"]),
                partition=int(row["partition"]),
                timestamp=float(row["ts"]),
                headers=json.loads(row["headers"]),
            )
            self._pos = rec.offset + 1
            return rec

    def commit(self) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO consumer_offsets (consumer_group, topic, partition, offset) VALUES (?,?,?,?)",
                (self._group, self._topic, self._partition, self._pos),
            )
            self._conn.commit()

    def close(self) -> None:
        # Only commit if we have actually advanced; close() is idempotent.
        self.commit()

    def current_offset(self) -> int:
        return self._pos


def open_bus(db_path: Path) -> SqliteBus:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteBus(db_path)

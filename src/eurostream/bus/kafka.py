from __future__ import annotations

import logging
import re
from typing import Any

from . import Consumer, Producer, Record, RecordMetadata

logger = logging.getLogger(__name__)


def _parse_bootstrap(bootstrap: str) -> str:
    """Clean bootstrap string — handles full Service URI, masked ***, quotes, and whitespace.

    Accepts:
      - host:port
      - "host:port" or 'host:port'
      - https://user:pass@host:port
      - kafka://user:pass@host:port
      - kafka+ssl://user:pass@host:port
      - sasl_ssl://user:pass@host:port/db
    Returns host:port or raises ValueError if unusable.
    """
    raw = (bootstrap or "").strip().strip("\"' \t\r\n")
    if not raw or raw == "***" or raw.count("*") >= 3:
        raise ValueError(
            "Kafka bootstrap is not set — check EUROSTREAM_KAFKA_BOOTSTRAP_SERVERS secret"
        )
    # Strip scheme://
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    # Strip user:pass@
    if "@" in raw:
        raw = raw.split("@", 1)[1]
    # Strip trailing path /... and query parameters ?...
    raw = raw.split("/", 1)[0].split("?", 1)[0].strip("\"' \t\r\n")
    # Basic host:port validation
    if not re.match(r"^[\w\-.]+:\d+(\s*,\s*[\w\-.]+:\d+)*$", raw):
        # Allow but warn — confluent will validate
        logger.warning("Bootstrap looks unusual: %s", raw[:30])
    return raw


class KafkaProducer(Producer):
    """Adapter over confluent-kafka so the same Producer interface works when
    the platform is deployed on a real cluster. The local default (SqliteBus)
    is used for development; this class keeps the application code unchanged.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        username: str | None = None,
        password: str | None = None,
        security_protocol: str = "SASL_SSL",
        sasl_mechanism: str = "SCRAM-SHA-256",
    ) -> None:
        from confluent_kafka import Producer as _CProducer

        clean = _parse_bootstrap(bootstrap_servers)
        conf: dict[str, Any] = {
            "bootstrap.servers": clean,
            "broker.address.family": "v4",
            "socket.timeout.ms": 10000,
            "request.timeout.ms": 10000,
            "message.timeout.ms": 15000,
        }
        if username and password:
            conf.update(
                {
                    "security.protocol": security_protocol,
                    "sasl.mechanism": sasl_mechanism,
                    "sasl.username": str(username).strip("\"' "),
                    "sasl.password": str(password).strip("\"' "),
                }
            )
        self._producer = _CProducer(conf)

    def produce(
        self, topic: str, key: str, value: str, headers: dict[str, str] | None = None
    ) -> RecordMetadata:
        def _on_delivery(err: object, msg: Any) -> None:
            if err is not None:
                logger.error("Kafka delivery error on topic %s: %s", topic, err)

        # confluent-kafka expects headers as list of (str, bytes)
        kafka_headers = [(k, v.encode()) for k, v in (headers or {}).items()] if headers else None
        self._producer.produce(
            topic,
            key=key.encode(),
            value=value.encode(),
            headers=kafka_headers,
            on_delivery=_on_delivery,
        )
        # Serve delivery callbacks without blocking synchronous roundtrips
        self._producer.poll(0)
        return RecordMetadata(topic=topic, partition=0, offset=0)

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all outstanding messages to be delivered."""
        return self._producer.flush(timeout)


class KafkaConsumer(Consumer):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "latest",
        username: str | None = None,
        password: str | None = None,
        security_protocol: str = "SASL_SSL",
        sasl_mechanism: str = "SCRAM-SHA-256",
    ) -> None:
        from confluent_kafka import Consumer as _CConsumer

        clean = _parse_bootstrap(bootstrap_servers)
        conf: dict[str, Any] = {
            "bootstrap.servers": clean,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
            "broker.address.family": "v4",
            "socket.timeout.ms": 10000,
            "session.timeout.ms": 15000,
        }
        if username and password:
            conf.update(
                {
                    "security.protocol": security_protocol,
                    "sasl.mechanism": sasl_mechanism,
                    "sasl.username": str(username).strip("\"' "),
                    "sasl.password": str(password).strip("\"' "),
                }
            )
        self._consumer = _CConsumer(conf)
        self._consumer.subscribe([topic])

    def poll(self, timeout: float = 0.0) -> Record | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        err = msg.error()
        if err is not None:
            # _PARTITION_EOF is benign (end of partition), others are real errors.
            try:
                from confluent_kafka import KafkaError

                if err.code() == KafkaError._PARTITION_EOF:
                    return None
            except Exception:  # noqa: S110
                pass
            logger.warning("Kafka poll error: %s", err)
            return None
        return Record(
            topic=msg.topic(),
            key=msg.key().decode() if msg.key() else "",
            value=msg.value().decode() if msg.value() else "",
            offset=msg.offset(),
            partition=msg.partition(),
            timestamp=msg.timestamp()[1] / 1000.0,
            headers={
                k: (v.decode() if isinstance(v, (bytes, bytearray)) else (v or ""))
                for k, v in (msg.headers() or [])
            },
        )

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


class KafkaBus(Producer):
    """Unified bus wrapper so `SqliteBus` and Kafka share the same `bus.produce` + `bus.consumer` interface.

    Lets `cli._fresh()` switch with one `if settings.event_bus_backend == "kafka"` branch.
    """

    def __init__(self, settings: Any) -> None:  # Settings to avoid circular import type
        self._settings = settings
        self._producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            username=settings.kafka_username,
            password=settings.kafka_password,
            security_protocol=settings.kafka_security_protocol,
            sasl_mechanism=settings.kafka_sasl_mechanism,
        )

    def produce(
        self, topic: str, key: str, value: str, headers: dict[str, str] | None = None
    ) -> RecordMetadata:
        return self._producer.produce(topic, key, value, headers)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def consumer(
        self, topic: str, group_id: str, auto_offset_reset: str = "latest"
    ) -> KafkaConsumer:
        return KafkaConsumer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            topic=topic,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            username=self._settings.kafka_username,
            password=self._settings.kafka_password,
            security_protocol=self._settings.kafka_security_protocol,
            sasl_mechanism=self._settings.kafka_sasl_mechanism,
        )

    def close(self) -> None:
        self._producer.flush()

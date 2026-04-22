"""Generic Kafka producer with JSON serialization."""

from __future__ import annotations

import orjson
from confluent_kafka import Producer


class WhaleProducer:
    """Thin wrapper around confluent_kafka.Producer for JSON messages."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "compression.type": "lz4",
        })

    def send(self, topic: str, value: dict, key: str | None = None) -> None:
        """Serialize *value* as JSON and produce to *topic*."""
        payload = orjson.dumps(value)
        self._producer.produce(
            topic=topic,
            value=payload,
            key=key.encode() if key else None,
            callback=self._delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    @staticmethod
    def _delivery_callback(err, msg) -> None:  # noqa: ANN001
        if err is not None:
            print(f"[kafka-producer] Delivery failed: {err}")

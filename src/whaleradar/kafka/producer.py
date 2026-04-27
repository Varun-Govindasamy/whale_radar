"""Generic Kafka producer with JSON serialization."""

from __future__ import annotations

import orjson
from confluent_kafka import Producer


class WhaleProducer:
    """Thin wrapper around confluent_kafka.Producer for JSON messages."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "whaleradar-producer",
            "linger.ms": 5,
            "batch.num.messages": 100,
            "compression.type": "lz4",
            "socket.timeout.ms": 10000,
            "message.timeout.ms": 30000,
        })
        self._sent_count = 0
        self._error_count = 0

    def send(self, topic: str, value: dict, key: str | None = None) -> None:
        """Serialize *value* as JSON and produce to *topic*."""
        payload = orjson.dumps(value)
        try:
            self._producer.produce(
                topic=topic,
                value=payload,
                key=key.encode() if key else None,
                callback=self._delivery_callback,
            )
            self._sent_count += 1
            # Flush every 50 messages to ensure delivery
            if self._sent_count % 50 == 0:
                self._producer.flush(5)
            else:
                self._producer.poll(0)
        except BufferError:
            self._producer.flush(5)
            self._producer.produce(
                topic=topic,
                value=payload,
                key=key.encode() if key else None,
                callback=self._delivery_callback,
            )

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    def _delivery_callback(self, err, msg) -> None:  # noqa: ANN001
        if err is not None:
            self._error_count += 1
            if self._error_count <= 5:
                print(f"[kafka-producer] Delivery failed: {err}")
            elif self._error_count == 6:
                print("[kafka-producer] Suppressing further error logs...")
        elif self._sent_count <= 3:
            print(f"[kafka-producer] Delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}")

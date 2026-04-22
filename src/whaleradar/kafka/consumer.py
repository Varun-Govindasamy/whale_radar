"""Generic Kafka consumer with JSON deserialization."""

from __future__ import annotations

from collections.abc import Generator

import orjson
from confluent_kafka import Consumer, KafkaError


class WhaleConsumer:
    """Thin wrapper around confluent_kafka.Consumer yielding dicts."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "latest",
    ) -> None:
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
        })
        self._consumer.subscribe(topics)

    def consume(self, timeout: float = 1.0) -> Generator[dict, None, None]:
        """Yield deserialized messages forever."""
        while True:
            msg = self._consumer.poll(timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[kafka-consumer] Error: {msg.error()}")
                continue
            try:
                yield orjson.loads(msg.value())
            except Exception as exc:
                print(f"[kafka-consumer] Deserialization error: {exc}")

    def close(self) -> None:
        self._consumer.close()

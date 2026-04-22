"""Kafka topic constants and admin utilities."""

from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic

TOPIC_RAW_TRADES = "raw-trades"
TOPIC_WHALE_ALERTS = "whale-alerts"
TOPIC_EXCHANGE_FLOWS = "exchange-flows"
TOPIC_WHALE_DETECTIONS = "whale-detections"
TOPIC_SIGNALS = "signals"

ALL_TOPICS = [
    TOPIC_RAW_TRADES,
    TOPIC_WHALE_ALERTS,
    TOPIC_EXCHANGE_FLOWS,
    TOPIC_WHALE_DETECTIONS,
    TOPIC_SIGNALS,
]


def ensure_topics(bootstrap_servers: str, num_partitions: int = 3, replication_factor: int = 1) -> None:
    """Create all required Kafka topics if they don't exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing = set(admin.list_topics(timeout=10).topics.keys())
    new_topics = [
        NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
        for topic in ALL_TOPICS
        if topic not in existing
    ]

    if not new_topics:
        return

    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[kafka] Created topic: {topic}")
        except Exception as exc:
            print(f"[kafka] Topic {topic} creation failed: {exc}")

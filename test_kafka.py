"""Quick Kafka connectivity test — run after docker compose up -d."""
import sys
import time

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient

BOOTSTRAP = "localhost:9094"

print(f"[test] Connecting to Kafka at {BOOTSTRAP}...")

# 1. Check broker metadata
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
md = admin.list_topics(timeout=10)
print(f"[test] Cluster ID: {md.cluster_id}")
for broker_id, broker in md.brokers.items():
    print(f"[test] Broker {broker_id}: {broker.host}:{broker.port}")
print(f"[test] Topics: {list(md.topics.keys())}")

# 2. Produce a test message
print("\n[test] Producing test message...")
p = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "test-producer"})


def delivery_cb(err, msg):
    if err:
        print(f"[test] PRODUCE FAILED: {err}")
    else:
        print(f"[test] PRODUCED OK: {msg.topic()}[{msg.partition()}]@{msg.offset()}")


p.produce("raw-trades", value=b'{"test": true, "symbol": "BTCUSDT"}', callback=delivery_cb)
p.flush(10)

# 3. Consume it back
print("\n[test] Consuming from raw-trades...")
c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "test-consumer",
    "auto.offset.reset": "earliest",
})
c.subscribe(["raw-trades"])

found = False
for _ in range(10):
    msg = c.poll(2.0)
    if msg and not msg.error():
        print(f"[test] CONSUMED OK: {msg.value().decode()}")
        found = True
        break
    elif msg and msg.error():
        print(f"[test] Consumer error: {msg.error()}")

if not found:
    print("[test] WARNING: No messages consumed (may be offset issue)")

c.close()
print("\n[test] Done!")

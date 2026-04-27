"""WhaleRadar — Main orchestrator entry point.

Starts all services concurrently:
1. Kafka topic creation
2. Binance WebSocket collector
3. Blockchain monitor (Blockchair + Mempool.space)
4. Whale detection consumer → LangGraph agents → MCP tools
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
from datetime import datetime, timezone

# Fix Windows console encoding for UTF-8
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from whaleradar.agents.graph import analyze_whale_event
from whaleradar.config import settings
from whaleradar.kafka.consumer import WhaleConsumer
from whaleradar.kafka.topics import (
    TOPIC_EXCHANGE_FLOWS,
    TOPIC_WHALE_ALERTS,
    TOPIC_WHALE_DETECTIONS,
    ensure_topics,
)
from whaleradar.mcp.dashboard_tool import push_event_direct, push_signal_direct
from whaleradar.mcp.telegram_tool import send_alert_direct
from whaleradar.storage.hdfs_client import HDFSClient


async def run_agent_pipeline(event: dict, ml_prediction: dict | None = None) -> None:
    """Run the full LangGraph agent pipeline on a whale event and dispatch results."""
    if ml_prediction is None:
        ml_prediction = {
            "intent": "unknown",
            "confidence": 0.5,
            "features_used": {},
            "model_version": "v1",
        }

    try:
        result = await analyze_whale_event(event, ml_prediction)

        signal_data = {
            "signal_id": result.get("signal_id", ""),
            "whale_event": event,
            "intent": ml_prediction.get("intent", "unknown"),
            "confidence": ml_prediction.get("confidence", 0),
            "risk_level": result.get("risk_level", "unknown"),
            "signal_text": result.get("signal_text", ""),
            "suggested_action": result.get("suggested_action", ""),
            "timeframe_minutes": 120,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Dispatch via MCP tools
        push_event_direct(event)
        push_signal_direct(signal_data)

        # Send Telegram alert for high/critical
        if result.get("risk_level") in ("high", "critical"):
            await send_alert_direct(result.get("signal_text", "Whale alert!"))

        # Save to HDFS
        try:
            hdfs = HDFSClient()
            hdfs.write_event(event)
            hdfs.write_signal(signal_data)
        except Exception as exc:
            print(f"[orchestrator] HDFS write failed: {exc}")

        print(
            f"[orchestrator] Signal {result.get('signal_id')} generated — "
            f"risk={result.get('risk_level')}"
        )

    except Exception as exc:
        print(f"[orchestrator] Agent pipeline error: {exc}")


async def consume_whale_events() -> None:
    """Consume whale events from Kafka and run through agent pipeline."""
    from confluent_kafka import KafkaError

    from whaleradar.kafka.consumer import WhaleConsumer

    consumer = WhaleConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="whaleradar-agents",
        topics=[TOPIC_WHALE_ALERTS, TOPIC_EXCHANGE_FLOWS, TOPIC_WHALE_DETECTIONS],
    )

    print("[orchestrator] Whale event consumer started.")

    loop = asyncio.get_event_loop()
    try:
        while True:
            # Poll Kafka in a thread so we don't block the event loop
            msg = await loop.run_in_executor(None, consumer._consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[kafka-consumer] Error: {msg.error()}")
                continue
            try:
                import orjson
                event = orjson.loads(msg.value())
                await run_agent_pipeline(event)
            except Exception as exc:
                print(f"[kafka-consumer] Error processing: {exc}")
    except asyncio.CancelledError:
        pass
    finally:
        consumer.close()


async def main_async() -> None:
    """Start all components concurrently."""
    from whaleradar.collectors.binance_ws import run_binance_collector
    from whaleradar.collectors.blockchain_monitor import run_blockchain_monitor

    print("=" * 60)
    print("  🐋 WhaleRadar — CryptoWhaleTracker")
    print("  Starting all services...")
    print("=" * 60)

    # Step 1: Ensure Kafka topics exist
    print("[startup] Creating Kafka topics...")
    ensure_topics(settings.kafka_bootstrap_servers)

    # Step 2: Start all services concurrently
    tasks = [
        asyncio.create_task(run_binance_collector()),
        asyncio.create_task(run_blockchain_monitor()),
        asyncio.create_task(consume_whale_events()),
    ]

    print("[startup] All services started.")
    print(f"[startup] Monitoring: {', '.join(settings.symbols_list)}")
    print(f"[startup] Whale threshold: ${settings.whale_threshold_usd:,.0f}")
    print("=" * 60)

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n[shutdown] Shutting down WhaleRadar...")
        for t in tasks:
            t.cancel()


def main() -> None:
    """Entry point."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n[shutdown] WhaleRadar stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()

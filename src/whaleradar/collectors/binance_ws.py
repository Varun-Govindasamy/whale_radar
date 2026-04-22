"""Binance WebSocket trade stream collector.

Connects to Binance's combined stream for BTC/ETH trades and publishes
every trade to the Kafka `raw-trades` topic.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import websockets

from whaleradar.config import settings
from whaleradar.kafka.producer import WhaleProducer
from whaleradar.kafka.topics import TOPIC_RAW_TRADES

BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"


def _build_stream_url(symbols: list[str]) -> str:
    """Build combined stream URL for multiple symbols."""
    streams = "/".join(f"{s}@trade" for s in symbols)
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


async def run_binance_collector() -> None:
    """Connect to Binance WS and stream trades to Kafka forever."""
    producer = WhaleProducer(settings.kafka_bootstrap_servers)
    url = _build_stream_url(settings.symbols_list)

    print(f"[binance-ws] Connecting to {len(settings.symbols_list)} streams...")

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print("[binance-ws] Connected.")
                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                        data = msg.get("data", msg)

                        price = float(data["p"])
                        qty = float(data["q"])
                        quote_vol = price * qty

                        trade = {
                            "symbol": data["s"],
                            "price": price,
                            "quantity": qty,
                            "quote_volume": quote_vol,
                            "trade_time": datetime.fromtimestamp(
                                data["T"] / 1000, tz=timezone.utc
                            ).isoformat(),
                            "is_buyer_maker": data["m"],
                        }

                        producer.send(
                            topic=TOPIC_RAW_TRADES,
                            value=trade,
                            key=data["s"],
                        )
                    except (KeyError, ValueError) as exc:
                        print(f"[binance-ws] Parse error: {exc}")

        except websockets.ConnectionClosed:
            print("[binance-ws] Connection closed, reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as exc:
            print(f"[binance-ws] Error: {exc}, reconnecting in 10s...")
            await asyncio.sleep(10)

"""Blockchain monitor — Blockchair + Mempool.space polling.

Watches for large on-chain BTC/ETH transactions and publishes whale
movements to Kafka `whale-alerts` and `exchange-flows` topics.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from whaleradar.collectors.exchange_addresses import lookup_exchange
from whaleradar.config import settings
from whaleradar.kafka.producer import WhaleProducer
from whaleradar.kafka.topics import TOPIC_EXCHANGE_FLOWS, TOPIC_WHALE_ALERTS

BLOCKCHAIR_BTC_URL = "https://api.blockchair.com/bitcoin/transactions"
BLOCKCHAIR_ETH_URL = "https://api.blockchair.com/ethereum/transactions"
MEMPOOL_RECENT_URL = "https://mempool.space/api/mempool/recent"


class BlockchainMonitor:
    """Polls blockchain APIs for large transactions."""

    def __init__(self) -> None:
        self.producer = WhaleProducer(settings.kafka_bootstrap_servers)
        self.client = httpx.AsyncClient(timeout=30.0)
        self._seen_txs: set[str] = set()
        self._max_seen = 10_000

    async def _check_blockchair_btc(self) -> None:
        """Fetch recent large BTC transactions from Blockchair."""
        try:
            params = {
                "key": settings.blockchair_api_key,
                "s": "output_total(desc)",
                "limit": 10,
                "q": f"output_total({int(settings.onchain_min_btc * 1e8)}..),"
                     f"time({datetime.now(timezone.utc).strftime('%Y-%m-%d')})",
            }
            resp = await self.client.get(BLOCKCHAIR_BTC_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

            for tx in data.get("data", []):
                tx_hash = tx.get("hash", "")
                if tx_hash in self._seen_txs:
                    continue
                self._seen_txs.add(tx_hash)

                amount_btc = tx.get("output_total", 0) / 1e8
                # Estimate USD from output_total_usd if available
                amount_usd = tx.get("output_total_usd", amount_btc * 60000)

                event = self._build_event(
                    tx_hash=tx_hash,
                    blockchain="bitcoin",
                    from_addr=tx.get("input_address", "unknown"),
                    to_addr=tx.get("output_address", "unknown"),
                    amount=amount_btc,
                    amount_usd=amount_usd,
                )
                self._publish(event)

        except Exception as exc:
            print(f"[blockchain] Blockchair BTC error: {exc}")

    async def _check_mempool(self) -> None:
        """Check mempool.space for large unconfirmed BTC transactions."""
        try:
            resp = await self.client.get(MEMPOOL_RECENT_URL)
            resp.raise_for_status()
            txs = resp.json()

            for tx in txs:
                txid = tx.get("txid", "")
                if txid in self._seen_txs:
                    continue

                value_btc = tx.get("value", 0) / 1e8
                if value_btc < settings.onchain_min_btc:
                    continue

                self._seen_txs.add(txid)
                event = self._build_event(
                    tx_hash=txid,
                    blockchain="bitcoin",
                    from_addr="mempool",
                    to_addr="mempool",
                    amount=value_btc,
                    amount_usd=value_btc * 60000,  # rough estimate
                )
                self._publish(event)

        except Exception as exc:
            print(f"[blockchain] Mempool.space error: {exc}")

    async def _check_blockchair_eth(self) -> None:
        """Fetch recent large ETH transactions from Blockchair."""
        try:
            params = {
                "key": settings.blockchair_api_key,
                "s": "value(desc)",
                "limit": 10,
                "q": f"value({int(settings.onchain_min_eth * 1e18)}..),"
                     f"time({datetime.now(timezone.utc).strftime('%Y-%m-%d')})",
            }
            resp = await self.client.get(BLOCKCHAIR_ETH_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

            for tx in data.get("data", []):
                tx_hash = tx.get("hash", "")
                if tx_hash in self._seen_txs:
                    continue
                self._seen_txs.add(tx_hash)

                amount_eth = tx.get("value", 0) / 1e18
                amount_usd = tx.get("value_usd", amount_eth * 3000)

                event = self._build_event(
                    tx_hash=tx_hash,
                    blockchain="ethereum",
                    from_addr=tx.get("sender", "unknown"),
                    to_addr=tx.get("recipient", "unknown"),
                    amount=amount_eth,
                    amount_usd=amount_usd,
                )
                self._publish(event)

        except Exception as exc:
            print(f"[blockchain] Blockchair ETH error: {exc}")

    def _build_event(
        self,
        tx_hash: str,
        blockchain: str,
        from_addr: str,
        to_addr: str,
        amount: float,
        amount_usd: float,
    ) -> dict:
        exchange = lookup_exchange(to_addr, blockchain)
        return {
            "tx_hash": tx_hash,
            "blockchain": blockchain,
            "from_address": from_addr,
            "to_address": to_addr,
            "amount": amount,
            "amount_usd": amount_usd,
            "is_exchange_deposit": exchange is not None,
            "exchange_name": exchange,
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

    def _publish(self, event: dict) -> None:
        topic = TOPIC_EXCHANGE_FLOWS if event["is_exchange_deposit"] else TOPIC_WHALE_ALERTS
        self.producer.send(topic=topic, value=event, key=event["tx_hash"])
        action = "EXCHANGE DEPOSIT" if event["is_exchange_deposit"] else "WHALE MOVE"
        print(
            f"[blockchain] {action}: {event['amount']:.4f} {event['blockchain'].upper()} "
            f"(~${event['amount_usd']:,.0f}) tx={event['tx_hash'][:16]}..."
        )

    def _trim_seen(self) -> None:
        if len(self._seen_txs) > self._max_seen:
            self._seen_txs = set(list(self._seen_txs)[-5000:])

    async def run(self, poll_interval: int = 30) -> None:
        """Poll blockchain APIs forever."""
        print(f"[blockchain] Starting monitor (poll every {poll_interval}s)...")
        while True:
            await asyncio.gather(
                self._check_blockchair_btc(),
                self._check_blockchair_eth(),
                self._check_mempool(),
            )
            self._trim_seen()
            await asyncio.sleep(poll_interval)

    async def close(self) -> None:
        await self.client.aclose()
        self.producer.flush()


async def run_blockchain_monitor() -> None:
    monitor = BlockchainMonitor()
    try:
        await monitor.run()
    finally:
        await monitor.close()

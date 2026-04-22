"""MCP Tool — Blockchain Explorer for mid-reasoning wallet lookups.

Allows agents to query Blockchair for additional details about a wallet
during the reasoning process.
"""

from __future__ import annotations

import httpx
from langchain_core.tools import tool

from whaleradar.config import settings


@tool
async def lookup_wallet(address: str, blockchain: str = "bitcoin") -> str:
    """Look up detailed information about a cryptocurrency wallet address.

    Args:
        address: The wallet address to look up.
        blockchain: The blockchain to query ("bitcoin" or "ethereum").

    Returns:
        Formatted wallet info string.
    """
    if blockchain == "bitcoin":
        url = f"https://api.blockchair.com/bitcoin/dashboards/address/{address}"
    elif blockchain == "ethereum":
        url = f"https://api.blockchair.com/ethereum/dashboards/address/{address}"
    else:
        return f"Unsupported blockchain: {blockchain}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(url, params={"key": settings.blockchair_api_key})
            resp.raise_for_status()
            data = resp.json()

            addr_data = data.get("data", {}).get(address, {}).get("address", {})
            if not addr_data:
                return f"No data found for address {address} on {blockchain}."

            if blockchain == "bitcoin":
                balance = addr_data.get("balance", 0) / 1e8
                tx_count = addr_data.get("transaction_count", 0)
                received = addr_data.get("received", 0) / 1e8
                spent = addr_data.get("spent", 0) / 1e8
                first_seen = addr_data.get("first_seen_receiving")
                return (
                    f"🔍 Bitcoin Wallet: {address[:20]}...\n"
                    f"  Balance: {balance:.8f} BTC\n"
                    f"  Transactions: {tx_count}\n"
                    f"  Total Received: {received:.8f} BTC\n"
                    f"  Total Spent: {spent:.8f} BTC\n"
                    f"  First Seen: {first_seen}\n"
                )
            else:
                balance = addr_data.get("balance", 0) / 1e18
                tx_count = addr_data.get("transaction_count", 0)
                return (
                    f"🔍 Ethereum Wallet: {address[:20]}...\n"
                    f"  Balance: {balance:.6f} ETH\n"
                    f"  Transactions: {tx_count}\n"
                )

        except httpx.HTTPStatusError as exc:
            return f"API error: {exc.response.status_code}"
        except Exception as exc:
            return f"Lookup failed: {exc}"


async def lookup_wallet_direct(address: str, blockchain: str = "bitcoin") -> str:
    """Direct lookup without LangChain wrapper."""
    return await lookup_wallet.ainvoke({"address": address, "blockchain": blockchain})

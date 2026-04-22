"""Agent 1 — The Whale Profiler.

When a whale wallet is spotted, this agent queries HDFS for historical
data about the wallet and builds or retrieves a profile.
"""

from __future__ import annotations

from datetime import datetime, timezone

from langchain_core.messages import SystemMessage
from langchain_openai import ChatOpenAI

from whaleradar.agents.state import WhaleAnalysisState
from whaleradar.config import settings
from whaleradar.storage.hdfs_client import HDFSClient

_SYSTEM_PROMPT = """You are a Whale Profiler agent analyzing cryptocurrency whale wallets.
Given a wallet's transaction history, summarize the behavioral pattern:
- How many times has this wallet appeared?
- What percentage of appearances resulted in dumps vs pumps?
- What is the typical transaction size?
- Is this wallet known or new?
- What is your confidence level in predicting this wallet's behavior?

Be concise and data-driven. Output a clear behavioral summary."""


def whale_profiler(state: WhaleAnalysisState) -> dict:
    """Profile the whale wallet by checking HDFS history."""
    whale_event = state["whale_event"]
    from_address = whale_event.get("from_address", "unknown")

    # Query HDFS for wallet history
    try:
        hdfs = HDFSClient()
        existing_profile = hdfs.get_wallet_profile(from_address)
        wallet_history = hdfs.query_wallet_history(from_address)
    except Exception:
        existing_profile = None
        wallet_history = []

    if existing_profile:
        profile = existing_profile
        profile["total_appearances"] = profile.get("total_appearances", 0) + 1
        profile["last_seen"] = datetime.now(timezone.utc).isoformat()
        profile["is_known"] = True
    else:
        profile = {
            "address": from_address,
            "blockchain": whale_event.get("blockchain", "unknown"),
            "total_appearances": 1,
            "dump_count": 0,
            "pump_count": 0,
            "neutral_count": 0,
            "avg_amount": whale_event.get("amount", 0),
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "is_known": False,
            "notes": "First appearance — new wallet profile created.",
        }

    # Use LLM to generate behavioral summary
    llm = ChatOpenAI(
        model="gpt-4o-mini",
        api_key=settings.openai_api_key,
        temperature=0.1,
    )

    history_summary = (
        f"Wallet {from_address} has appeared {profile['total_appearances']} times. "
        f"Dump count: {profile['dump_count']}, Pump count: {profile['pump_count']}, "
        f"Neutral: {profile['neutral_count']}. "
        f"Avg amount: {profile['avg_amount']:.4f}. "
        f"Known: {profile['is_known']}. "
        f"Current event: {whale_event.get('amount', 0):.4f} {whale_event.get('blockchain', '').upper()} "
        f"(~${whale_event.get('amount_usd', 0):,.0f}). "
        f"Exchange deposit: {whale_event.get('is_exchange_deposit', False)}."
    )

    if wallet_history:
        history_summary += f"\nPast {len(wallet_history)} events found in history."

    response = llm.invoke([
        SystemMessage(content=_SYSTEM_PROMPT),
        SystemMessage(content=history_summary),
    ])

    # Save updated profile
    try:
        hdfs.save_wallet_profile(profile)
    except Exception:
        pass

    return {
        "wallet_profile": profile,
        "wallet_history": wallet_history,
        "profile_summary": response.content,
        "messages": [response],
    }

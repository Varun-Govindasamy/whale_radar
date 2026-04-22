"""Agent 3 — The Signal Generator.

Takes the final conclusion from the Impact Predictor and produces
a clean, human-readable signal for delivery via MCP tools.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from whaleradar.agents.state import WhaleAnalysisState
from whaleradar.config import settings

_SYSTEM_PROMPT = """You are a Signal Generator agent for a cryptocurrency whale tracking system.
Your job is to take the analysis from the Impact Predictor agent and produce a clear,
actionable, human-readable alert.

Format the alert as follows:
🚨 WHALE ALERT: [brief title]

📊 Details:
- [key metrics]

🔮 Analysis:
- [prediction and reasoning summary]

⚡ Suggested Action:
- [what the user should do]

⏰ Timeframe: [expected impact window]

Keep it concise, clear, and actionable. Use emojis for visual clarity.
The alert should be understandable by a crypto trader in 10 seconds."""


def signal_generator(state: WhaleAnalysisState) -> dict:
    """Generate a clean, human-readable signal from the analysis."""
    whale_event = state["whale_event"]
    wallet_profile = state.get("wallet_profile", {})
    ml_prediction = state["ml_prediction"]
    risk_level = state.get("risk_level", "unknown")
    final_assessment = state.get("final_assessment", "No assessment available.")
    market_condition = state.get("market_condition", "unknown")

    llm = ChatOpenAI(
        model="gpt-4o-mini",
        api_key=settings.openai_api_key,
        temperature=0.3,
    )

    context = (
        f"Generate a whale alert signal based on this analysis:\n\n"
        f"## Event\n"
        f"- Blockchain: {whale_event.get('blockchain', 'unknown').upper()}\n"
        f"- Amount: {whale_event.get('amount', 0):.4f}\n"
        f"- USD Value: ${whale_event.get('amount_usd', 0):,.0f}\n"
        f"- Exchange Deposit: {whale_event.get('is_exchange_deposit', False)}\n"
        f"- Exchange: {whale_event.get('exchange_name', 'N/A')}\n"
        f"- Wallet: {whale_event.get('from_address', 'unknown')[:20]}...\n\n"
        f"## ML Prediction\n"
        f"- Intent: {ml_prediction.get('intent', 'unknown')}\n"
        f"- Confidence: {ml_prediction.get('confidence', 0):.1%}\n\n"
        f"## Impact Analysis\n"
        f"- Market Condition: {market_condition}\n"
        f"- Risk Level: {risk_level}\n"
        f"- Assessment: {final_assessment}\n\n"
        f"## Wallet History\n"
        f"- Appearances: {wallet_profile.get('total_appearances', 0)}\n"
        f"- Dump ratio: {wallet_profile.get('dump_count', 0)}/{max(wallet_profile.get('total_appearances', 1), 1)}\n"
    )

    response = llm.invoke([
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=context),
    ])

    signal_text = response.content
    signal_id = f"SIG-{uuid.uuid4().hex[:8].upper()}"

    # Derive suggested action
    if risk_level == "critical":
        suggested_action = "Immediate attention required. Consider reducing exposure."
    elif risk_level == "high":
        suggested_action = "Monitor closely. Set stop-losses if exposed."
    elif risk_level == "medium":
        suggested_action = "Watch for confirmation. No immediate action needed."
    else:
        suggested_action = "Low risk. Continue normal operations."

    return {
        "signal_text": signal_text,
        "suggested_action": suggested_action,
        "signal_id": signal_id,
        "messages": [response],
    }

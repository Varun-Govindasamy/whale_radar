"""Agent 2 — The Impact Predictor.

Takes the Whale Profiler's findings + Spark ML score and loops through
a reasoning process until reaching a confident conclusion about the
whale's likely market impact.
"""

from __future__ import annotations

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from whaleradar.agents.state import WhaleAnalysisState
from whaleradar.config import settings

_SYSTEM_PROMPT = """You are an Impact Predictor agent for cryptocurrency whale movements.
You analyze whale events and determine their likely market impact.

You receive:
1. A whale event (on-chain movement details)
2. A wallet behavioral profile from the Whale Profiler agent
3. An ML model prediction with confidence score

Your job is to reason through these questions:
- What is the current market condition? (bull/bear/sideways)
- What happened historically in similar conditions?
- Does the on-chain data confirm the exchange trade data?
- Should this be escalated to an alert or dismissed as noise?

Think step-by-step. After reasoning, provide:
- market_condition: bull/bear/sideways
- risk_level: low/medium/high/critical
- final_assessment: a clear 2-3 sentence conclusion

Format your response as:
MARKET_CONDITION: <bull|bear|sideways>
RISK_LEVEL: <low|medium|high|critical>
REASONING: <your step-by-step reasoning>
ASSESSMENT: <final conclusion>"""

MAX_REASONING_ITERATIONS = 3
CONFIDENCE_THRESHOLD = 0.7


def impact_predictor(state: WhaleAnalysisState) -> dict:
    """Reason through market impact in a loop until confident."""
    whale_event = state["whale_event"]
    ml_prediction = state["ml_prediction"]
    profile_summary = state.get("profile_summary", "No profile available.")
    wallet_profile = state.get("wallet_profile", {})
    iteration = state.get("iteration_count", 0)

    llm = ChatOpenAI(
        model="gpt-4o-mini",
        api_key=settings.openai_api_key,
        temperature=0.2,
    )

    # Build context for reasoning
    context = (
        f"## Whale Event\n"
        f"- Blockchain: {whale_event.get('blockchain', 'unknown').upper()}\n"
        f"- Amount: {whale_event.get('amount', 0):.4f} (~${whale_event.get('amount_usd', 0):,.0f})\n"
        f"- Exchange Deposit: {whale_event.get('is_exchange_deposit', False)}\n"
        f"- Exchange: {whale_event.get('exchange_name', 'N/A')}\n"
        f"- From: {whale_event.get('from_address', 'unknown')[:20]}...\n"
        f"- To: {whale_event.get('to_address', 'unknown')[:20]}...\n\n"
        f"## Wallet Profile Summary\n{profile_summary}\n\n"
        f"## ML Prediction\n"
        f"- Intent: {ml_prediction.get('intent', 'unknown')}\n"
        f"- Confidence: {ml_prediction.get('confidence', 0):.1%}\n\n"
        f"## Historical Stats\n"
        f"- Total appearances: {wallet_profile.get('total_appearances', 0)}\n"
        f"- Dump ratio: {wallet_profile.get('dump_count', 0)}/{wallet_profile.get('total_appearances', 1)}\n"
    )

    if iteration > 0:
        context += (
            f"\n## Previous Reasoning (iteration {iteration})\n"
            f"Previous steps: {state.get('reasoning_steps', [])}\n"
            f"You were not confident enough. Dig deeper.\n"
        )

    response = llm.invoke([
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=context),
    ])

    content = response.content

    # Parse structured output
    market_condition = _extract_field(content, "MARKET_CONDITION", "sideways")
    risk_level = _extract_field(content, "RISK_LEVEL", "medium")
    reasoning = _extract_field(content, "REASONING", content)
    assessment = _extract_field(content, "ASSESSMENT", "Unable to determine impact.")

    reasoning_steps = state.get("reasoning_steps", [])
    reasoning_steps.append(f"[Iteration {iteration + 1}] {reasoning[:500]}")

    # Check if we need another iteration
    ml_confidence = ml_prediction.get("confidence", 0)
    should_continue = (
        iteration < MAX_REASONING_ITERATIONS - 1
        and ml_confidence < CONFIDENCE_THRESHOLD
        and risk_level in ("medium", "high")
    )

    return {
        "market_condition": market_condition,
        "risk_level": risk_level,
        "reasoning_steps": reasoning_steps,
        "final_assessment": assessment,
        "iteration_count": iteration + 1,
        "messages": [response],
        "_should_continue": should_continue,
    }


def should_continue_reasoning(state: WhaleAnalysisState) -> str:
    """LangGraph conditional edge: continue reasoning or move to signal generation."""
    if state.get("_should_continue", False):
        return "impact_predictor"
    return "signal_generator"


def _extract_field(text: str, field: str, default: str) -> str:
    """Extract a labeled field from LLM response text."""
    for line in text.split("\n"):
        if line.strip().upper().startswith(f"{field}:"):
            return line.split(":", 1)[1].strip()
    return default

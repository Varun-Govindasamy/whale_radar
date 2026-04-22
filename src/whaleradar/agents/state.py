"""LangGraph shared state schema for the whale analysis pipeline."""

from __future__ import annotations

from typing import Annotated, TypedDict

from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


class WhaleAnalysisState(TypedDict):
    """State passed between agents in the LangGraph pipeline.

    Fields populated progressively by each agent:
    - whale_profiler fills: wallet_profile, wallet_history, profile_summary
    - impact_predictor fills: market_condition, reasoning_steps, risk_level, final_assessment
    - signal_generator fills: signal_text, suggested_action
    """

    # ── Input (from Spark / Kafka) ──────────────────────────
    whale_event: dict  # serialized WhaleEvent
    ml_prediction: dict  # serialized MLPrediction

    # ── Whale Profiler output ───────────────────────────────
    wallet_profile: dict  # serialized WalletProfile
    wallet_history: list[dict]
    profile_summary: str

    # ── Impact Predictor output ─────────────────────────────
    market_condition: str  # bull / bear / sideways
    reasoning_steps: list[str]
    risk_level: str  # low / medium / high / critical
    final_assessment: str
    iteration_count: int  # reasoning loop counter

    # ── Signal Generator output ─────────────────────────────
    signal_text: str
    suggested_action: str
    signal_id: str

    # ── LLM messages (for agent reasoning traces) ──────────
    messages: Annotated[list[BaseMessage], add_messages]

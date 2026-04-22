"""LangGraph state graph wiring the 3 agents together.

Flow: whale_profiler → impact_predictor (loop) → signal_generator
"""

from __future__ import annotations

from langgraph.graph import END, StateGraph

from whaleradar.agents.impact_predictor import (
    impact_predictor,
    should_continue_reasoning,
)
from whaleradar.agents.signal_generator import signal_generator
from whaleradar.agents.state import WhaleAnalysisState
from whaleradar.agents.whale_profiler import whale_profiler


def build_whale_analysis_graph() -> StateGraph:
    """Build and compile the 3-agent whale analysis pipeline."""
    graph = StateGraph(WhaleAnalysisState)

    # ── Add nodes ───────────────────────────────────────────
    graph.add_node("whale_profiler", whale_profiler)
    graph.add_node("impact_predictor", impact_predictor)
    graph.add_node("signal_generator", signal_generator)

    # ── Wire edges ──────────────────────────────────────────
    graph.set_entry_point("whale_profiler")

    graph.add_edge("whale_profiler", "impact_predictor")

    # Impact predictor can loop back to itself or proceed
    graph.add_conditional_edges(
        "impact_predictor",
        should_continue_reasoning,
        {
            "impact_predictor": "impact_predictor",
            "signal_generator": "signal_generator",
        },
    )

    graph.add_edge("signal_generator", END)

    return graph.compile()


# Pre-compiled graph instance
whale_analysis_graph = build_whale_analysis_graph()


async def analyze_whale_event(whale_event: dict, ml_prediction: dict) -> dict:
    """Run the full 3-agent analysis pipeline on a whale event.

    Returns the final state containing signal_text, risk_level, etc.
    """
    initial_state: WhaleAnalysisState = {
        "whale_event": whale_event,
        "ml_prediction": ml_prediction,
        "wallet_profile": {},
        "wallet_history": [],
        "profile_summary": "",
        "market_condition": "",
        "reasoning_steps": [],
        "risk_level": "",
        "final_assessment": "",
        "iteration_count": 0,
        "signal_text": "",
        "suggested_action": "",
        "signal_id": "",
        "messages": [],
    }

    result = await whale_analysis_graph.ainvoke(initial_state)
    return result

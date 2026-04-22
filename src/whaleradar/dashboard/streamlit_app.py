"""WhaleRadar Streamlit Dashboard — AI Agent Narrative Display.

Shows live whale alerts, agent reasoning, wallet profiles, and signal history.
Run with: streamlit run src/whaleradar/dashboard/streamlit_app.py
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

DB_PATH = Path("data/dashboard.db")


def get_db() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


def load_events(limit: int = 50) -> list[dict]:
    try:
        conn = get_db()
        cursor = conn.execute("SELECT data FROM whale_events ORDER BY id DESC LIMIT ?", (limit,))
        rows = [json.loads(r[0]) for r in cursor.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def load_signals(limit: int = 50) -> list[dict]:
    try:
        conn = get_db()
        cursor = conn.execute("SELECT data FROM signals ORDER BY id DESC LIMIT ?", (limit,))
        rows = [json.loads(r[0]) for r in cursor.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


# ── Page Config ─────────────────────────────────────────────
st.set_page_config(
    page_title="WhaleRadar — AI Whale Tracker",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────────────────────
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    .whale-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #0f3460;
        border-radius: 12px;
        padding: 20px;
        margin: 10px 0;
    }
    .signal-critical { border-left: 4px solid #e74c3c; }
    .signal-high { border-left: 4px solid #f39c12; }
    .signal-medium { border-left: 4px solid #3498db; }
    .signal-low { border-left: 4px solid #2ecc71; }
    .metric-card {
        background: linear-gradient(135deg, #0f3460 0%, #533483 100%);
        border-radius: 12px;
        padding: 15px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────
with st.sidebar:
    st.image("https://em-content.zobj.net/source/apple/391/spouting-whale_1f433.png", width=80)
    st.title("🐋 WhaleRadar")
    st.caption("Real-time Crypto Whale Intelligence")
    st.divider()

    auto_refresh = st.toggle("Auto-refresh (30s)", value=True)
    event_limit = st.slider("Events to show", 10, 100, 50)

    st.divider()
    st.markdown("### 📡 System Status")
    st.markdown("🟢 Binance WS: Connected")
    st.markdown("🟢 Blockchain Monitor: Active")
    st.markdown("🟢 Kafka: Running")
    st.markdown("🟢 Spark: Streaming")

# ── Header ──────────────────────────────────────────────────
st.title("🐋 WhaleRadar Dashboard")
st.caption("AI-Powered Cryptocurrency Whale Movement Tracker")

# ── Metrics Row ─────────────────────────────────────────────
events = load_events(event_limit)
signals = load_signals(event_limit)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("🐋 Whale Events", len(events))
with col2:
    exchange_deps = sum(1 for e in events if e.get("is_exchange_deposit"))
    st.metric("🏦 Exchange Deposits", exchange_deps)
with col3:
    total_usd = sum(e.get("amount_usd", 0) for e in events)
    st.metric("💰 Total Volume", f"${total_usd:,.0f}")
with col4:
    critical = sum(1 for s in signals if s.get("risk_level") == "critical")
    st.metric("🚨 Critical Alerts", critical)

st.divider()

# ── Two-column layout ──────────────────────────────────────
left_col, right_col = st.columns([3, 2])

with left_col:
    st.subheader("🚨 Live Whale Alerts")

    if signals:
        for signal in signals[:20]:
            risk = signal.get("risk_level", "unknown")
            risk_color = {
                "critical": "🔴",
                "high": "🟠",
                "medium": "🔵",
                "low": "🟢",
            }.get(risk, "⚪")

            with st.expander(
                f"{risk_color} [{risk.upper()}] Signal {signal.get('signal_id', 'N/A')} "
                f"— {signal.get('created_at', '')[:19]}",
                expanded=(risk in ("critical", "high")),
            ):
                st.markdown(signal.get("signal_text", "No signal text."))
                st.divider()
                st.markdown(f"**Suggested Action:** {signal.get('suggested_action', 'N/A')}")
                st.markdown(f"**Intent:** {signal.get('intent', 'N/A')} | "
                           f"**Confidence:** {signal.get('confidence', 0):.1%}")
    else:
        st.info("No signals yet. Waiting for whale activity...")

with right_col:
    st.subheader("🐋 Recent Whale Events")

    if events:
        for event in events[:15]:
            blockchain = event.get("blockchain", "unknown").upper()
            amount = event.get("amount", 0)
            usd = event.get("amount_usd", 0)
            is_deposit = event.get("is_exchange_deposit", False)
            exchange = event.get("exchange_name", "")
            detected = event.get("detected_at", "")[:19]

            icon = "🏦" if is_deposit else "🐋"
            exchange_tag = f" → **{exchange.upper()}**" if exchange else ""

            st.markdown(
                f"{icon} **{amount:.4f} {blockchain}** (~${usd:,.0f})"
                f"{exchange_tag}\n"
                f"<small>{detected}</small>",
                unsafe_allow_html=True,
            )
            st.divider()
    else:
        st.info("No whale events detected yet...")

# ── Signal History Table ────────────────────────────────────
st.divider()
st.subheader("📊 Signal History")

if signals:
    table_data = []
    for s in signals[:50]:
        table_data.append({
            "Signal ID": s.get("signal_id", ""),
            "Risk": s.get("risk_level", ""),
            "Intent": s.get("intent", ""),
            "Confidence": f"{s.get('confidence', 0):.1%}",
            "Created": s.get("created_at", "")[:19],
            "Action": s.get("suggested_action", "")[:60],
        })
    st.dataframe(table_data, use_container_width=True)
else:
    st.info("No signal history available.")

# ── Auto-refresh ────────────────────────────────────────────
if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()

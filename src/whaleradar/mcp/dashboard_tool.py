"""MCP Tool — Dashboard data writer.

Pushes whale events and signals to a local SQLite store that is read
by the Streamlit dashboard and Grafana.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from langchain_core.tools import tool

DB_PATH = Path("data/dashboard.db")


def _get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS whale_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT,
            blockchain TEXT,
            amount REAL,
            amount_usd REAL,
            is_exchange_deposit INTEGER,
            exchange_name TEXT,
            from_address TEXT,
            to_address TEXT,
            detected_at TEXT,
            data TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT UNIQUE,
            risk_level TEXT,
            signal_text TEXT,
            suggested_action TEXT,
            confidence REAL,
            intent TEXT,
            created_at TEXT,
            data TEXT
        )
    """)
    conn.commit()
    return conn


@tool
def push_whale_event(event_json: str) -> str:
    """Push a whale event to the dashboard database.

    Args:
        event_json: JSON string of the whale event.

    Returns:
        Confirmation with row ID.
    """
    event = json.loads(event_json)
    conn = _get_connection()
    cursor = conn.execute(
        """INSERT INTO whale_events
           (tx_hash, blockchain, amount, amount_usd, is_exchange_deposit,
            exchange_name, from_address, to_address, detected_at, data)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            event.get("tx_hash"),
            event.get("blockchain"),
            event.get("amount"),
            event.get("amount_usd"),
            int(event.get("is_exchange_deposit", False)),
            event.get("exchange_name"),
            event.get("from_address"),
            event.get("to_address"),
            event.get("detected_at", datetime.now(timezone.utc).isoformat()),
            json.dumps(event),
        ),
    )
    conn.commit()
    row_id = cursor.lastrowid
    conn.close()
    return f"Whale event pushed to dashboard (id={row_id})"


@tool
def push_signal(signal_json: str) -> str:
    """Push a generated signal to the dashboard database.

    Args:
        signal_json: JSON string of the signal.

    Returns:
        Confirmation with signal ID.
    """
    signal = json.loads(signal_json)
    conn = _get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO signals
           (signal_id, risk_level, signal_text, suggested_action,
            confidence, intent, created_at, data)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            signal.get("signal_id"),
            signal.get("risk_level"),
            signal.get("signal_text"),
            signal.get("suggested_action"),
            signal.get("confidence"),
            signal.get("intent"),
            signal.get("created_at", datetime.now(timezone.utc).isoformat()),
            json.dumps(signal),
        ),
    )
    conn.commit()
    conn.close()
    return f"Signal pushed to dashboard (signal_id={signal.get('signal_id')})"


def push_event_direct(event: dict) -> None:
    """Direct push without LangChain wrapper."""
    push_whale_event.invoke(json.dumps(event))


def push_signal_direct(signal: dict) -> None:
    """Direct push without LangChain wrapper."""
    push_signal.invoke(json.dumps(signal))


def get_recent_events(limit: int = 50) -> list[dict]:
    """Query recent whale events for dashboard display."""
    conn = _get_connection()
    cursor = conn.execute(
        "SELECT data FROM whale_events ORDER BY id DESC LIMIT ?", (limit,)
    )
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_recent_signals(limit: int = 50) -> list[dict]:
    """Query recent signals for dashboard display."""
    conn = _get_connection()
    cursor = conn.execute(
        "SELECT data FROM signals ORDER BY id DESC LIMIT ?", (limit,)
    )
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    conn.close()
    return rows

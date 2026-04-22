"""Airflow DAG — Daily whale activity summary.

Runs every morning at 08:00 UTC. Generates a summary report of
all whale activity from the past 24 hours and sends via Telegram.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "whaleradar",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def generate_daily_summary_task() -> None:
    """Generate and send a daily whale activity summary."""
    from whaleradar.mcp.dashboard_tool import get_recent_events, get_recent_signals
    from whaleradar.mcp.telegram_tool import send_alert_direct
    from whaleradar.storage.hdfs_client import HDFSClient

    events = get_recent_events(limit=100)
    signals = get_recent_signals(limit=50)

    # Filter to last 24h
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_str = cutoff.isoformat()

    recent_events = [
        e for e in events if e.get("detected_at", "") >= cutoff_str
    ]
    recent_signals = [
        s for s in signals if s.get("created_at", "") >= cutoff_str
    ]

    # Build summary
    total_events = len(recent_events)
    exchange_deposits = sum(1 for e in recent_events if e.get("is_exchange_deposit"))
    total_volume = sum(e.get("amount_usd", 0) for e in recent_events)

    critical = sum(1 for s in recent_signals if s.get("risk_level") == "critical")
    high = sum(1 for s in recent_signals if s.get("risk_level") == "high")

    report = (
        f"📊 *WhaleRadar Daily Summary*\n"
        f"_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}_\n\n"
        f"🐋 Whale Events: *{total_events}*\n"
        f"🏦 Exchange Deposits: *{exchange_deposits}*\n"
        f"💰 Total Volume: *${total_volume:,.0f}*\n\n"
        f"🚨 Signals Generated: *{len(recent_signals)}*\n"
        f"  🔴 Critical: {critical}\n"
        f"  🟠 High: {high}\n\n"
    )

    if recent_signals:
        report += "*Top Signals:*\n"
        for sig in recent_signals[:5]:
            report += f"  • [{sig.get('risk_level', '?').upper()}] {sig.get('signal_id', 'N/A')}\n"

    # Save report to HDFS
    try:
        hdfs = HDFSClient()
        hdfs.write_report(
            {"summary": report, "events_count": total_events, "signals_count": len(recent_signals)},
            "daily_summary",
        )
    except Exception as exc:
        print(f"[daily-summary] HDFS write failed: {exc}")

    # Send via Telegram
    asyncio.run(send_alert_direct(report))
    print("[daily-summary] Report sent.")


with DAG(
    dag_id="whaleradar_daily_summary",
    default_args=default_args,
    description="Daily whale activity summary report",
    schedule_interval="0 8 * * *",  # 08:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["whaleradar", "report", "daily"],
) as dag:
    summary = PythonOperator(
        task_id="generate_daily_summary",
        python_callable=generate_daily_summary_task,
    )

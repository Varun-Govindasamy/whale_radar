"""Airflow DAG — Weekly backtesting.

Runs every Sunday at 10:00 UTC. Checks how accurate last week's
signals were against actual price movements. Logs accuracy metrics.
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


def run_backtest_task() -> None:
    """Backtest last week's signals against actual price movements."""
    import httpx

    from whaleradar.mcp.dashboard_tool import get_recent_signals
    from whaleradar.mcp.telegram_tool import send_alert_direct
    from whaleradar.storage.hdfs_client import HDFSClient

    signals = get_recent_signals(limit=200)
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    cutoff_str = cutoff.isoformat()

    weekly_signals = [
        s for s in signals if s.get("created_at", "") >= cutoff_str
    ]

    if not weekly_signals:
        print("[backtest] No signals to backtest.")
        return

    correct = 0
    incorrect = 0
    inconclusive = 0

    for signal in weekly_signals:
        intent = signal.get("intent", "neutral")
        # Simple backtest: check if predicted direction matched
        # In production, this would fetch actual price data
        outcome = signal.get("outcome")
        if outcome is None:
            inconclusive += 1
        elif outcome == intent:
            correct += 1
        else:
            incorrect += 1

    total = len(weekly_signals)
    evaluated = correct + incorrect
    accuracy = correct / evaluated * 100 if evaluated > 0 else 0

    report = {
        "period_start": cutoff_str,
        "period_end": datetime.now(timezone.utc).isoformat(),
        "total_signals": total,
        "correct": correct,
        "incorrect": incorrect,
        "inconclusive": inconclusive,
        "accuracy_pct": accuracy,
    }

    # Save to HDFS
    try:
        hdfs = HDFSClient()
        hdfs.write_report(report, "weekly_backtest")
    except Exception as exc:
        print(f"[backtest] HDFS write failed: {exc}")

    # Send summary via Telegram
    message = (
        f"📈 *WhaleRadar Weekly Backtest*\n"
        f"_Week ending {datetime.now(timezone.utc).strftime('%Y-%m-%d')}_\n\n"
        f"Total Signals: *{total}*\n"
        f"✅ Correct: *{correct}*\n"
        f"❌ Incorrect: *{incorrect}*\n"
        f"❓ Inconclusive: *{inconclusive}*\n"
        f"🎯 Accuracy: *{accuracy:.1f}%*\n"
    )

    asyncio.run(send_alert_direct(message))
    print(f"[backtest] Report sent. Accuracy: {accuracy:.1f}%")


with DAG(
    dag_id="whaleradar_weekly_backtest",
    default_args=default_args,
    description="Weekly backtesting of signal accuracy",
    schedule_interval="0 10 * * 0",  # Sunday 10:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["whaleradar", "backtest", "weekly"],
) as dag:
    backtest = PythonOperator(
        task_id="run_backtest",
        python_callable=run_backtest_task,
    )

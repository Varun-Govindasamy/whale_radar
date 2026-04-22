"""Airflow DAG — Nightly model retraining.

Runs at midnight UTC every day. Pulls latest data from HDFS,
retrains the Spark ML model, and saves the updated model.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "whaleradar",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def retrain_model_task() -> None:
    """Pull data from HDFS and retrain the whale intent model."""
    from whaleradar.spark.ml_model import (
        build_training_session,
        generate_synthetic_training_data,
        save_model,
        train_model,
    )
    from whaleradar.storage.hdfs_client import HDFSClient

    spark = build_training_session()

    # Try to load real training data from HDFS
    try:
        hdfs = HDFSClient()
        records = hdfs.read_training_data()
        if len(records) >= 100:
            training_df = spark.createDataFrame(records)
            print(f"[retrain] Using {len(records)} real training records from HDFS.")
        else:
            raise ValueError("Not enough real data")
    except Exception:
        print("[retrain] Insufficient real data — using synthetic data for bootstrap.")
        training_df = generate_synthetic_training_data(spark, n_rows=10000)

    model = train_model(training_df)

    version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    save_model(model, f"whale_intent_model_{version}")

    # Also save to HDFS for persistence
    try:
        import pickle
        model_bytes = pickle.dumps(model)
        hdfs = HDFSClient()
        hdfs.save_model(model_bytes, version)
        print(f"[retrain] Model v{version} saved to HDFS.")
    except Exception as exc:
        print(f"[retrain] HDFS save failed: {exc}")

    spark.stop()


with DAG(
    dag_id="whaleradar_retrain_model",
    default_args=default_args,
    description="Nightly retraining of whale intent ML model",
    schedule_interval="0 0 * * *",  # midnight UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["whaleradar", "ml", "nightly"],
) as dag:
    retrain = PythonOperator(
        task_id="retrain_model",
        python_callable=retrain_model_task,
    )

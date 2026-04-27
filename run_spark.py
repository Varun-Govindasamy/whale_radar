"""Run the Spark ML training job and streaming pipeline.

Usage (from project root):
    $env:PYTHONUNBUFFERED="1"; uv run python run_spark.py --train
    $env:PYTHONUNBUFFERED="1"; uv run python run_spark.py --stream
    $env:PYTHONUNBUFFERED="1"; uv run python run_spark.py --both
"""

from __future__ import annotations

import argparse
import sys

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def run_training() -> None:
    """Train the GBT ML model using synthetic data and save it."""
    import os
    # PySpark 4.x on Windows needs explicit Python path
    python_path = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    print("=" * 60)
    print("  [Spark] Training Whale Intent ML Model")
    print(f"  Python: {python_path}")
    print("=" * 60)

    from whaleradar.spark.ml_model import (
        build_training_session,
        generate_synthetic_training_data,
        save_model,
        train_model,
    )

    spark = build_training_session()
    spark.sparkContext.setLogLevel("WARN")
    print("[spark] Session created — check Spark UI at http://localhost:8082")

    # Generate synthetic training data (bootstrapping)
    print("[spark] Generating 10,000 synthetic training rows...")
    training_data = generate_synthetic_training_data(spark, n_rows=10000)
    training_data.show(5)
    training_data.printSchema()

    # Train GBT model
    print("[spark] Training GBT classifier...")
    model = train_model(training_data)

    # Save model
    save_path = save_model(model)
    print(f"[spark] Model saved to: {save_path}")

    # Show model details
    from pyspark.ml.classification import GBTClassificationModel
    gbt_model = model.stages[-1]
    print(f"[spark] Number of trees: {gbt_model.getNumTrees}")
    print(f"[spark] Feature importances: {gbt_model.featureImportances}")
    print(f"[spark] Total iterations: {gbt_model.totalNumNodes}")

    print("\n[spark] Training complete! Model is ready for inference.")
    print("[spark] Refresh Spark UI at http://localhost:8082 to see the completed job.")

    spark.stop()


def run_streaming() -> None:
    """Start the Spark Structured Streaming pipeline."""
    print("=" * 60)
    print("  [Spark] Starting Streaming Pipeline")
    print("=" * 60)

    from whaleradar.spark.streaming_job import run_streaming_job

    print("[spark] Streaming from Kafka raw-trades -> whale-detections")
    print("[spark] Check Spark UI at http://localhost:8082")
    run_streaming_job()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WhaleRadar Spark Jobs")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--train", action="store_true", help="Train the ML model")
    group.add_argument("--stream", action="store_true", help="Start streaming pipeline")
    group.add_argument("--both", action="store_true", help="Train then stream")
    args = parser.parse_args()

    if args.train:
        run_training()
    elif args.stream:
        run_streaming()
    elif args.both:
        run_training()
        run_streaming()

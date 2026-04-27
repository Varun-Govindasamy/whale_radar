"""ML model for whale intent prediction using Spark MLlib.

GBT (Gradient Boosted Trees) classifier predicting whether a whale
movement will result in a pump, dump, or neutral price action.
"""

from __future__ import annotations

import pickle
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession

from whaleradar.config import settings

MODEL_DIR = "whale_intent_model"
FEATURE_COLS = [
    "quote_volume",
    "price_momentum",
    "volume_ratio",
    "hour_of_day",
    "is_buyer_maker_num",
]


def build_training_session() -> SparkSession:
    """Create a local Spark session for batch training."""
    import os
    return (
        SparkSession.builder
        .appName("WhaleRadar-ModelTraining")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.pyspark.python", os.environ.get("PYSPARK_PYTHON", "python"))
        .config("spark.pyspark.driver.python", os.environ.get("PYSPARK_DRIVER_PYTHON", "python"))
        .getOrCreate()
    )


def prepare_features(df: DataFrame) -> DataFrame:
    """Add derived feature columns to a DataFrame of trades."""
    from pyspark.sql import functions as F

    return (
        df
        .withColumn("hour_of_day", F.hour("trade_time"))
        .withColumn("is_buyer_maker_num", F.col("is_buyer_maker").cast("int"))
        .withColumn(
            "volume_ratio",
            F.col("quote_volume") / F.avg("quote_volume").over(
                F.window("trade_time", "1 hour")
            ),
        )
        .withColumn(
            "price_momentum",
            F.coalesce(
                (F.col("price") - F.lag("price", 1).over(
                    F.partitionBy("symbol").orderBy("trade_time")
                )) / F.lag("price", 1).over(
                    F.partitionBy("symbol").orderBy("trade_time")
                ) * 100,
                F.lit(0.0),
            ),
        )
        .na.fill(0.0)
    )


def train_model(training_data: DataFrame) -> PipelineModel:
    """Train a GBT classifier for whale intent prediction.

    Labels: 0=neutral, 1=pump, 2=dump
    """
    label_indexer = StringIndexer(inputCol="label", outputCol="indexed_label")
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")

    gbt = GBTClassifier(
        labelCol="indexed_label",
        featuresCol="features",
        maxIter=50,
        maxDepth=5,
        stepSize=0.1,
    )

    pipeline = Pipeline(stages=[label_indexer, assembler, gbt])

    # Split data
    train_df, test_df = training_data.randomSplit([0.8, 0.2], seed=42)

    # Train
    model = pipeline.fit(train_df)

    # Evaluate
    predictions = model.transform(test_df)
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexed_label",
        predictionCol="prediction",
        metricName="accuracy",
    )
    accuracy = evaluator.evaluate(predictions)
    print(f"[ml] Model accuracy: {accuracy:.4f}")

    return model


def save_model(model: PipelineModel, path: str | None = None) -> str:
    """Save trained model to disk."""
    save_path = path or MODEL_DIR
    model.write().overwrite().save(save_path)
    print(f"[ml] Model saved to {save_path}")
    return save_path


def load_model(path: str | None = None) -> PipelineModel | None:
    """Load a trained model from disk."""
    load_path = path or MODEL_DIR
    try:
        return PipelineModel.load(load_path)
    except Exception:
        print(f"[ml] No model found at {load_path}")
        return None


def predict(model: PipelineModel, df: DataFrame) -> DataFrame:
    """Run inference on a DataFrame of whale trades."""
    featured = prepare_features(df)
    return model.transform(featured)


def generate_synthetic_training_data(spark: SparkSession, n_rows: int = 10000) -> DataFrame:
    """Generate synthetic training data for initial model bootstrap.

    In production, this is replaced by real HDFS historical data.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        FloatType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    import random

    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", FloatType()),
        StructField("quote_volume", FloatType()),
        StructField("trade_time", TimestampType()),
        StructField("is_buyer_maker", IntegerType()),
        StructField("price_momentum", FloatType()),
        StructField("volume_ratio", FloatType()),
        StructField("hour_of_day", IntegerType()),
        StructField("is_buyer_maker_num", IntegerType()),
        StructField("label", StringType()),
    ])

    rows = []
    for _ in range(n_rows):
        label = random.choice(["pump", "dump", "neutral"])
        vol = random.uniform(500_000, 50_000_000)
        momentum = {
            "pump": random.uniform(0.5, 5.0),
            "dump": random.uniform(-5.0, -0.5),
            "neutral": random.uniform(-0.3, 0.3),
        }[label]

        rows.append((
            random.choice(["BTCUSDT", "ETHUSDT"]),
            random.uniform(20000, 100000),
            vol,
            None,  # trade_time
            random.choice([0, 1]),
            momentum,
            random.uniform(0.5, 10.0),
            random.randint(0, 23),
            random.choice([0, 1]),
            label,
        ))

    return spark.createDataFrame(rows, schema)

"""Standalone Spark ML training script — runs inside Docker Spark container.

No whaleradar package dependencies — only uses pyspark + stdlib.
"""
import random
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    FloatType, IntegerType, StringType, StructField, StructType
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler

FEATURE_COLS = ["quote_volume", "price_momentum", "volume_ratio", "hour_of_day", "is_buyer_maker_num"]
MODEL_DIR = "/opt/spark/work-dir/whale_intent_model"

def generate_training_data(spark, n_rows=10000):
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", FloatType()),
        StructField("quote_volume", FloatType()),
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
        momentum = {"pump": random.uniform(0.5, 5.0), "dump": random.uniform(-5.0, -0.5), "neutral": random.uniform(-0.3, 0.3)}[label]
        rows.append((
            random.choice(["BTCUSDT", "ETHUSDT"]),
            random.uniform(20000, 100000), vol,
            random.choice([0, 1]), momentum,
            random.uniform(0.5, 10.0), random.randint(0, 23),
            random.choice([0, 1]), label,
        ))
    return spark.createDataFrame(rows, schema)

def train():
    spark = (SparkSession.builder
        .appName("WhaleRadar-ModelTraining")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  [Spark] WhaleRadar ML Model Training")
    print("=" * 60)

    # Generate synthetic data
    print("[spark] Generating 10,000 synthetic training rows...")
    data = generate_training_data(spark)
    data.show(5, truncate=False)
    data.printSchema()
    print(f"[spark] Total rows: {data.count()}")

    # Build pipeline
    label_indexer = StringIndexer(inputCol="label", outputCol="indexed_label")
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    rf = RandomForestClassifier(labelCol="indexed_label", featuresCol="features",
                                numTrees=100, maxDepth=5, seed=42)
    pipeline = Pipeline(stages=[label_indexer, assembler, rf])

    # Split & train
    train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)
    print(f"[spark] Training: {train_df.count()} rows, Test: {test_df.count()} rows")
    print("[spark] Training Random Forest classifier (100 trees)...")
    model = pipeline.fit(train_df)

    # Evaluate
    predictions = model.transform(test_df)
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexed_label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    # Model details
    rf_model = model.stages[-1]
    print("\n" + "=" * 60)
    print(f"  MODEL RESULTS")
    print(f"  Accuracy:            {accuracy:.4f}")
    print(f"  Number of trees:     {rf_model.getNumTrees}")
    print(f"  Feature importances: {rf_model.featureImportances}")
    print(f"  Total nodes:         {rf_model.totalNumNodes}")
    print("=" * 60)

    # Show predictions sample
    print("\n[spark] Sample predictions:")
    predictions.select("symbol", "label", "prediction", "probability", "quote_volume", "price_momentum").show(10, truncate=False)

    # Save model
    model.write().overwrite().save(MODEL_DIR)
    print(f"[spark] Model saved to {MODEL_DIR}")
    print("[spark] Training complete!")

    spark.stop()

if __name__ == "__main__":
    train()

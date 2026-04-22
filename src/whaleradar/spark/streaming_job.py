"""Spark Structured Streaming job for real-time trade processing.

Reads from Kafka `raw-trades`, classifies trades by size, computes
technical indicators, runs ML inference, and writes whale detections
to Kafka `whale-detections` + HDFS.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from whaleradar.config import settings
from whaleradar.kafka.topics import TOPIC_RAW_TRADES, TOPIC_WHALE_DETECTIONS

TRADE_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", FloatType(), True),
    StructField("quote_volume", FloatType(), True),
    StructField("trade_time", TimestampType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
])


def classify_trade(quote_volume: float) -> str:
    """Classify trade by USD size."""
    if quote_volume >= settings.whale_threshold_usd:
        return "whale"
    if quote_volume >= settings.medium_threshold_usd:
        return "medium"
    return "retail"


classify_trade_udf = F.udf(classify_trade, StringType())


def build_spark_session() -> SparkSession:
    """Create a Spark session configured for Kafka + HDFS."""
    return (
        SparkSession.builder
        .appName("WhaleRadar-Streaming")
        .master(settings.spark_master)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation",
                f"{settings.hdfs_namenode}/whaleradar/checkpoints/streaming")
        .config("spark.streaming.kafka.maxRatePerPartition", "1000")
        .getOrCreate()
    )


def run_streaming_job() -> None:
    """Main Spark streaming pipeline."""
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ─────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", TOPIC_RAW_TRADES)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON values ───────────────────────────────────
    trades = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("trade"))
        .select("trade.*")
    )

    # ── Classify trades ─────────────────────────────────────
    classified = trades.withColumn(
        "classification",
        classify_trade_udf(F.col("quote_volume")),
    )

    # ── Windowed aggregations (5-minute windows) ────────────
    windowed = (
        classified
        .withWatermark("trade_time", "1 minute")
        .groupBy(
            F.window("trade_time", "5 minutes", "1 minute"),
            "symbol",
        )
        .agg(
            F.sum("quote_volume").alias("total_volume"),
            F.avg("price").alias("avg_price"),
            F.count("*").alias("trade_count"),
            F.sum(F.when(F.col("classification") == "whale", 1).otherwise(0)).alias("whale_count"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.first("price").alias("open"),
        )
        .withColumn("price_momentum", (F.col("close") - F.col("open")) / F.col("open") * 100)
        .withColumn(
            "volume_spike",
            F.when(F.col("total_volume") > 0, F.col("total_volume")).otherwise(0),
        )
    )

    # ── Filter whale detections ─────────────────────────────
    whale_detections = (
        classified
        .filter(F.col("classification") == "whale")
        .withColumn("detected_at", F.current_timestamp())
        .withColumn("ml_dump_probability", F.lit(0.5))  # placeholder — replaced by ML model
    )

    # ── Write whale detections to Kafka ─────────────────────
    kafka_query = (
        whale_detections
        .select(
            F.col("symbol").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("topic", TOPIC_WHALE_DETECTIONS)
        .option("checkpointLocation",
                f"{settings.hdfs_namenode}/whaleradar/checkpoints/whale_detections")
        .outputMode("append")
        .start()
    )

    # ── Write windowed aggregations to HDFS (parquet) ───────
    hdfs_query = (
        windowed
        .writeStream
        .format("parquet")
        .option("path", f"{settings.hdfs_namenode}/whaleradar/aggregations")
        .option("checkpointLocation",
                f"{settings.hdfs_namenode}/whaleradar/checkpoints/aggregations")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Console output for debugging ────────────────────────
    console_query = (
        whale_detections
        .writeStream
        .format("console")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[spark] Streaming job started. Waiting for termination...")
    spark.streams.awaitAnyTermination()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka streaming ingestion to the Bronze Kafka validation table.

This job intentionally writes to bronze_sensor_kafka instead of the current
bronze_sensor_raw table, so the existing offline historical data remains
untouched while the realtime Kafka path is verified.
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_date,
    coalesce, regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

DEFAULT_KAFKA_BOOTSTRAP = "192.168.1.87:9092,192.168.1.19:9092"
DEFAULT_OUTPUT_PATH = "hdfs://node1:9000/lake/stream/bronze/bronze_streaming"
DEFAULT_CHECKPOINT_PATH = "hdfs://node1:9000/checkpoints/stream/kafka_to_bronze_streaming"


def create_spark_session(master: str):
    """Create Spark Session."""
    spark = (
        SparkSession.builder
        .appName("Kafka_Streaming_to_Bronze")
        .master(master)
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_args():
    parser = argparse.ArgumentParser(description="Stream Kafka sensor data to Bronze Delta table")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--subscribe-pattern",
        default=os.getenv("KAFKA_SUBSCRIBE_PATTERN", "sensor_.*"),
        help="Kafka topic subscribePattern",
    )
    parser.add_argument(
        "--output-path",
        default=os.getenv("BRONZE_KAFKA_OUTPUT_PATH", DEFAULT_OUTPUT_PATH),
        help="Delta output path",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=os.getenv("BRONZE_KAFKA_CHECKPOINT_PATH", DEFAULT_CHECKPOINT_PATH),
        help="Structured Streaming checkpoint path",
    )
    parser.add_argument(
        "--master",
        default=os.getenv("SPARK_MASTER", "spark://node2:7077"),
        help="Spark master URL",
    )
    parser.add_argument(
        "--trigger",
        default=os.getenv("STREAM_TRIGGER", "10 seconds"),
        help="Processing time trigger interval",
    )
    parser.add_argument(
        "--starting-offsets",
        default=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
        choices=["earliest", "latest"],
        help="Kafka starting offsets",
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000")),
        help="Maximum Kafka records consumed per micro-batch",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark_session(args.master)

    print("=" * 60)
    print("Kafka streaming ingestion started")
    print("=" * 60)
    print(f"Spark master: {args.master}")
    print(f"Kafka bootstrap: {args.kafka_bootstrap}")
    print(f"Subscribe pattern: {args.subscribe_pattern}")
    print(f"Output path: {args.output_path}")
    print(f"Checkpoint path: {args.checkpoint_path}")

    # Expected message:
    # {"sensor_id": "10LDSCS_T", "label": "...", "timestamp": "...",
    #  "value": 8.8, "is_simulated": false, "push_time": "..."}
    json_schema = StructType([
        StructField("sensor_id", StringType(), False),
        StructField("label", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("is_simulated", BooleanType(), True),
        StructField("push_time", StringType(), True)
    ])

    # 从 Kafka 读取流数据
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribePattern", args.subscribe_pattern)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Keep the Bronze Kafka validation schema compatible with the historical
    # bronze_sensor_raw schema currently used by downstream jobs.
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_str",
                    "CAST(key AS STRING) as kafka_key",
                    "topic as source_topic",
                    "partition as partition_id",
                    "offset as offset_id",
                    "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_str"), json_schema).alias("data"),
            col("kafka_key"),
            col("source_topic"),
            col("partition_id"),
            col("offset_id"),
            col("kafka_timestamp")
        )
        .select(
            col("data.timestamp").alias("timestamp"),
            col("data.value").alias("value"),
            coalesce(
                col("data.sensor_id"),
                col("kafka_key"),
                regexp_replace(col("source_topic"), "^sensor_", "")
            ).alias("sensor_id"),
            col("data.label").alias("label"),
            col("data.push_time").cast(TimestampType()).alias("push_time"),
            col("data.is_simulated").alias("is_simulated"),
            col("source_topic"),
            col("partition_id"),
            col("offset_id"),
            current_timestamp().alias("ingest_time")
        )
        .withColumn("dt", to_date(col("timestamp")))
        .select(
            "timestamp",
            "value",
            "sensor_id",
            "label",
            "source_topic",
            "partition_id",
            "offset_id",
            "is_simulated",
            "push_time",
            "ingest_time",
            "dt",
        )
    )

    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .option("path", args.output_path)
        .partitionBy("dt")
        .trigger(processingTime=args.trigger)
        .start()
    )

    print("Streaming query is running")
    print("=" * 60)
    print(f"Query ID: {query.id}")
    print(f"Status: {query.status}")
    print("=" * 60)

    query.awaitTermination()


if __name__ == "__main__":
    main()

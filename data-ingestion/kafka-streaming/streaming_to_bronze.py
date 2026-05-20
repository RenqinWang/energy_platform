#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 流式数据接入到 Bronze 层
使用 Spark Structured Streaming 订阅 Kafka topic，写入 bronze_sensor_raw 表
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_date,
    lit, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Kafka_Streaming_to_Bronze")
        .master("spark://node2:7077")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = create_spark_session()

    print("=" * 60)
    print("Kafka 流式数据接入启动")
    print("=" * 60)

    # 定义 Kafka 消息的 JSON Schema
    # 预期格式: {"sensor_id": "S_1LDSCS_T", "label": "1#站1#冷机冷冻水出水温度",
    #            "timestamp": "2018-02-01T00:04:47Z", "value": 8.8,
    #            "is_simulated": true, "push_time": "2026-05-20T12:00:00Z"}
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
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribePattern", "sensor_.*")  # 订阅所有 sensor_ 开头的 topic
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    print("✅ 已连接到 Kafka，订阅模式: sensor_.*")

    # 解析 JSON 消息并添加元数据
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_str",
                    "topic as source_topic",
                    "partition as partition_id",
                    "offset as offset_id",
                    "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_str"), json_schema).alias("data"),
            col("source_topic"),
            col("partition_id"),
            col("offset_id"),
            col("kafka_timestamp")
        )
        .select(
            col("data.sensor_id").alias("sensor_id"),
            col("data.label").alias("label"),
            col("data.timestamp").cast(TimestampType()).alias("event_time"),
            col("data.value").alias("value"),
            col("data.is_simulated").alias("is_simulated"),
            col("data.push_time").cast(TimestampType()).alias("push_time"),
            col("source_topic"),
            col("partition_id"),
            col("offset_id"),
            current_timestamp().alias("ingest_time")
        )
        .withColumn("dt", to_date(col("event_time")))  # 分区字段
    )

    # 定义输出路径
    output_path = "hdfs://node1:9000/lake/bronze/bronze_sensor_raw"
    checkpoint_path = "hdfs://node1:9000/checkpoints/kafka_to_bronze"

    print(f"📁 输出路径: {output_path}")
    print(f"📁 Checkpoint 路径: {checkpoint_path}")
    print("⏳ 开始流式写入...")

    # 写入 Delta Lake（使用 append 模式）
    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("dt")
        .trigger(processingTime="1 minute")  # 每分钟触发一次微批处理
        .start()
    )

    print("✅ 流式任务已启动")
    print("=" * 60)
    print("监控信息：")
    print(f"  - 任务ID: {query.id}")
    print(f"  - 任务名称: {query.name}")
    print(f"  - 状态: {query.status}")
    print("=" * 60)
    print("\n按 Ctrl+C 停止任务...")

    # 等待流式任务终止
    query.awaitTermination()

if __name__ == "__main__":
    main()

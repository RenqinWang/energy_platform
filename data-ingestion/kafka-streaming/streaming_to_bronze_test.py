#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 流式数据接入到 Bronze 层 - 测试版本
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
        .appName("Kafka_Streaming_to_Bronze_Test")
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
    print("Kafka 流式数据接入测试启动")
    print("=" * 60)

    # 定义 Kafka 消息的 JSON Schema
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
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribePattern", "sensor_.*")
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
        .withColumn("dt", to_date(col("event_time")))
    )

    # 定义输出路径
    output_path = "hdfs://node1:9000/lake/bronze/bronze_sensor_raw"
    checkpoint_path = "hdfs://node1:9000/checkpoints/kafka_to_bronze"

    print(f"📁 输出路径: {output_path}")
    print(f"📁 Checkpoint 路径: {checkpoint_path}")
    print("⏳ 开始流式写入（测试模式：运行5分钟后自动停止）...")

    # 写入 Delta Lake
    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("dt")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("✅ 流式任务已启动")
    print("=" * 60)
    print("监控信息：")
    print(f"  - 任务ID: {query.id}")
    print(f"  - 状态: {query.status}")
    print("=" * 60)

    # 测试模式：运行5分钟后停止
    import time
    timeout = 300  # 5分钟
    start_time = time.time()

    while time.time() - start_time < timeout:
        time.sleep(10)
        if not query.isActive:
            print("⚠️  流式任务已停止")
            break

        # 显示进度
        progress = query.lastProgress
        if progress:
            print(f"📊 进度: 已处理 {progress.get('numInputRows', 0)} 行")

    print("\n⏰ 测试时间到，停止流式任务...")
    query.stop()

    # 验证写入结果
    print("\n🔍 验证写入结果：")
    try:
        df_read = spark.read.format("delta").load(output_path)
        count = df_read.count()
        print(f"   ✅ Bronze 层总记录数: {count}")
        print(f"   ✅ 字段列表: {df_read.columns}")
        print("\n📊 数据样例：")
        df_read.show(5, truncate=False)
    except Exception as e:
        print(f"   ⚠️  读取失败: {e}")

    spark.stop()
    print("\n✅ 测试完成！")

if __name__ == "__main__":
    main()

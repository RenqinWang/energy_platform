#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 流式数据接入到 Bronze 层 - 快速测试版本
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
import time

# 创建 Spark Session
spark = (
    SparkSession.builder
    .appName("Kafka_to_Bronze_Quick_Test")
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

print("=" * 60)
print("✅ Spark Session 已创建")
print("=" * 60)

# JSON Schema
json_schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("label", StringType(), True),
    StructField("timestamp", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("is_simulated", BooleanType(), True),
    StructField("push_time", StringType(), True)
])

# 从 Kafka 读取（只订阅3个topic进行测试）
print("📡 连接 Kafka...")
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "sensor_1LDSCS_T,sensor_2LDSCS_T,sensor_3LDSCS_T")  # 只订阅3个topic
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

print("✅ 已连接到 Kafka")

# 解析数据
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json_str",
                "topic as source_topic",
                "partition as partition_id",
                "offset as offset_id")
    .select(
        from_json(col("json_str"), json_schema).alias("data"),
        col("source_topic"),
        col("partition_id"),
        col("offset_id")
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

# 输出路径
output_path = "hdfs://node1:9000/lake/bronze/bronze_sensor_raw"
checkpoint_path = "hdfs://node1:9000/checkpoints/kafka_to_bronze_quick"

print(f"📁 输出: {output_path}")
print(f"📁 Checkpoint: {checkpoint_path}")
print("⏳ 启动流式写入...")

# 启动流式任务
query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("path", output_path)
    .partitionBy("dt")
    .trigger(processingTime="10 seconds")  # 每10秒触发一次
    .start()
)

print("✅ 流式任务已启动")
print(f"   任务ID: {query.id}")

# 运行2分钟
timeout = 120
start_time = time.time()
batch_count = 0

while time.time() - start_time < timeout:
    time.sleep(10)
    if not query.isActive:
        print("⚠️  任务已停止")
        break

    progress = query.lastProgress
    if progress:
        batch_count += 1
        num_rows = progress.get('numInputRows', 0)
        print(f"📊 批次 {batch_count}: 处理了 {num_rows} 行")

print("\n⏰ 测试完成，停止任务...")
query.stop()

# 验证结果
print("\n🔍 验证写入结果：")
try:
    df_read = spark.read.format("delta").load(output_path)
    count = df_read.count()
    print(f"   ✅ Bronze 层总记录数: {count}")
    if count > 0:
        print("\n📊 数据样例：")
        df_read.show(5, truncate=False)
except Exception as e:
    print(f"   ⚠️  读取失败: {e}")

spark.stop()
print("\n✅ 测试完成！")

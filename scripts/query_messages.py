#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询Bronze层的Kafka消息
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Query_Kafka_Messages")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = create_spark_session()

    # 读取Bronze层数据
    df = spark.read.format("delta").load("hdfs://node1:9000/lake/bronze/bronze_sensor_raw")

    print("=" * 80)
    print("查询选项:")
    print("1. 查看特定传感器的消息")
    print("2. 查看特定日期的消息")
    print("3. 查看最新的N条消息")
    print("4. 查看特定时间范围的消息")
    print("=" * 80)

    # 示例1: 查看特定传感器的最新10条消息
    print("\n示例1: 传感器 10LDSCS_T 的最新10条消息")
    df.filter(col("sensor_id") == "10LDSCS_T") \
      .orderBy(col("event_time").desc()) \
      .select("sensor_id", "label", "event_time", "value", "source_topic") \
      .show(10, truncate=False)

    # 示例2: 查看2018-01-01的所有消息
    print("\n示例2: 2018-01-01的所有消息（前10条）")
    df.filter(col("dt") == "2018-01-01") \
      .orderBy("event_time") \
      .select("sensor_id", "label", "event_time", "value") \
      .show(10, truncate=False)

    # 示例3: 查看最新的20条消息
    print("\n示例3: 最新的20条消息")
    df.orderBy(col("event_time").desc()) \
      .select("sensor_id", "label", "event_time", "value", "ingest_time") \
      .show(20, truncate=False)

    # 示例4: 查看特定时间范围
    print("\n示例4: 2018-01-01 00:00:00 到 2018-01-01 01:00:00 的消息")
    df.filter((col("event_time") >= "2018-01-01 00:00:00") &
              (col("event_time") <= "2018-01-01 01:00:00")) \
      .orderBy("event_time") \
      .select("sensor_id", "label", "event_time", "value") \
      .show(20, truncate=False)

    # 示例5: 查看完整的消息结构（包括Kafka元数据）
    print("\n示例5: 完整消息结构（1条）")
    df.limit(1).show(1, truncate=False, vertical=True)

    # 统计信息
    print("\n" + "=" * 80)
    print("数据统计:")
    print(f"总消息数: {df.count():,}")
    print(f"传感器数: {df.select('sensor_id').distinct().count()}")
    print(f"日期范围: {df.agg({'dt': 'min'}).collect()[0][0]} 到 {df.agg({'dt': 'max'}).collect()[0][0]}")
    print("=" * 80)

    spark.stop()

if __name__ == "__main__":
    main()

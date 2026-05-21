#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加载历史数据到Bronze层
从/home/student/作业2数据/positions/目录加载txt文件
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
from datetime import datetime

def create_spark_session():
    """创建Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Load_Historical_Data")
        .master("spark://node2:7077")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = create_spark_session()

    print("=" * 60)
    print("加载历史数据到Bronze层")
    print("=" * 60)

    # 数据目录
    data_dir = "/home/student/作业2数据/positions/"
    bronze_path = "hdfs://node1:9000/lake/bronze/bronze_sensor_raw"

    # 定义schema
    schema = StructType([
        StructField("timestamp", StringType(), False),
        StructField("value", DoubleType(), False)
    ])

    # 获取所有txt文件
    txt_files = [f for f in os.listdir(data_dir) if f.endswith('.txt')]
    print(f"\n找到 {len(txt_files)} 个数据文件")

    all_data = []

    for i, filename in enumerate(txt_files, 1):  # 加载全部传感器
        sensor_id = filename.replace('.txt', '')
        file_path = os.path.join(data_dir, filename)

        print(f"[{i}/{len(txt_files)}] 加载 {sensor_id}...")

        try:
            # 读取文件
            df = spark.read.csv(
                file_path,
                schema=schema,
                sep="\t",
                header=False
            )

            # 添加元数据
            df = df.withColumn("sensor_id", lit(sensor_id)) \
                   .withColumn("label", lit(f"传感器_{sensor_id}")) \
                   .withColumn("source_topic", lit(f"sensor_{sensor_id}")) \
                   .withColumn("partition_id", lit(0)) \
                   .withColumn("offset_id", lit(0)) \
                   .withColumn("is_simulated", lit(False)) \
                   .withColumn("push_time", lit(None).cast(StringType())) \
                   .withColumn("ingest_time", current_timestamp()) \
                   .withColumn("dt", to_date(col("timestamp")))

            all_data.append(df)

        except Exception as e:
            print(f"  ⚠️  加载失败: {e}")
            continue

    if not all_data:
        print("\n❌ 没有成功加载任何数据")
        return

    # 合并所有数据
    print(f"\n合并 {len(all_data)} 个数据集...")
    combined_df = all_data[0]
    for df in all_data[1:]:
        combined_df = combined_df.union(df)

    total_records = combined_df.count()
    print(f"总记录数: {total_records:,}")

    # 写入Delta Lake
    print(f"\n写入Bronze层: {bronze_path}")
    combined_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("dt") \
        .save(bronze_path)

    print("\n✅ 历史数据加载完成！")

    # 显示统计信息
    print("\n" + "=" * 60)
    print("数据统计")
    print("=" * 60)

    stats_df = spark.read.format("delta").load(bronze_path)
    print(f"Bronze层总记录数: {stats_df.count():,}")
    print(f"传感器数量: {stats_df.select('sensor_id').distinct().count()}")
    print(f"日期范围: {stats_df.select('dt').distinct().count()} 天")

    spark.stop()

if __name__ == "__main__":
    main()

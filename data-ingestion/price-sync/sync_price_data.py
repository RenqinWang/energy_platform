#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
能源价格数据同步脚本
从 HTTP 接口获取价格数据（全量+增量），写入 bronze_price_raw 表
"""

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit, current_timestamp, to_date, date_format

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Price_Data_Sync")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def fetch_full_price_data(api_url):
    """
    从 HTTP 接口获取全量价格数据

    Args:
        api_url: API 地址

    Returns:
        list: 价格数据列表
    """
    try:
        response = requests.get(f"{api_url}/full", timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"✅ 成功获取全量价格数据: {len(data)} 条记录")
        return data
    except Exception as e:
        print(f"❌ 获取全量价格数据失败: {e}")
        return []

def fetch_incremental_price_data(api_url, last_sync_time=None):
    """
    从 HTTP 接口获取增量价格数据

    Args:
        api_url: API 地址
        last_sync_time: 上次同步时间（可选）

    Returns:
        list: 价格数据列表
    """
    try:
        # 尝试不同的增量接口
        endpoints = [
            "/",
            "/incremental",
            "/binlog"
        ]

        for endpoint in endpoints:
            try:
                url = f"{api_url}{endpoint}"
                if last_sync_time:
                    url += f"?since={last_sync_time}"

                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        print(f"✅ 成功获取增量价格数据: {len(data)} 条记录 (接口: {endpoint})")
                        return data
            except:
                continue

        print("⚠️  未找到可用的增量接口，将使用全量接口")
        return []
    except Exception as e:
        print(f"❌ 获取增量价格数据失败: {e}")
        return []

def write_to_bronze(spark, data, source_type="full"):
    """
    将价格数据写入 Bronze 层

    Args:
        spark: SparkSession
        data: 价格数据列表
        source_type: 数据源类型 (full/incremental)
    """
    if not data:
        print("⚠️  没有数据需要写入")
        return

    # 定义 Schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("station_code", StringType(), False),
        StructField("price_type", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])

    # 创建 DataFrame
    df = spark.createDataFrame(data, schema)

    # 转换时间字段为 TimestampType
    df = df.withColumn("created_at", col("created_at").cast(TimestampType())) \
           .withColumn("updated_at", col("updated_at").cast(TimestampType()))

    # 添加元数据字段
    df = df.withColumn("source_type", lit(source_type)) \
           .withColumn("ingest_time", current_timestamp()) \
           .withColumn("price_year_month", date_format(col("updated_at"), "yyyyMM"))

    # 输出路径
    output_path = "hdfs://node1:9000/lake/bronze/bronze_price_raw"

    print(f"\n📊 数据预览：")
    df.show(5, truncate=False)

    print(f"\n💾 写入 Bronze 层: {output_path}")
    print(f"   数据源类型: {source_type}")
    print(f"   记录数: {df.count()}")

    # 写入 Delta Lake（使用 merge 模式避免重复）
    try:
        # 先尝试读取已有数据
        existing_df = spark.read.format("delta").load(output_path)
        print(f"   已有记录数: {existing_df.count()}")

        # 使用 merge 模式更新
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, output_path)

        # 合并逻辑：基于 id 和 updated_at 去重
        delta_table.alias("target").merge(
            df.alias("source"),
            "target.id = source.id AND target.updated_at = source.updated_at"
        ).whenNotMatchedInsertAll().execute()

        print("✅ 数据已合并到 Bronze 层（去重）")

    except Exception as e:
        # 如果表不存在，直接写入
        if "Path does not exist" in str(e) or "is not a Delta table" in str(e):
            print("   表不存在，创建新表...")
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("price_year_month") \
                .save(output_path)
            print("✅ 数据已写入 Bronze 层（新建表）")
        else:
            print(f"❌ 写入失败: {e}")
            raise

def verify_data(spark, output_path):
    """验证写入的数据"""
    try:
        df = spark.read.format("delta").load(output_path)
        count = df.count()

        print("\n🔍 验证写入结果：")
        print(f"   总记录数: {count}")
        print(f"   字段列表: {df.columns}")

        print("\n📈 按站点和价格类型统计：")
        df.groupBy("station_code", "price_type").count().orderBy("station_code", "price_type").show()

        print("\n📊 最新数据样例：")
        df.orderBy(col("updated_at").desc()).show(10, truncate=False)

        return True
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return False

def main():
    print("=" * 60)
    print("能源价格数据同步任务启动")
    print("=" * 60)

    # 配置
    api_url = "http://122.9.74.124:8000"
    output_path = "hdfs://node1:9000/lake/bronze/bronze_price_raw"

    # 创建 Spark Session
    spark = create_spark_session()

    # 1. 获取全量价格数据
    print("\n📥 步骤 1: 获取全量价格数据...")
    full_data = fetch_full_price_data(api_url)

    if full_data:
        # 写入 Bronze 层
        write_to_bronze(spark, full_data, source_type="full")

    # 2. 尝试获取增量数据（可选）
    print("\n📥 步骤 2: 尝试获取增量价格数据...")
    incremental_data = fetch_incremental_price_data(api_url)

    if incremental_data:
        write_to_bronze(spark, incremental_data, source_type="incremental")
    else:
        print("⚠️  增量接口不可用，跳过增量同步")

    # 3. 验证数据
    print("\n🔍 步骤 3: 验证写入结果...")
    verify_data(spark, output_path)

    spark.stop()

    print("\n" + "=" * 60)
    print("✅ 价格数据同步任务完成！")
    print("=" * 60)

if __name__ == "__main__":
    main()

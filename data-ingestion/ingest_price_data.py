#!/usr/bin/env python3
"""
价格数据采集脚本 - 从 HTTP API 获取数据并保存到 Delta Lake Bronze 层
"""
import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, LongType
from pyspark.sql.functions import lit, current_timestamp

# API 配置
API_BASE_URL = "http://122.9.74.124:8000"
HDFS_NAMENODE = "hdfs://node1:9000"
BRONZE_PATH = f"{HDFS_NAMENODE}/lake/bronze/bronze_price_raw"

def create_spark_session():
    """创建 Spark Session"""
    spark = SparkSession.builder \
        .appName("PriceDataIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def fetch_price_data():
    """从 API 获取全量价格数据"""
    print(f"正在从 {API_BASE_URL}/full 获取价格数据...")

    try:
        response = requests.get(f"{API_BASE_URL}/full", timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"成功获取 {len(data)} 条价格记录")
        return data
    except Exception as e:
        print(f"获取数据失败: {e}")
        raise

def save_to_bronze(spark, data):
    """保存数据到 Bronze 层"""
    print(f"正在保存数据到 Bronze 层: {BRONZE_PATH}")

    # 定义 Schema（使用 DoubleType 而不是 DecimalType）
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("station_code", StringType(), True),
        StructField("price_type", StringType(), True),
        StructField("price", StringType(), True),  # 先作为字符串读取
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    # 创建 DataFrame
    df = spark.createDataFrame(data, schema)

    # 转换 price 为 double 类型
    from pyspark.sql.functions import col
    df = df.withColumn("price", col("price").cast("double"))

    # 添加元数据字段
    df = df.withColumn("ingest_time", current_timestamp()) \
           .withColumn("source", lit("http_api")) \
           .withColumn("api_endpoint", lit(f"{API_BASE_URL}/full"))

    # 添加分区字段（使用 updated_at 的日期部分）
    from pyspark.sql.functions import to_date
    df = df.withColumn("dt", to_date(col("updated_at")))

    # 显示数据样例
    print("\n数据样例:")
    df.show(5, truncate=False)

    # 保存到 Delta Lake（覆盖模式）
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("dt") \
        .save(BRONZE_PATH)

    print(f"✅ 成功保存 {df.count()} 条记录到 Bronze 层")

    return df

def verify_data(spark):
    """验证保存的数据"""
    print("\n正在验证 Bronze 层数据...")

    df = spark.read.format("delta").load(BRONZE_PATH)

    print(f"\n总记录数: {df.count()}")
    print("\n按站点和价格类型统计:")
    df.groupBy("station_code", "price_type").count().orderBy("station_code", "price_type").show()

    print("\n最新价格数据:")
    df.select("station_code", "price_type", "price", "updated_at").show(15, truncate=False)

def main():
    """主函数"""
    print("=" * 60)
    print("价格数据采集任务开始")
    print("=" * 60)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 创建 Spark Session
    spark = create_spark_session()

    try:
        # 1. 获取数据
        data = fetch_price_data()

        # 2. 保存到 Bronze 层
        df = save_to_bronze(spark, data)

        # 3. 验证数据
        verify_data(spark)

        print("\n" + "=" * 60)
        print("✅ 价格数据采集任务完成")
        print("=" * 60)
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"\n❌ 任务失败: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

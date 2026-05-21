#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
价格维表生成脚本
从 bronze_price_raw 读取原始价格数据，标准化字段，去重，生成 silver_price_dim 表
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, date_format, row_number, current_timestamp
)
from pyspark.sql.window import Window

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Generate_Price_Dim")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    print("=" * 80)
    print("价格维表生成任务启动")
    print("=" * 80)

    spark = create_spark_session()

    # 1. 读取Bronze层价格数据
    print("\n📥 步骤 1: 读取 Bronze 层价格数据...")
    bronze_path = "hdfs://node1:9000/lake/bronze/bronze_price_raw"
    df_bronze = spark.read.format("delta").load(bronze_path)

    bronze_count = df_bronze.count()
    print(f"   ✅ 读取成功: {bronze_count} 条记录")

    # 2. 数据预览
    print("\n📊 原始数据预览:")
    df_bronze.show(10, truncate=False)

    # 3. 标准化字段
    print("\n🔄 步骤 2: 标准化字段...")

    # 添加 effective_date 字段（从 updated_at 提取日期）
    df_price = df_bronze.withColumn(
        "effective_date",
        to_date(col("updated_at"))
    )

    # 4. 去重：同一 station_code + price_type + effective_date 保留最新 updated_at
    print("\n🔧 步骤 3: 去重处理...")

    # 定义窗口：按 station_code, price_type, effective_date 分区，按 updated_at 降序排序
    window_spec = Window.partitionBy(
        "station_code", "price_type", "effective_date"
    ).orderBy(col("updated_at").desc())

    # 添加行号
    df_price = df_price.withColumn("row_num", row_number().over(window_spec))

    # 只保留每组的第一条记录（最新的）
    df_price = df_price.filter(col("row_num") == 1).drop("row_num")

    dedup_count = df_price.count()
    print(f"   ✅ 去重后记录数: {dedup_count}")

    # 5. 选择最终字段并添加派生字段
    print("\n📋 步骤 4: 选择最终字段...")

    # 添加 price_year_month 字段（格式：YYYY-MM）
    df_price = df_price.withColumn(
        "price_year_month",
        date_format(col("effective_date"), "yyyy-MM")
    )

    df_price = df_price.select(
        "station_code",
        "price_type",
        "price",
        "effective_date",
        "updated_at",
        col("source").alias("source_type"),  # 重命名 source 为 source_type
        "price_year_month"
    )

    # 添加元数据字段
    df_price = df_price.withColumn("created_at", current_timestamp())

    # 6. 数据预览
    print("\n📊 标准化后数据预览:")
    df_price.orderBy("station_code", "price_type").show(20, truncate=False)

    print("\n📈 按站点和价格类型统计:")
    df_price.groupBy("station_code", "price_type").count().orderBy("station_code", "price_type").show()

    print("\n📈 按有效日期统计:")
    df_price.groupBy("effective_date").count().orderBy("effective_date").show()

    # 7. 写入Silver层
    output_path = "hdfs://node1:9000/lake/silver/silver_price_dim"
    print(f"\n💾 步骤 5: 写入 Silver 层: {output_path}")

    df_price.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("price_year_month") \
        .save(output_path)

    print("   ✅ 数据已写入 Silver 层")

    # 8. 验证写入结果
    print("\n🔍 步骤 6: 验证写入结果...")
    df_verify = spark.read.format("delta").load(output_path)
    verify_count = df_verify.count()

    print(f"   总记录数: {verify_count}")
    print(f"   字段列表: {df_verify.columns}")

    print("\n📊 验证数据样例:")
    df_verify.orderBy("station_code", "price_type").show(20, truncate=False)

    spark.stop()

    print("\n" + "=" * 80)
    print("✅ 价格维表生成任务完成！")
    print("=" * 80)

if __name__ == "__main__":
    main()

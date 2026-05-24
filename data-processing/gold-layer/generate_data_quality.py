#!/usr/bin/env python3
"""
数据质量评分系统
为每个设备的每日数据计算质量评分
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as _sum, avg, lit, current_timestamp,
    to_date, isnan, isnull, expr
)
from pyspark.sql.window import Window
import sys

def create_spark_session():
    """创建Spark会话"""
    spark = SparkSession.builder \
        .appName("GenerateDataQuality") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def calculate_data_quality(spark):
    """计算数据质量评分"""

    print("=" * 80)
    print("开始计算数据质量评分")
    print("=" * 80)

    # 读取小时供能曲线数据
    print("\n1. 读取 gold_supply_curve_hourly 数据...")
    hourly_df = spark.read.format("delta").load("hdfs://node1:9000/lake/gold/gold_supply_curve_hourly")
    print(f"   总记录数: {hourly_df.count()}")

    # 添加日期字段
    print("\n2. 添加日期字段...")
    hourly_with_date = hourly_df.withColumn("stat_date", to_date(col("stat_hour")))

    # 按设备和日期分组，计算质量指标
    print("\n3. 计算数据质量指标...")

    # 定义关键字段
    key_fields = [
        "avg_supply_temp",
        "avg_power",
        "energy_consumption_kwh",
        "cooling_capacity_kw",
        "cooling_supply_kwh"
    ]

    # 计算每个字段的缺失情况
    quality_metrics = hourly_with_date.groupBy("station_id", "equipment_id", "stat_date").agg(
        # 总记录数
        count("*").alias("total_records"),

        # 预期记录数（24小时）
        lit(24).alias("expected_records"),

        # 各字段的有效记录数。功率 / 能耗 / 供冷只在设备运行时强制要求；
        # 非运行小时的 0 能耗是合理数据，不再按缺失处理。
        count(when(col("avg_supply_temp").isNotNull(), 1)).alias("valid_supply_temp"),
        count(when((col("runtime_hours") <= 0) | col("avg_power").isNotNull(), 1)).alias("valid_power"),
        count(when((col("runtime_hours") <= 0) | col("energy_consumption_kwh").isNotNull(), 1)).alias("valid_energy"),
        count(when((col("runtime_hours") <= 0) | col("cooling_capacity_kw").isNotNull(), 1)).alias("valid_cooling_capacity"),
        count(when((col("runtime_hours") <= 0) | col("cooling_supply_kwh").isNotNull(), 1)).alias("valid_cooling_supply"),
        count(when(col("runtime_hours") > 0, 1)).alias("running_records"),
        count(when((col("runtime_hours") > 0) & col("avg_power").isNull(), 1)).alias("missing_running_power_records"),
        count(when((col("runtime_hours") > 0) & col("energy_consumption_kwh").isNull(), 1)).alias("missing_running_energy_records"),
        count(when((col("runtime_hours") > 0) & col("cooling_supply_kwh").isNull(), 1)).alias("missing_running_cooling_records"),

        # 各字段的零值记录数
        count(when(col("avg_supply_temp") == 0, 1)).alias("zero_supply_temp"),
        count(when(col("avg_power") == 0, 1)).alias("zero_power"),
        count(when(col("energy_consumption_kwh") == 0, 1)).alias("zero_energy"),
        count(when(col("cooling_supply_kwh") == 0, 1)).alias("zero_cooling_supply"),

        # 运行时长
        _sum("run_minutes").alias("total_run_minutes")
    )

    # 计算质量评分
    print("\n4. 计算质量评分...")
    quality_scores = quality_metrics \
        .withColumn("completeness_rate", col("total_records") / col("expected_records")) \
        .withColumn("supply_temp_valid_rate", col("valid_supply_temp") / col("total_records")) \
        .withColumn("power_valid_rate", col("valid_power") / col("total_records")) \
        .withColumn("energy_valid_rate", col("valid_energy") / col("total_records")) \
        .withColumn("cooling_capacity_valid_rate", col("valid_cooling_capacity") / col("total_records")) \
        .withColumn("cooling_supply_valid_rate", col("valid_cooling_supply") / col("total_records")) \
        .withColumn("avg_field_valid_rate",
                   (col("supply_temp_valid_rate") +
                    col("power_valid_rate") +
                    col("energy_valid_rate") +
                    col("cooling_capacity_valid_rate") +
                    col("cooling_supply_valid_rate")) / 5) \
        .withColumn("data_quality_score",
                   (col("completeness_rate") * 0.4 + col("avg_field_valid_rate") * 0.6) * 100) \
        .withColumn("quality_flag",
                   when(col("data_quality_score") >= 80, "good")
                   .when(col("data_quality_score") >= 60, "warning")
                   .otherwise("poor")) \
        .withColumn("missing_records", col("expected_records") - col("total_records")) \
        .withColumn("null_field_count",
                   (col("total_records") - col("valid_supply_temp")) +
                   (col("total_records") - col("valid_power")) +
                   (col("total_records") - col("valid_energy")) +
                   (col("total_records") - col("valid_cooling_capacity")) +
                   (col("total_records") - col("valid_cooling_supply"))) \
        .withColumn("dt", col("stat_date")) \
        .withColumn("created_at", current_timestamp())

    # 选择最终字段
    quality_final = quality_scores.select(
        "station_id",
        "equipment_id",
        "stat_date",
        "total_records",
        "expected_records",
        "running_records",
        "missing_running_power_records",
        "missing_running_energy_records",
        "missing_running_cooling_records",
        "missing_records",
        "completeness_rate",
        "supply_temp_valid_rate",
        "power_valid_rate",
        "energy_valid_rate",
        "cooling_capacity_valid_rate",
        "cooling_supply_valid_rate",
        "avg_field_valid_rate",
        "null_field_count",
        "zero_supply_temp",
        "zero_power",
        "zero_energy",
        "zero_cooling_supply",
        "total_run_minutes",
        "data_quality_score",
        "quality_flag",
        "dt",
        "created_at"
    )

    # 显示统计信息
    print("\n5. 数据质量统计信息:")
    print(f"   生成质量评分记录数: {quality_final.count()}")

    print("\n   样例数据:")
    quality_final.orderBy(col("stat_date").desc(), "equipment_id").show(10, truncate=False)

    # 按质量等级统计
    print("\n6. 按质量等级统计:")
    quality_final.groupBy("quality_flag").count().orderBy("quality_flag").show()

    # 按设备统计平均质量分
    print("\n7. 按设备统计平均质量分:")
    quality_final.groupBy("equipment_id").agg(
        avg("data_quality_score").alias("avg_quality_score"),
        avg("completeness_rate").alias("avg_completeness_rate"),
        count("*").alias("day_count")
    ).orderBy(col("avg_quality_score").desc()).show(20, truncate=False)

    # 保存到Delta Lake
    print("\n8. 保存数据质量评分到 Delta Lake...")
    output_path = "hdfs://node1:9000/lake/gold/gold_data_quality"

    quality_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("dt") \
        .save(output_path)

    print(f"   ✓ 数据质量评分已保存到: {output_path}")

    return quality_final

def main():
    """主函数"""
    spark = create_spark_session()

    try:
        quality_data = calculate_data_quality(spark)

        print("\n" + "=" * 80)
        print("✓ 数据质量评分计算完成")
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

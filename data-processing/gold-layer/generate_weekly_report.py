#!/usr/bin/env python3
"""
周度报表生成脚本
从 gold_supply_curve_hourly 按周聚合生成周度综合报表
包含峰值、谷值、总供能量、峰值期时长、谷值期时长、设备利用率等指标
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, max as _max, min as _min, count,
    when, date_format, to_date, expr, lit, current_timestamp,
    weekofyear, year, date_sub, dayofweek, coalesce
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

def create_spark_session():
    """创建Spark会话"""
    spark = SparkSession.builder \
        .appName("GenerateWeeklyReport") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def calculate_time_based_price(hour):
    """
    根据小时计算分时电价
    峰时（8-11, 18-23）: 1.2元/kWh
    平时（7-8, 11-18）: 0.8元/kWh
    谷时（23-7）: 0.4元/kWh
    """
    return when((col(hour) >= 8) & (col(hour) < 11), 1.2) \
        .when((col(hour) >= 18) & (col(hour) < 23), 1.2) \
        .when((col(hour) >= 7) & (col(hour) < 8), 0.8) \
        .when((col(hour) >= 11) & (col(hour) < 18), 0.8) \
        .otherwise(0.4)

def generate_weekly_report(spark):
    """生成周度报表"""

    print("=" * 80)
    print("开始生成周度报表")
    print("=" * 80)

    # 读取小时供能曲线数据
    print("\n1. 读取 gold_supply_curve_hourly 数据...")
    hourly_df = spark.read.format("delta").load("hdfs://node1:9000/lake/gold/gold_supply_curve_hourly")

    print(f"   总记录数: {hourly_df.count()}")

    # 添加周相关字段
    print("\n2. 添加周相关字段...")
    hourly_with_week = hourly_df \
        .withColumn("stat_year", year(col("stat_hour"))) \
        .withColumn("stat_week", weekofyear(col("stat_hour"))) \
        .withColumn("week_start_date",
                   date_sub(to_date(col("stat_hour")), dayofweek(col("stat_hour")) - 2)) \
        .withColumn("hour_of_day", expr("hour(stat_hour)")) \
        .withColumn("energy_price", calculate_time_based_price("hour_of_day"))

    # 按设备和周分组聚合
    print("\n3. 按设备和周聚合数据...")

    # 先计算每个设备每周的基础统计
    weekly_agg = hourly_with_week.groupBy(
        "station_id",
        "equipment_id",
        "stat_year",
        "stat_week",
        "week_start_date"
    ).agg(
        # 峰值谷值
        _max("cooling_supply_kwh").alias("peak_cooling_kwh"),
        _min(when(col("cooling_supply_kwh") > 0, col("cooling_supply_kwh"))).alias("valley_cooling_kwh"),

        # 总量指标
        _sum("cooling_supply_kwh").alias("raw_total_cooling_supply_kwh"),
        _sum("energy_consumption_kwh").alias("raw_total_energy_consumption_kwh"),
        _sum("run_minutes").alias("total_run_minutes"),
        count(when((col("runtime_hours") > 0) & col("energy_consumption_kwh").isNull(), 1))
            .alias("missing_running_energy_hours"),
        count(when((col("runtime_hours") > 0) & col("cooling_supply_kwh").isNull(), 1))
            .alias("missing_running_cooling_hours"),

        # 平均指标
        avg("cooling_supply_kwh").alias("avg_cooling_supply_kwh"),
        avg("avg_supply_temp").alias("avg_supply_temp"),

        # 数据质量
        count("*").alias("hour_count"),

        # 经济指标（使用分时电价）
        _sum(col("energy_consumption_kwh") * col("energy_price")).alias("raw_total_energy_cost"),
        _sum(col("cooling_supply_kwh") * lit(0.3)).alias("raw_total_cooling_revenue")
    )

    # 计算派生指标
    print("\n4. 计算派生指标...")
    weekly_report = weekly_agg \
        .withColumn("total_energy_consumption_kwh",
                   when(col("missing_running_energy_hours") > 0, lit(None).cast("double"))
                   .otherwise(coalesce(col("raw_total_energy_consumption_kwh"), lit(0.0)))) \
        .withColumn("total_cooling_supply_kwh",
                   when(col("missing_running_cooling_hours") > 0, lit(None).cast("double"))
                   .otherwise(coalesce(col("raw_total_cooling_supply_kwh"), lit(0.0)))) \
        .withColumn("total_energy_cost",
                   when(col("missing_running_energy_hours") > 0, lit(None).cast("double"))
                   .otherwise(coalesce(col("raw_total_energy_cost"), lit(0.0)))) \
        .withColumn("total_cooling_revenue",
                   when(col("missing_running_cooling_hours") > 0, lit(None).cast("double"))
                   .otherwise(coalesce(col("raw_total_cooling_revenue"), lit(0.0)))) \
        .withColumn("avg_cop",
                   when(col("total_energy_consumption_kwh") > 0,
                        col("total_cooling_supply_kwh") / col("total_energy_consumption_kwh"))) \
        .withColumn("week_end_date", expr("date_add(week_start_date, 6)")) \
        .withColumn("stat_week_str",
                   expr("concat(stat_year, '-W', lpad(stat_week, 2, '0'))")) \
        .withColumn("total_runtime_hours", col("total_run_minutes") / 60.0) \
        .withColumn("equipment_utilization_rate",
                   col("total_run_minutes") / (7 * 24 * 60)) \
        .withColumn("load_factor",
                   when(col("peak_cooling_kwh") > 0,
                        col("avg_cooling_supply_kwh") / col("peak_cooling_kwh"))
                   .otherwise(0)) \
        .withColumn("peak_valley_ratio",
                   when(col("valley_cooling_kwh") > 0,
                        col("peak_cooling_kwh") / col("valley_cooling_kwh"))
                   .otherwise(0)) \
        .withColumn("data_completeness_rate",
                   col("hour_count") / (7 * 24)) \
        .withColumn("net_profit",
                   col("total_cooling_revenue") - col("total_energy_cost")) \
        .withColumn("dt", col("week_start_date")) \
        .withColumn("created_at", current_timestamp())

    # 计算峰值期和谷值期时长
    print("\n5. 计算峰值期和谷值期时长...")

    # 为每个设备每周计算阈值
    window_spec = Window.partitionBy("station_id", "equipment_id", "stat_year", "stat_week")

    hourly_with_threshold = hourly_with_week \
        .withColumn("week_avg", avg("cooling_supply_kwh").over(window_spec)) \
        .withColumn("is_peak_period",
                   when(col("cooling_supply_kwh") > col("week_avg") * 1.2, 1).otherwise(0)) \
        .withColumn("is_valley_period",
                   when((col("cooling_supply_kwh") < col("week_avg") * 0.8) &
                        (col("cooling_supply_kwh") > 0), 1).otherwise(0))

    # 聚合峰谷时长
    peak_valley_duration = hourly_with_threshold.groupBy(
        "station_id", "equipment_id", "stat_year", "stat_week"
    ).agg(
        _sum("is_peak_period").alias("peak_duration_hours"),
        _sum("is_valley_period").alias("valley_duration_hours")
    )

    # 合并峰谷时长到周报表
    weekly_report_final = weekly_report.join(
        peak_valley_duration,
        ["station_id", "equipment_id", "stat_year", "stat_week"],
        "left"
    ).select(
        "station_id",
        "equipment_id",
        "stat_week_str",
        "week_start_date",
        "week_end_date",
        "peak_cooling_kwh",
        "valley_cooling_kwh",
        "peak_valley_ratio",
        "peak_duration_hours",
        "valley_duration_hours",
        "total_cooling_supply_kwh",
        "total_energy_consumption_kwh",
        "total_runtime_hours",
        "avg_cooling_supply_kwh",
        "avg_cop",
        "avg_supply_temp",
        "equipment_utilization_rate",
        "load_factor",
        "total_energy_cost",
        "total_cooling_revenue",
        "net_profit",
        "data_completeness_rate",
        "hour_count",
        "dt",
        "created_at"
    )

    # 显示统计信息
    print("\n6. 周度报表统计信息:")
    print(f"   生成周报表记录数: {weekly_report_final.count()}")

    print("\n   样例数据:")
    weekly_report_final.orderBy(col("week_start_date").desc(), "equipment_id").show(5, truncate=False)

    # 保存到Delta Lake
    print("\n7. 保存周度报表到 Delta Lake...")
    output_path = "hdfs://node1:9000/lake/gold/gold_report_weekly"

    weekly_report_final.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("dt") \
        .save(output_path)

    print(f"   ✓ 周度报表已保存到: {output_path}")

    # 显示按设备统计
    print("\n8. 按设备统计周报表数量:")
    weekly_report_final.groupBy("equipment_id").count() \
        .orderBy(col("count").desc()).show(20, truncate=False)

    return weekly_report_final

def main():
    """主函数"""
    spark = create_spark_session()

    try:
        weekly_report = generate_weekly_report(spark)

        print("\n" + "=" * 80)
        print("✓ 周度报表生成完成")
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

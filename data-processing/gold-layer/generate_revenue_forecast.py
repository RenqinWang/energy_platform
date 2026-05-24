#!/usr/bin/env python3
"""
收益预测生成脚本
基于 gold_forecast_supply 预测数据和能源价格计算收益预测
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, expr, lit, current_timestamp, to_date, hour
)
import sys

def create_spark_session():
    """创建Spark会话"""
    return SparkSession.builder \
        .appName("GenerateRevenueForecast") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def calculate_time_based_price(hour_col):
    """
    根据小时计算分时电价
    峰时（8-11, 18-23）: 1.2元/kWh
    平时（7-8, 11-18）: 0.8元/kWh
    谷时（23-7）: 0.4元/kWh
    """
    return when((col(hour_col) >= 8) & (col(hour_col) < 11), 1.2) \
        .when((col(hour_col) >= 18) & (col(hour_col) < 23), 1.2) \
        .when((col(hour_col) >= 7) & (col(hour_col) < 8), 0.8) \
        .when((col(hour_col) >= 11) & (col(hour_col) < 18), 0.8) \
        .otherwise(0.4)

def generate_revenue_forecast(spark):
    """生成收益预测"""

    print("=" * 80)
    print("开始生成收益预测")
    print("=" * 80)

    # 读取预测数据
    print("\n1. 读取 gold_forecast_supply 数据...")
    forecast_df = spark.read.format("delta").load("hdfs://node1:9000/lake/gold/gold_forecast_supply")
    print(f"   总记录数: {forecast_df.count()}")

    # 添加时间和价格字段
    print("\n2. 添加时间和价格字段...")
    forecast_with_price = forecast_df \
        .withColumn("forecast_date", to_date(col("target_hour"))) \
        .withColumn("forecast_hour", hour(col("target_hour"))) \
        .withColumn("energy_price", calculate_time_based_price("forecast_hour")) \
        .withColumn("cooling_price", lit(0.3))

    # 计算预测能耗（假设平均COP=3.5）
    print("\n3. 计算预测能耗和收益...")
    avg_cop = 3.5

    revenue_forecast = forecast_with_price \
        .withColumn("predicted_energy_kwh",
                   col("predicted_cooling_kwh") / lit(avg_cop)) \
        .withColumn("predicted_energy_cost",
                   col("predicted_energy_kwh") * col("energy_price")) \
        .withColumn("predicted_cooling_revenue",
                   col("predicted_cooling_kwh") * col("cooling_price")) \
        .withColumn("predicted_profit",
                   col("predicted_cooling_revenue") - col("predicted_energy_cost")) \
        .withColumn("profit_margin",
                   when(col("predicted_cooling_revenue") > 0,
                        col("predicted_profit") / col("predicted_cooling_revenue"))
                   .otherwise(0)) \
        .withColumn("dt", col("forecast_date")) \
        .withColumn("created_at", current_timestamp())

    # 选择最终字段
    revenue_forecast_final = revenue_forecast.select(
        "station_id",
        "equipment_id",
        "forecast_date",
        "target_hour",
        "forecast_hour",
        "predicted_cooling_kwh",
        "predicted_energy_kwh",
        "energy_price",
        "cooling_price",
        "predicted_energy_cost",
        "predicted_cooling_revenue",
        "predicted_profit",
        "profit_margin",
        "model_version",
        "dt",
        "created_at"
    )

    # 显示统计信息
    print("\n4. 收益预测统计信息:")
    print(f"   生成收益预测记录数: {revenue_forecast_final.count()}")

    print("\n   样例数据:")
    revenue_forecast_final.orderBy(col("target_hour").desc(), "equipment_id").show(10, truncate=False)

    # 显示收益汇总
    print("\n5. 按设备汇总预测收益:")
    revenue_summary = revenue_forecast_final.groupBy("equipment_id").agg(
        expr("sum(predicted_cooling_kwh) as total_predicted_cooling"),
        expr("sum(predicted_energy_cost) as total_predicted_cost"),
        expr("sum(predicted_cooling_revenue) as total_predicted_revenue"),
        expr("sum(predicted_profit) as total_predicted_profit")
    )
    revenue_summary.orderBy(col("total_predicted_profit").desc()).show(20, truncate=False)

    # 保存到Delta Lake
    print("\n6. 保存收益预测到 Delta Lake...")
    output_path = "hdfs://node1:9000/lake/gold/gold_revenue_forecast"

    revenue_forecast_final.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("dt") \
        .save(output_path)

    print(f"   ✓ 收益预测已保存到: {output_path}")

    return revenue_forecast_final

def main():
    """主函数"""
    spark = create_spark_session()

    try:
        revenue_forecast = generate_revenue_forecast(spark)

        print("\n" + "=" * 80)
        print("✓ 收益预测生成完成")
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

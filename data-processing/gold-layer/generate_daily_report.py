#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日报表生成脚本
从 gold_supply_curve_hourly 读取小时数据，按日期汇总生成日报表
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as spark_sum, avg, max as spark_max,
    min as spark_min, count, current_timestamp, when, lit, coalesce
)
from pyspark.sql.window import Window

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Generate_Daily_Report")
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
    print("日报表生成任务启动")
    print("=" * 80)

    spark = create_spark_session()

    # 1. 读取Gold层供能曲线表
    print("\n📥 步骤 1: 读取 Gold 层供能曲线表...")
    curve_path = "hdfs://node1:9000/lake/gold/gold_supply_curve_hourly"
    df_hourly = spark.read.format("delta").load(curve_path)

    hourly_count = df_hourly.count()
    print(f"   ✅ 读取成功: {hourly_count:,} 条小时记录")

    # 2. 读取Silver层价格维表
    print("\n📥 步骤 2: 读取 Silver 层价格维表...")
    price_path = "hdfs://node1:9000/lake/silver/silver_price_dim"
    df_price = spark.read.format("delta").load(price_path)

    price_count = df_price.count()
    print(f"   ✅ 读取成功: {price_count} 条价格记录")

    # 3. 提取日期字段
    print("\n🗓️  步骤 3: 提取日期字段...")
    df_hourly = df_hourly.withColumn(
        "stat_date",
        to_date(col("stat_hour"), "yyyy-MM-dd HH:mm:ss")
    )

    # 4. 按日期和设备聚合
    print("\n📊 步骤 4: 按日期和设备聚合...")

    df_daily = df_hourly.groupBy("station_id", "equipment_id", "stat_date", "dt").agg(
        # 温度统计
        avg(col("avg_supply_temp")).alias("avg_supply_temp"),
        spark_max(col("max_supply_temp")).alias("max_supply_temp"),
        spark_min(col("min_supply_temp")).alias("min_supply_temp"),

        # 能耗统计（kWh）。运行中但功率缺失的小时单独标记，避免汇总时悄悄低估。
        spark_sum(col("energy_consumption_kwh")).alias("raw_total_energy_consumption_kwh"),
        count(when((col("runtime_hours") > 0) & col("energy_consumption_kwh").isNull(), 1))
            .alias("missing_running_energy_hours"),

        # 供冷统计（kWh）
        spark_max(col("cooling_supply_kwh")).alias("peak_cooling_kwh"),
        spark_min(when(col("cooling_supply_kwh") > 0, col("cooling_supply_kwh"))).alias("valley_cooling_kwh"),
        spark_sum(col("cooling_supply_kwh")).alias("raw_total_cooling_supply_kwh"),
        avg(col("cooling_supply_kwh")).alias("avg_cooling_supply_kwh"),
        count(when((col("runtime_hours") > 0) & col("cooling_supply_kwh").isNull(), 1))
            .alias("missing_running_cooling_hours"),

        # 运行统计
        spark_sum(col("runtime_hours")).alias("total_runtime_hours"),  # 累加每小时的运行时长
        spark_max(col("cumulative_runtime_hours")).alias("cumulative_runtime_hours"),  # 累计运行时长
        spark_max(col("start_count")).alias("total_start_count"),
        spark_sum(col("run_minutes")).alias("total_run_minutes"),

        # 小时数
        count("*").alias("hour_count")
    )

    # 5. 计算派生指标
    print("\n➕ 步骤 5: 计算派生指标...")

    df_daily = df_daily.withColumn(
        "total_energy_consumption_kwh",
        when(col("missing_running_energy_hours") > 0, lit(None).cast("double"))
        .otherwise(coalesce(col("raw_total_energy_consumption_kwh"), lit(0.0)))
    ).withColumn(
        "total_cooling_supply_kwh",
        when(col("missing_running_cooling_hours") > 0, lit(None).cast("double"))
        .otherwise(coalesce(col("raw_total_cooling_supply_kwh"), lit(0.0)))
    )

    # 日运行率（%）= 运行分钟数 / (24 * 60)
    df_daily = df_daily.withColumn(
        "daily_operation_rate",
        col("total_run_minutes") / (24.0 * 60.0) * 100.0
    )

    # 平均COP（能效比）= 供冷量 / 能耗
    df_daily = df_daily.withColumn(
        "avg_cop",
        when(col("total_energy_consumption_kwh") > 0,
             col("total_cooling_supply_kwh") / col("total_energy_consumption_kwh"))
    )

    # 峰谷比
    df_daily = df_daily.withColumn(
        "peak_valley_ratio",
        when(col("valley_cooling_kwh") > 0,
             col("peak_cooling_kwh") / col("valley_cooling_kwh")).otherwise(0)
    )

    # 峰值期/谷值期时长：按同设备同日平均供冷量作为动态阈值。
    daily_window = Window.partitionBy("station_id", "equipment_id", "stat_date")
    df_peak_valley_duration = df_hourly \
        .withColumn("daily_avg_cooling", avg("cooling_supply_kwh").over(daily_window)) \
        .withColumn(
            "is_peak_period",
            when(col("cooling_supply_kwh") > col("daily_avg_cooling") * 1.2, 1).otherwise(0)
        ) \
        .withColumn(
            "is_valley_period",
            when(
                (col("cooling_supply_kwh") < col("daily_avg_cooling") * 0.8)
                & (col("cooling_supply_kwh") > 0),
                1
            ).otherwise(0)
        ) \
        .groupBy("station_id", "equipment_id", "stat_date", "dt") \
        .agg(
            spark_sum("is_peak_period").alias("peak_duration_hours"),
            spark_sum("is_valley_period").alias("valley_duration_hours")
        )

    df_daily = df_daily.join(
        df_peak_valley_duration,
        on=["station_id", "equipment_id", "stat_date", "dt"],
        how="left"
    )

    # 6. 关联价格数据（简化版：使用固定价格）
    print("\n💰 步骤 6: 计算收益和成本...")

    # 由于价格表中station_code与station_id不匹配，这里使用平均价格
    # 实际应用中需要建立站点映射关系
    avg_electricity_price = df_price.filter(col("price_type") == "electricity") \
                                     .agg(avg("price")).collect()[0][0]
    avg_cooling_price = df_price.filter(col("price_type") == "cooling") \
                                 .agg(avg("price")).collect()[0][0]

    print(f"   平均电价: {avg_electricity_price:.4f} 元/kWh")
    print(f"   平均冷价: {avg_cooling_price:.4f} 元/kWh")

    # 计算成本（元）= 能耗 * 电价
    df_daily = df_daily.withColumn(
        "energy_cost",
        col("total_energy_consumption_kwh") * avg_electricity_price
    )

    # 计算收益（元）= 供冷量 * 冷价
    df_daily = df_daily.withColumn(
        "cooling_revenue",
        col("total_cooling_supply_kwh") * avg_cooling_price
    )

    # 计算净利润（元）= 收益 - 成本
    df_daily = df_daily.withColumn(
        "net_profit",
        col("cooling_revenue") - col("energy_cost")
    )

    # 7. 添加元数据字段
    print("\n📋 步骤 7: 添加元数据字段...")
    df_daily = df_daily.withColumn("created_at", current_timestamp()) \
                       .withColumn("updated_at", current_timestamp())

    # 8. 选择最终字段并排序
    df_daily = df_daily.select(
        "station_id",
        "equipment_id",
        "stat_date",
        # 峰谷指标
        "peak_cooling_kwh",
        "valley_cooling_kwh",
        "peak_valley_ratio",
        "peak_duration_hours",
        "valley_duration_hours",
        # 温度指标
        "avg_supply_temp",
        "max_supply_temp",
        "min_supply_temp",
        # 能耗指标
        "total_energy_consumption_kwh",
        # 供冷指标
        "total_cooling_supply_kwh",
        "avg_cooling_supply_kwh",
        # 运行指标
        "total_runtime_hours",  # 该日的实际运行时长（小时）
        "cumulative_runtime_hours",  # 累计运行时长
        "total_start_count",
        "total_run_minutes",  # 该日的运行分钟数
        "daily_operation_rate",  # 运行率（0-100%）
        # 效率指标
        "avg_cop",
        # 经济指标
        "energy_cost",
        "cooling_revenue",
        "net_profit",
        # 质量指标
        "hour_count",
        # 分区字段
        "dt",
        # 元数据
        "created_at",
        "updated_at"
    ).orderBy("station_id", "equipment_id", "stat_date")

    # 9. 数据预览
    print("\n📊 数据预览:")
    df_daily.show(10, truncate=False)

    print("\n📈 按设备统计:")
    df_daily.groupBy("equipment_id").agg(
        count("*").alias("day_count"),
        spark_sum("total_cooling_supply_kwh").alias("total_cooling_kwh"),
        spark_sum("energy_cost").alias("total_cost"),
        spark_sum("cooling_revenue").alias("total_revenue"),
        spark_sum("net_profit").alias("total_profit")
    ).orderBy("equipment_id").show()

    print("\n📈 按日期统计（前10天）:")
    df_daily.groupBy("stat_date").agg(
        spark_sum("total_cooling_supply_kwh").alias("total_cooling_kwh"),
        spark_sum("energy_cost").alias("total_cost"),
        spark_sum("cooling_revenue").alias("total_revenue"),
        spark_sum("net_profit").alias("total_profit")
    ).orderBy("stat_date").show(10)

    # 10. 写入Gold层
    output_path = "hdfs://node1:9000/lake/gold/gold_report_daily"
    print(f"\n💾 步骤 8: 写入 Gold 层: {output_path}")

    df_daily.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("dt") \
        .save(output_path)

    print("   ✅ 数据已写入 Gold 层")

    # 11. 验证写入结果
    print("\n🔍 步骤 9: 验证写入结果...")
    df_verify = spark.read.format("delta").load(output_path)
    verify_count = df_verify.count()

    print(f"   总记录数: {verify_count:,}")
    print(f"   字段列表: {df_verify.columns}")

    print("\n📊 验证数据样例:")
    df_verify.select(
        "station_id", "equipment_id", "stat_date",
        "total_cooling_supply_kwh", "energy_cost", "cooling_revenue", "net_profit"
    ).show(10, truncate=False)

    spark.stop()

    print("\n" + "=" * 80)
    print("✅ 日报表生成任务完成！")
    print("=" * 80)

if __name__ == "__main__":
    main()

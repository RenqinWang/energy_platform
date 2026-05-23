#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
供能曲线表生成脚本
从 silver_chiller_status 读取设备状态数据，按小时统计供能量，生成 gold_supply_curve_hourly 表
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, date_format, sum as spark_sum, avg, max as spark_max,
    min as spark_min, count, current_timestamp, to_timestamp, when, lit
)

ESTIMATED_CHILLER_COP = 3.0

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Generate_Supply_Curve")
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
    print("供能曲线表生成任务启动")
    print("=" * 80)

    spark = create_spark_session()

    # 1. 读取Silver层冷机设备状态宽表
    print("\n📥 步骤 1: 读取 Silver 层冷机设备状态宽表...")
    silver_path = "hdfs://node1:9000/lake/silver/silver_chiller_status"
    df_status = spark.read.format("delta").load(silver_path)

    status_count = df_status.count()
    print(f"   ✅ 读取成功: {status_count:,} 条记录")

    # 2. 数据预览
    print("\n📊 原始数据预览:")
    df_status.show(10, truncate=False)

    # 3. 生成小时时间窗口
    print("\n🕐 步骤 2: 生成小时时间窗口...")

    # 将stat_time转换为timestamp类型
    df_status = df_status.withColumn(
        "stat_timestamp",
        to_timestamp(col("stat_time"), "yyyy-MM-dd HH:mm:ss")
    )

    # 生成小时时间窗口（精确到小时）
    df_status = df_status.withColumn(
        "stat_hour",
        date_format(col("stat_timestamp"), "yyyy-MM-dd HH:00:00")
    )

    print("   ✅ 时间窗口生成完成")

    # 4. 按小时聚合统计
    print("\n📊 步骤 3: 按小时聚合统计...")

    df_hourly = df_status.groupBy("station_id", "equipment_id", "stat_hour", "dt").agg(
        # 温度统计
        avg(col("supply_temp")).alias("avg_supply_temp"),
        spark_max(col("supply_temp")).alias("max_supply_temp"),
        spark_min(col("supply_temp")).alias("min_supply_temp"),

        avg(col("return_temp")).alias("avg_return_temp"),

        # 压力统计
        avg(col("pressure")).alias("avg_pressure"),
        spark_max(col("pressure")).alias("max_pressure"),
        spark_min(col("pressure")).alias("min_pressure"),

        # 流量统计
        avg(col("flow")).alias("avg_flow"),
        spark_max(col("flow")).alias("max_flow"),
        spark_min(col("flow")).alias("min_flow"),

        # 功率统计（kW）
        avg(col("power")).alias("avg_power"),
        spark_max(col("power")).alias("max_power"),
        spark_min(col("power")).alias("min_power"),

        # 累计运行时长（小时）- 从Silver层获取的累计值
        spark_max(col("runtime_hours")).alias("cumulative_runtime_hours"),

        # 启动次数
        spark_max(col("start_count")).alias("start_count"),

        # 运行分钟数（该小时内运行状态为1的分钟数，0-60之间）
        spark_sum(col("run_flag")).alias("run_minutes"),

        # 记录数（用于质量检查）
        count("*").alias("record_count")
    )

    # 5. 计算派生指标
    print("\n➕ 步骤 4: 计算派生指标...")

    # 计算该小时的实际运行时长（小时）= 运行分钟数 / 60
    df_hourly = df_hourly.withColumn(
        "runtime_hours",
        col("run_minutes") / 60.0
    )

    # 计算能耗（kWh）= 平均功率 * 运行时长
    df_hourly = df_hourly.withColumn(
        "energy_consumption_kwh",
        col("avg_power") * col("runtime_hours")
    )

    # 计算制冷量（kW）：优先使用水侧公式 Q = 1.163 * flow * ΔT。
    # 若水侧条件不满足但功率可用，则用固定 COP=3.0 回退估算。
    df_hourly = df_hourly.withColumn(
        "cooling_capacity_kw",
        when(
            col("avg_flow").isNotNull()
            & col("avg_return_temp").isNotNull()
            & col("avg_supply_temp").isNotNull()
            & ((col("avg_return_temp") - col("avg_supply_temp")) > 0),
            lit(1.163) * col("avg_flow") * (col("avg_return_temp") - col("avg_supply_temp"))
        ).when(
            col("avg_power").isNotNull(),
            col("avg_power") * ESTIMATED_CHILLER_COP
        )
    )

    # 计算供冷量（kWh）= 制冷量 * 运行时长
    df_hourly = df_hourly.withColumn(
        "cooling_supply_kwh",
        col("cooling_capacity_kw") * col("runtime_hours")
    )

    # 计算运行率（%）= (运行分钟数 / 60) * 100
    # 正常范围: 0% ~ 100%
    df_hourly = df_hourly.withColumn(
        "operation_rate",
        (col("run_minutes") / 60.0) * 100.0
    )

    # 6. 添加元数据字段
    print("\n📋 步骤 5: 添加元数据字段...")
    df_hourly = df_hourly.withColumn("created_at", current_timestamp()) \
                         .withColumn("updated_at", current_timestamp())

    # 7. 选择最终字段并排序
    df_hourly = df_hourly.select(
        "station_id",
        "equipment_id",
        "stat_hour",
        # 温度指标
        "avg_supply_temp",
        "max_supply_temp",
        "min_supply_temp",
        "avg_return_temp",
        # 压力指标
        "avg_pressure",
        "max_pressure",
        "min_pressure",
        # 流量指标
        "avg_flow",
        "max_flow",
        "min_flow",
        # 功率指标
        "avg_power",
        "max_power",
        "min_power",
        # 能耗指标
        "energy_consumption_kwh",
        # 供冷指标
        "cooling_capacity_kw",
        "cooling_supply_kwh",
        # 运行指标
        "runtime_hours",  # 该小时的实际运行时长（0-1小时）
        "cumulative_runtime_hours",  # 累计运行时长（从Silver层获取）
        "start_count",
        "run_minutes",  # 该小时的运行分钟数（0-60分钟）
        "operation_rate",  # 运行率（0-100%）
        # 质量指标
        "record_count",
        # 分区字段
        "dt",
        # 元数据
        "created_at",
        "updated_at"
    ).orderBy("station_id", "equipment_id", "stat_hour")

    # 8. 数据预览
    print("\n📊 数据预览:")
    df_hourly.show(10, truncate=False)

    print("\n📈 按设备统计:")
    df_hourly.groupBy("equipment_id").agg(
        count("*").alias("hour_count"),
        spark_sum("cooling_supply_kwh").alias("total_cooling_kwh"),
        spark_sum("energy_consumption_kwh").alias("total_energy_kwh"),
        avg("operation_rate").alias("avg_operation_rate")
    ).orderBy("equipment_id").show()

    print("\n📈 按日期统计:")
    df_hourly.groupBy("dt").agg(
        count("*").alias("hour_count"),
        spark_sum("cooling_supply_kwh").alias("total_cooling_kwh"),
        spark_sum("energy_consumption_kwh").alias("total_energy_kwh")
    ).orderBy("dt").show()

    # 9. 写入Gold层
    output_path = "hdfs://node1:9000/lake/gold/gold_supply_curve_hourly"
    print(f"\n💾 步骤 6: 写入 Gold 层: {output_path}")

    df_hourly.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("dt") \
        .save(output_path)

    print("   ✅ 数据已写入 Gold 层")

    # 10. 验证写入结果
    print("\n🔍 步骤 7: 验证写入结果...")
    df_verify = spark.read.format("delta").load(output_path)
    verify_count = df_verify.count()

    print(f"   总记录数: {verify_count:,}")
    print(f"   字段列表: {df_verify.columns}")

    print("\n📊 验证数据样例:")
    df_verify.select(
        "station_id", "equipment_id", "stat_hour",
        "avg_supply_temp", "cooling_supply_kwh", "energy_consumption_kwh", "operation_rate"
    ).show(10, truncate=False)

    spark.stop()

    print("\n" + "=" * 80)
    print("✅ 供能曲线表生成任务完成！")
    print("=" * 80)

if __name__ == "__main__":
    main()

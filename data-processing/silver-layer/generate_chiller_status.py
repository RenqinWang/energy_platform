#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
冷机设备状态宽表生成脚本
从 silver_point_fact 筛选冷机系统数据，按设备聚合生成宽表
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, max as spark_max, count, current_timestamp, date_format
)

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Generate_Chiller_Status")
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
    print("冷机设备状态宽表生成任务启动")
    print("=" * 80)

    spark = create_spark_session()

    # 1. 读取Silver层点位事实表
    print("\n📥 步骤 1: 读取 Silver 层点位事实表...")
    fact_path = "hdfs://node1:9000/lake/silver/silver_point_fact"
    df_fact = spark.read.format("delta").load(fact_path)

    fact_count = df_fact.count()
    print(f"   ✅ 读取成功: {fact_count:,} 条记录")

    # 2. 筛选冷机系统数据
    print("\n🔍 步骤 2: 筛选冷机系统数据...")
    df_chiller = df_fact.filter(col("system_type") == "chiller")

    chiller_count = df_chiller.count()
    print(f"   ✅ 筛选成功: {chiller_count:,} 条冷机记录")

    # 查看冷机系统的主题分布
    print("\n📊 冷机系统主题分布:")
    df_chiller.groupBy("theme").count().orderBy("count", ascending=False).show()

    print("\n📊 冷机系统测点角色分布:")
    df_chiller.groupBy("measure_role").count().orderBy("count", ascending=False).show()

    # 查看冷机设备分布
    print("\n📊 冷机设备分布:")
    df_chiller.groupBy("equipment_id").count().orderBy("equipment_id").show()

    # 3. 生成时间窗口（按1分钟聚合）
    print("\n🕐 步骤 3: 生成时间窗口（按1分钟聚合）...")

    # 添加stat_time字段（精确到分钟）
    df_chiller = df_chiller.withColumn(
        "stat_time",
        date_format(col("event_time"), "yyyy-MM-dd HH:mm:00")
    )

    # 4. 透视数据：将不同主题的点位转换为列
    print("\n🔄 步骤 4: 透视数据，生成宽表...")

    # 按 station_id, equipment_id, stat_time 分组聚合
    # 使用条件聚合来实现透视
    df_status = df_chiller.groupBy("station_id", "equipment_id", "stat_time", "dt").agg(
        # 温度相关
        spark_max(when(col("measure_role") == "supply_temp", col("value"))).alias("supply_temp"),
        spark_max(when(col("measure_role") == "return_temp", col("value"))).alias("return_temp"),

        # 压力相关
        spark_max(when(col("measure_role") == "pressure", col("value"))).alias("pressure"),

        # 流量相关
        spark_max(when(col("measure_role") == "flow", col("value"))).alias("flow"),

        # 功率相关
        spark_max(when(col("measure_role") == "power", col("value"))).alias("power"),

        # 运行时长（累计）- 从"运行时间"点位获取
        spark_max(when((col("theme") == "status") & col("point_name").contains("运行时间"), col("value"))).alias("runtime_hours"),

        # 启动次数（累计）
        spark_max(when(col("theme") == "count", col("value"))).alias("start_count"),

        # 运行状态（0/1）- 从"运行"点位获取，排除"运行时间"
        spark_max(when((col("theme") == "status") & ~col("point_name").contains("运行时间"), col("value"))).alias("run_flag"),

        # 记录数（用于质量检查）
        count("*").alias("record_count")
    )

    # 5. 添加派生字段
    print("\n➕ 步骤 5: 添加派生字段...")

    # 添加元数据字段
    df_status = df_status.withColumn("created_at", current_timestamp()) \
                         .withColumn("updated_at", current_timestamp())

    # 6. 选择最终字段并排序
    print("\n📋 步骤 6: 选择最终字段...")
    df_status = df_status.select(
        "station_id",
        "equipment_id",
        "stat_time",
        "supply_temp",
        "return_temp",
        "pressure",
        "flow",
        "power",
        "runtime_hours",
        "start_count",
        "run_flag",
        "record_count",
        "dt",
        "created_at",
        "updated_at"
    ).orderBy("station_id", "equipment_id", "stat_time")

    # 7. 数据预览
    print("\n📊 数据预览:")
    df_status.show(10, truncate=False)

    print("\n📈 按设备统计:")
    df_status.groupBy("equipment_id").count().orderBy("equipment_id").show()

    print("\n📈 数据质量检查（每分钟记录数分布）:")
    df_status.groupBy("record_count").count().orderBy("record_count").show()

    # 8. 写入Silver层
    output_path = "hdfs://node1:9000/lake/silver/silver_chiller_status"
    print(f"\n💾 步骤 7: 写入 Silver 层: {output_path}")

    df_status.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("dt") \
        .save(output_path)

    print("   ✅ 数据已写入 Silver 层")

    # 9. 验证写入结果
    print("\n🔍 步骤 8: 验证写入结果...")
    df_verify = spark.read.format("delta").load(output_path)
    verify_count = df_verify.count()

    print(f"   总记录数: {verify_count:,}")
    print(f"   字段列表: {df_verify.columns}")

    print("\n📊 按日期统计:")
    df_verify.groupBy("dt").count().orderBy("dt").show(10)

    print("\n📊 数据样例（带完整字段）:")
    df_verify.select(
        "station_id", "equipment_id", "stat_time",
        "supply_temp", "return_temp", "pressure", "flow", "power", "run_flag"
    ).show(10, truncate=False)

    spark.stop()

    print("\n" + "=" * 80)
    print("✅ 冷机设备状态宽表生成任务完成！")
    print("=" * 80)

if __name__ == "__main__":
    main()

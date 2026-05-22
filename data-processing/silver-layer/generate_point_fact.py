#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
点位事实表生成脚本
从 bronze_sensor_raw 读取原始流数据，关联 silver_point_meta_dim 点位字典，
补齐业务字段，处理缺失值，生成 silver_point_fact 表
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lag, lit, current_timestamp,
    unix_timestamp, count
)
from pyspark.sql.window import Window

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Generate_Point_Fact")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def handle_missing_values(df):
    """
    处理缺失值

    策略：
    1. 状态型字段：前值保持（超过1小时置为unknown）
    2. 连续型字段：短时缺失（≤5分钟）线性插值，长时缺失保留NULL并标记
    3. 累计型字段：保留缺失，标记quality_flag
    """

    # 定义窗口：按point_code分区，按event_time排序
    window_spec = Window.partitionBy("point_code").orderBy("event_time")

    # 1. 计算时间差（秒）
    df = df.withColumn(
        "time_diff_seconds",
        unix_timestamp("event_time") - unix_timestamp(lag("event_time", 1).over(window_spec))
    )

    # 2. 初始化quality_flag
    df = df.withColumn("quality_flag", lit("normal"))

    # 3. 处理缺失值
    # 对于value为NULL的情况，根据主题类型处理
    df = df.withColumn(
        "value_filled",
        when(col("value").isNull(),
             # 如果是状态型，使用前值保持
             when(col("theme") == "status",
                  lag("value", 1).over(window_spec))
             # 如果是连续型且时间差≤300秒（5分钟），使用前值
             .when((col("theme").isin("temperature", "pressure", "flow", "power")) &
                   (col("time_diff_seconds") <= 300),
                   lag("value", 1).over(window_spec))
             # 否则保留NULL
             .otherwise(None)
        ).otherwise(col("value"))
    )

    # 4. 标记质量问题
    df = df.withColumn(
        "quality_flag",
        # 长时间缺失（>5分钟）
        when((col("value").isNull()) & (col("time_diff_seconds") > 300),
             lit("long_missing"))
        # 累计型字段缺失
        .when((col("value").isNull()) & (col("theme").isin("cumulative", "runtime")),
              lit("cumulative_gap"))
        # 状态型字段超过1小时未更新
        .when((col("theme") == "status") & (col("time_diff_seconds") > 3600),
              lit("status_stale"))
        .otherwise(col("quality_flag"))
    )

    # 5. 使用填充后的值替换原值
    df = df.withColumn("value", col("value_filled")).drop("value_filled", "time_diff_seconds")

    return df

def main():
    print("=" * 80)
    print("点位事实表生成任务启动")
    print("=" * 80)

    spark = create_spark_session()

    # 1. 读取Bronze层传感器数据
    print("\n📥 步骤 1: 读取 Bronze 层传感器数据...")
    bronze_path = "hdfs://node1:9000/lake/bronze/bronze_sensor_raw"
    df_bronze = spark.read.format("delta").load(bronze_path)

    bronze_count = df_bronze.count()
    print(f"   ✅ 读取成功: {bronze_count:,} 条记录")

    # 2. 读取Silver层点位字典
    print("\n📥 步骤 2: 读取 Silver 层点位字典...")
    dict_path = "hdfs://node1:9000/lake/silver/silver_point_meta_dim"
    df_dict = spark.read.format("delta").load(dict_path)

    dict_count = df_dict.count()
    print(f"   ✅ 读取成功: {dict_count} 个点位")

    print("\n🔍 点位字典唯一性检查...")
    duplicate_dict = (
        df_dict.groupBy("point_code")
        .agg(count("*").alias("duplicate_count"))
        .filter(col("duplicate_count") > 1)
    )
    duplicate_dict_count = duplicate_dict.count()
    if duplicate_dict_count > 0:
        print(f"   ⚠️  发现 {duplicate_dict_count} 个重复 point_code，Join 前将保留第一条")
        duplicate_dict.show(20, truncate=False)
        df_dict = df_dict.dropDuplicates(["point_code"])
        print(f"   去重后点位数: {df_dict.count()}")
    else:
        print("   ✅ point_code 唯一")

    # 3. 关联点位字典，补齐业务字段
    print("\n🔗 步骤 3: 关联点位字典，补齐业务字段...")

    # 使用sensor_id关联point_code
    df_joined = df_bronze.join(
        df_dict.select(
            col("point_code").alias("sensor_id"),
            col("point_name"),
            col("station_id"),
            col("system_type"),
            col("equipment_id"),
            col("theme"),
            col("unit"),
            col("measure_role")
        ),
        on="sensor_id",
        how="left"
    )

    # 检查未匹配的记录
    unmatched_count = df_joined.filter(col("point_name").isNull()).count()
    if unmatched_count > 0:
        print(f"   ⚠️  警告: {unmatched_count} 条记录未匹配到点位字典")
        print("   未匹配的sensor_id:")
        df_joined.filter(col("point_name").isNull()) \
                 .select("sensor_id").distinct().show(10, truncate=False)
    else:
        print(f"   ✅ 所有记录都已成功匹配")

    # 4. 选择和重命名字段
    print("\n🔄 步骤 4: 标准化字段...")
    df_fact = df_joined.select(
        col("station_id"),
        col("sensor_id").alias("point_code"),
        col("point_name"),
        col("system_type"),
        col("equipment_id"),
        col("theme"),
        col("measure_role"),
        col("timestamp").alias("event_time"),  # 修正：使用timestamp字段
        col("value"),
        col("unit"),
        col("is_simulated"),
        col("dt")
    )

    # 5. 处理缺失值
    print("\n🔧 步骤 5: 处理缺失值...")
    df_fact = handle_missing_values(df_fact)

    # 统计质量标记
    print("\n📊 数据质量统计:")
    df_fact.groupBy("quality_flag").count().orderBy("count", ascending=False).show()

    # 6. 添加元数据字段
    print("\n➕ 步骤 6: 添加元数据字段...")
    df_fact = df_fact.withColumn("created_at", current_timestamp()) \
                     .withColumn("updated_at", current_timestamp())

    # 7. 数据预览
    print("\n📊 数据预览:")
    df_fact.show(10, truncate=False)

    print("\n📈 按系统类型统计:")
    df_fact.groupBy("system_type").count().orderBy("count", ascending=False).show()

    print("\n📈 按主题统计:")
    df_fact.groupBy("theme").count().orderBy("count", ascending=False).show()

    print("\n📈 按测点角色统计:")
    df_fact.groupBy("measure_role").count().orderBy("count", ascending=False).show()

    # 8. 写入Silver层
    output_path = "hdfs://node1:9000/lake/silver/silver_point_fact"
    print(f"\n💾 步骤 7: 写入 Silver 层: {output_path}")

    df_fact.write \
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

    spark.stop()

    print("\n" + "=" * 80)
    print("✅ 点位事实表生成任务完成！")
    print("=" * 80)

if __name__ == "__main__":
    main()

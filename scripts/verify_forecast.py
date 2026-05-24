#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
预测功能验证脚本
验证gold_forecast_supply表的数据完整性和质量
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min as spark_min, max as spark_max


def create_spark_session():
    """创建Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Verify_Forecast")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def verify_forecast(spark, forecast_path):
    """
    验证预测数据

    Args:
        spark: SparkSession
        forecast_path: 预测数据路径
    """
    print("="*60)
    print("预测功能验证")
    print("="*60)
    print(f"数据路径: {forecast_path}\n")

    try:
        # 读取数据
        df = spark.read.format("delta").load(forecast_path)

        # 基本统计
        total_count = df.count()
        print(f"✓ 总记录数: {total_count:,}")

        if total_count == 0:
            print("\n❌ 预测表为空")
            return False

        # 设备统计
        equipment_count = df.select("equipment_id").distinct().count()
        print(f"✓ 设备数量: {equipment_count}")

        # 按设备统计
        print("\n按设备统计:")
        df.groupBy("equipment_id").agg(
            count("*").alias("record_count"),
            spark_min("target_hour").alias("min_target_hour"),
            spark_max("target_hour").alias("max_target_hour"),
            avg("predicted_cooling_kwh").alias("avg_predicted_cooling")
        ).orderBy("equipment_id").show(truncate=False)

        # 预测值统计
        print("\n预测值统计:")
        df.select(
            spark_min("predicted_cooling_kwh").alias("min_predicted"),
            spark_max("predicted_cooling_kwh").alias("max_predicted"),
            avg("predicted_cooling_kwh").alias("avg_predicted")
        ).show()

        # 置信区间统计
        print("\n置信区间统计:")
        df.select(
            avg("confidence_lower").alias("avg_lower"),
            avg("confidence_upper").alias("avg_upper"),
            avg(col("confidence_upper") - col("confidence_lower")).alias("avg_interval_width")
        ).show()

        # 检查空值
        print("\n空值检查:")
        null_counts = df.select([
            count(col(c).isNull().cast("int")).alias(c)
            for c in ["predicted_cooling_kwh", "confidence_lower", "confidence_upper"]
        ])
        null_counts.show()

        # 检查负值
        negative_count = df.filter(col("predicted_cooling_kwh") < 0).count()
        if negative_count > 0:
            print(f"\n⚠️  发现 {negative_count} 条负值预测")
        else:
            print("\n✓ 无负值预测")

        # 检查每个设备的预测小时数
        print("\n每个设备的预测小时数:")
        hours_per_equipment = df.groupBy("equipment_id").agg(
            count("*").alias("hours")
        ).collect()

        all_24_hours = all(row['hours'] == 24 for row in hours_per_equipment)
        if all_24_hours:
            print("✓ 所有设备都有24小时预测")
        else:
            print("⚠️  部分设备预测小时数不足24:")
            for row in hours_per_equipment:
                if row['hours'] != 24:
                    print(f"  {row['equipment_id']}: {row['hours']} 小时")

        print("\n" + "="*60)
        print("✅ 预测功能验证完成")
        print("="*60)

        return True

    except Exception as e:
        print(f"\n❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    forecast_path = "hdfs://node1:9000/lake/gold/gold_forecast_supply"

    spark = create_spark_session()

    try:
        success = verify_forecast(spark, forecast_path)
        exit(0 if success else 1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

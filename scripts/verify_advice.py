#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
建议功能验证脚本
验证gold_operation_advice表的数据完整性和质量
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def create_spark_session():
    """创建Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Verify_Advice")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def verify_advice(spark, advice_path):
    """
    验证建议数据

    Args:
        spark: SparkSession
        advice_path: 建议数据路径
    """
    print("="*60)
    print("建议功能验证")
    print("="*60)
    print(f"数据路径: {advice_path}\n")

    try:
        # 读取数据
        df = spark.read.format("delta").load(advice_path)

        # 基本统计
        total_count = df.count()
        print(f"✓ 总建议数: {total_count:,}")

        if total_count == 0:
            print("\n⚠️  建议表为空（可能当前无触发规则）")
            return True

        # 按建议类型统计
        print("\n按建议类型统计:")
        df.groupBy("advice_type").agg(
            count("*").alias("count")
        ).orderBy("advice_type").show(truncate=False)

        # 按风险等级统计
        print("\n按风险等级统计:")
        df.groupBy("risk_level").agg(
            count("*").alias("count")
        ).orderBy("risk_level").show(truncate=False)

        # 按规则统计
        print("\n按规则统计:")
        df.groupBy("rule_id", "rule_name").agg(
            count("*").alias("count")
        ).orderBy("rule_id").show(truncate=False)

        # 按设备统计
        print("\n按设备统计:")
        df.groupBy("equipment_id").agg(
            count("*").alias("advice_count")
        ).orderBy("equipment_id").show(truncate=False)

        # 显示建议样本
        print("\n建议样本 (前5条):")
        df.select(
            "equipment_id", "advice_type", "risk_level",
            "advice_text", "rule_id"
        ).show(5, truncate=False)

        # 检查必填字段
        print("\n必填字段检查:")
        required_fields = [
            "station_id", "equipment_id", "advice_time",
            "advice_type", "risk_level", "advice_text",
            "evidence_metrics", "rule_id"
        ]

        for field in required_fields:
            null_count = df.filter(col(field).isNull()).count()
            if null_count > 0:
                print(f"  ⚠️  {field}: {null_count} 条空值")
            else:
                print(f"  ✓ {field}: 无空值")

        print("\n" + "="*60)
        print("✅ 建议功能验证完成")
        print("="*60)

        return True

    except Exception as e:
        print(f"\n❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    advice_path = "hdfs://node1:9000/lake/gold/gold_operation_advice"

    spark = create_spark_session()

    try:
        success = verify_advice(spark, advice_path)
        exit(0 if success else 1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

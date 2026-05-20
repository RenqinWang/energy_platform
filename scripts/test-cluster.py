#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版集群测试脚本 - 不依赖 Delta Lake
测试 Spark 集群和 HDFS 的基本功能
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 创建 Spark Session
spark = (
    SparkSession.builder
    .appName("Simple_Cluster_Test")
    .master("spark://node2:7077")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", "1")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)

print("=" * 60)
print("✅ Spark 集群连接成功!")
print(f"   Spark 版本: {spark.version}")
print(f"   Master: {spark.sparkContext.master}")
print("=" * 60)
print()

# 分布式计算示例
print("🔢 开始分布式计算测试...")
print("   生成 1000 万个数字并进行计算...")

numbers = spark.range(0, 10_000_000)

result = (
    numbers
    .withColumn("squared", F.col("id") * F.col("id"))
    .filter(F.col("squared") % 100 == 0)
    .agg(
        F.count("id").alias("count"),
        F.sum("squared").alias("total_sum"),
        F.avg("squared").alias("avg_squared")
    )
)

print("\n📊 计算结果：")
result.show(truncate=False)

# 测试 HDFS 写入
print("\n💾 测试 HDFS 写入...")
hdfs_path = "hdfs:///test/cluster_test_result"

result.write.mode("overwrite").parquet(hdfs_path)
print(f"   ✅ 数据已保存到: {hdfs_path}")

# 读取验证
print("\n📖 验证 HDFS 读取...")
df_read = spark.read.parquet(hdfs_path)
df_read.show(truncate=False)

print("\n" + "=" * 60)
print("✅ 集群测试完成！所有功能正常")
print("=" * 60)

spark.stop()

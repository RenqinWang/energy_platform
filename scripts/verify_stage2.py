#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阶段2数据验证脚本
验证三个数据源是否都已成功接入到数据湖
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Stage2_Verification")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def verify_bronze_sensor_data(spark):
    """验证Bronze层传感器数据"""
    print("\n" + "=" * 60)
    print("1. 验证 Bronze 层传感器数据 (bronze_sensor_raw)")
    print("=" * 60)

    try:
        path = "hdfs://node1:9000/lake/bronze/bronze_sensor_raw"
        df = spark.read.format("delta").load(path)

        total_count = df.count()
        sensor_count = df.select(countDistinct("sensor_id")).collect()[0][0]
        date_count = df.select(countDistinct("dt")).collect()[0][0]

        print(f"✅ 传感器数据验证成功")
        print(f"   总记录数: {total_count:,}")
        print(f"   传感器数量: {sensor_count}")
        print(f"   日期范围: {date_count} 天")

        # 显示日期分布
        print("\n📊 按日期统计记录数：")
        df.groupBy("dt").count().orderBy("dt").show(10)

        # 显示传感器样例
        print("\n📊 传感器样例：")
        df.select("sensor_id", "label", "event_time", "value").show(5, truncate=False)

        return True
    except Exception as e:
        print(f"❌ 传感器数据验证失败: {e}")
        return False

def verify_bronze_price_data(spark):
    """验证Bronze层价格数据"""
    print("\n" + "=" * 60)
    print("2. 验证 Bronze 层价格数据 (bronze_price_raw)")
    print("=" * 60)

    try:
        path = "hdfs://node1:9000/lake/bronze/bronze_price_raw"
        df = spark.read.format("delta").load(path)

        total_count = df.count()
        station_count = df.select(countDistinct("station_code")).collect()[0][0]
        price_type_count = df.select(countDistinct("price_type")).collect()[0][0]

        print(f"✅ 价格数据验证成功")
        print(f"   总记录数: {total_count}")
        print(f"   站点数量: {station_count}")
        print(f"   价格类型数量: {price_type_count}")

        # 显示价格统计
        print("\n📊 按站点和价格类型统计：")
        df.groupBy("station_code", "price_type").count().orderBy("station_code", "price_type").show()

        # 显示价格样例
        print("\n📊 价格数据样例：")
        df.select("station_code", "price_type", "price", "updated_at").show(10, truncate=False)

        return True
    except Exception as e:
        print(f"❌ 价格数据验证失败: {e}")
        return False

def verify_silver_point_meta(spark):
    """验证Silver层点位字典"""
    print("\n" + "=" * 60)
    print("3. 验证 Silver 层点位字典 (silver_point_meta_dim)")
    print("=" * 60)

    try:
        path = "hdfs://node1:9000/lake/silver/silver_point_meta_dim"
        df = spark.read.format("delta").load(path)

        total_count = df.count()
        system_count = df.select(countDistinct("system_type")).collect()[0][0]
        theme_count = df.select(countDistinct("theme")).collect()[0][0]

        print(f"✅ 点位字典验证成功")
        print(f"   总点位数: {total_count}")
        print(f"   系统类型数量: {system_count}")
        print(f"   主题数量: {theme_count}")

        # 显示系统类型统计
        print("\n📊 按系统类型统计：")
        df.groupBy("system_type").count().orderBy(col("count").desc()).show()

        # 显示主题统计
        print("\n📊 按主题统计：")
        df.groupBy("theme").count().orderBy(col("count").desc()).show()

        # 显示点位样例
        print("\n📊 点位字典样例：")
        df.select("point_code", "point_name", "system_type", "theme", "unit").show(10, truncate=False)

        return True
    except Exception as e:
        print(f"❌ 点位字典验证失败: {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("阶段2数据验证 - 数据采集完整性检查")
    print("=" * 60)

    spark = create_spark_session()

    results = []

    # 验证三个数据源
    results.append(("Bronze层传感器数据", verify_bronze_sensor_data(spark)))
    results.append(("Bronze层价格数据", verify_bronze_price_data(spark)))
    results.append(("Silver层点位字典", verify_silver_point_meta(spark)))

    # 汇总结果
    print("\n" + "=" * 60)
    print("验证结果汇总")
    print("=" * 60)

    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{name}: {status}")

    all_passed = all(result for _, result in results)

    if all_passed:
        print("\n🎉 阶段2数据采集完整性验证通过！")
        print("   - Kafka流式数据已成功接入Bronze层")
        print("   - 价格数据已成功同步到Bronze层")
        print("   - 点位字典已成功解析到Silver层")
        print("\n✅ 可以继续进行阶段3：数据治理与标准化")
    else:
        print("\n⚠️  部分数据源验证失败，请检查上述错误信息")

    spark.stop()

    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()

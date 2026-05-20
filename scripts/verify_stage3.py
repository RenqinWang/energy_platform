#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阶段3数据验证脚本
验证三个Silver层表的数据质量和完整性
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

def create_spark_session():
    """创建 Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Stage3_Verification")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def verify_point_fact(spark):
    """验证点位事实表"""
    print("\n" + "=" * 80)
    print("1. 验证 Silver 层点位事实表 (silver_point_fact)")
    print("=" * 80)

    try:
        path = "hdfs://node1:9000/lake/silver/silver_point_fact"
        df = spark.read.format("delta").load(path)

        total_count = df.count()
        point_count = df.select(countDistinct("point_code")).collect()[0][0]
        station_count = df.select(countDistinct("station_id")).collect()[0][0]
        system_count = df.select(countDistinct("system_type")).collect()[0][0]

        print(f"✅ 点位事实表验证成功")
        print(f"   总记录数: {total_count:,}")
        print(f"   点位数量: {point_count}")
        print(f"   站点数量: {station_count}")
        print(f"   系统类型数量: {system_count}")

        print("\n📊 按系统类型统计:")
        df.groupBy("system_type").count().orderBy("count", ascending=False).show()

        print("\n📊 按主题统计:")
        df.groupBy("theme").count().orderBy("count", ascending=False).show()

        print("\n📊 数据质量统计:")
        df.groupBy("quality_flag").count().orderBy("count", ascending=False).show()

        print("\n📊 数据样例:")
        df.select("station_id", "point_code", "point_name", "system_type", "theme", "value", "unit").show(5, truncate=False)

        return True
    except Exception as e:
        print(f"❌ 点位事实表验证失败: {e}")
        return False

def verify_chiller_status(spark):
    """验证冷机设备状态宽表"""
    print("\n" + "=" * 80)
    print("2. 验证 Silver 层冷机设备状态宽表 (silver_chiller_status)")
    print("=" * 80)

    try:
        path = "hdfs://node1:9000/lake/silver/silver_chiller_status"
        df = spark.read.format("delta").load(path)

        total_count = df.count()
        equipment_count = df.select(countDistinct("equipment_id")).collect()[0][0]
        station_count = df.select(countDistinct("station_id")).collect()[0][0]

        print(f"✅ 冷机设备状态宽表验证成功")
        print(f"   总记录数: {total_count:,}")
        print(f"   设备数量: {equipment_count}")
        print(f"   站点数量: {station_count}")

        print("\n📊 按设备统计:")
        df.groupBy("equipment_id").count().orderBy("equipment_id").show()

        print("\n📊 字段完整性检查:")
        print(f"   supply_temp 非空记录: {df.filter(col('supply_temp').isNotNull()).count():,}")
        print(f"   return_temp 非空记录: {df.filter(col('return_temp').isNotNull()).count():,}")
        print(f"   pressure 非空记录: {df.filter(col('pressure').isNotNull()).count():,}")
        print(f"   flow 非空记录: {df.filter(col('flow').isNotNull()).count():,}")
        print(f"   power 非空记录: {df.filter(col('power').isNotNull()).count():,}")

        print("\n📊 数据样例:")
        df.select("station_id", "equipment_id", "stat_time", "supply_temp", "pressure", "flow", "power").show(5, truncate=False)

        return True
    except Exception as e:
        print(f"❌ 冷机设备状态宽表验证失败: {e}")
        return False

def verify_price_dim(spark):
    """验证价格维表"""
    print("\n" + "=" * 80)
    print("3. 验证 Silver 层价格维表 (silver_price_dim)")
    print("=" * 80)

    try:
        path = "hdfs://node1:9000/lake/silver/silver_price_dim"
        df = spark.read.format("delta").load(path)

        total_count = df.count()
        station_count = df.select(countDistinct("station_code")).collect()[0][0]
        price_type_count = df.select(countDistinct("price_type")).collect()[0][0]

        print(f"✅ 价格维表验证成功")
        print(f"   总记录数: {total_count}")
        print(f"   站点数量: {station_count}")
        print(f"   价格类型数量: {price_type_count}")

        print("\n📊 按站点和价格类型统计:")
        df.groupBy("station_code", "price_type").count().orderBy("station_code", "price_type").show()

        print("\n📊 价格范围:")
        df.groupBy("price_type").agg({"price": "min", "price": "max", "price": "avg"}).show()

        print("\n📊 数据样例:")
        df.select("station_code", "price_type", "price", "effective_date").show(10, truncate=False)

        return True
    except Exception as e:
        print(f"❌ 价格维表验证失败: {e}")
        return False

def main():
    print("\n" + "=" * 80)
    print("阶段3数据验证 - 数据治理与标准化完整性检查")
    print("=" * 80)

    spark = create_spark_session()

    results = []

    # 验证三个Silver层表
    results.append(("Silver层点位事实表", verify_point_fact(spark)))
    results.append(("Silver层冷机设备状态宽表", verify_chiller_status(spark)))
    results.append(("Silver层价格维表", verify_price_dim(spark)))

    # 汇总结果
    print("\n" + "=" * 80)
    print("验证结果汇总")
    print("=" * 80)

    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{name}: {status}")

    all_passed = all(result for _, result in results)

    if all_passed:
        print("\n🎉 阶段3数据治理与标准化验证通过！")
        print("   - 点位事实表已成功生成")
        print("   - 冷机设备状态宽表已成功生成")
        print("   - 价格维表已成功生成")
        print("\n✅ 可以继续进行阶段4：数据分析计算")
    else:
        print("\n⚠️  部分数据源验证失败，请检查上述错误信息")

    spark.stop()

    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()

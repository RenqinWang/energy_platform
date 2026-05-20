#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
点位字典解析脚本
从 pos.ttl 文件解析点位元数据，生成 silver_point_meta_dim 维表
"""

import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

def parse_ttl_file(file_path):
    """
    解析 TTL 文件，提取点位代码和标签
    """
    points = []

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 正则匹配：<点位代码> rdf:type sosa:FeatureOfInterest ; rdfs:label "标签"@zh .
    pattern = r'<([^>]+)>\s+rdf:type\s+sosa:FeatureOfInterest\s*;\s*rdfs:label\s+"([^"]+)"@zh'
    matches = re.findall(pattern, content)

    for point_code, label in matches:
        points.append({
            'point_code': point_code,
            'point_name': label
        })

    return points

def extract_metadata(point_code, point_name):
    """
    从点位代码和名称中提取元数据

    规则：
    - 站点：从名称中提取 "X#站" -> station_id = "station_X"
    - 系统类型：根据关键词判断（冷机、锅炉、三联供、发电机等）
    - 设备ID：从名称中提取设备编号
    - 主题：根据名称判断（温度、压力、流量、功率等）
    - 单位：根据主题推断
    """

    # 提取站点ID
    station_match = re.search(r'(\d+)#站', point_name)
    if station_match:
        station_id = f"station_{station_match.group(1)}"
    else:
        station_id = "unknown"

    # 判断系统类型
    if '冷机' in point_name or 'JZ' in point_code or 'LDSCS' in point_code or 'LDSHS' in point_code:
        system_type = 'chiller'
    elif '锅炉' in point_name or 'Boiler' in point_code or 'BOILER' in point_code:
        system_type = 'boiler'
    elif '三联供' in point_name or 'TT' in point_code:
        system_type = 'cchp'  # Combined Cooling, Heating and Power
    elif '发电机' in point_name or 'FDJ' in point_code or 'gen' in point_code:
        system_type = 'generator'
    elif '燃烧机' in point_name or 'Burner' in point_code:
        system_type = 'burner'
    else:
        system_type = 'other'

    # 提取设备ID
    equipment_match = re.search(r'(\d+)#(冷机|锅炉|三联供|发电机|燃烧机)', point_name)
    if equipment_match:
        equipment_id = f"{system_type}_{equipment_match.group(1)}"
    else:
        equipment_id = point_code

    # 判断主题（theme）
    if '温度' in point_name or '_T' in point_code or 'Temp' in point_code:
        theme = 'temperature'
        unit = '℃'
    elif '压力' in point_name or '_P' in point_code or 'Press' in point_code:
        theme = 'pressure'
        unit = 'MPa'
    elif '流量' in point_name or '_F' in point_code or 'Flow' in point_code:
        theme = 'flow'
        unit = 'm³/h'
    elif '功率' in point_name or 'Power' in point_code:
        theme = 'power'
        unit = 'kW'
    elif '运行' in point_name or 'RUN' in point_code or 'Run' in point_code:
        theme = 'status'
        unit = 'bool'
    elif '累计' in point_name or 'TOTAL' in point_code or 'ADDH' in point_code:
        theme = 'cumulative'
        unit = 'kWh'
    elif '时间' in point_name or 'runtime' in point_code or 'Runtimes' in point_code:
        theme = 'runtime'
        unit = 'hours'
    elif '次数' in point_name or 'Starttimes' in point_code:
        theme = 'count'
        unit = 'times'
    elif '负荷' in point_name or 'Load' in point_code:
        theme = 'load'
        unit = '%'
    else:
        theme = 'other'
        unit = 'unknown'

    return {
        'station_id': station_id,
        'system_type': system_type,
        'equipment_id': equipment_id,
        'theme': theme,
        'unit': unit
    }

def main():
    # 创建 Spark Session
    spark = (
        SparkSession.builder
        .appName("Parse_Point_Dictionary")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .getOrCreate()
    )

    print("=" * 60)
    print("开始解析点位字典...")
    print("=" * 60)

    # 解析 TTL 文件
    ttl_file = "/home/student/作业2数据/pos.ttl"
    points = parse_ttl_file(ttl_file)
    print(f"✅ 从 TTL 文件中提取了 {len(points)} 个点位")

    # 提取元数据
    enriched_points = []
    for point in points:
        metadata = extract_metadata(point['point_code'], point['point_name'])
        enriched_points.append({
            'point_code': point['point_code'],
            'point_name': point['point_name'],
            'station_id': metadata['station_id'],
            'system_type': metadata['system_type'],
            'equipment_id': metadata['equipment_id'],
            'theme': metadata['theme'],
            'unit': metadata['unit']
        })

    # 定义 Schema
    schema = StructType([
        StructField("point_code", StringType(), False),
        StructField("point_name", StringType(), False),
        StructField("station_id", StringType(), True),
        StructField("system_type", StringType(), True),
        StructField("equipment_id", StringType(), True),
        StructField("theme", StringType(), True),
        StructField("unit", StringType(), True)
    ])

    # 创建 DataFrame
    df = spark.createDataFrame(enriched_points, schema)

    # 添加元数据字段
    df = df.withColumn("created_at", current_timestamp()) \
           .withColumn("updated_at", current_timestamp()) \
           .withColumn("is_active", lit(True))

    print("\n📊 点位元数据样例：")
    df.show(10, truncate=False)

    print("\n📈 按系统类型统计：")
    df.groupBy("system_type").count().orderBy("count", ascending=False).show()

    print("\n📈 按主题统计：")
    df.groupBy("theme").count().orderBy("count", ascending=False).show()

    # 写入 Delta Lake
    output_path = "hdfs://node1:9000/lake/silver/silver_point_meta_dim"
    print(f"\n💾 写入 Delta Lake: {output_path}")

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)

    print("✅ 点位字典维表生成完成！")
    print("=" * 60)

    # 验证写入
    print("\n🔍 验证写入结果：")
    df_read = spark.read.format("delta").load(output_path)
    print(f"   总记录数: {df_read.count()}")
    print(f"   字段列表: {df_read.columns}")

    spark.stop()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从Kafka采集数据到Bronze层
对比与直接加载本地文件的差异
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def create_spark_session():
    """创建Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Load_Kafka_To_Bronze")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    print("=" * 80)
    print("从Kafka采集数据到Bronze层")
    print("=" * 80)

    spark = create_spark_session()

    # Kafka配置 - 连接到Docker容器内的Kafka
    kafka_bootstrap_servers = "localhost:9092"
    
    # 定义传感器数据schema
    sensor_schema = StructType([
        StructField("timestamp", StringType(), False),
        StructField("value", DoubleType(), False)
    ])

    # Bronze层输出路径
    bronze_path = "hdfs://node1:9000/lake/bronze/bronze_sensor_kafka"

    print(f"\n📡 Kafka配置:")
    print(f"   Bootstrap Servers: {kafka_bootstrap_servers}")
    print(f"   输出路径: {bronze_path}")

    # 读取Kafka数据（批处理模式，读取所有历史数据）
    print("\n📥 从Kafka读取数据...")
    
    try:
        df_kafka = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribePattern", "sensor\\..*") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()

        print(f"   ✓ Kafka连接成功")

        # 解析Kafka消息
        df_parsed = df_kafka.selectExpr(
            "topic",
            "CAST(key AS STRING) as sensor_id",
            "CAST(value AS STRING) as json_value",
            "partition as partition_id",
            "offset as offset_id",
            "timestamp as push_time"
        )

        # 解析JSON数据
        df_parsed = df_parsed.withColumn(
            "data",
            from_json(col("json_value"), sensor_schema)
        )

        # 展开数据
        df_final = df_parsed.select(
            col("data.timestamp").alias("timestamp"),
            col("data.value").alias("value"),
            col("sensor_id"),
            col("topic").alias("source_topic"),
            col("partition_id"),
            col("offset_id"),
            col("push_time").cast(StringType()),
            current_timestamp().alias("ingest_time"),
            to_date(col("data.timestamp")).alias("dt")
        ).withColumn("label", col("sensor_id")) \
         .withColumn("is_simulated", lit(False))

        # 统计信息
        total_records = df_final.count()
        sensor_count = df_final.select("sensor_id").distinct().count()
        date_count = df_final.select("dt").distinct().count()
        
        print(f"\n📊 采集统计:")
        print(f"   总记录数: {total_records:,}")
        print(f"   传感器数量: {sensor_count}")
        print(f"   日期范围: {date_count} 天")

        if total_records == 0:
            print("\n⚠️  Kafka中没有数据！")
            print("   请确保生产者正在运行")
            spark.stop()
            return

        # 显示样例数据
        print("\n📋 数据样例:")
        df_final.select("sensor_id", "timestamp", "value", "source_topic", "dt").show(10, truncate=False)

        # 写入Bronze层
        print(f"\n💾 写入Bronze层: {bronze_path}")
        df_final.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("dt") \
            .save(bronze_path)

        print("\n✅ Kafka数据采集完成！")

        # 验证写入结果
        print("\n🔍 验证写入结果...")
        df_verify = spark.read.format("delta").load(bronze_path)
        verify_count = df_verify.count()
        verify_sensors = df_verify.select("sensor_id").distinct().count()
        verify_dates = df_verify.select("dt").distinct().count()

        print(f"   总记录数: {verify_count:,}")
        print(f"   传感器数量: {verify_sensors}")
        print(f"   日期范围: {verify_dates} 天")

    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()

    print("\n" + "=" * 80)
    print("✅ 任务完成！")
    print("=" * 80)

if __name__ == "__main__":
    main()

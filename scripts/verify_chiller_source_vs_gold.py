#!/usr/bin/env python3
"""Verify chiller source data against Gold hourly supply values."""

from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_timestamp,
    when,
)


SILVER_CHILLER_STATUS = "hdfs://node1:9000/lake/silver/silver_chiller_status"
GOLD_CHILLER_HOURLY = "hdfs://node1:9000/lake/gold/gold_supply_curve_hourly"
GOLD_SYSTEM_HOURLY = "hdfs://node1:9000/lake/gold/gold_system_supply_hourly"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("VerifyChillerSourceVsGold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    spark = create_spark_session()
    try:
        silver = (
            spark.read.format("delta")
            .load(SILVER_CHILLER_STATUS)
            .withColumn("stat_ts", to_timestamp(col("stat_time"), "yyyy-MM-dd HH:mm:ss"))
        )
        gold = spark.read.format("delta").load(GOLD_CHILLER_HOURLY)
        system_gold = spark.read.format("delta").load(GOLD_SYSTEM_HOURLY).filter(col("system_type") == "chiller")

        print("SILVER_CHILLER_SOURCE_TOTAL_BEGIN")
        silver.groupBy("equipment_id").agg(
            count("*").alias("source_rows"),
            spark_min("stat_time").alias("min_time"),
            spark_max("stat_time").alias("max_time"),
            spark_sum(when(col("run_flag") > 0, 1).otherwise(0)).alias("running_samples"),
            spark_sum(when(col("power").isNotNull() & (col("power") > 0), 1).otherwise(0)).alias("positive_power_samples"),
            spark_sum(when(col("flow").isNotNull() & (col("flow") > 0), 1).otherwise(0)).alias("positive_flow_samples"),
            avg(when(col("run_flag") > 0, col("power"))).alias("avg_running_power"),
            avg(when(col("run_flag") > 0, col("flow"))).alias("avg_running_flow"),
        ).orderBy("equipment_id").show(50, truncate=False)
        print("SILVER_CHILLER_SOURCE_TOTAL_END")

        latest_time = silver.agg(spark_max("stat_ts").alias("latest_ts")).collect()[0]["latest_ts"]
        print(f"LATEST_SOURCE_TS|{latest_time}")

        recent_24 = silver.filter(col("stat_ts") >= lit(latest_time - timedelta(hours=24)))
        print("SILVER_CHILLER_SOURCE_LAST_24H_BEGIN")
        recent_24.groupBy("equipment_id").agg(
            count("*").alias("source_rows_24h"),
            spark_sum(when(col("run_flag") > 0, 1).otherwise(0)).alias("running_samples_24h"),
            spark_sum(when(col("power").isNotNull() & (col("power") > 0), 1).otherwise(0)).alias("positive_power_samples_24h"),
            spark_sum(when(col("flow").isNotNull() & (col("flow") > 0), 1).otherwise(0)).alias("positive_flow_samples_24h"),
            avg("run_flag").alias("avg_run_flag_24h"),
            avg("power").alias("avg_power_24h"),
            avg("flow").alias("avg_flow_24h"),
        ).orderBy("equipment_id").show(50, truncate=False)
        print("SILVER_CHILLER_SOURCE_LAST_24H_END")

        recent = silver.filter(col("stat_ts") >= lit(latest_time - timedelta(hours=96)))
        print("SILVER_CHILLER_SOURCE_LAST_96H_BEGIN")
        recent.groupBy("equipment_id").agg(
            count("*").alias("source_rows_96h"),
            spark_sum(when(col("run_flag") > 0, 1).otherwise(0)).alias("running_samples_96h"),
            spark_sum(when(col("power").isNotNull() & (col("power") > 0), 1).otherwise(0)).alias("positive_power_samples_96h"),
            spark_sum(when(col("flow").isNotNull() & (col("flow") > 0), 1).otherwise(0)).alias("positive_flow_samples_96h"),
            avg("run_flag").alias("avg_run_flag_96h"),
            avg("power").alias("avg_power_96h"),
            avg("flow").alias("avg_flow_96h"),
        ).orderBy("equipment_id").show(50, truncate=False)
        print("SILVER_CHILLER_SOURCE_LAST_96H_END")

        print("GOLD_CHILLER_TOTAL_BEGIN")
        gold.groupBy("equipment_id").agg(
            count("*").alias("gold_hours"),
            spark_sum("cooling_supply_kwh").alias("total_cooling_kwh"),
            spark_sum("energy_consumption_kwh").alias("total_energy_kwh"),
            spark_sum(when(col("cooling_supply_kwh") > 0, 1).otherwise(0)).alias("positive_supply_hours"),
            spark_max("stat_hour").alias("max_hour"),
        ).orderBy("equipment_id").show(50, truncate=False)
        print("GOLD_CHILLER_TOTAL_END")

        gold_with_ts = gold.withColumn("stat_ts", to_timestamp(col("stat_hour"), "yyyy-MM-dd HH:mm:ss"))
        latest_hour = gold_with_ts.agg(spark_max("stat_ts").alias("latest_hour")).collect()[0]["latest_hour"]
        print(f"LATEST_GOLD_HOUR|{latest_hour}")

        recent_gold_24 = gold_with_ts.filter(col("stat_ts") >= lit(latest_hour - timedelta(hours=24)))
        print("GOLD_CHILLER_LAST_24H_BEGIN")
        recent_gold_24.groupBy("equipment_id").agg(
            count("*").alias("gold_hours_24h"),
            spark_sum("cooling_supply_kwh").alias("cooling_kwh_24h"),
            spark_sum("energy_consumption_kwh").alias("energy_kwh_24h"),
            spark_sum(when(col("cooling_supply_kwh") > 0, 1).otherwise(0)).alias("positive_supply_hours_24h"),
            avg("operation_rate").alias("avg_operation_rate_24h"),
        ).orderBy("equipment_id").show(50, truncate=False)
        print("GOLD_CHILLER_LAST_24H_END")

        recent_gold = gold_with_ts.filter(col("stat_ts") >= lit(latest_hour - timedelta(hours=96)))
        print("GOLD_CHILLER_LAST_96H_BEGIN")
        recent_gold.groupBy("equipment_id").agg(
            count("*").alias("gold_hours_96h"),
            spark_sum("cooling_supply_kwh").alias("cooling_kwh_96h"),
            spark_sum("energy_consumption_kwh").alias("energy_kwh_96h"),
            spark_sum(when(col("cooling_supply_kwh") > 0, 1).otherwise(0)).alias("positive_supply_hours_96h"),
            avg("operation_rate").alias("avg_operation_rate_96h"),
        ).orderBy("equipment_id").show(50, truncate=False)
        print("GOLD_CHILLER_LAST_96H_END")

        print("GOLD_SYSTEM_MATCH_BEGIN")
        system_gold.groupBy("equipment_id").agg(
            spark_sum("supply_kwh").alias("system_total_supply_kwh"),
            spark_sum("cooling_supply_kwh").alias("system_total_cooling_kwh"),
            spark_sum(when(col("supply_kwh") > 0, 1).otherwise(0)).alias("system_positive_supply_hours"),
        ).orderBy("equipment_id").show(50, truncate=False)
        print("GOLD_SYSTEM_MATCH_END")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

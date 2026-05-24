#!/usr/bin/env python3
"""Verify raw flow, temperature-difference, and cumulative energy mappings.

This script compares Silver raw point roles with the Gold hourly tables for
chiller, heating, and CCHP systems.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    countDistinct,
    lit,
    lower,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_timestamp,
    when,
)
from pyspark.sql.window import Window


HDFS_ROOT = "hdfs://node1:9000/lake"
SILVER_META = f"{HDFS_ROOT}/silver/silver_point_meta_dim"
SILVER_FACT = f"{HDFS_ROOT}/silver/silver_point_fact"
SILVER_CHILLER_STATUS = f"{HDFS_ROOT}/silver/silver_chiller_status"
GOLD_CHILLER_HOURLY = f"{HDFS_ROOT}/gold/gold_supply_curve_hourly"
GOLD_SYSTEM_HOURLY = f"{HDFS_ROOT}/gold/gold_system_supply_hourly"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("VerifyRawFieldMapping")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def section(name):
    print(f"\n{name}_BEGIN")


def end_section(name):
    print(f"{name}_END")


def candidate_filter(df):
    text = lower(
        concat_ws(
            " ",
            col("point_code"),
            col("point_name"),
            col("theme"),
            col("measure_role"),
            col("unit"),
        )
    )
    return df.filter(
        (col("theme").isin("flow", "temperature", "cumulative", "energy"))
        | (col("measure_role").isin("flow", "supply_temp", "return_temp"))
        | text.contains("flow")
        | text.contains("temp")
        | text.contains("累计")
        | text.contains("热量")
        | text.contains("能量")
        | text.contains("流量")
        | text.contains("供水")
        | text.contains("回水")
        | col("point_code").isin("14BOILER_GAS_TOTAL", "23BOILER_GAS_TOTAL", "FT2005_RL_C", "P9_ADDH", "FDJ_GAS_TOTAL")
        | col("point_code").rlike("^SLG_JZ[0-9]+_DD_YG_ZZ1$")
    )


def main():
    spark = create_spark_session()
    try:
        meta = spark.read.format("delta").load(SILVER_META).cache()
        fact = spark.read.format("delta").load(SILVER_FACT).cache()
        chiller_status = spark.read.format("delta").load(SILVER_CHILLER_STATUS).cache()
        chiller_gold = spark.read.format("delta").load(GOLD_CHILLER_HOURLY).cache()
        system_gold = spark.read.format("delta").load(GOLD_SYSTEM_HOURLY).cache()

        section("SILVER_META_ROLE_SUMMARY")
        meta.filter(col("system_type").isin("chiller", "boiler", "burner", "cchp", "generator")).groupBy(
            "system_type", "theme", "measure_role", "unit"
        ).agg(
            count("*").alias("point_count"),
            countDistinct("equipment_id").alias("equipment_count"),
        ).orderBy("system_type", "theme", "measure_role", "unit").show(300, truncate=False)
        end_section("SILVER_META_ROLE_SUMMARY")

        section("SILVER_FACT_ROLE_SUMMARY")
        fact.filter(col("system_type").isin("chiller", "boiler", "burner", "cchp", "generator")).groupBy(
            "system_type", "theme", "measure_role", "unit"
        ).agg(
            count("*").alias("rows"),
            countDistinct("equipment_id").alias("equipment_count"),
            countDistinct("point_code").alias("point_count"),
            spark_min("event_time").alias("min_event_time"),
            spark_max("event_time").alias("max_event_time"),
            spark_min("value").alias("min_value"),
            avg("value").alias("avg_value"),
            spark_max("value").alias("max_value"),
            spark_sum(when(col("value") > 0, 1).otherwise(0)).alias("positive_rows"),
        ).orderBy("system_type", "theme", "measure_role", "unit").show(300, truncate=False)
        end_section("SILVER_FACT_ROLE_SUMMARY")

        section("SILVER_CANDIDATE_POINTS")
        candidate_filter(
            fact.filter(col("system_type").isin("chiller", "boiler", "burner", "cchp", "generator"))
        ).groupBy(
            "system_type",
            "equipment_id",
            "point_code",
            "point_name",
            "theme",
            "measure_role",
            "unit",
        ).agg(
            count("*").alias("rows"),
            spark_min("value").alias("min_value"),
            avg("value").alias("avg_value"),
            spark_max("value").alias("max_value"),
            spark_sum(when(col("value") > 0, 1).otherwise(0)).alias("positive_rows"),
        ).orderBy("system_type", "equipment_id", "theme", "measure_role", "point_code").show(400, truncate=False)
        end_section("SILVER_CANDIDATE_POINTS")

        section("SILVER_CHILLER_STATUS_FIELD_SUMMARY")
        chiller_status.groupBy("equipment_id").agg(
            count("*").alias("rows"),
            spark_min("stat_time").alias("min_stat_time"),
            spark_max("stat_time").alias("max_stat_time"),
            spark_sum(when(col("flow").isNotNull(), 1).otherwise(0)).alias("flow_not_null_rows"),
            spark_sum(when(col("flow") > 0, 1).otherwise(0)).alias("positive_flow_rows"),
            avg("flow").alias("avg_flow"),
            spark_sum(when(col("supply_temp").isNotNull(), 1).otherwise(0)).alias("supply_temp_not_null_rows"),
            spark_sum(when(col("return_temp").isNotNull(), 1).otherwise(0)).alias("return_temp_not_null_rows"),
            avg(col("return_temp") - col("supply_temp")).alias("avg_temp_diff"),
            spark_sum(when((col("return_temp") - col("supply_temp")) > 0, 1).otherwise(0)).alias("positive_temp_diff_rows"),
            spark_sum(when(col("run_flag") > 0, 1).otherwise(0)).alias("running_rows"),
        ).orderBy("equipment_id").show(100, truncate=False)
        end_section("SILVER_CHILLER_STATUS_FIELD_SUMMARY")

        section("GOLD_CHILLER_HOURLY_FIELD_SUMMARY")
        chiller_gold.groupBy("equipment_id").agg(
            count("*").alias("hours"),
            spark_sum("cooling_supply_kwh").alias("total_cooling_supply_kwh"),
            spark_sum("energy_consumption_kwh").alias("total_energy_consumption_kwh"),
            spark_sum(when(col("cooling_supply_kwh") > 0, 1).otherwise(0)).alias("positive_supply_hours"),
            spark_sum(when(col("avg_flow").isNotNull(), 1).otherwise(0)).alias("avg_flow_not_null_hours"),
            spark_sum(when(col("avg_flow") > 0, 1).otherwise(0)).alias("positive_avg_flow_hours"),
            avg("avg_flow").alias("avg_hourly_flow"),
            spark_sum(when(col("avg_supply_temp").isNotNull(), 1).otherwise(0)).alias("supply_temp_not_null_hours"),
            spark_sum(when(col("avg_return_temp").isNotNull(), 1).otherwise(0)).alias("return_temp_not_null_hours"),
            avg(col("avg_return_temp") - col("avg_supply_temp")).alias("avg_temp_diff"),
            spark_sum(when((col("avg_return_temp") - col("avg_supply_temp")) > 0, 1).otherwise(0)).alias("positive_temp_diff_hours"),
        ).orderBy("equipment_id").show(100, truncate=False)
        end_section("GOLD_CHILLER_HOURLY_FIELD_SUMMARY")

        section("GOLD_SYSTEM_HOURLY_FIELD_SUMMARY")
        thermal_temp_diff = when(
            col("system_type") == "chiller",
            col("avg_return_temp") - col("avg_supply_temp"),
        ).otherwise(col("avg_supply_temp") - col("avg_return_temp"))
        system_gold.groupBy("system_type", "equipment_id").agg(
            count("*").alias("hours"),
            spark_min("stat_hour").alias("min_hour"),
            spark_max("stat_hour").alias("max_hour"),
            spark_sum("supply_kwh").alias("total_supply_kwh"),
            spark_sum("cooling_supply_kwh").alias("total_cooling_supply_kwh"),
            spark_sum("heating_supply_kwh").alias("total_heating_supply_kwh"),
            spark_sum("electric_supply_kwh").alias("total_electric_supply_kwh"),
            spark_sum("energy_consumption_kwh").alias("total_energy_consumption_kwh"),
            spark_sum(when(col("supply_kwh") > 0, 1).otherwise(0)).alias("positive_supply_hours"),
            spark_sum(when(col("avg_flow").isNotNull(), 1).otherwise(0)).alias("avg_flow_not_null_hours"),
            spark_sum(when(col("avg_flow") > 0, 1).otherwise(0)).alias("positive_avg_flow_hours"),
            avg("avg_flow").alias("avg_hourly_flow"),
            spark_sum(when(col("avg_supply_temp").isNotNull(), 1).otherwise(0)).alias("supply_temp_not_null_hours"),
            spark_sum(when(col("avg_return_temp").isNotNull(), 1).otherwise(0)).alias("return_temp_not_null_hours"),
            avg(thermal_temp_diff).alias("avg_thermal_temp_diff"),
            spark_sum(when(thermal_temp_diff > 0, 1).otherwise(0)).alias("positive_thermal_temp_diff_hours"),
        ).orderBy("system_type", "equipment_id").show(200, truncate=False)
        end_section("GOLD_SYSTEM_HOURLY_FIELD_SUMMARY")

        section("CUMULATIVE_DELTA_MAPPING_SUMMARY")
        key_points = fact.filter(
            col("point_code").isin("14BOILER_GAS_TOTAL", "23BOILER_GAS_TOTAL", "FT2005_RL_C", "P9_ADDH", "FDJ_GAS_TOTAL")
            | col("point_code").rlike("^SLG_JZ[0-9]+_DD_YG_ZZ1$")
        ).withColumn("event_ts", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX"))
        w = Window.partitionBy("station_id", "point_code").orderBy("event_ts")
        key_delta = key_points.withColumn("prev_value", spark_min("value").over(w.rowsBetween(-1, -1))).withColumn(
            "raw_delta", col("value") - col("prev_value")
        )
        key_delta.groupBy(
            "system_type",
            "equipment_id",
            "point_code",
            "point_name",
            "theme",
            "measure_role",
            "unit",
        ).agg(
            count("*").alias("rows"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            spark_sum(when(col("raw_delta") > 0, col("raw_delta")).otherwise(0.0)).alias("sum_positive_raw_delta"),
            spark_sum(when(col("raw_delta") > 0, 1).otherwise(0)).alias("positive_delta_rows"),
            spark_sum(when(col("raw_delta") < 0, 1).otherwise(0)).alias("negative_delta_rows"),
        ).orderBy("system_type", "point_code").show(100, truncate=False)
        end_section("CUMULATIVE_DELTA_MAPPING_SUMMARY")

        fact.unpersist()
        meta.unpersist()
        chiller_status.unpersist()
        chiller_gold.unpersist()
        system_gold.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Compact raw-to-Gold mapping summary for flow, temperature difference, and cumulative energy."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
)


HDFS_ROOT = "hdfs://node1:9000/lake"
SILVER_META = f"{HDFS_ROOT}/silver/silver_point_meta_dim"
SILVER_FACT = f"{HDFS_ROOT}/silver/silver_point_fact"
GOLD_SYSTEM_HOURLY = f"{HDFS_ROOT}/gold/gold_system_supply_hourly"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("CheckRawMappingSummary")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def print_section(name):
    print(f"\n{name}_BEGIN")


def end_section(name):
    print(f"{name}_END")


def main():
    spark = create_spark_session()
    try:
        meta = spark.read.format("delta").load(SILVER_META)
        fact = spark.read.format("delta").load(SILVER_FACT)
        gold = spark.read.format("delta").load(GOLD_SYSTEM_HOURLY)

        print_section("SOURCE_REQUIRED_FIELD_COVERAGE")
        fact.filter(col("system_type").isin("chiller", "boiler", "burner", "cchp", "generator")).groupBy(
            "system_type", "theme", "measure_role", "unit"
        ).agg(
            count("*").alias("fact_rows"),
            countDistinct("equipment_id").alias("equipment_count"),
            countDistinct("point_code").alias("point_count"),
            spark_sum(when(col("value") > 0, 1).otherwise(0)).alias("positive_rows"),
            spark_min("event_time").alias("min_event_time"),
            spark_max("event_time").alias("max_event_time"),
        ).orderBy("system_type", "theme", "measure_role", "unit").show(80, truncate=False)
        end_section("SOURCE_REQUIRED_FIELD_COVERAGE")

        print_section("META_FLOW_POINTS_WITHOUT_FACT_CHECK")
        meta.filter(col("measure_role") == "flow").select(
            "system_type", "equipment_id", "point_code", "point_name", "theme", "measure_role", "unit"
        ).orderBy("system_type", "equipment_id", "point_code").show(80, truncate=False)
        fact.filter(col("measure_role") == "flow").groupBy("system_type", "equipment_id", "point_code").agg(
            count("*").alias("fact_rows"),
            spark_sum(when(col("value") > 0, 1).otherwise(0)).alias("positive_rows"),
        ).orderBy("system_type", "equipment_id", "point_code").show(80, truncate=False)
        end_section("META_FLOW_POINTS_WITHOUT_FACT_CHECK")

        print_section("GOLD_FIELD_MAPPING_BY_EQUIPMENT")
        thermal_temp_diff = when(
            col("system_type") == "chiller",
            col("avg_return_temp") - col("avg_supply_temp"),
        ).otherwise(col("avg_supply_temp") - col("avg_return_temp"))
        gold.groupBy("system_type", "equipment_id").agg(
            count("*").alias("gold_hours"),
            spark_sum("supply_kwh").alias("total_supply_kwh"),
            spark_sum("cooling_supply_kwh").alias("total_cooling_kwh"),
            spark_sum("heating_supply_kwh").alias("total_heating_kwh"),
            spark_sum("electric_supply_kwh").alias("total_electric_kwh"),
            spark_sum(when(col("supply_kwh") > 0, 1).otherwise(0)).alias("positive_supply_hours"),
            spark_sum(when(col("avg_flow").isNotNull(), 1).otherwise(0)).alias("flow_not_null_hours"),
            spark_sum(when(col("avg_flow") > 0, 1).otherwise(0)).alias("positive_flow_hours"),
            avg("avg_flow").alias("avg_flow"),
            spark_sum(when(col("avg_supply_temp").isNotNull(), 1).otherwise(0)).alias("supply_temp_hours"),
            spark_sum(when(col("avg_return_temp").isNotNull(), 1).otherwise(0)).alias("return_temp_hours"),
            avg(thermal_temp_diff).alias("avg_thermal_temp_diff"),
            spark_sum(when(thermal_temp_diff > 0, 1).otherwise(0)).alias("positive_temp_diff_hours"),
        ).orderBy("system_type", "equipment_id").show(40, truncate=False)
        end_section("GOLD_FIELD_MAPPING_BY_EQUIPMENT")

        print_section("CUMULATIVE_ENERGY_SOURCE_POINTS")
        fact.filter(
            col("point_code").isin("14BOILER_GAS_TOTAL", "23BOILER_GAS_TOTAL", "FT2005_RL_C", "P9_ADDH", "FDJ_GAS_TOTAL")
            | col("point_code").rlike("^SLG_JZ[0-9]+_DD_YG_ZZ1$")
        ).groupBy("system_type", "equipment_id", "point_code", "point_name", "theme", "unit").agg(
            count("*").alias("rows"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
        ).orderBy("system_type", "point_code").show(40, truncate=False)
        end_section("CUMULATIVE_ENERGY_SOURCE_POINTS")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

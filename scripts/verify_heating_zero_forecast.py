#!/usr/bin/env python3
"""Summarize heating historical, forecast, and revenue values by equipment."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    sum as spark_sum,
    when,
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


GOLD_SYSTEM_HOURLY = "hdfs://node1:9000/lake/gold/gold_system_supply_hourly"
GOLD_SYSTEM_FORECAST = "hdfs://node1:9000/lake/gold/gold_system_forecast_supply"
GOLD_SYSTEM_REVENUE = "hdfs://node1:9000/lake/gold/gold_system_revenue_forecast"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("VerifyHeatingZeroForecast")
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
        hourly = spark.read.format("delta").load(GOLD_SYSTEM_HOURLY).filter(col("system_type") == "heating")
        forecast = spark.read.format("delta").load(GOLD_SYSTEM_FORECAST).filter(col("system_type") == "heating")
        revenue = spark.read.format("delta").load(GOLD_SYSTEM_REVENUE).filter(col("system_type") == "heating")

        print("HEATING_HIST_TOTAL_BEGIN")
        hourly.groupBy("equipment_id").agg(
            count("*").alias("hours"),
            spark_sum("supply_kwh").alias("total_supply_kwh"),
            spark_sum("heating_supply_kwh").alias("total_heating_kwh"),
            spark_sum("energy_consumption_kwh").alias("total_energy_kwh"),
            spark_sum(when(col("supply_kwh") > 0, 1).otherwise(0)).alias("positive_supply_hours"),
            avg("operation_rate").alias("avg_operation_rate"),
            spark_max("stat_hour").alias("max_hour"),
        ).orderBy("equipment_id").show(20, truncate=False)
        print("HEATING_HIST_TOTAL_END")

        latest_window = Window.partitionBy("equipment_id").orderBy(col("stat_hour").desc())
        recent = hourly.withColumn("rn", row_number().over(latest_window)).filter(col("rn") <= 96)
        print("HEATING_HIST_LAST_96H_BEGIN")
        recent.groupBy("equipment_id").agg(
            count("*").alias("hours_96h"),
            spark_sum("supply_kwh").alias("supply_kwh_96h"),
            spark_sum("heating_supply_kwh").alias("heating_kwh_96h"),
            spark_sum("energy_consumption_kwh").alias("energy_kwh_96h"),
            spark_sum(when(col("supply_kwh") > 0, 1).otherwise(0)).alias("positive_supply_hours_96h"),
            avg("operation_rate").alias("avg_operation_rate_96h"),
        ).orderBy("equipment_id").show(20, truncate=False)
        print("HEATING_HIST_LAST_96H_END")

        print("HEATING_FORECAST_BEGIN")
        forecast.groupBy("equipment_id").agg(
            count("*").alias("forecast_hours"),
            spark_sum("predicted_supply_kwh").alias("predicted_supply_kwh"),
            spark_sum("predicted_heating_kwh").alias("predicted_heating_kwh"),
            spark_sum("predicted_energy_kwh").alias("predicted_energy_kwh"),
            spark_sum(when(col("predicted_supply_kwh") > 0, 1).otherwise(0)).alias("positive_forecast_hours"),
            spark_max("current_operation_status").alias("current_operation_status"),
        ).orderBy("equipment_id").show(20, truncate=False)
        print("HEATING_FORECAST_END")

        print("HEATING_REVENUE_BEGIN")
        revenue.groupBy("equipment_id").agg(
            count("*").alias("revenue_hours"),
            spark_sum("predicted_supply_revenue").alias("predicted_supply_revenue"),
            spark_sum("predicted_energy_cost").alias("predicted_energy_cost"),
            spark_sum("predicted_profit").alias("predicted_profit"),
        ).orderBy("equipment_id").show(20, truncate=False)
        print("HEATING_REVENUE_END")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

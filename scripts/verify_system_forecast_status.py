#!/usr/bin/env python3
"""Verify unified forecast status fields after model training."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


GOLD_SYSTEM_FORECAST = "hdfs://node1:9000/lake/gold/gold_system_forecast_supply"
GOLD_SYSTEM_FORECAST_METRICS = "hdfs://node1:9000/lake/gold/gold_system_forecast_metrics"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("VerifySystemForecastStatus")
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
        forecast = spark.read.format("delta").load(GOLD_SYSTEM_FORECAST)
        metrics = spark.read.format("delta").load(GOLD_SYSTEM_FORECAST_METRICS)

        print("FORECAST_SYSTEM_SUMMARY_BEGIN")
        forecast.groupBy("system_type", "algorithm").agg(
            spark_sum("predicted_supply_kwh").alias("predicted_supply_kwh"),
        ).orderBy("system_type").show(20, truncate=False)
        print("FORECAST_SYSTEM_SUMMARY_END")

        print("CHILLER_STATUS_SAMPLE_BEGIN")
        forecast.filter(
            (col("system_type") == "chiller")
            & col("equipment_id").isin("chiller_1", "chiller_10")
        ).select(
            "equipment_id",
            "target_hour",
            "predicted_supply_kwh",
            "recent_24_supply_kwh",
            "recent_24_operation_rate",
            "current_operation_status",
            "forecast_interpretation",
            "model_version",
        ).orderBy("equipment_id", "target_hour").show(6, truncate=False)
        print("CHILLER_STATUS_SAMPLE_END")

        print("METRICS_TEXT_SAMPLE_BEGIN")
        metrics.filter(
            (col("system_type") == "chiller")
            & (col("equipment_id") == "chiller_1")
        ).select(
            "equipment_id",
            "model_version",
            "algorithm",
            "result_summary",
            "model_reason",
        ).show(1, truncate=False)
        print("METRICS_TEXT_SAMPLE_END")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

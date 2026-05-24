#!/usr/bin/env python3
"""Inspect boiler gas cumulative deltas used by heating Gold generation."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    date_format,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.window import Window


SILVER_POINT_FACT = "hdfs://node1:9000/lake/silver/silver_point_fact"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("InspectHeatingGasDeltas")
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
        gas = (
            spark.read.format("delta")
            .load(SILVER_POINT_FACT)
            .filter(col("point_code").isin("14BOILER_GAS_TOTAL", "23BOILER_GAS_TOTAL"))
            .withColumn("event_ts", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX"))
            .withColumn("stat_hour", date_format(col("event_ts"), "yyyy-MM-dd HH:00:00"))
            .withColumn("dt", to_date(col("event_ts")))
            .withColumn("value_clean", when(col("value") >= 0, col("value")))
        )

        hourly_end = (
            gas.groupBy("station_id", "point_code", "stat_hour", "dt")
            .agg(spark_max("value_clean").alias("hour_end_value"))
        )
        w = Window.partitionBy("station_id", "point_code").orderBy("stat_hour")
        deltas = (
            hourly_end.withColumn("prev_hour_end_value", spark_max("hour_end_value").over(w.rowsBetween(-1, -1)))
            .withColumn("raw_delta", col("hour_end_value") - col("prev_hour_end_value"))
            .withColumn("accepted_200k", when((col("raw_delta") >= 0) & (col("raw_delta") <= 200000), col("raw_delta")))
            .withColumn("accepted_1m", when((col("raw_delta") >= 0) & (col("raw_delta") <= 1000000), col("raw_delta")))
        )

        print("GAS_HOURLY_DELTA_SUMMARY_BEGIN")
        deltas.groupBy("point_code").agg(
            count("*").alias("hour_count"),
            spark_min("hour_end_value").alias("min_hour_end_value"),
            spark_max("hour_end_value").alias("max_hour_end_value"),
            spark_sum(when(col("raw_delta") > 0, col("raw_delta")).otherwise(0.0)).alias("sum_positive_hourly_delta"),
            spark_sum("accepted_200k").alias("sum_accepted_200k"),
            spark_sum("accepted_1m").alias("sum_accepted_1m"),
            spark_sum(when(col("raw_delta") > 0, 1).otherwise(0)).alias("positive_delta_hours"),
            spark_sum(when(col("raw_delta") < 0, 1).otherwise(0)).alias("negative_delta_hours"),
            spark_max("raw_delta").alias("max_raw_delta"),
        ).orderBy("point_code").show(20, truncate=False)
        print("GAS_HOURLY_DELTA_SUMMARY_END")

        print("GAS_TOP_POSITIVE_DELTAS_BEGIN")
        deltas.filter(col("raw_delta") > 0).orderBy(col("raw_delta").desc()).show(80, truncate=False)
        print("GAS_TOP_POSITIVE_DELTAS_END")

        print("GAS_NEGATIVE_DELTAS_BEGIN")
        deltas.filter(col("raw_delta") < 0).orderBy("point_code", "stat_hour").show(80, truncate=False)
        print("GAS_NEGATIVE_DELTAS_END")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Print event-time and ingest-time ranges for lake Delta tables."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, min as spark_min


TABLES = {
    "bronze_sensor_raw": (
        "hdfs://node1:9000/lake/bronze/bronze_sensor_raw",
        "timestamp",
        "ingest_time",
    ),
    "bronze_sensor_kafka": (
        "hdfs://node1:9000/lake/bronze/bronze_sensor_kafka",
        "timestamp",
        "ingest_time",
    ),
    "silver_point_fact": (
        "hdfs://node1:9000/lake/silver/silver_point_fact",
        "event_time",
        "created_at",
    ),
    "silver_chiller_status": (
        "hdfs://node1:9000/lake/silver/silver_chiller_status",
        "stat_time",
        "created_at",
    ),
    "gold_supply_curve_hourly": (
        "hdfs://node1:9000/lake/gold/gold_supply_curve_hourly",
        "stat_hour",
        "created_at",
    ),
    "gold_report_daily": (
        "hdfs://node1:9000/lake/gold/gold_report_daily",
        "stat_date",
        "created_at",
    ),
    "gold_operation_advice": (
        "hdfs://node1:9000/lake/gold/gold_operation_advice",
        "advice_time",
        "created_at",
    ),
}


def create_spark():
    spark = (
        SparkSession.builder
        .appName("CheckDataTimeliness")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def summarize(spark, name, path, event_col, ingest_col):
    try:
        df = spark.read.format("delta").load(path)
        columns = df.columns
        aggs = [count("*").alias("rows")]
        if event_col in columns:
            aggs.extend([
                spark_min(col(event_col)).alias("min_event_time"),
                spark_max(col(event_col)).alias("max_event_time"),
            ])
        if ingest_col in columns:
            aggs.extend([
                spark_min(col(ingest_col)).alias("min_ingest_time"),
                spark_max(col(ingest_col)).alias("max_ingest_time"),
            ])
        if "dt" in columns:
            aggs.extend([
                spark_min(col("dt")).alias("min_dt"),
                spark_max(col("dt")).alias("max_dt"),
            ])
        row = df.agg(*aggs).collect()[0].asDict()
        payload = "|".join(f"{key}={value}" for key, value in row.items())
        print(f"TIMELINESS|{name}|{payload}")
    except Exception as exc:
        print(f"TIMELINESS|{name}|ERROR|{str(exc).splitlines()[0]}")


def main():
    spark = create_spark()
    try:
        for name, (path, event_col, ingest_col) in TABLES.items():
            summarize(spark, name, path, event_col, ingest_col)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

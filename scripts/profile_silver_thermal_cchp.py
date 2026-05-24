#!/usr/bin/env python3
"""Profile boiler/burner/cchp/generator point facts before Gold aggregation."""

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


SILVER_FACT = "hdfs://node1:9000/lake/silver/silver_point_fact"
SILVER_META = "hdfs://node1:9000/lake/silver/silver_point_meta_dim"
SYSTEMS = ["boiler", "burner", "cchp", "generator"]


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ProfileSilverThermalCchp")
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
        fact = spark.read.format("delta").load(SILVER_FACT).filter(col("system_type").isin(SYSTEMS))
        meta = spark.read.format("delta").load(SILVER_META).filter(col("system_type").isin(SYSTEMS))

        print("SCHEMA_FACT_BEGIN")
        fact.printSchema()
        print("SCHEMA_FACT_END")

        print("FACT_SYSTEM_SUMMARY_BEGIN")
        fact.groupBy("system_type").agg(
            count("*").alias("rows"),
            countDistinct("equipment_id").alias("equipment_count"),
            countDistinct("point_code").alias("point_count"),
            spark_min("event_time").alias("min_event_time"),
            spark_max("event_time").alias("max_event_time"),
        ).orderBy("system_type").show(50, truncate=False)
        print("FACT_SYSTEM_SUMMARY_END")

        print("META_ROLE_SUMMARY_BEGIN")
        meta.groupBy("system_type", "theme", "measure_role", "unit").agg(
            count("*").alias("point_count")
        ).orderBy("system_type", "theme", "measure_role", "unit").show(200, truncate=False)
        print("META_ROLE_SUMMARY_END")

        print("FACT_ROLE_VALUE_SUMMARY_BEGIN")
        fact.groupBy("system_type", "theme", "measure_role", "unit").agg(
            count("*").alias("rows"),
            countDistinct("equipment_id").alias("equipment_count"),
            countDistinct("point_code").alias("point_count"),
            spark_min("value").alias("min_value"),
            avg("value").alias("avg_value"),
            spark_max("value").alias("max_value"),
            spark_sum(when(col("value") > 0, 1).otherwise(0)).alias("positive_rows"),
        ).orderBy("system_type", "theme", "measure_role", "unit").show(300, truncate=False)
        print("FACT_ROLE_VALUE_SUMMARY_END")

        print("POINT_SAMPLE_BEGIN")
        fact.groupBy(
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
        ).orderBy(
            "system_type",
            "equipment_id",
            "theme",
            "measure_role",
            "point_code",
        ).show(220, truncate=False)
        print("POINT_SAMPLE_END")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Check which systems and price types currently exist in the Delta lake."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, max as spark_max, min as spark_min


TABLES = {
    "silver_point_meta_dim": "hdfs://node1:9000/lake/silver/silver_point_meta_dim",
    "silver_point_fact": "hdfs://node1:9000/lake/silver/silver_point_fact",
    "silver_price_dim": "hdfs://node1:9000/lake/silver/silver_price_dim",
    "gold_supply_curve_hourly": "hdfs://node1:9000/lake/gold/gold_supply_curve_hourly",
    "gold_report_daily": "hdfs://node1:9000/lake/gold/gold_report_daily",
}


def spark_session():
    spark = (
        SparkSession.builder
        .appName("CheckLakeSystemCoverage")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def show_group(df, name, cols):
    print(f"{name}_BEGIN")
    df.groupBy(*cols).agg(count("*").alias("rows")).orderBy(*cols).show(100, truncate=False)
    print(f"{name}_END")


def main():
    spark = spark_session()
    try:
        meta = spark.read.format("delta").load(TABLES["silver_point_meta_dim"])
        fact = spark.read.format("delta").load(TABLES["silver_point_fact"])
        price = spark.read.format("delta").load(TABLES["silver_price_dim"])
        gold_hourly = spark.read.format("delta").load(TABLES["gold_supply_curve_hourly"])
        gold_daily = spark.read.format("delta").load(TABLES["gold_report_daily"])

        print("TABLE|silver_point_meta_dim|rows={}|systems={}|equipment={}".format(
            meta.count(),
            meta.select("system_type").distinct().count(),
            meta.select("equipment_id").distinct().count(),
        ))
        show_group(meta, "META_SYSTEMS", ["system_type"])
        show_group(meta, "META_SYSTEM_EQUIPMENT", ["system_type", "equipment_id"])

        fact_stats = fact.agg(
            count("*").alias("rows"),
            countDistinct("system_type").alias("systems"),
            countDistinct("equipment_id").alias("equipment"),
            spark_min("event_time").alias("min_event_time"),
            spark_max("event_time").alias("max_event_time"),
        ).collect()[0].asDict()
        print("TABLE|silver_point_fact|" + "|".join(f"{k}={v}" for k, v in fact_stats.items()))
        show_group(fact, "FACT_SYSTEMS", ["system_type"])

        print("TABLE|silver_price_dim|rows={}|stations={}|price_types={}".format(
            price.count(),
            price.select("station_code").distinct().count(),
            price.select("price_type").distinct().count(),
        ))
        show_group(price, "PRICE_TYPES", ["price_type"])
        print("PRICE_SAMPLE_BEGIN")
        price.orderBy("station_code", "price_type").show(30, truncate=False)
        print("PRICE_SAMPLE_END")

        print("TABLE|gold_supply_curve_hourly|rows={}|equipment={}".format(
            gold_hourly.count(),
            gold_hourly.select("equipment_id").distinct().count(),
        ))
        show_group(gold_hourly, "GOLD_HOURLY_EQUIPMENT", ["equipment_id"])

        print("TABLE|gold_report_daily|rows={}|equipment={}".format(
            gold_daily.count(),
            gold_daily.select("equipment_id").distinct().count(),
        ))
        show_group(gold_daily, "GOLD_DAILY_EQUIPMENT", ["equipment_id"])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

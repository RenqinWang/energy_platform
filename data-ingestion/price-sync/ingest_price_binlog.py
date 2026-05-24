#!/usr/bin/env python3
"""Ingest price change events from the `/binlog_all` API into Bronze Delta.

The price API exposes `/full` as the current table snapshot and `/binlog_all`
as JSONL MySQL binlog events. This job converts WriteRowsEvent and
UpdateRowsEvent records for `energy_prices.prices` into the same business
columns used by `bronze_price_raw`, while preserving binlog lineage columns.
"""

import json
from typing import Any, Dict, Iterable, List

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


API_BASE_URL = "http://122.9.74.124:8000"
BINLOG_ENDPOINT = f"{API_BASE_URL}/binlog_all"
BRONZE_PATH = "hdfs://node1:9000/lake/bronze/bronze_price_raw"

PRICE_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("station_code", StringType(), True),
    StructField("price_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("source", StringType(), True),
    StructField("api_endpoint", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("log_file", StringType(), True),
    StructField("log_pos", LongType(), True),
    StructField("binlog_timestamp", LongType(), True),
    StructField("row_index", LongType(), True),
    StructField("event_id", StringType(), True),
])


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("PriceBinlogIngestion")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def map_price_values(values: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(values["UNKNOWN_COL0"]),
        "station_code": values["UNKNOWN_COL1"],
        "price_type": values["UNKNOWN_COL2"],
        "price": float(values["UNKNOWN_COL3"]),
        "created_at": values["UNKNOWN_COL4"],
        "updated_at": values["UNKNOWN_COL5"],
    }


def iter_binlog_events() -> Iterable[Dict[str, Any]]:
    with requests.get(BINLOG_ENDPOINT, stream=True, timeout=60) as response:
        response.raise_for_status()
        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue
            yield json.loads(line)


def fetch_price_binlog_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    event_count = 0

    for event in iter_binlog_events():
        if event.get("schema") != "energy_prices" or event.get("table") != "prices":
            continue
        if event.get("type") not in {"WriteRowsEvent", "UpdateRowsEvent"}:
            continue

        event_count += 1
        for row_index, row in enumerate(event.get("rows", [])):
            values = row.get("after_values") or row.get("values")
            if not values:
                continue
            parsed = map_price_values(values)
            parsed.update({
                "source": "binlog_all",
                "api_endpoint": BINLOG_ENDPOINT,
                "event_type": event["type"],
                "log_file": event.get("log_file"),
                "log_pos": int(event.get("log_pos") or 0),
                "binlog_timestamp": int(event.get("timestamp") or 0),
                "row_index": int(row_index),
                "event_id": f"{event.get('log_file')}:{event.get('log_pos')}:{row_index}:{parsed['id']}:{parsed['updated_at']}",
            })
            rows.append(parsed)

    print(f"Fetched {len(rows)} price rows from {event_count} binlog row events")
    return rows


def align_to_schema(df, schema):
    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df.select([field.name for field in schema.fields])


def write_bronze(spark, rows: List[Dict[str, Any]]):
    if not rows:
        raise RuntimeError("No price rows parsed from binlog")

    binlog_df = spark.createDataFrame(rows, PRICE_SCHEMA)
    binlog_df = binlog_df.withColumn("ingest_time", current_timestamp()).withColumn("dt", to_date(col("updated_at")))

    target_schema = StructType(PRICE_SCHEMA.fields + [
        StructField("ingest_time", StringType(), True),
    ])
    # Use the actual DataFrame dtypes for ingest_time by aligning columns through
    # Spark casts below instead of forcing a string in the final write.
    final_columns = [field.name for field in PRICE_SCHEMA.fields] + ["ingest_time", "dt"]

    try:
        existing = spark.read.format("delta").load(BRONZE_PATH)
        existing = align_to_schema(existing, binlog_df.select(final_columns).schema)
        combined = existing.unionByName(binlog_df.select(final_columns))
    except Exception as exc:
        if "Path does not exist" not in str(exc) and "is not a Delta table" not in str(exc):
            raise
        combined = binlog_df.select(final_columns)

    deduped = combined.dropDuplicates(["source", "event_id", "id", "updated_at"])
    print("Bronze price rows by source before write:")
    deduped.groupBy("source").count().orderBy("source").show(truncate=False)

    (
        deduped.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("dt")
        .save(BRONZE_PATH)
    )

    verify = spark.read.format("delta").load(BRONZE_PATH)
    print("Bronze price verification:")
    print(f"rows={verify.count()}")
    verify.groupBy("source").count().orderBy("source").show(truncate=False)
    verify.groupBy("price_type").count().orderBy("price_type").show(truncate=False)
    verify.select("station_code", "price_type", "price", "updated_at", "source", "event_type", "log_file", "log_pos").orderBy(
        col("updated_at").desc(), "station_code", "price_type"
    ).show(30, truncate=False)


def main():
    print("=" * 80)
    print("Price binlog ingestion started")
    print("=" * 80)
    rows = fetch_price_binlog_rows()
    spark = create_spark_session()
    try:
        write_bronze(spark, rows)
    finally:
        spark.stop()
    print("=" * 80)
    print("Price binlog ingestion completed")
    print("=" * 80)


if __name__ == "__main__":
    main()

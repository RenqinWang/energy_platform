#!/usr/bin/env python3
"""Incrementally build stream Silver tables from streaming Bronze data.

This job reads the simulated streaming Bronze Delta table, selects records
newer than the last processed ingest-time watermark, enriches them with the
point metadata dimension, and appends/merges them into stream Silver.
"""

import os
import argparse
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    lit,
    max as spark_max,
    min as spark_min,
    to_date,
    to_timestamp,
    when,
)


HDFS_ROOT = os.getenv("HDFS_LAKE_PATH", "hdfs://node1:9000/lake/stream")
FULL_HDFS_ROOT = os.getenv("FULL_HDFS_LAKE_PATH", "hdfs://node1:9000/lake/full")
CONTROL_ROOT = os.getenv("HDFS_CONTROL_PATH", "hdfs://node1:9000/lake/control")

BRONZE_STREAMING = os.getenv("BRONZE_STREAMING_PATH", f"{HDFS_ROOT}/bronze/bronze_streaming")
SILVER_POINT_FACT = f"{HDFS_ROOT}/silver/silver_point_fact"
SILVER_POINT_META = f"{HDFS_ROOT}/silver/silver_point_meta_dim"
SILVER_PRICE_DIM = f"{HDFS_ROOT}/silver/silver_price_dim"
WATERMARK_TABLE = f"{CONTROL_ROOT}/etl_watermark"
BATCH_POINT_FACT = f"{CONTROL_ROOT}/stream_last_batch_point_fact"
JOB_NAME = "stream_silver_point_fact"
STATUS_FILE = os.getenv("STREAM_MICROBATCH_STATUS_FILE", "/tmp/stream_silver_last_batch_rows")


def create_spark_session():
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    driver_host = os.getenv("SPARK_DRIVER_HOST", "192.168.0.94")
    hdfs_replication = os.getenv("HDFS_REPLICATION", "1")
    spark = (
        SparkSession.builder
        .appName("Stream_Microbatch_Silver")
        .master(spark_master)
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.hadoop.dfs.replication", hdfs_replication)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def is_delta_table(spark, path):
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def path_exists(spark, path):
    try:
        hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        return fs.exists(hadoop_path)
    except Exception:
        return False


def delete_path(spark, path):
    try:
        hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if fs.exists(hadoop_path):
            fs.delete(hadoop_path, True)
    except Exception:
        pass


def read_control_table(spark, path):
    if not path_exists(spark, path):
        return None
    try:
        return spark.read.parquet(path)
    except Exception:
        if is_delta_table(spark, path):
            return spark.read.format("delta").load(path)
        raise


def write_control_table(df, path):
    spark = df.sparkSession
    delete_path(spark, path)
    df.write.mode("overwrite").parquet(path)


def format_status_time(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def write_status(processed_rows, min_event_time=None, max_event_time=None, min_ingest_time=None, max_ingest_time=None):
    if not STATUS_FILE:
        return
    with open(STATUS_FILE, "w", encoding="utf-8") as status_file:
        status_file.write(f"ROWS={int(processed_rows)}\n")
        status_file.write(f"MIN_EVENT_TIME='{format_status_time(min_event_time)}'\n")
        status_file.write(f"MAX_EVENT_TIME='{format_status_time(max_event_time)}'\n")
        status_file.write(f"MIN_INGEST_TIME='{format_status_time(min_ingest_time)}'\n")
        status_file.write(f"MAX_INGEST_TIME='{format_status_time(max_ingest_time)}'\n")


def read_status_file():
    values = {}
    if not STATUS_FILE or not os.path.exists(STATUS_FILE):
        return values
    with open(STATUS_FILE, "r", encoding="utf-8") as status_file:
        for raw_line in status_file:
            line = raw_line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key] = value.strip().strip("'").strip('"')
    return values


def write_last_batch_table(spark, silver_df):
    deduped = silver_df.dropDuplicates(["source_topic", "partition_id", "offset_id"])
    write_control_table(deduped, BATCH_POINT_FACT)
    print(f"Last batch detail table written: {BATCH_POINT_FACT}")


def stage_last_batch_from_status(spark):
    status = read_status_file()
    min_ingest_time = status.get("MIN_INGEST_TIME")
    max_ingest_time = status.get("MAX_INGEST_TIME")
    if not min_ingest_time or not max_ingest_time:
        raise RuntimeError(f"Cannot stage last batch; missing ingest window in {STATUS_FILE}")
    if not is_delta_table(spark, SILVER_POINT_FACT):
        raise RuntimeError(f"Silver point fact is missing: {SILVER_POINT_FACT}")

    def read_batch(start_time, end_time):
        return (
            spark.read.format("delta")
            .load(SILVER_POINT_FACT)
            .filter(
                (col("ingest_time") >= to_timestamp(lit(start_time)))
                & (col("ingest_time") <= to_timestamp(lit(end_time)))
            )
        )

    batch_df = read_batch(min_ingest_time, max_ingest_time)
    batch_count = batch_df.count()

    if batch_count == 0:
        shifted_min = datetime.strptime(min_ingest_time[:19], "%Y-%m-%d %H:%M:%S") - timedelta(hours=8, seconds=1)
        shifted_max = datetime.strptime(max_ingest_time[:19], "%Y-%m-%d %H:%M:%S") - timedelta(hours=8) + timedelta(seconds=1)
        shifted_min_text = shifted_min.strftime("%Y-%m-%d %H:%M:%S")
        shifted_max_text = shifted_max.strftime("%Y-%m-%d %H:%M:%S")
        print(f"No rows with local ingest window; retrying as UTC window: {shifted_min_text} -> {shifted_max_text}")
        batch_df = read_batch(shifted_min_text, shifted_max_text)
        batch_count = batch_df.count()

    write_last_batch_table(spark, batch_df)
    print(f"Last batch detail rows staged from status: {batch_count:,}")


def copy_dimension_if_missing(spark, table_name):
    target = f"{HDFS_ROOT}/silver/{table_name}"
    source = f"{FULL_HDFS_ROOT}/silver/{table_name}"
    if is_delta_table(spark, target):
        print(f"Dimension exists: {target}")
        return

    if not is_delta_table(spark, source):
        raise RuntimeError(f"Required full dimension is missing or not Delta: {source}")

    print(f"Copying dimension {source} -> {target}")
    (
        spark.read.format("delta")
        .load(source)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target)
    )


def read_last_watermark(spark):
    watermark = read_control_table(spark, WATERMARK_TABLE)
    if watermark is None:
        return None

    rows = (
        watermark
        .filter(col("job_name") == JOB_NAME)
        .select("last_ingest_time")
        .collect()
    )
    if not rows:
        return None
    return rows[0]["last_ingest_time"]


def update_watermark(spark, processed_rows, last_ingest_time, last_event_time):
    watermark_df = spark.createDataFrame(
        [
            (
                JOB_NAME,
                BRONZE_STREAMING,
                SILVER_POINT_FACT,
                last_ingest_time,
                last_event_time,
                int(processed_rows),
            )
        ],
        "job_name string, source_path string, target_path string, "
        "last_ingest_time timestamp, last_event_time timestamp, processed_rows long",
    ).withColumn("updated_at", current_timestamp())

    write_control_table(watermark_df, WATERMARK_TABLE)


def mark_current_bronze_as_processed(spark):
    """Advance the watermark to the current Bronze max ingest time.

    This is used when switching Kafka ingestion to `latest` offsets: existing
    Bronze backlog remains available for inspection, but future micro-batches
    start at the cutover point instead of spending cycles on old Kafka data.
    """
    if not is_delta_table(spark, BRONZE_STREAMING):
        print("No streaming Bronze Delta table yet; watermark cutover skipped.")
        return

    bronze_df = spark.read.format("delta").load(BRONZE_STREAMING)
    watermarks = bronze_df.agg(
        spark_max("ingest_time").alias("last_ingest_time"),
        spark_max(to_timestamp(col("timestamp"))).alias("last_event_time"),
        count("*").alias("bronze_rows"),
    ).collect()[0]

    last_ingest_time = watermarks["last_ingest_time"]
    if last_ingest_time is None:
        print("Bronze table has no ingest_time; watermark cutover skipped.")
        return

    update_watermark(
        spark,
        0,
        last_ingest_time,
        watermarks["last_event_time"],
    )
    print("Watermark cutover completed")
    print(f"Bronze rows at cutover: {watermarks['bronze_rows']:,}")
    print(f"Cutover ingest_time: {last_ingest_time}")
    print(f"Cutover event_time: {watermarks['last_event_time']}")


def build_silver_fact(spark, bronze_df):
    meta_df = (
        spark.read.format("delta")
        .load(SILVER_POINT_META)
        .dropDuplicates(["point_code"])
    )

    joined = bronze_df.join(
        meta_df.select(
            col("point_code").alias("sensor_id"),
            "point_name",
            "station_id",
            "system_type",
            "equipment_id",
            "theme",
            "unit",
            "measure_role",
        ),
        on="sensor_id",
        how="left",
    )

    unmatched = joined.filter(col("point_name").isNull()).select("sensor_id").distinct().count()
    if unmatched:
        print(f"Warning: {unmatched} sensor_id values were not found in silver_point_meta_dim")

    return (
        joined.select(
            col("station_id"),
            col("sensor_id").alias("point_code"),
            col("point_name"),
            col("system_type"),
            col("equipment_id"),
            col("theme"),
            col("measure_role"),
            to_timestamp(col("timestamp")).alias("event_time"),
            col("value").cast("double").alias("value"),
            col("unit"),
            col("is_simulated"),
            col("source_topic"),
            col("partition_id").cast("int").alias("partition_id"),
            col("offset_id").cast("long").alias("offset_id"),
            col("ingest_time"),
            to_date(col("timestamp")).alias("dt"),
        )
        .filter(col("event_time").isNotNull())
        .withColumn("quality_flag", when(col("value").isNull(), lit("missing")).otherwise(lit("normal")))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
    )


def upsert_silver_fact(spark, silver_df):
    deduped = silver_df.dropDuplicates(["source_topic", "partition_id", "offset_id"])

    if not is_delta_table(spark, SILVER_POINT_FACT):
        (
            deduped.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("dt")
            .save(SILVER_POINT_FACT)
        )
        return deduped.count()

    (
        DeltaTable.forPath(spark, SILVER_POINT_FACT)
        .alias("t")
        .merge(
            deduped.alias("s"),
            "t.dt = s.dt "
            "AND t.source_topic = s.source_topic "
            "AND t.partition_id = s.partition_id "
            "AND t.offset_id = s.offset_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return deduped.count()


def parse_args():
    parser = argparse.ArgumentParser(description="Build stream Silver point fact incrementally")
    parser.add_argument(
        "--mark-current",
        action="store_true",
        help="Advance the Silver watermark to current Bronze max ingest_time without processing rows",
    )
    parser.add_argument(
        "--stage-from-status",
        action="store_true",
        help="Rebuild the last-batch detail table from the local status ingest-time window",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print("=" * 80)
    print("Stream Silver micro-batch started")
    print("=" * 80)
    print(f"Stream root: {HDFS_ROOT}")
    print(f"Bronze input: {BRONZE_STREAMING}")
    print(f"Silver output: {SILVER_POINT_FACT}")
    print(f"Watermark table: {WATERMARK_TABLE}")

    spark = create_spark_session()

    copy_dimension_if_missing(spark, "silver_point_meta_dim")
    copy_dimension_if_missing(spark, "silver_price_dim")

    if args.mark_current:
        mark_current_bronze_as_processed(spark)
        spark.stop()
        print("=" * 80)
        print("Stream Silver watermark cutover finished")
        print("=" * 80)
        return

    if args.stage_from_status:
        stage_last_batch_from_status(spark)
        spark.stop()
        print("=" * 80)
        print("Stream Silver last-batch staging finished")
        print("=" * 80)
        return

    if not is_delta_table(spark, BRONZE_STREAMING):
        print("No streaming Bronze Delta table yet. Start Kafka-to-Bronze first.")
        write_status(0)
        spark.stop()
        return

    last_watermark = read_last_watermark(spark)
    bronze_df = spark.read.format("delta").load(BRONZE_STREAMING)
    if last_watermark is not None:
        bronze_df = bronze_df.filter(col("ingest_time") > lit(last_watermark))

    batch_count = bronze_df.count()
    print(f"New Bronze rows: {batch_count:,}")
    if batch_count == 0:
        write_status(0)
        spark.stop()
        return

    silver_df = build_silver_fact(spark, bronze_df).cache()
    processed_rows = upsert_silver_fact(spark, silver_df)
    write_last_batch_table(spark, silver_df)

    watermarks = silver_df.agg(
        spark_min("ingest_time").alias("min_ingest_time"),
        spark_max("ingest_time").alias("last_ingest_time"),
        spark_min("event_time").alias("min_event_time"),
        spark_max("event_time").alias("last_event_time"),
        count("*").alias("silver_rows"),
    ).collect()[0]

    write_status(
        processed_rows,
        min_event_time=watermarks["min_event_time"],
        max_event_time=watermarks["last_event_time"],
        min_ingest_time=watermarks["min_ingest_time"],
        max_ingest_time=watermarks["last_ingest_time"],
    )

    update_watermark(
        spark,
        processed_rows,
        watermarks["last_ingest_time"],
        watermarks["last_event_time"],
    )

    print(f"Silver rows processed: {processed_rows:,}")
    print(f"Last ingest_time: {watermarks['last_ingest_time']}")
    print(f"Last event_time: {watermarks['last_event_time']}")

    silver_df.unpersist()
    spark.stop()
    print("=" * 80)
    print("Stream Silver micro-batch finished")
    print("=" * 80)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Ensure an existing Parquet table path is registered as a Delta table."""

import argparse
import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def build_spark(master: str) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "192.168.0.94")
    os.environ.setdefault("SPARK_DRIVER_HOST", "192.168.0.94")
    spark = (
        SparkSession.builder
        .appName("Ensure_Delta_Table")
        .master(master)
        .config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert an existing parquet table to Delta if needed")
    parser.add_argument("--path", required=True, help="HDFS path to the table")
    parser.add_argument("--partition-schema", default="dt date", help="DDL partition schema")
    parser.add_argument("--master", default="local[*]", help="Spark master")
    args = parser.parse_args()

    spark = build_spark(args.master)
    try:
        if DeltaTable.isDeltaTable(spark, args.path):
            print(f"Delta table already exists at {args.path}")
            return 0

        try:
            sample = spark.read.parquet(args.path).limit(1).count()
            if sample == 0:
                print(f"Path {args.path} exists but has no readable parquet rows; skipping conversion")
                return 0
        except Exception as exc:
            print(f"Path {args.path} is not a readable parquet table: {exc}")
            return 0

        print(f"Converting parquet table to Delta at {args.path}")
        DeltaTable.convertToDelta(
            spark,
            f"parquet.`{args.path}`",
            args.partition_schema,
        )

        if not DeltaTable.isDeltaTable(spark, args.path):
            raise RuntimeError(f"Conversion finished but {args.path} is still not recognized as Delta")

        print(f"Conversion complete: {args.path}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

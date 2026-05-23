#!/usr/bin/env python3
"""Verify Silver layer governance fixes."""

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum as spark_sum, when


def create_spark(master: str) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "192.168.0.94")
    os.environ.setdefault("SPARK_DRIVER_HOST", "192.168.0.94")
    spark = (
        SparkSession.builder
        .appName("Verify_Silver_Governance")
        .master(master)
        .config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> int:
    master = os.getenv("SPARK_MASTER", "local[*]")
    base = os.getenv("HDFS_LAKE_PATH", "hdfs://node1:9000/lake")
    spark = create_spark(master)

    try:
        bronze = spark.read.format("delta").load(f"{base}/bronze/bronze_sensor_raw")
        meta = spark.read.format("delta").load(f"{base}/silver/silver_point_meta_dim")
        fact = spark.read.format("delta").load(f"{base}/silver/silver_point_fact")
        chiller = spark.read.format("delta").load(f"{base}/silver/silver_chiller_status")

        meta_count = meta.count()
        meta_distinct = meta.select(countDistinct("point_code")).collect()[0][0]
        duplicate_point_codes = (
            meta.groupBy("point_code").count()
            .filter(col("count") > 1)
            .count()
        )
        slg_jz_bad = (
            meta.filter(col("point_code").contains("SLG_JZ"))
            .filter(col("system_type") != "generator")
            .count()
        )

        nonnull_exprs = [
            spark_sum(when(col(name).isNotNull(), 1).otherwise(0)).alias(name)
            for name in [
                "supply_temp",
                "return_temp",
                "pressure",
                "flow",
                "power",
                "runtime_hours",
                "start_count",
                "run_flag",
            ]
        ]
        chiller_nonnull = chiller.agg(*nonnull_exprs).collect()[0].asDict()

        summary = {
            "bronze_count": bronze.count(),
            "meta_count": meta_count,
            "meta_distinct_point_code": meta_distinct,
            "duplicate_point_codes": duplicate_point_codes,
            "pt2005_p_count": meta.filter(col("point_code") == "PT2005_P").count(),
            "slg_jz_non_generator_count": slg_jz_bad,
            "fact_count": fact.count(),
            "fact_unmatched_rows": fact.filter(col("point_name").isNull()).count(),
            "chiller_count": chiller.count(),
            "chiller_nonnull": chiller_nonnull,
        }

        print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))

        ok = (
            summary["meta_count"] == summary["meta_distinct_point_code"]
            and summary["duplicate_point_codes"] == 0
            and summary["pt2005_p_count"] == 1
            and summary["slg_jz_non_generator_count"] == 0
            and summary["fact_count"] == summary["bronze_count"]
            and summary["fact_unmatched_rows"] == 0
            and summary["chiller_nonnull"]["return_temp"] > 0
            and summary["chiller_nonnull"]["pressure"] > 0
            and summary["chiller_nonnull"]["flow"] > 0
            and summary["chiller_nonnull"]["power"] > 0
            and summary["chiller_nonnull"]["run_flag"] > 0
        )
        return 0 if ok else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

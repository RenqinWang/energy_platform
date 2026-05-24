#!/usr/bin/env python3
"""Benchmark single-core vs parallel Spark workloads on the HDFS Delta lake.

The validation keeps input data and business logic identical, then compares:

- local[1]: single Spark execution thread on one machine.
- local[*]: parallel Spark execution threads reading the same 3-node HDFS lake.

This is intentionally read-only. It does not overwrite Gold tables.
"""

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    lag,
    max as spark_max,
    min as spark_min,
    row_number,
    sum as spark_sum,
    to_date,
    to_timestamp,
)
from pyspark.sql.window import Window


HDFS_NAMENODE = "hdfs://node1:9000"
GOLD_SYSTEM_HOURLY = f"{HDFS_NAMENODE}/lake/gold/gold_system_supply_hourly"


WORKLOADS = [
    "report_generation",
    "history_query_window",
    "forecast_feature_window",
]


def create_spark(master: str, app_suffix: str, shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(f"DistributedEffectivenessBenchmark-{app_suffix}")
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def scaled_hourly(spark: SparkSession, scale_factor: int):
    base = spark.read.format("delta").load(GOLD_SYSTEM_HOURLY)
    scale = spark.range(scale_factor).withColumnRenamed("id", "sim_id")
    return (
        base.crossJoin(scale)
        .withColumn("sim_station_id", concat_ws("_sim_", col("station_id"), col("sim_id").cast("string")))
        .withColumn("stat_ts", to_timestamp(col("stat_hour"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("stat_date", to_date(col("stat_ts")))
    )


def workload_report_generation(df):
    report = (
        df.groupBy("sim_station_id", "system_type", "equipment_id", "stat_date")
        .agg(
            spark_max("supply_kwh").alias("peak_supply_kwh"),
            spark_min("supply_kwh").alias("valley_supply_kwh"),
            spark_sum("supply_kwh").alias("total_supply_kwh"),
            spark_sum("energy_consumption_kwh").alias("total_energy_consumption_kwh"),
            avg("operation_rate").alias("avg_operation_rate"),
            count("*").alias("hour_count"),
        )
    )
    row = report.agg(
        count("*").alias("output_rows"),
        spark_sum("total_supply_kwh").alias("supply_checksum"),
        spark_sum("total_energy_consumption_kwh").alias("energy_checksum"),
    ).collect()[0]
    return {
        "output_rows": int(row["output_rows"]),
        "supply_checksum": float(row["supply_checksum"] or 0.0),
        "energy_checksum": float(row["energy_checksum"] or 0.0),
    }


def workload_history_query_window(df):
    window = Window.partitionBy("sim_station_id", "system_type", "equipment_id").orderBy(col("stat_ts").desc())
    latest = (
        df.withColumn("rn", row_number().over(window))
        .filter(col("rn") <= 168)
        .groupBy("sim_station_id", "system_type", "equipment_id")
        .agg(
            count("*").alias("latest_hours"),
            spark_sum("supply_kwh").alias("latest_supply_kwh"),
            spark_sum("energy_consumption_kwh").alias("latest_energy_kwh"),
        )
    )
    row = latest.agg(
        count("*").alias("output_rows"),
        spark_sum("latest_hours").alias("total_latest_hours"),
        spark_sum("latest_supply_kwh").alias("supply_checksum"),
    ).collect()[0]
    return {
        "output_rows": int(row["output_rows"]),
        "total_latest_hours": int(row["total_latest_hours"] or 0),
        "supply_checksum": float(row["supply_checksum"] or 0.0),
    }


def workload_forecast_feature_window(df):
    order_window = Window.partitionBy("sim_station_id", "system_type", "equipment_id").orderBy("stat_ts")
    rolling_24 = order_window.rowsBetween(-24, -1)
    features = (
        df.withColumn("lag_24h", lag("supply_kwh", 24).over(order_window))
        .withColumn("rolling_24h_avg", avg("supply_kwh").over(rolling_24))
        .withColumn("rolling_24h_max", spark_max("supply_kwh").over(rolling_24))
        .filter(col("rolling_24h_avg").isNotNull())
        .groupBy("system_type")
        .agg(
            count("*").alias("feature_rows"),
            avg("rolling_24h_avg").alias("avg_rolling_24h_supply"),
            spark_max("rolling_24h_max").alias("max_rolling_24h_supply"),
        )
    )
    row = features.agg(
        spark_sum("feature_rows").alias("output_rows"),
        avg("avg_rolling_24h_supply").alias("rolling_checksum"),
    ).collect()[0]
    return {
        "output_rows": int(row["output_rows"] or 0),
        "rolling_checksum": float(row["rolling_checksum"] or 0.0),
    }


def timed(workload_name, fn, df):
    started = time.perf_counter()
    result = fn(df)
    elapsed = time.perf_counter() - started
    return {
        "workload": workload_name,
        "elapsed_sec": round(elapsed, 4),
        **result,
    }


def run_mode(mode, scale_factor, trials, shuffle_partitions):
    spark = create_spark(mode["master"], mode["name"], shuffle_partitions)
    try:
        hourly = spark.read.format("delta").load(GOLD_SYSTEM_HOURLY)
        source_rows = hourly.count()
        source_partitions = hourly.rdd.getNumPartitions()
        default_parallelism = spark.sparkContext.defaultParallelism

        # Warm up Delta metadata and class loading outside measured workloads.
        hourly.limit(1).count()

        trial_results = []
        workload_functions = {
            "report_generation": workload_report_generation,
            "history_query_window": workload_history_query_window,
            "forecast_feature_window": workload_forecast_feature_window,
        }
        for trial_index in range(1, trials + 1):
            df = scaled_hourly(spark, scale_factor)
            for workload in WORKLOADS:
                result = timed(workload, workload_functions[workload], df)
                result["trial"] = trial_index
                trial_results.append(result)

        return {
            "name": mode["name"],
            "master": mode["master"],
            "source_rows": int(source_rows),
            "source_partitions": int(source_partitions),
            "scale_factor": int(scale_factor),
            "simulated_rows": int(source_rows * scale_factor),
            "default_parallelism": int(default_parallelism),
            "shuffle_partitions": int(shuffle_partitions),
            "trials": trial_results,
        }
    finally:
        spark.stop()


def summarize(results):
    by_mode = {mode["name"]: mode for mode in results["modes"]}
    summary = []
    for workload in WORKLOADS:
        row = {"workload": workload}
        for mode_name, mode in by_mode.items():
            values = [item["elapsed_sec"] for item in mode["trials"] if item["workload"] == workload]
            row[mode_name] = {
                "avg_sec": round(sum(values) / len(values), 4),
                "min_sec": round(min(values), 4),
                "max_sec": round(max(values), 4),
            }
        single = row["single_local_1"]["avg_sec"]
        parallel = row["parallel_local_all"]["avg_sec"]
        row["speedup"] = round(single / parallel, 3) if parallel > 0 else None
        summary.append(row)
    results["summary"] = summary
    return results


def write_outputs(results, output_dir):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_path / f"distributed_effectiveness_benchmark_{stamp}.json"
    latest_path = output_path / "distributed_effectiveness_benchmark_latest.json"
    payload = json.dumps(results, ensure_ascii=False, indent=2)
    json_path.write_text(payload + "\n", encoding="utf-8")
    latest_path.write_text(payload + "\n", encoding="utf-8")
    return str(json_path), str(latest_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale-factor", type=int, default=20)
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument("--single-master", default="local[1]")
    parser.add_argument("--parallel-master", default="local[*]")
    args = parser.parse_args()

    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    os.environ.setdefault("SPARK_HOME", "/home/student/spark-3.5.7-bin-hadoop3")

    modes = [
        {"name": "single_local_1", "master": args.single_master},
        {"name": "parallel_local_all", "master": args.parallel_master},
    ]

    results = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "hdfs_namenode": HDFS_NAMENODE,
        "gold_table": GOLD_SYSTEM_HOURLY,
        "validation_type": "performance_comparison",
        "scale_factor": args.scale_factor,
        "trials_per_mode": args.trials,
        "workloads": WORKLOADS,
        "modes": [],
    }

    for mode in modes:
        print(f"RUN_MODE_BEGIN|{mode['name']}|{mode['master']}")
        mode_result = run_mode(mode, args.scale_factor, args.trials, args.shuffle_partitions)
        results["modes"].append(mode_result)
        print(f"RUN_MODE_END|{mode['name']}")

    summarize(results)
    json_path, latest_path = write_outputs(results, args.output_dir)

    print("BENCHMARK_SUMMARY_BEGIN")
    for row in results["summary"]:
        single = row["single_local_1"]["avg_sec"]
        parallel = row["parallel_local_all"]["avg_sec"]
        print(f"{row['workload']}|single_avg={single}|parallel_avg={parallel}|speedup={row['speedup']}")
    print(f"RESULT_JSON|{json_path}")
    print(f"RESULT_LATEST|{latest_path}")
    print("BENCHMARK_SUMMARY_END")


if __name__ == "__main__":
    main()

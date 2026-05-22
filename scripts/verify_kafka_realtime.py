#!/usr/bin/env python3
"""
Verify the Kafka realtime ingestion path:
Kafka brokers/topics/offsets plus Delta bronze_sensor_kafka contents.
"""

import argparse
import json
import os
import subprocess
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, max as spark_max, min as spark_min


def run(cmd, check=True):
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


def kafka_exec(args, node="node2", check=True):
    cmd = [
        "ssh",
        f"student@{node}",
        "docker",
        "exec",
        "kafka",
        *args,
    ]
    return run(cmd, check=check)


def get_topics(bootstrap):
    result = kafka_exec([
        "kafka-topics",
        "--bootstrap-server",
        bootstrap,
        "--list",
    ])
    return sorted([line.strip() for line in result.stdout.splitlines() if line.strip()])


def get_offsets(bootstrap, topics):
    if not topics:
        return 0

    sample_topics = topics[:3]
    total = 0
    for topic in sample_topics:
        try:
            result = kafka_exec([
                "kafka-run-class",
                "kafka.tools.GetOffsetShell",
                "--broker-list",
                bootstrap,
                "--topic",
                topic,
                "--time",
                "-1",
            ], check=False)
            if result.returncode != 0:
                continue
            for line in result.stdout.splitlines():
                parts = line.strip().split(":")
                if len(parts) == 3:
                    total += int(parts[2])
        except Exception:
            continue
    return total


def create_spark(master):
    os.environ.setdefault("SPARK_LOCAL_IP", "192.168.0.94")
    os.environ.setdefault("SPARK_DRIVER_HOST", "192.168.0.94")
    spark = (
        SparkSession.builder
        .appName("Verify_Kafka_Realtime_Ingestion")
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def verify_delta(path, master):
    spark = create_spark(master)
    try:
        df = spark.read.format("delta").load(path)
        stats = df.agg(
            countDistinct("sensor_id").alias("sensor_count"),
            countDistinct("source_topic").alias("topic_count"),
            countDistinct("dt").alias("date_count"),
            spark_min("dt").alias("min_dt"),
            spark_max("dt").alias("max_dt"),
        ).collect()[0].asDict()
        stats["record_count"] = df.count()
        sample = [row.asDict() for row in df.orderBy(col("ingest_time").desc()).limit(3).collect()]
        return {"exists": True, "stats": stats, "sample": sample}
    except Exception as exc:
        return {"exists": False, "error": str(exc)}
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="192.168.1.87:9092")
    parser.add_argument("--delta-path", default="hdfs://node1:9000/lake/bronze/bronze_sensor_kafka")
    parser.add_argument("--master", default="spark://192.168.1.87:7077")
    parser.add_argument("--wait-seconds", type=int, default=0)
    args = parser.parse_args()

    if args.wait_seconds:
        time.sleep(args.wait_seconds)

    summary = {
        "bootstrap": args.bootstrap,
        "brokers": {},
        "topics": {},
        "delta": {},
    }

    for node, host in [("node2", "192.168.1.87"), ("node3", "192.168.1.19")]:
        result = run(["bash", "-lc", f"timeout 2 bash -lc 'cat < /dev/null > /dev/tcp/{host}/9092'"], check=False)
        summary["brokers"][node] = result.returncode == 0

    topics = get_topics(args.bootstrap)
    sensor_topics = [topic for topic in topics if topic.startswith("sensor_")]
    summary["topics"]["total"] = len(topics)
    summary["topics"]["sensor_total"] = len(sensor_topics)
    summary["topics"]["sensor_sample"] = sensor_topics[:10]
    summary["topics"]["latest_offsets_sample_total"] = get_offsets(args.bootstrap, sensor_topics)
    summary["delta"] = verify_delta(args.delta_path, args.master)

    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))

    ok = (
        all(summary["brokers"].values())
        and summary["topics"]["sensor_total"] > 0
        and summary["delta"].get("exists")
        and summary["delta"]["stats"]["record_count"] > 0
    )
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()

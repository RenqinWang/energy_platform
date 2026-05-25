#!/usr/bin/env python3
"""Collect one JSON snapshot for stream distributed-validation runs."""

import argparse
import json
import os
import subprocess
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


NODES = [
    ("node1", "local", "192.168.0.94"),
    ("node2", "student@node2", "192.168.1.87"),
    ("node3", "student@node3", "192.168.1.19"),
]


def run(
    cmd: List[str],
    timeout: int = 20,
    check: bool = False,
    input_text: Optional[str] = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=check,
        input=input_text,
    )


def run_shell(command: str, timeout: int = 20) -> subprocess.CompletedProcess:
    return run(["bash", "-lc", command], timeout=timeout)


def fetch_json(url: str, timeout: int = 5) -> Optional[Dict[str, Any]]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {"error": str(exc), "url": url}


def node_metrics_command() -> str:
    return r"""python3 - <<'PY'
import json
import os
import time

def cpu_times():
    values = list(map(int, open('/proc/stat', encoding='utf-8').readline().split()[1:]))
    idle = values[3] + values[4]
    total = sum(values)
    return idle, total

idle1, total1 = cpu_times()
time.sleep(0.25)
idle2, total2 = cpu_times()
total_delta = max(total2 - total1, 1)
idle_delta = max(idle2 - idle1, 0)
cpu_percent = round((1 - idle_delta / total_delta) * 100, 2)

mem = {}
for line in open('/proc/meminfo', encoding='utf-8'):
    parts = line.split()
    if parts:
        mem[parts[0].rstrip(':')] = int(parts[1])
mem_total = mem.get('MemTotal', 0)
mem_available = mem.get('MemAvailable', 0)
mem_used_percent = round((1 - mem_available / mem_total) * 100, 2) if mem_total else None

print(json.dumps({
    'loadavg': os.getloadavg(),
    'cpu_percent': cpu_percent,
    'mem_total_mb': round(mem_total / 1024, 1),
    'mem_available_mb': round(mem_available / 1024, 1),
    'mem_used_percent': mem_used_percent,
}, ensure_ascii=False))
PY"""


def collect_node_metrics() -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}
    command = node_metrics_command()
    for name, target, ip in NODES:
        if target == "local":
            result = run_shell(command, timeout=10)
        else:
            result = run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", target, command], timeout=15)
        if result.returncode == 0:
            try:
                metrics[name] = json.loads(result.stdout.strip())
            except Exception:
                metrics[name] = {"error": "failed_to_parse", "stdout": result.stdout[-500:]}
        else:
            metrics[name] = {"error": result.stderr.strip() or result.stdout.strip(), "ip": ip}
    return metrics


def collect_process_snapshot() -> Dict[str, str]:
    command = "ps -eo pid,comm,pcpu,pmem,args --sort=-pcpu | head -12"
    result = run_shell(command, timeout=10)
    return {
        "node1_top_cpu": result.stdout.strip(),
    }


def collect_kafka_offsets(bootstrap: str) -> Dict[str, Any]:
    script = f"""
set -e
topics=$(docker exec kafka kafka-topics --bootstrap-server {bootstrap} --list | grep '^sensor_' || true)
topic_count=$(printf '%s\n' "$topics" | sed '/^$/d' | wc -l)
total=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list {bootstrap} --time -1 2>/dev/null \
  | awk -F: '/^sensor_/ {{sum += $3}} END {{print sum + 0}}')
printf '{{"sensor_topic_count":%s,"latest_offsets_total":%s}}\n' "$topic_count" "$total"
"""
    result = run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "student@node2", "bash", "-s"],
        timeout=180,
        input_text=script,
    )
    if result.returncode != 0:
        return {"error": result.stderr.strip() or result.stdout.strip()}
    try:
        return json.loads(result.stdout.strip().splitlines()[-1])
    except Exception:
        return {"error": "failed_to_parse", "stdout": result.stdout[-1000:]}


def collect_lake_metrics(hdfs_root: str, spark_master: str) -> Dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import countDistinct, max as spark_max, min as spark_min, sum as spark_sum

    spark = (
        SparkSession.builder
        .appName("CollectStreamValidationLakeMetrics")
        .master(spark_master)
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "192.168.0.94"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    def table_stats(path: str, fields: List[str]) -> Dict[str, Any]:
        try:
            df = spark.read.format("delta").load(path)
            result: Dict[str, Any] = {"exists": True, "rows": df.count()}
            for field in fields:
                if field in df.columns:
                    row = df.agg(spark_min(field).alias("min"), spark_max(field).alias("max")).collect()[0]
                    result[f"min_{field}"] = str(row["min"]) if row["min"] is not None else None
                    result[f"max_{field}"] = str(row["max"]) if row["max"] is not None else None
            if "sensor_id" in df.columns:
                result["distinct_sensors"] = df.agg(countDistinct("sensor_id")).collect()[0][0]
            if "source_topic" in df.columns:
                result["distinct_topics"] = df.agg(countDistinct("source_topic")).collect()[0][0]
            if "supply_kwh" in df.columns:
                result["total_supply_kwh"] = float(df.agg(spark_sum("supply_kwh")).collect()[0][0] or 0.0)
            return result
        except Exception as exc:
            return {"exists": False, "error": str(exc)}

    try:
        return {
            "bronze_streaming": table_stats(f"{hdfs_root}/bronze/bronze_streaming", ["timestamp", "ingest_time", "dt"]),
            "silver_point_fact": table_stats(f"{hdfs_root}/silver/silver_point_fact", ["event_time", "dt"]),
            "gold_system_supply_hourly": table_stats(f"{hdfs_root}/gold/gold_system_supply_hourly", ["stat_hour", "dt"]),
            "gold_system_report_daily": table_stats(f"{hdfs_root}/gold/gold_system_report_daily", ["stat_date", "dt"]),
            "gold_system_forecast_supply": table_stats(f"{hdfs_root}/gold/gold_system_forecast_supply", ["target_hour", "dt"]),
            "gold_system_revenue_forecast": table_stats(f"{hdfs_root}/gold/gold_system_revenue_forecast", ["target_hour", "dt"]),
        }
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--spark-master", default="local[*]")
    parser.add_argument("--hdfs-root", default="hdfs://node1:9000/lake/stream")
    parser.add_argument("--include-lake", action="store_true")
    parser.add_argument("--output-dir", default="reports/stream_validation")
    args = parser.parse_args()

    snapshot: Dict[str, Any] = {
        "label": args.label,
        "mode": args.mode,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "bootstrap": args.bootstrap,
        "spark_master": args.spark_master,
        "producer": fetch_json("http://192.168.1.87:8000/producer/status"),
        "spark_cluster": fetch_json("http://192.168.1.87:8080/json/"),
        "nodes": collect_node_metrics(),
        "processes": collect_process_snapshot(),
        "kafka": collect_kafka_offsets(args.bootstrap),
    }
    if args.include_lake:
        snapshot["lake"] = collect_lake_metrics(args.hdfs_root, args.spark_master)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{args.label}.json"
    output_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(str(output_path))


if __name__ == "__main__":
    main()

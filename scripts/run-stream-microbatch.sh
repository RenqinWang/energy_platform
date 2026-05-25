#!/bin/bash
# Run one stream micro-batch, or loop every 5 minutes for simulated realtime.

set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
SPARK_HOME="${SPARK_HOME:-/home/student/spark-3.5.7-bin-hadoop3}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
HDFS_BIN="${HDFS_BIN:-/home/student/hadoop-3.3.6/bin/hdfs}"
HDFS_LAKE_PATH="${HDFS_LAKE_PATH:-hdfs://node1:9000/lake/stream}"
FULL_HDFS_LAKE_PATH="${FULL_HDFS_LAKE_PATH:-hdfs://node1:9000/lake/full}"
HDFS_CONTROL_PATH="${HDFS_CONTROL_PATH:-hdfs://node1:9000/lake/control}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-192.168.0.94}"
HDFS_REPLICATION="${HDFS_REPLICATION:-1}"
STREAM_SETREP_ENABLED="${STREAM_SETREP_ENABLED:-false}"
INTERVAL_SECONDS="${STREAM_MICROBATCH_INTERVAL_SECONDS:-300}"
STATUS_FILE="${STREAM_MICROBATCH_STATUS_FILE:-/tmp/stream_silver_last_batch_rows}"
LOCK_FILE="${STREAM_MICROBATCH_LOCK_FILE:-/tmp/stream_microbatch_loop.lock}"
MODE="${1:-once}"

export JAVA_HOME
export SPARK_HOME
export HDFS_LAKE_PATH
export FULL_HDFS_LAKE_PATH
export HDFS_CONTROL_PATH
export SPARK_MASTER
export SPARK_DRIVER_HOST
export HDFS_REPLICATION
export STREAM_SETREP_ENABLED
export STREAM_MICROBATCH_STATUS_FILE="$STATUS_FILE"
if [ "${PYSPARK_PYTHON:-}" = "" ] || [ "${PYSPARK_PYTHON:-}" = "python3" ]; then
  PYSPARK_PYTHON="/usr/bin/python3"
fi
if [ "${PYSPARK_DRIVER_PYTHON:-}" = "" ] || [ "${PYSPARK_DRIVER_PYTHON:-}" = "python3" ]; then
  PYSPARK_DRIVER_PYTHON="/usr/bin/python3"
fi
export PYSPARK_PYTHON
export PYSPARK_DRIVER_PYTHON
export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "Another stream micro-batch process is already running; exiting."
  exit 0
fi

setrep_if_exists() {
  local wait_flag="$1"
  shift
  local path

  if [ "$STREAM_SETREP_ENABLED" != "true" ] || [ "$HDFS_REPLICATION" = "1" ]; then
    return
  fi

  for path in "$@"; do
    if "$HDFS_BIN" dfs -test -e "$path" >/dev/null 2>&1; then
      if [ "$wait_flag" = "wait" ]; then
        "$HDFS_BIN" dfs -setrep -R -w "$HDFS_REPLICATION" "$path" >/dev/null 2>&1 || true
      else
        "$HDFS_BIN" dfs -setrep -R "$HDFS_REPLICATION" "$path" >/dev/null 2>&1 || true
      fi
    fi
  done
}

setrep_stream_paths() {
  setrep_if_exists nowait \
    "${HDFS_LAKE_PATH}/bronze/bronze_streaming" \
    "${HDFS_LAKE_PATH}/silver/silver_point_meta_dim" \
    "${HDFS_LAKE_PATH}/silver/silver_price_dim" \
    "${HDFS_LAKE_PATH}/silver/silver_point_fact" \
    "${HDFS_LAKE_PATH}/silver/silver_chiller_status" \
    "${HDFS_LAKE_PATH}/gold"
}

setrep_control_paths() {
  setrep_if_exists wait \
    "${HDFS_CONTROL_PATH}/etl_watermark" \
    "${HDFS_CONTROL_PATH}/stream_last_batch_point_fact"
}

run_once() {
  echo "=== Stream micro-batch started at $(date '+%F %T') ==="
  setrep_stream_paths
  setrep_control_paths
  rm -f "$STATUS_FILE"
  "$SPARK_HOME/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
    --conf "spark.driver.bindAddress=0.0.0.0" \
    --conf "spark.pyspark.python=${PYSPARK_PYTHON}" \
    --conf "spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON}" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON}" \
    --conf "spark.hadoop.dfs.replication=${HDFS_REPLICATION}" \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    "$PROJECT_HOME/data-processing/stream/stream_microbatch_silver.py"
  setrep_stream_paths
  setrep_control_paths

  SILVER_BATCH_ROWS=0
  STREAM_BATCH_MIN_EVENT_TIME=""
  STREAM_BATCH_MAX_EVENT_TIME=""
  if [ -f "$STATUS_FILE" ]; then
    # shellcheck disable=SC1090
    source "$STATUS_FILE"
    SILVER_BATCH_ROWS="${ROWS:-0}"
    STREAM_BATCH_MIN_EVENT_TIME="${MIN_EVENT_TIME:-}"
    STREAM_BATCH_MAX_EVENT_TIME="${MAX_EVENT_TIME:-}"
  fi
  if [ "$SILVER_BATCH_ROWS" = "0" ]; then
    echo "No new Bronze rows after watermark; downstream Gold refresh skipped."
    echo "=== Stream micro-batch finished at $(date '+%F %T') ==="
    return 0
  fi

  export STREAM_INCREMENTAL=true
  export STREAM_BATCH_ROWS="$SILVER_BATCH_ROWS"
  export STREAM_BATCH_MIN_EVENT_TIME
  export STREAM_BATCH_MAX_EVENT_TIME

  echo "Incremental batch rows: $STREAM_BATCH_ROWS"
  echo "Incremental event window: $STREAM_BATCH_MIN_EVENT_TIME -> $STREAM_BATCH_MAX_EVENT_TIME"

  if ! "$HDFS_BIN" dfs -test -e "${HDFS_LAKE_PATH}/silver/silver_point_fact/_delta_log"; then
    echo "No stream Silver fact table yet; downstream Gold refresh skipped."
    echo "Start Kafka-to-Bronze first, then run this micro-batch again."
    return 0
  fi

  "$SPARK_HOME/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
    --conf "spark.driver.bindAddress=0.0.0.0" \
    --conf "spark.pyspark.python=${PYSPARK_PYTHON}" \
    --conf "spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON}" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON}" \
    --conf "spark.hadoop.dfs.replication=${HDFS_REPLICATION}" \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    "$PROJECT_HOME/data-processing/silver-layer/generate_chiller_status.py"
  setrep_stream_paths

  "$SPARK_HOME/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
    --conf "spark.driver.bindAddress=0.0.0.0" \
    --conf "spark.pyspark.python=${PYSPARK_PYTHON}" \
    --conf "spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON}" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON}" \
    --conf "spark.hadoop.dfs.replication=${HDFS_REPLICATION}" \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    "$PROJECT_HOME/data-processing/gold-layer/generate_supply_curve.py"
  setrep_stream_paths

  "$SPARK_HOME/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
    --conf "spark.driver.bindAddress=0.0.0.0" \
    --conf "spark.pyspark.python=${PYSPARK_PYTHON}" \
    --conf "spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON}" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON}" \
    --conf "spark.hadoop.dfs.replication=${HDFS_REPLICATION}" \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    "$PROJECT_HOME/data-processing/gold-layer/generate_system_gold.py"
  setrep_stream_paths
  echo "=== Stream micro-batch finished at $(date '+%F %T') ==="
}

case "$MODE" in
  once)
    run_once
    ;;
  loop)
    while true; do
      run_once
      sleep "$INTERVAL_SECONDS"
    done
    ;;
  *)
    echo "Usage: $0 [once|loop]"
    exit 2
    ;;
esac

#!/bin/bash
# Cut the simulated streaming pipeline over to "new data only".
#
# This script does not delete lake data. It:
#   1. stops Kafka-to-Bronze and the 5-minute micro-batch loop,
#   2. advances the Silver watermark to the current Bronze max ingest_time,
#   3. clears the Kafka-to-Bronze checkpoint so `startingOffsets=latest` applies,
#   4. restarts Kafka-to-Bronze and the 5-minute micro-batch loop.

set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
SPARK_HOME="${SPARK_HOME:-/home/student/spark-3.5.7-bin-hadoop3}"
HADOOP_HOME="${HADOOP_HOME:-/home/student/hadoop-3.3.6}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/home/student/hdfs-conf}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
HDFS_LAKE_PATH="${HDFS_LAKE_PATH:-hdfs://node1:9000/lake/stream}"
FULL_HDFS_LAKE_PATH="${FULL_HDFS_LAKE_PATH:-hdfs://node1:9000/lake/full}"
HDFS_CONTROL_PATH="${HDFS_CONTROL_PATH:-hdfs://node1:9000/lake/control}"
CHECKPOINT_PATH="${BRONZE_KAFKA_CHECKPOINT_PATH:-hdfs://node1:9000/checkpoints/stream/kafka_to_bronze_streaming}"
KAFKA_PID_FILE="${KAFKA_PID_FILE:-/tmp/kafka_to_bronze_streaming.pid}"
MICROBATCH_PID_FILE="${MICROBATCH_PID_FILE:-/tmp/stream_microbatch_loop.pid}"
MICROBATCH_LOG_FILE="${MICROBATCH_LOG_FILE:-/tmp/stream_microbatch_loop.log}"

export JAVA_HOME
export SPARK_HOME
export HADOOP_HOME
export HADOOP_CONF_DIR
export HDFS_LAKE_PATH
export FULL_HDFS_LAKE_PATH
export HDFS_CONTROL_PATH
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}"
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

stop_pid_file() {
  local pid_file="$1"
  local name="$2"
  if [ ! -f "$pid_file" ]; then
    echo "$name is not running: no pid file"
    return
  fi

  local pid
  pid="$(cat "$pid_file")"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "$name is not running: stale pid $pid"
    rm -f "$pid_file"
    return
  fi

  echo "Stopping $name process group $pid"
  kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 30); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      echo "$name stopped"
      return
    fi
    sleep 1
  done

  echo "$name did not stop after 30s; killing process group $pid"
  kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
  rm -f "$pid_file"
}

stop_matching_process_groups() {
  local pattern="$1"
  local name="$2"
  local pids

  pids="$(pgrep -f "$pattern" 2>/dev/null || true)"
  if [ -z "$pids" ]; then
    return
  fi

  local pid
  for pid in $pids; do
    if [ "$pid" = "$$" ]; then
      continue
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      continue
    fi
    echo "Stopping stray $name process group $pid"
    kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
  done

  sleep 3
  for pid in $pids; do
    if [ "$pid" = "$$" ]; then
      continue
    fi
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stray $name $pid did not stop; killing"
      kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
    fi
  done
}

cd "$PROJECT_HOME"

echo "=== Cutting stream pipeline over to Kafka latest offsets ==="
stop_pid_file "$KAFKA_PID_FILE" "Kafka-to-Bronze"
stop_pid_file "$MICROBATCH_PID_FILE" "stream micro-batch loop"
stop_matching_process_groups "run-stream-microbatch.sh loop" "stream micro-batch loop"

echo "Advancing Silver watermark to current Bronze max ingest_time"
"$SPARK_HOME/bin/spark-submit" \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  "$PROJECT_HOME/data-processing/stream/stream_microbatch_silver.py" \
  --mark-current

echo "Removing Kafka-to-Bronze checkpoint: $CHECKPOINT_PATH"
hdfs dfs -rm -r -skipTrash "$CHECKPOINT_PATH" >/dev/null 2>&1 || true

echo "Restarting Kafka-to-Bronze from latest offsets"
KAFKA_STARTING_OFFSETS=latest RESET_KAFKA_CHECKPOINT=false "$PROJECT_HOME/scripts/start-kafka-to-bronze.sh"

echo "Restarting 5-minute stream micro-batch loop"
nohup setsid bash -lc "cd '$PROJECT_HOME' && ./scripts/run-stream-microbatch.sh loop" \
  > "$MICROBATCH_LOG_FILE" 2>&1 &
echo $! > "$MICROBATCH_PID_FILE"
echo "  PID: $(cat "$MICROBATCH_PID_FILE")"
echo "  Log: $MICROBATCH_LOG_FILE"

echo "=== Stream pipeline now consumes only records produced after this cutover ==="

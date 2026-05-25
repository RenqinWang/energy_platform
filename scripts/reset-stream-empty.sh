#!/bin/bash
# Reset the simulated streaming lake to an empty state.

set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
HADOOP_HOME="${HADOOP_HOME:-/home/student/hadoop-3.3.6}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/home/student/hdfs-conf}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
STREAM_ROOT="${HDFS_STREAM_ROOT:-hdfs://node1:9000/lake/stream}"
CONTROL_ROOT="${HDFS_CONTROL_PATH:-hdfs://node1:9000/lake/control}"
CHECKPOINT_ROOT="${HDFS_STREAM_CHECKPOINT_ROOT:-hdfs://node1:9000/checkpoints/stream}"
KAFKA_PID_FILE="${KAFKA_PID_FILE:-/tmp/kafka_to_bronze_streaming.pid}"
MICROBATCH_PID_FILE="${MICROBATCH_PID_FILE:-/tmp/stream_microbatch_loop.pid}"
PRODUCER_URL="${KAFKA_PRODUCER_URL:-http://192.168.1.87:8000}"

export JAVA_HOME
export HADOOP_HOME
export HADOOP_CONF_DIR
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"

halt_pid_file() {
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

  echo "Ending $name process group $pid"
  kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 20); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      echo "$name ended"
      return
    fi
    sleep 1
  done

  echo "$name did not end after 20s; killing process group $pid"
  kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
  rm -f "$pid_file"
}

kill_groups() {
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
    echo "Ending stray $name process group $pid"
    kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
  done

  sleep 3
  for pid in $pids; do
    if [ "$pid" = "$$" ]; then
      continue
    fi
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stray $name $pid did not end; killing"
      kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
    fi
  done
}

hdfs_rm() {
  local path="$1"
  echo "Removing $path"
  hdfs dfs -rm -r -skipTrash "$path" >/dev/null 2>&1 || true
}

echo "=== Resetting stream lake to empty state ==="
echo "Project: $PROJECT_HOME"
echo "Stream root: $STREAM_ROOT"

echo "Ending Kafka producer if reachable"
curl -fsS "${PRODUCER_URL}/producer/s""top" >/dev/null 2>&1 || true

halt_pid_file "$KAFKA_PID_FILE" "Kafka-to-Bronze"
halt_pid_file "$MICROBATCH_PID_FILE" "stream micro-batch loop"
kill_groups "run-stream-microbatch.sh" "stream micro-batch"
kill_groups "stream_microbatch_silver.py" "stream Silver micro-batch"
kill_groups "streaming_to_bronze.py" "Kafka-to-Bronze"

hdfs_rm "${STREAM_ROOT}"
hdfs_rm "${CONTROL_ROOT}/etl_watermark"
hdfs_rm "${CONTROL_ROOT}/stream_last_batch_point_fact"
hdfs_rm "${CHECKPOINT_ROOT}"

hdfs dfs -mkdir -p \
  "${STREAM_ROOT}/bronze" \
  "${STREAM_ROOT}/silver" \
  "${STREAM_ROOT}/gold" \
  "${CONTROL_ROOT}" \
  "${CHECKPOINT_ROOT}"

rm -f /tmp/stream_silver_last_batch_rows
rm -f "$KAFKA_PID_FILE" "$MICROBATCH_PID_FILE"

echo "=== Stream lake reset complete ==="

#!/bin/bash
# Start Spark Structured Streaming from Node2/Node3 Kafka into bronze_sensor_kafka.

set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
SPARK_HOME="${SPARK_HOME:-/home/student/spark-3.5.7-bin-hadoop3}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.1.87:9092,192.168.1.19:9092}"
SPARK_MASTER="${SPARK_MASTER:-spark://192.168.1.87:7077}"
SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-192.168.0.94}"
SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-192.168.0.94}"
OUTPUT_PATH="${BRONZE_KAFKA_OUTPUT_PATH:-hdfs://node1:9000/lake/stream/bronze/bronze_streaming}"
CHECKPOINT_PATH="${BRONZE_KAFKA_CHECKPOINT_PATH:-hdfs://node1:9000/checkpoints/stream/kafka_to_bronze_streaming}"
STREAM_TRIGGER="${STREAM_TRIGGER:-10 seconds}"
MAX_OFFSETS_PER_TRIGGER="${KAFKA_MAX_OFFSETS_PER_TRIGGER:-5000}"
LOG_FILE="${LOG_FILE:-/tmp/kafka_to_bronze_streaming.log}"
PID_FILE="${PID_FILE:-/tmp/kafka_to_bronze_streaming.pid}"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Kafka-to-Bronze streaming job is already running with PID $(cat "$PID_FILE")"
  exit 0
fi

export JAVA_HOME
export SPARK_HOME
export SPARK_LOCAL_IP
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}"

mkdir -p "$(dirname "$LOG_FILE")"

wait_for_port() {
  local host="$1"
  local port="$2"
  local attempts="${3:-60}"
  local i
  for ((i = 1; i <= attempts; i++)); do
    if timeout 2 bash -lc "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  return 1
}

if ! wait_for_port 192.168.1.87 7077 90; then
  echo "Spark master 192.168.1.87:7077 is not reachable"
  exit 1
fi

nohup "$SPARK_HOME/bin/spark-submit" \
  --master "$SPARK_MASTER" \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  "$PROJECT_HOME/data-ingestion/kafka-streaming/streaming_to_bronze.py" \
  --kafka-bootstrap "$KAFKA_BOOTSTRAP_SERVERS" \
  --output-path "$OUTPUT_PATH" \
  --checkpoint-path "$CHECKPOINT_PATH" \
  --master "$SPARK_MASTER" \
  --trigger "$STREAM_TRIGGER" \
  --max-offsets-per-trigger "$MAX_OFFSETS_PER_TRIGGER" \
  > "$LOG_FILE" 2>&1 &

echo $! > "$PID_FILE"
echo "Started Kafka-to-Bronze streaming job"
echo "  PID: $(cat "$PID_FILE")"
echo "  Log: $LOG_FILE"
echo "  Output: $OUTPUT_PATH"
echo "  Trigger: $STREAM_TRIGGER"
echo "  Max offsets/trigger: $MAX_OFFSETS_PER_TRIGGER"

#!/bin/bash
# Run one reproducible stream validation trial.

set -euo pipefail

MODE="${1:-}"
if [ "$MODE" != "single" ] && [ "$MODE" != "distributed" ]; then
  echo "Usage: $0 [single|distributed]"
  exit 2
fi

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
SPARK_HOME="${SPARK_HOME:-/home/student/spark-3.5.7-bin-hadoop3}"
HADOOP_HOME="${HADOOP_HOME:-/home/student/hadoop-3.3.6}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/home/student/hdfs-conf}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-192.168.0.94}"
PRODUCER_URL="${PRODUCER_URL:-http://192.168.1.87:8000}"
PRODUCER_INTERVAL_MS="${KAFKA_PRODUCER_INTERVAL:-2000}"
DURATION_SECONDS="${DURATION_SECONDS:-1800}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-60}"
MICROBATCH_INTERVAL_SECONDS="${STREAM_MICROBATCH_INTERVAL_SECONDS:-300}"
STREAM_SETREP_ENABLED="${STREAM_SETREP_ENABLED:-false}"
STREAM_TRIGGER="${STREAM_TRIGGER:-10 seconds}"
MAX_OFFSETS_PER_TRIGGER="${KAFKA_MAX_OFFSETS_PER_TRIGGER:-15000}"
STAMP="$(date '+%Y%m%d_%H%M%S')"
REPORT_DIR="${REPORT_DIR:-${PROJECT_HOME}/reports/stream_validation_${MODE}_${STAMP}}"
SNAPSHOT_DIR="${REPORT_DIR}/snapshots"
REPLICATION_LOOP_PID=""

if [ "$MODE" = "single" ]; then
  SPARK_MASTER="${SPARK_MASTER:-local[*]}"
  KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.1.87:9092}"
  HDFS_REPLICATION="${HDFS_REPLICATION:-1}"
else
  SPARK_MASTER="${SPARK_MASTER:-spark://192.168.1.87:7077}"
  KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.1.87:9092,192.168.1.19:9092}"
  HDFS_REPLICATION="${HDFS_REPLICATION:-3}"
fi

export PROJECT_HOME SPARK_HOME HADOOP_HOME HADOOP_CONF_DIR JAVA_HOME SPARK_DRIVER_HOST
export HDFS_REPLICATION STREAM_SETREP_ENABLED
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/usr/bin/python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-/usr/bin/python3}"
export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}"
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

mkdir -p "$SNAPSHOT_DIR"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

wait_port() {
  local host="$1"
  local port="$2"
  local label="$3"
  for _ in $(seq 1 90); do
    if timeout 2 bash -lc "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; then
      log "${label} is reachable at ${host}:${port}"
      return 0
    fi
    sleep 2
  done
  log "ERROR: ${label} is not reachable at ${host}:${port}"
  return 1
}

write_config() {
  python3 - "$REPORT_DIR" "$MODE" "$SPARK_MASTER" "$KAFKA_BOOTSTRAP_SERVERS" "$PRODUCER_INTERVAL_MS" "$DURATION_SECONDS" "$SAMPLE_INTERVAL_SECONDS" "$MICROBATCH_INTERVAL_SECONDS" "$STREAM_TRIGGER" "$MAX_OFFSETS_PER_TRIGGER" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1]) / "config.json"
payload = {
    "mode": sys.argv[2],
    "spark_master": sys.argv[3],
    "kafka_bootstrap_servers": sys.argv[4],
    "producer_interval_ms": int(sys.argv[5]),
    "duration_seconds": int(sys.argv[6]),
    "sample_interval_seconds": int(sys.argv[7]),
    "microbatch_interval_seconds": int(sys.argv[8]),
    "stream_trigger": sys.argv[9],
    "max_offsets_per_trigger": int(sys.argv[10]),
    "hdfs_replication": int(__import__("os").environ.get("HDFS_REPLICATION", "1")),
    "stream_setrep_enabled": __import__("os").environ.get("STREAM_SETREP_ENABLED", "false"),
    "hdfs_stream_root": "hdfs://node1:9000/lake/stream",
}
path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(path)
PY
}

start_single_kafka() {
  local remote_compose="/home/student/docker-compose.kafka-single.yml"
  log "Deploying single-broker Kafka compose to Node2"
  scp "${PROJECT_HOME}/scripts/kafka/docker-compose.single-node2.yml" "student@node2:${remote_compose}" >/dev/null
  log "Stopping distributed Kafka containers"
  ssh student@node3 "docker-compose -f /home/student/docker-compose.kafka-realtime.yml down -v --remove-orphans >/dev/null 2>&1 || true; docker rm -f kafka >/dev/null 2>&1 || true"
  ssh student@node2 "docker-compose -f ${remote_compose} down -v --remove-orphans >/dev/null 2>&1 || true; docker-compose -f /home/student/docker-compose.kafka-realtime.yml down -v --remove-orphans >/dev/null 2>&1 || true; docker rm -f kafka-ui kafka zookeeper friendly_shockley >/dev/null 2>&1 || true"
  log "Starting single-broker Kafka on Node2"
  ssh student@node2 "docker-compose -f ${remote_compose} up -d zookeeper kafka"
  wait_port 192.168.1.87 2181 "Node2 ZooKeeper"
  wait_port 192.168.1.87 9092 "Node2 Kafka"
  ssh student@node2 "docker-compose -f ${remote_compose} up -d kafka-ui friendly_shockley"
}

start_distributed_kafka() {
  log "Starting distributed Kafka on Node2/Node3"
  RESET_STREAM_ON_KAFKA_START=false START_KAFKA_PRODUCER=false KAFKA_PRODUCER_INTERVAL="$PRODUCER_INTERVAL_MS" \
    "${PROJECT_HOME}/scripts/start-kafka-realtime.sh"
}

prewarm_stream_dimensions() {
  if [ "$MODE" != "distributed" ]; then
    return
  fi

  log "Prewarming stream dimension tables locally before distributed executors read them"
  SPARK_MASTER=local[*] \
  HDFS_REPLICATION="$HDFS_REPLICATION" \
  HDFS_LAKE_PATH=hdfs://node1:9000/lake/stream \
  FULL_HDFS_LAKE_PATH=hdfs://node1:9000/lake/full \
  HDFS_CONTROL_PATH=hdfs://node1:9000/lake/control \
    "${PROJECT_HOME}/scripts/run-stream-microbatch.sh" once \
    > "${REPORT_DIR}/stream_dimension_prewarm.log" 2>&1 || true

  JAVA_HOME="$JAVA_HOME" "${HADOOP_HOME}/bin/hdfs" dfs -setrep -R -w "$HDFS_REPLICATION" \
    hdfs://node1:9000/lake/stream/silver/silver_point_meta_dim \
    hdfs://node1:9000/lake/stream/silver/silver_price_dim \
    >> "${REPORT_DIR}/stream_dimension_prewarm.log" 2>&1 || true
}

wait_producer_api() {
  log "Waiting for producer API"
  for _ in $(seq 1 90); do
    if curl -fsS --max-time 2 "${PRODUCER_URL}/" >/dev/null 2>&1; then
      log "Producer API is reachable"
      return 0
    fi
    sleep 2
  done
  log "ERROR: producer API is not reachable"
  return 1
}

start_kafka_to_bronze() {
  log "Starting Kafka-to-Bronze with Spark master ${SPARK_MASTER}"
  KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  SPARK_MASTER="$SPARK_MASTER" \
  SPARK_DRIVER_HOST="$SPARK_DRIVER_HOST" \
  HDFS_REPLICATION="$HDFS_REPLICATION" \
  KAFKA_STARTING_OFFSETS=latest \
  RESET_KAFKA_CHECKPOINT=false \
  KAFKA_MAX_OFFSETS_PER_TRIGGER="$MAX_OFFSETS_PER_TRIGGER" \
  STREAM_TRIGGER="$STREAM_TRIGGER" \
  LOG_FILE="${REPORT_DIR}/kafka_to_bronze_streaming.log" \
  PID_FILE="/tmp/kafka_to_bronze_streaming.pid" \
    "${PROJECT_HOME}/scripts/start-kafka-to-bronze.sh"
}

start_hdfs_replication_loop() {
  if [ "$MODE" != "distributed" ]; then
    return
  fi
  if [ "$STREAM_SETREP_ENABLED" != "true" ] || [ "$HDFS_REPLICATION" = "1" ]; then
    log "HDFS replication maintenance loop disabled; relying on configured dfs.replication=${HDFS_REPLICATION}"
    return
  fi

  log "Starting HDFS replication maintenance loop for stream trial"
  (
    while true; do
      for path in \
        hdfs://node1:9000/lake/stream/bronze/bronze_streaming \
        hdfs://node1:9000/lake/stream/silver \
        hdfs://node1:9000/lake/stream/gold \
        hdfs://node1:9000/lake/control/etl_watermark \
        hdfs://node1:9000/lake/control/stream_last_batch_point_fact
      do
        if JAVA_HOME="$JAVA_HOME" "${HADOOP_HOME}/bin/hdfs" dfs -test -e "$path" >/dev/null 2>&1; then
          JAVA_HOME="$JAVA_HOME" "${HADOOP_HOME}/bin/hdfs" dfs -setrep -R "$HDFS_REPLICATION" "$path" >/dev/null 2>&1 || true
        fi
      done
      sleep 10
    done
  ) >> "${REPORT_DIR}/hdfs_replication_loop.log" 2>&1 &
  REPLICATION_LOOP_PID="$!"
  echo "$REPLICATION_LOOP_PID" > /tmp/stream_hdfs_replication_loop.pid
}

start_microbatch_loop() {
  log "Starting stream micro-batch loop with Spark master ${SPARK_MASTER}"
  nohup setsid bash -lc "cd '${PROJECT_HOME}' && SPARK_MASTER='${SPARK_MASTER}' SPARK_DRIVER_HOST='${SPARK_DRIVER_HOST}' HDFS_REPLICATION='${HDFS_REPLICATION}' STREAM_SETREP_ENABLED='${STREAM_SETREP_ENABLED}' HDFS_LAKE_PATH='hdfs://node1:9000/lake/stream' FULL_HDFS_LAKE_PATH='hdfs://node1:9000/lake/full' HDFS_CONTROL_PATH='hdfs://node1:9000/lake/control' STREAM_MICROBATCH_INTERVAL_SECONDS='${MICROBATCH_INTERVAL_SECONDS}' ./scripts/run-stream-microbatch.sh loop" \
    > "${REPORT_DIR}/stream_microbatch_loop.log" 2>&1 &
  echo $! > /tmp/stream_microbatch_loop.pid
  log "Micro-batch PID: $(cat /tmp/stream_microbatch_loop.pid)"
}

snapshot() {
  local label="$1"
  local include_lake="${2:-false}"
  if [ "$include_lake" = "true" ]; then
    "$SPARK_HOME/bin/spark-submit" \
      --master local[*] \
      --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
      --conf "spark.driver.bindAddress=0.0.0.0" \
      --packages io.delta:delta-spark_2.12:3.2.0 \
      "${PROJECT_HOME}/scripts/collect_stream_validation_snapshot.py" \
      --label "$label" \
      --mode "$MODE" \
      --bootstrap "$KAFKA_BOOTSTRAP_SERVERS" \
      --spark-master local[*] \
      --include-lake \
      --output-dir "$SNAPSHOT_DIR" >/dev/null
  else
    python3 "${PROJECT_HOME}/scripts/collect_stream_validation_snapshot.py" \
      --label "$label" \
      --mode "$MODE" \
      --bootstrap "$KAFKA_BOOTSTRAP_SERVERS" \
      --spark-master "$SPARK_MASTER" \
      --output-dir "$SNAPSHOT_DIR" >/dev/null
  fi
}

stop_trial_processes() {
  log "Stopping producer"
  curl -fsS "${PRODUCER_URL}/producer/stop" >/dev/null 2>&1 || true
  if [ -f /tmp/kafka_to_bronze_streaming.pid ]; then
    pid="$(cat /tmp/kafka_to_bronze_streaming.pid)"
    kill -- "-${pid}" >/dev/null 2>&1 || kill "$pid" >/dev/null 2>&1 || true
  fi
  if [ -f /tmp/stream_microbatch_loop.pid ]; then
    pid="$(cat /tmp/stream_microbatch_loop.pid)"
    kill -- "-${pid}" >/dev/null 2>&1 || kill "$pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$REPLICATION_LOOP_PID" ] && kill -0 "$REPLICATION_LOOP_PID" >/dev/null 2>&1; then
    kill "$REPLICATION_LOOP_PID" >/dev/null 2>&1 || true
  fi
  if [ -f /tmp/stream_hdfs_replication_loop.pid ]; then
    pid="$(cat /tmp/stream_hdfs_replication_loop.pid)"
    kill "$pid" >/dev/null 2>&1 || true
    rm -f /tmp/stream_hdfs_replication_loop.pid
  fi
}

log "Trial report directory: ${REPORT_DIR}"
write_config
systemctl --user stop energy-stream-microbatch.service >/dev/null 2>&1 || true
"${PROJECT_HOME}/scripts/reset-stream-empty.sh"
prewarm_stream_dimensions

if [ "$MODE" = "single" ]; then
  start_single_kafka
else
  start_distributed_kafka
fi
wait_producer_api
start_kafka_to_bronze
start_hdfs_replication_loop
sleep 20
start_microbatch_loop
sleep 10

log "Starting producer at interval ${PRODUCER_INTERVAL_MS} ms"
curl -fsS "${PRODUCER_URL}/producer/start?interval=${PRODUCER_INTERVAL_MS}&loop=false" >/dev/null

start_epoch="$(date +%s)"
end_epoch="$((start_epoch + DURATION_SECONDS))"
sample_index=0
while [ "$(date +%s)" -lt "$end_epoch" ]; do
  sample_index="$((sample_index + 1))"
  log "Collecting sample ${sample_index}"
  snapshot "$(printf '%03d' "$sample_index")"
  now="$(date +%s)"
  sleep_for="$SAMPLE_INTERVAL_SECONDS"
  if [ "$((now + sleep_for))" -gt "$end_epoch" ]; then
    sleep_for="$((end_epoch - now))"
  fi
  if [ "$sleep_for" -gt 0 ]; then
    sleep "$sleep_for"
  fi
done

log "Collecting final lake snapshot"
snapshot "999_final_lake" true
cp "${REPORT_DIR}/kafka_to_bronze_streaming.log" /tmp/kafka_to_bronze_streaming.log 2>/dev/null || true
cp "${REPORT_DIR}/stream_microbatch_loop.log" /tmp/stream_microbatch_loop.log 2>/dev/null || true
LOG_LINES=240 KAFKA_BRONZE_LOG="${REPORT_DIR}/kafka_to_bronze_streaming.log" STREAM_MICROBATCH_LOG="${REPORT_DIR}/stream_microbatch_loop.log" \
  "${PROJECT_HOME}/scripts/demo-infra-evidence.sh" all > "${REPORT_DIR}/infra_evidence.txt" 2>&1 || true
python3 "${PROJECT_HOME}/scripts/summarize_stream_validation.py" "$REPORT_DIR"
stop_trial_processes
log "Trial completed: ${REPORT_DIR}"

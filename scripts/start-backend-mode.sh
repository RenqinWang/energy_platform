#!/bin/bash
# Start one FastAPI backend instance against the full or stream lake namespace.

set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
SPARK_HOME="${SPARK_HOME:-/home/student/spark-3.5.7-bin-hadoop3}"
HADOOP_HOME="${HADOOP_HOME:-/home/student/hadoop-3.3.6}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/home/student/hdfs-conf}"
MODE="${1:-full}"

case "$MODE" in
  full)
    PORT="${BACKEND_PORT:-8001}"
    HDFS_LAKE_PATH="${HDFS_LAKE_PATH:-hdfs://node1:9000/lake/full}"
    ;;
  stream)
    PORT="${BACKEND_PORT:-8002}"
    HDFS_LAKE_PATH="${HDFS_LAKE_PATH:-hdfs://node1:9000/lake/stream}"
    ;;
  *)
    echo "Usage: $0 [full|stream]"
    exit 2
    ;;
esac

LOG_FILE="${LOG_FILE:-/tmp/backend_${MODE}.log}"
PID_FILE="${PID_FILE:-/tmp/backend_${MODE}.pid}"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "${MODE} backend is already running with PID $(cat "$PID_FILE")"
  exit 0
fi

export JAVA_HOME
export SPARK_HOME
export HADOOP_HOME
export HADOOP_CONF_DIR
export ENERGY_DATA_MODE="$MODE"
export HDFS_LAKE_PATH
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}"
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

cd "$PROJECT_HOME/backend"
nohup setsid python -m uvicorn main:app --host 0.0.0.0 --port "$PORT" > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

echo "Started ${MODE} backend"
echo "  PID: $(cat "$PID_FILE")"
echo "  URL: http://0.0.0.0:${PORT}"
echo "  Lake: ${HDFS_LAKE_PATH}"
echo "  Log: ${LOG_FILE}"

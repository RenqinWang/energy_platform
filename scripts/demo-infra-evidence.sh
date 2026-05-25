#!/bin/bash
# Print concise evidence for the assignment demo:
# 1) three-node deployment and HDFS DataNode status
# 2) Kafka producer status and Kafka-to-Bronze ingestion logs
# 3) HDFS Delta Lake layer paths

set -uo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
HADOOP_HOME="${HADOOP_HOME:-/home/student/hadoop-3.3.6}"
HDFS_BIN="${HDFS_BIN:-${HADOOP_HOME}/bin/hdfs}"
HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://node1:9000}"
HDFS_LAKE_ROOT="${HDFS_LAKE_ROOT:-${HDFS_NAMENODE}/lake}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-http://192.168.1.87:8080/json/}"
KAFKA_PRODUCER_API="${KAFKA_PRODUCER_API:-http://192.168.1.87:8000}"
KAFKA_BRONZE_LOG="${KAFKA_BRONZE_LOG:-/tmp/kafka_to_bronze_streaming.log}"
STREAM_MICROBATCH_LOG="${STREAM_MICROBATCH_LOG:-/tmp/stream_microbatch_loop.log}"
LOG_LINES="${LOG_LINES:-80}"

export JAVA_HOME

section() {
  printf '\n'
  printf '======================================================================\n'
  printf '%s\n' "$1"
  printf '======================================================================\n'
}

subsection() {
  printf '\n-- %s --\n' "$1"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

have() {
  command -v "$1" >/dev/null 2>&1
}

json_pretty() {
  if have python3; then
    python3 -m json.tool 2>/dev/null || cat
  else
    cat
  fi
}

run_optional() {
  local description="$1"
  shift
  subsection "$description"
  "$@" || warn "$description failed"
}

hdfs_optional() {
  local description="$1"
  shift
  run_optional "$description" "$HDFS_BIN" "$@"
}

print_architecture() {
  section "1. Three-node deployment architecture"
  cat <<'EOF'
Node   Public IP        Private IP      Roles
-----  ---------------  --------------  -----------------------------------------------
Node1  115.120.208.241  192.168.0.94    Entry, HDFS NameNode/DataNode, Spark Worker,
                                        FastAPI full/stream backend, React frontend
Node2  1.94.113.240     192.168.1.87    Spark Master, HDFS DataNode, Spark Worker,
                                        Kafka Broker, producer API
Node3  123.60.106.233   192.168.1.19    HDFS DataNode, Spark Worker, Kafka Broker

Core endpoints
--------------
HDFS NameNode:     hdfs://node1:9000
Spark Master:      spark://192.168.1.87:7077
Kafka Brokers:     192.168.1.87:9092, 192.168.1.19:9092
Frontend:          http://115.120.208.241:3001
Full backend:      http://115.120.208.241:8001
Stream backend:    http://115.120.208.241:8002
EOF
}

print_hdfs_status() {
  section "1. HDFS DataNode status"
  if [ ! -x "$HDFS_BIN" ]; then
    warn "HDFS binary not found: $HDFS_BIN"
    return
  fi

  local report_file
  report_file="$(mktemp /tmp/hdfs-report.XXXXXX)"
  if ! "$HDFS_BIN" dfsadmin -fs "$HDFS_NAMENODE" -report >"$report_file" 2>&1; then
    cat "$report_file"
    rm -f "$report_file"
    warn "Failed to query HDFS report"
    return
  fi

  subsection "Cluster health summary"
  awk '
    /^Live datanodes/ {print; exit}
    /^Configured Capacity:/ {print; next}
    /^Present Capacity:/ {print; next}
    /^DFS Remaining:/ {print; next}
    /^DFS Used:/ {print; next}
    /^DFS Used%:/ {print; next}
    /Under replicated blocks:/ {print; next}
    /Blocks with corrupt replicas:/ {print; next}
    /Missing blocks:/ {print; next}
  ' "$report_file"

  subsection "Live DataNode details"
  awk '
    /^Name: / {seen=1; print ""; print $0; next}
    seen && /^Hostname: / {print $0; next}
    seen && /^Decommission Status/ {print $0; next}
    seen && /^Configured Capacity:/ {print $0; next}
    seen && /^DFS Used:/ {print $0; next}
    seen && /^DFS Used%:/ {print $0; next}
    seen && /^DFS Remaining%:/ {print $0; next}
    seen && /^Last contact:/ {print $0; next}
  ' "$report_file" | sed -n '1,120p'

  rm -f "$report_file"
}

print_spark_status() {
  section "Spark standalone cluster status"
  subsection "Spark Master JSON"
  if curl -fsS --max-time 8 "$SPARK_MASTER_URL" | json_pretty | sed -n '1,80p'; then
    :
  else
    warn "Failed to query Spark Master: $SPARK_MASTER_URL"
  fi
}

print_kafka_status() {
  section "2. Kafka producer and Kafka-to-Bronze ingestion"

  subsection "Kafka producer API status"
  if curl -fsS --max-time 8 "${KAFKA_PRODUCER_API}/producer/status" | json_pretty; then
    :
  else
    warn "Failed to query Kafka producer API: ${KAFKA_PRODUCER_API}/producer/status"
  fi

  subsection "Kafka processes on Node2 and Node3"
  ssh -o ConnectTimeout=5 student@node2 'jps | grep -E "Kafka|QuorumPeerMain|Jps" || true' || warn "Cannot query node2 jps"
  ssh -o ConnectTimeout=5 student@node3 'jps | grep -E "Kafka|QuorumPeerMain|Jps" || true' || warn "Cannot query node3 jps"

  subsection "Kafka-to-Bronze process on Node1"
  if pgrep -af 'streaming_to_bronze.py|Kafka_Streaming_to_Bronze' >/tmp/kafka-to-bronze-pgrep.$$ 2>/dev/null; then
    while IFS= read -r line; do
      pid="${line%% *}"
      if printf '%s\n' "$line" | grep -q 'streaming_to_bronze.py'; then
        printf '%s streaming_to_bronze.py (running)\n' "$pid"
      else
        printf '%s %s\n' "$pid" "${line#* }"
      fi
    done </tmp/kafka-to-bronze-pgrep.$$
    rm -f /tmp/kafka-to-bronze-pgrep.$$
  else
    rm -f /tmp/kafka-to-bronze-pgrep.$$
    warn "Kafka-to-Bronze process not found by pgrep"
  fi

  subsection "Kafka-to-Bronze log evidence: ${KAFKA_BRONZE_LOG}"
  if [ -f "$KAFKA_BRONZE_LOG" ]; then
    printf 'Filtered latest log lines:\n'
    tail -n "$LOG_LINES" "$KAFKA_BRONZE_LOG" \
      | grep -E 'Kafka streaming ingestion started|Streaming query is running|Query ID|Spark master|Kafka bootstrap|Output path|Checkpoint path|Starting offsets|Max offsets|ProcessingTimeExecutor|ERROR|Exception|WARN' \
      | tail -n 40
    printf '\nRaw tail:\n'
    tail -n 20 "$KAFKA_BRONZE_LOG"
  else
    warn "Log file does not exist: $KAFKA_BRONZE_LOG"
  fi

  subsection "Stream micro-batch log evidence: ${STREAM_MICROBATCH_LOG}"
  if [ -f "$STREAM_MICROBATCH_LOG" ]; then
    tail -n "$LOG_LINES" "$STREAM_MICROBATCH_LOG" \
      | grep -E 'Stream micro-batch started|New Bronze rows:|Silver rows processed:|No new Bronze rows|Incremental batch rows|Writing unified hourly table|report summary|completed|failed|ERROR|Exception|WARN' \
      | tail -n 60
  else
    warn "Log file does not exist: $STREAM_MICROBATCH_LOG"
  fi
}

print_hdfs_layer_paths() {
  section "3. HDFS Delta Lake layer paths"
  if [ ! -x "$HDFS_BIN" ]; then
    warn "HDFS binary not found: $HDFS_BIN"
    return
  fi

  hdfs_optional "Lake root" dfs -ls "$HDFS_LAKE_ROOT"

  for path in \
    "${HDFS_LAKE_ROOT}/bronze" \
    "${HDFS_LAKE_ROOT}/silver" \
    "${HDFS_LAKE_ROOT}/gold" \
    "${HDFS_LAKE_ROOT}/full" \
    "${HDFS_LAKE_ROOT}/full/silver" \
    "${HDFS_LAKE_ROOT}/full/gold" \
    "${HDFS_LAKE_ROOT}/stream" \
    "${HDFS_LAKE_ROOT}/stream/bronze" \
    "${HDFS_LAKE_ROOT}/stream/silver" \
    "${HDFS_LAKE_ROOT}/stream/gold" \
    "${HDFS_LAKE_ROOT}/control"
  do
    if "$HDFS_BIN" dfs -test -e "$path" >/dev/null 2>&1; then
      hdfs_optional "$path" dfs -ls "$path"
    else
      printf '\n-- %s --\n' "$path"
      printf 'Path not present in current lake namespace.\n'
    fi
  done

  subsection "Delta table markers (_delta_log)"
  for path in \
    "${HDFS_LAKE_ROOT}/stream/bronze/bronze_streaming" \
    "${HDFS_LAKE_ROOT}/stream/silver/silver_point_fact" \
    "${HDFS_LAKE_ROOT}/stream/gold/gold_system_supply_hourly" \
    "${HDFS_LAKE_ROOT}/full/silver/silver_point_fact" \
    "${HDFS_LAKE_ROOT}/full/gold/gold_system_supply_hourly" \
    "${HDFS_LAKE_ROOT}/full/gold/gold_system_report_daily" \
    "${HDFS_LAKE_ROOT}/full/gold/gold_system_forecast_supply" \
    "${HDFS_LAKE_ROOT}/full/gold/gold_system_revenue_forecast"
  do
    if "$HDFS_BIN" dfs -test -e "${path}/_delta_log" >/dev/null 2>&1; then
      printf '[OK]    %s\n' "$path"
    else
      printf '[MISS]  %s\n' "$path"
    fi
  done
}

usage() {
  cat <<'EOF'
Usage:
  scripts/demo-infra-evidence.sh [all|architecture|hdfs|spark|kafka|lake]

Environment overrides:
  HDFS_NAMENODE=hdfs://node1:9000
  KAFKA_PRODUCER_API=http://192.168.1.87:8000
  LOG_LINES=80

Recommended for demo:
  LOG_LINES=80 scripts/demo-infra-evidence.sh all
EOF
}

main() {
  local target="${1:-all}"
  case "$target" in
    all)
      print_architecture
      print_hdfs_status
      print_spark_status
      print_kafka_status
      print_hdfs_layer_paths
      ;;
    architecture)
      print_architecture
      ;;
    hdfs)
      print_hdfs_status
      ;;
    spark)
      print_spark_status
      ;;
    kafka)
      print_kafka_status
      ;;
    lake)
      print_hdfs_layer_paths
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage
      exit 2
      ;;
  esac
}

main "$@"

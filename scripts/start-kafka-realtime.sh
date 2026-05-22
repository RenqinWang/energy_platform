#!/bin/bash
# Start the Node2/Node3 Kafka ingestion path used for the realtime Bronze check.

set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
NODE2="${NODE2:-node2}"
NODE3="${NODE3:-node3}"
REMOTE_COMPOSE="/home/student/docker-compose.kafka-realtime.yml"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.1.87:9092,192.168.1.19:9092}"
COMPOSE="docker-compose"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

wait_port() {
  local host="$1"
  local port="$2"
  local name="$3"
  local attempts="${4:-60}"
  local i

  for ((i = 1; i <= attempts; i++)); do
    if timeout 2 bash -lc "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; then
      log "${name} is reachable at ${host}:${port}"
      return 0
    fi
    sleep 2
  done

  log "ERROR: ${name} is not reachable at ${host}:${port}"
  return 1
}

log "Deploying Kafka realtime docker-compose files"
scp "$PROJECT_HOME/scripts/kafka/docker-compose.node2.yml" "student@${NODE2}:${REMOTE_COMPOSE}" >/dev/null
scp "$PROJECT_HOME/scripts/kafka/docker-compose.node3.yml" "student@${NODE3}:${REMOTE_COMPOSE}" >/dev/null

log "Stopping old Kafka containers on Node2 and Node3"
ssh "student@${NODE2}" "${COMPOSE} -f ${REMOTE_COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true; docker rm -f kafka-ui kafka zookeeper friendly_shockley >/dev/null 2>&1 || true"
ssh "student@${NODE3}" "${COMPOSE} -f ${REMOTE_COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true; docker rm -f kafka-ui kafka zookeeper friendly_shockley >/dev/null 2>&1 || true"

log "Starting ZooKeeper and broker 1 on Node2"
ssh "student@${NODE2}" "${COMPOSE} -f ${REMOTE_COMPOSE} up -d zookeeper kafka"
wait_port "192.168.1.87" 2181 "Node2 ZooKeeper"
wait_port "192.168.1.87" 9092 "Node2 Kafka broker"

log "Starting broker 2 on Node3"
ssh "student@${NODE3}" "${COMPOSE} -f ${REMOTE_COMPOSE} up -d kafka"
wait_port "192.168.1.19" 9092 "Node3 Kafka broker"

log "Starting Kafka UI and producer API on Node2"
ssh "student@${NODE2}" "${COMPOSE} -f ${REMOTE_COMPOSE} up -d kafka-ui friendly_shockley"

log "Waiting for producer API"
for i in {1..60}; do
  if curl -fsS --max-time 2 "http://192.168.1.87:8000/" >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [ "$i" = 60 ]; then
    log "ERROR: producer API did not become ready"
    exit 1
  fi
done

log "Starting producer stream on Node2"
curl -fsS "http://192.168.1.87:8000/producer/start?interval=5&loop=false" >/dev/null

log "Kafka realtime ingestion source is running"
log "Bootstrap servers: ${BOOTSTRAP_SERVERS}"
log "Producer API: http://192.168.1.87:8000"
log "Kafka UI: http://192.168.1.87:8083"

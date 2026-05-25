#!/bin/bash
# Start both FastAPI backends and the React/Vite frontend in the background.
#
# Usage:
#   bash scripts/start-web-stack.sh
#
# Optional environment variables:
#   PUBLIC_HOST=115.120.208.241
#   FULL_BACKEND_PORT=8001
#   STREAM_BACKEND_PORT=8002
#   FRONTEND_PORT=3001

set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/student/energy-platform}"
PUBLIC_HOST="${PUBLIC_HOST:-115.120.208.241}"
FULL_BACKEND_PORT="${FULL_BACKEND_PORT:-8001}"
STREAM_BACKEND_PORT="${STREAM_BACKEND_PORT:-8002}"
FRONTEND_HOST="${FRONTEND_HOST:-0.0.0.0}"
FRONTEND_PORT="${FRONTEND_PORT:-3001}"
FRONTEND_LOG_FILE="${FRONTEND_LOG_FILE:-/tmp/frontend_vite.log}"
FRONTEND_PID_FILE="${FRONTEND_PID_FILE:-/tmp/frontend_vite.pid}"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

start_backend() {
  local mode="$1"
  local port="$2"

  log "Starting ${mode} backend on port ${port}"
  BACKEND_PORT="$port" bash "${PROJECT_HOME}/scripts/start-backend-mode.sh" "$mode"
}

start_frontend() {
  if [ -f "$FRONTEND_PID_FILE" ] && kill -0 "$(cat "$FRONTEND_PID_FILE")" 2>/dev/null; then
    log "frontend is already running with PID $(cat "$FRONTEND_PID_FILE")"
    return 0
  fi

  log "Starting frontend on port ${FRONTEND_PORT}"
  cd "${PROJECT_HOME}/frontend-new"
  nohup npm run dev -- --host "$FRONTEND_HOST" --port "$FRONTEND_PORT" > "$FRONTEND_LOG_FILE" 2>&1 &
  echo $! > "$FRONTEND_PID_FILE"
  log "frontend PID: $(cat "$FRONTEND_PID_FILE")"
  log "frontend log: ${FRONTEND_LOG_FILE}"
}

health_check() {
  local name="$1"
  local url="$2"
  local attempts="${3:-20}"
  local i

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
      log "${name} is ready: ${url}"
      return 0
    fi
    sleep 1
  done

  log "WARN: ${name} did not pass health check: ${url}"
  return 0
}

main() {
  log "Starting web stack"
  start_backend full "$FULL_BACKEND_PORT"
  start_backend stream "$STREAM_BACKEND_PORT"
  start_frontend

  health_check "full backend" "http://127.0.0.1:${FULL_BACKEND_PORT}/health"
  health_check "stream backend" "http://127.0.0.1:${STREAM_BACKEND_PORT}/health"
  health_check "frontend" "http://127.0.0.1:${FRONTEND_PORT}"

  echo
  echo "Web stack started."
  echo "  Frontend:       http://${PUBLIC_HOST}:${FRONTEND_PORT}"
  echo "  Full backend:   http://${PUBLIC_HOST}:${FULL_BACKEND_PORT}/health"
  echo "  Stream backend: http://${PUBLIC_HOST}:${STREAM_BACKEND_PORT}/health"
  echo
  echo "Logs:"
  echo "  Full backend:   /tmp/backend_full.log"
  echo "  Stream backend: /tmp/backend_stream.log"
  echo "  Frontend:       ${FRONTEND_LOG_FILE}"
}

main "$@"

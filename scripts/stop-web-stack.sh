#!/bin/bash
# Stop the FastAPI backends and React/Vite frontend started by start-web-stack.sh.

set -euo pipefail

FRONTEND_PID_FILE="${FRONTEND_PID_FILE:-/tmp/frontend_vite.pid}"

stop_pid_file() {
  local name="$1"
  local pid_file="$2"

  if [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "Stopping ${name} with PID $(cat "$pid_file")"
    kill "$(cat "$pid_file")" 2>/dev/null || true
    rm -f "$pid_file"
  else
    echo "${name} is not running from ${pid_file}"
    rm -f "$pid_file"
  fi
}

stop_pid_file "full backend" "/tmp/backend_full.pid"
stop_pid_file "stream backend" "/tmp/backend_stream.pid"
stop_pid_file "frontend" "$FRONTEND_PID_FILE"

# Clean up any process left behind by npm/vite or manual starts.
pkill -f "uvicorn main:app" 2>/dev/null || true
pkill -f "vite .*--port 3001" 2>/dev/null || true

echo "Web stack stopped."

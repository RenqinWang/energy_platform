#!/bin/bash
# Prepare full/stream lake namespaces without modifying the legacy /lake tables.

set -euo pipefail

HDFS_BIN="${HDFS_BIN:-/home/student/hadoop-3.3.6/bin/hdfs}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
LAKE_BASE="${LAKE_BASE:-hdfs://node1:9000/lake}"

export JAVA_HOME

"$HDFS_BIN" dfs -mkdir -p \
  "${LAKE_BASE}/full/bronze" \
  "${LAKE_BASE}/full/silver" \
  "${LAKE_BASE}/full/gold" \
  "${LAKE_BASE}/stream/bronze" \
  "${LAKE_BASE}/stream/silver" \
  "${LAKE_BASE}/stream/gold" \
  "${LAKE_BASE}/control"

copy_if_missing() {
  local source_path="$1"
  local target_path="$2"

  if "$HDFS_BIN" dfs -test -d "$target_path"; then
    if "$HDFS_BIN" dfs -test -e "${target_path}/_delta_log"; then
      echo "Exists: ${target_path}"
      return 0
    fi
  fi

  echo "Copying ${source_path} -> ${target_path}"
  "$HDFS_BIN" dfs -rm -r -skipTrash "$target_path" >/dev/null 2>&1 || true
  "$HDFS_BIN" dfs -cp "$source_path" "$target_path"
}

# Silver/Gold are the stable full-batch products used by the API. Bronze can
# stay on the legacy path unless a full raw snapshot is explicitly required.
copy_if_missing "${LAKE_BASE}/silver" "${LAKE_BASE}/full/silver"
copy_if_missing "${LAKE_BASE}/gold" "${LAKE_BASE}/full/gold"

echo "Lake namespaces are ready:"
"$HDFS_BIN" dfs -ls "${LAKE_BASE}"

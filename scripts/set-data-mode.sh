#!/bin/bash
# Print environment exports for selecting the lake result set.
#
# Usage:
#   source scripts/set-data-mode.sh full
#   source scripts/set-data-mode.sh stream

set -euo pipefail

MODE="${1:-full}"

case "$MODE" in
  full)
    export ENERGY_DATA_MODE="full"
    export HDFS_LAKE_PATH="hdfs://node1:9000/lake/full"
    ;;
  stream)
    export ENERGY_DATA_MODE="stream"
    export HDFS_LAKE_PATH="hdfs://node1:9000/lake/stream"
    ;;
  *)
    echo "Usage: source scripts/set-data-mode.sh [full|stream]" >&2
    return 2 2>/dev/null || exit 2
    ;;
esac

echo "ENERGY_DATA_MODE=${ENERGY_DATA_MODE}"
echo "HDFS_LAKE_PATH=${HDFS_LAKE_PATH}"

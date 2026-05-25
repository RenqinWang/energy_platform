#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASSET_DIR="${ROOT_DIR}/assets"
FORMAT="${1:-pdf}"

if ! command -v mmdc >/dev/null 2>&1; then
  cat >&2 <<'EOF'
未找到 mmdc。

安装方式：
  npm install -g @mermaid-js/mermaid-cli

用法：
  bash docs/render_mermaid_diagrams.sh pdf
  bash docs/render_mermaid_diagrams.sh svg
  bash docs/render_mermaid_diagrams.sh png
EOF
  exit 1
fi

case "${FORMAT}" in
  pdf|svg|png) ;;
  *)
    echo "不支持的格式：${FORMAT}，仅支持 pdf/svg/png" >&2
    exit 1
    ;;
esac

for name in fig_deployment fig_lake_layers fig_stream_microbatch fig_frontend_flow; do
  mmdc \
    -i "${ASSET_DIR}/${name}.mmd" \
    -o "${ASSET_DIR}/${name}.${FORMAT}" \
    -b transparent
  echo "generated ${ASSET_DIR}/${name}.${FORMAT}"
done

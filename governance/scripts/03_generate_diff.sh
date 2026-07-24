#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <parent_version_dir> <current_version_dir> [output_dir]"
  exit 2
fi

PARENT_DIR="$(realpath "$1")"
CURRENT_DIR="$(realpath "$2")"
OUT_DIR="${3:-${CURRENT_DIR}/audit/diff}"
mkdir -p "${OUT_DIR}"

diff -ru \
  --exclude='audit' \
  --exclude='query_outputs*' \
  --exclude='logs' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  "${PARENT_DIR}" "${CURRENT_DIR}" > "${OUT_DIR}/parent_vs_current.diff" || true

diff -rq \
  --exclude='audit' \
  --exclude='query_outputs*' \
  --exclude='logs' \
  --exclude='__pycache__' \
  "${PARENT_DIR}" "${CURRENT_DIR}" > "${OUT_DIR}/parent_vs_current.summary.txt" || true

grep -RInE '\.(count|collect|take|show|write|cache|persist|unpersist|repartition|coalesce)\(' \
  "${CURRENT_DIR}" --include='*.py' > "${OUT_DIR}/spark_actions_and_materialization.txt" || true

grep -RInE '(perf_counter|time\.time|monotonic|start_time|end_time|elapsed|runtime)' \
  "${CURRENT_DIR}" --include='*.py' > "${OUT_DIR}/timer_locations.txt" || true

echo "[PASS] Diff and risk hotspots captured: ${OUT_DIR}"

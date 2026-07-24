#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"
IFS=',' read -r -a QUERIES <<< "${QUERIES_CSV:-Q1.1,Q1.2,Q1.3,Q2.1,Q2.2,Q2.3,Q3.1,Q3.2,Q3.3,Q4.1,Q4.2,Q4.3}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
OUTPUT_BASE="${OUTPUT_BASE:-${PROJECT_DIR}/benchmark_outputs_vr15/profiling/${RUN_ID}}"
mkdir -p "${OUTPUT_BASE}"
FAILED=()
for QUERY_ID in "${QUERIES[@]}"; do
  echo "===== Profiling ${QUERY_ID} ====="
  if OUTPUT_ROOT="${OUTPUT_BASE}/${QUERY_ID//./_}" \
     DATASETS="${DATASETS:-dataset_1m}" PROFILE_REPEATS="${PROFILE_REPEATS:-1}" \
     "${PROJECT_DIR}/scripts/run_one_query_profile.sh" "${QUERY_ID}"; then
    echo "PASS ${QUERY_ID}"
  else
    FAILED+=("${QUERY_ID}")
  fi
done
if ((${#FAILED[@]})); then
  printf 'Các query lỗi: %s\n' "${FAILED[*]}"
  exit 1
fi

#!/usr/bin/env bash
set -u
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"
OUTPUT_ROOT="${OUTPUT_ROOT:-query_outputs_C2_vr15_benchmark_12queries_6datasets_r3}"
DATASETS="${DATASETS:-dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m}"
QUERY_IDS=(Q1.1 Q1.2 Q1.3 Q2.1 Q2.2 Q2.3 Q3.1 Q3.2 Q3.3 Q4.1 Q4.2 Q4.3)
PASSED=(); FAILED=()
for QUERY_ID in "${QUERY_IDS[@]}"; do
  echo "===== Vr15 benchmark ${QUERY_ID} ====="
  if python3 run_experiment.py \
      --query-id "${QUERY_ID}" --datasets "${DATASETS}" \
      --mode profiling_runtime_only --repeats 3 \
      --output-root "${OUTPUT_ROOT}"; then
    PASSED+=("${QUERY_ID}")
  else
    FAILED+=("${QUERY_ID}")
  fi
done
printf 'Hoàn thành: %s\n' "${PASSED[*]:-Không có}"
printf 'Lỗi: %s\n' "${FAILED[*]:-Không có}"
echo "Thư mục kết quả: ${OUTPUT_ROOT}"
((${#FAILED[@]} == 0))

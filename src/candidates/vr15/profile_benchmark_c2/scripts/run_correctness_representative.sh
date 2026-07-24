#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"
python3 run_experiment.py \
  --mode correctness \
  --query-id "${QUERY_ID:-Q4.3}" \
  --datasets "${DATASETS:-dataset_1m}" \
  --output-root "${OUTPUT_ROOT:-./query_outputs_vr15_correctness}" \
  --repeats "${REPEATS:-1}" \
  --require-nonzero \
  --time-window-duration-seconds "${TIME_WINDOW_DURATION_SECONDS:-10}" \
  --max-time-gap-seconds "${MAX_TIME_GAP_SECONDS:-120}" \
  --quick-exit-max-seconds "${QUICK_EXIT_MAX_SECONDS:-120}" \
  --exit-person-role "${EXIT_PERSON_ROLE:-P1}" \
  --minimum-partition-count "${MINIMUM_PARTITION_COUNT:-1}" \
  --quick-exit-reference-role "${QUICK_EXIT_REFERENCE_ROLE:-CARRY2}"

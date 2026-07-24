#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"
QUERY_ID="${1:-Q1.1}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
OUTPUT_ROOT="${OUTPUT_ROOT:-${PROJECT_DIR}/benchmark_outputs_vr15/profiling/${RUN_ID}/${QUERY_ID//./_}}"
python3 -m py_compile config.py graph_io.py queries.py baseline.py proposed_distributed.py run_experiment.py query_specs/*.py
python3 run_experiment.py \
  --mode profiling \
  --query-id "${QUERY_ID}" \
  --datasets "${DATASETS:-dataset_1m}" \
  --repeats "${PROFILE_REPEATS:-1}" \
  --output-root "${OUTPUT_ROOT}" \
  --spark-master "${SPARK_MASTER:-spark://master:7077}" \
  --driver-memory "${DRIVER_MEMORY:-3G}" \
  --executor-memory "${EXECUTOR_MEMORY:-3G}" \
  --executor-cores "${EXECUTOR_CORES:-1}" \
  --cores-max "${CORES_MAX:-4}" \
  --shuffle-partitions "${SHUFFLE_PARTITIONS:-6}" \
  --time-window-duration-seconds "${TIME_WINDOW_DURATION_SECONDS:-10}" \
  --max-time-gap-seconds "${MAX_TIME_GAP_SECONDS:-120}" \
  --quick-exit-max-seconds "${QUICK_EXIT_MAX_SECONDS:-120}" \
  --exit-person-role "${EXIT_PERSON_ROLE:-P1}" \
  --minimum-partition-count "${MINIMUM_PARTITION_COUNT:-1}" \
  --quick-exit-reference-role "${QUICK_EXIT_REFERENCE_ROLE:-CARRY2}"

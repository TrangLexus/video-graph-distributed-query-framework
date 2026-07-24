#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# VideoGraphDB C2 - Profiling Runtime-Only Benchmark - One query
#
# Purpose:
#   Run C2 materialized phase-boundary profiling for one selected query,
#   but keep only algorithm/phase runtime measurement (Muc A).
#
# Important:
#   This script calls run_experiment.py with --mode profiling_runtime_only.
#   It materializes/counts phase boundaries for profiling, but it does NOT
#   write full witnesses_csv. Results are for phase-cost attribution only,
#   not official runtime reporting.
#
# Usage:
#   chmod +x scripts/run_one_query_profile_runtime_only.sh
#   ./scripts/run_one_query_profile_runtime_only.sh Q1.1
#
# Optional env overrides:
#   DATASETS=dataset_1m PROFILE_RUNTIME_ONLY_REPEATS=3 ./scripts/run_one_query_profile_runtime_only.sh Q2.3
# ============================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_BASE="${PROJECT_DIR}/benchmark_outputs_profile"
RUN_ID="$(date +%Y%m%d_%H%M%S)"

PROFILE_RUNTIME_ONLY_BASE="${OUTPUT_BASE}/profiling_runtime_only/${RUN_ID}"
LOG_DIR="${OUTPUT_BASE}/logs"
STATUS_DIR="${OUTPUT_BASE}/status"

mkdir -p "${PROFILE_RUNTIME_ONLY_BASE}" "${LOG_DIR}" "${STATUS_DIR}"

QUERY_ID="${1:-Q1.1}"
QUERY_TAG="${QUERY_ID//./_}"

QUERY_OUTPUT_ROOT="${PROFILE_RUNTIME_ONLY_BASE}/query_outputs_profile_runtime_only_${QUERY_TAG}"
mkdir -p "${QUERY_OUTPUT_ROOT}"

LOG_FILE="${LOG_DIR}/run_one_query_profile_runtime_only_${QUERY_TAG}_${RUN_ID}.log"
STATUS_FILE="${STATUS_DIR}/run_one_query_profile_runtime_only_status_${QUERY_TAG}_${RUN_ID}.csv"

DATASETS="${DATASETS:-dataset_1m}"
PROFILE_RUNTIME_ONLY_REPEATS="${PROFILE_RUNTIME_ONLY_REPEATS:-${PROFILE_REPEATS:-1}}"

SPARK_MASTER="${SPARK_MASTER:-spark://master:7077}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
DRIVER_MEMORY="${DRIVER_MEMORY:-2G}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-5G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
CORES_MAX="${CORES_MAX:-8}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-16}"

HDFS_ROOT="${HDFS_ROOT:-hdfs:///data/videographdb}"

MAX_NEXTTW_HOPS="${MAX_NEXTTW_HOPS:-120}"
EPSILON_TIME_SECONDS="${EPSILON_TIME_SECONDS:-120}"
QUICK_EXIT_SECONDS="${QUICK_EXIT_SECONDS:-120}"

Q14_EXIT_PERSON_ROLE="${Q14_EXIT_PERSON_ROLE:-P1}"
Q14_MIN_PARTITION_COUNT="${Q14_MIN_PARTITION_COUNT:-1}"
Q14_QUICK_EXIT_REFERENCE="${Q14_QUICK_EXIT_REFERENCE:-CARRY2}"
Q14_REQUIRE_DIFFERENT_LOCATION="${Q14_REQUIRE_DIFFERENT_LOCATION:-false}"
ENABLE_Q14_DIAGNOSTICS="${ENABLE_Q14_DIAGNOSTICS:-false}"

AUTO_BROADCAST_JOIN_THRESHOLD="${AUTO_BROADCAST_JOIN_THRESHOLD:-2097152}"
FILES_MAX_PARTITION_BYTES="${FILES_MAX_PARTITION_BYTES:-67108864}"

cd "${PROJECT_DIR}"

echo "query_id,status,output_root,started_at,finished_at,log_file" > "${STATUS_FILE}"

STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
OVERALL_STATUS=0

{
  echo "============================================================"
  echo "VideoGraphDB one-query C2 profiling runtime-only benchmark"
  echo "Run ID:       ${RUN_ID}"
  echo "Project dir:  ${PROJECT_DIR}"
  echo "Output root:  ${QUERY_OUTPUT_ROOT}"
  echo "Query:        ${QUERY_ID}"
  echo "Datasets:     ${DATASETS}"
  echo "Repeats:      ${PROFILE_RUNTIME_ONLY_REPEATS}"
  echo "Mode:         profiling_runtime_only"
  echo "Benchmark:    C2 Muc A - materialized phase-boundary profiling without witnesses_csv export"
  echo "Spark master: ${SPARK_MASTER}"
  echo "MAX_NEXTTW_HOPS:       ${MAX_NEXTTW_HOPS}"
  echo "EPSILON_TIME_SECONDS:  ${EPSILON_TIME_SECONDS}"
  echo "QUICK_EXIT_SECONDS:    ${QUICK_EXIT_SECONDS}"
  echo "============================================================"

  echo
  echo "Step 1/3 - Python compile check"
  python3 -m py_compile \
    run_experiment.py \
    baseline.py \
    proposed_distributed.py \
    queries.py \
    graph_io.py \
    config.py \
    query_specs/*.py

  echo
  echo "Step 2/3 - Query registry check"
  QUERY_ID="${QUERY_ID}" python3 - <<'PY'
import os
from queries import QUERY_REGISTRY
query_id = os.environ["QUERY_ID"]
print("Supported queries:", sorted(QUERY_REGISTRY.keys()))
if query_id not in QUERY_REGISTRY:
    raise SystemExit(f"Query is not registered: {query_id}")
spec = QUERY_REGISTRY[query_id]
print("Selected query:", query_id)
print("Query name:", spec.query_name)
print("Logical type:", spec.logical_query_type)
print("Physical plan:", spec.physical_plan)
print("Witness type:", spec.witness_type)
print("Compare columns:", spec.compare_columns)
PY

  echo
  echo "Step 3/3 - C2 profiling runtime-only benchmark"
  CMD=(
    python3 run_experiment.py
    --mode profiling_runtime_only
    --datasets "${DATASETS}"
    --query-id "${QUERY_ID}"
    --repeats "${PROFILE_RUNTIME_ONLY_REPEATS}"
    --hdfs-root "${HDFS_ROOT}"
    --output-root "${QUERY_OUTPUT_ROOT}"
    --spark-master "${SPARK_MASTER}"
    --spark-submit "${SPARK_SUBMIT}"
    --driver-memory "${DRIVER_MEMORY}"
    --executor-memory "${EXECUTOR_MEMORY}"
    --executor-cores "${EXECUTOR_CORES}"
    --cores-max "${CORES_MAX}"
    --shuffle-partitions "${SHUFFLE_PARTITIONS}"
    --max-nexttw-hops "${MAX_NEXTTW_HOPS}"
    --epsilon-time-seconds "${EPSILON_TIME_SECONDS}"
    --quick-exit-seconds "${QUICK_EXIT_SECONDS}"
    --q14-exit-person-role "${Q14_EXIT_PERSON_ROLE}"
    --q14-min-partition-count "${Q14_MIN_PARTITION_COUNT}"
    --q14-quick-exit-reference "${Q14_QUICK_EXIT_REFERENCE}"
    --auto-broadcast-join-threshold "${AUTO_BROADCAST_JOIN_THRESHOLD}"
    --files-max-partition-bytes "${FILES_MAX_PARTITION_BYTES}"
  )

  if [[ "${Q14_REQUIRE_DIFFERENT_LOCATION}" =~ ^(1|true|TRUE|yes|YES|y|Y|on|ON)$ ]]; then
    CMD+=(--q14-require-different-location)
  fi
  if [[ "${ENABLE_Q14_DIAGNOSTICS}" =~ ^(1|true|TRUE|yes|YES|y|Y|on|ON)$ ]]; then
    CMD+=(--enable-q14-diagnostics)
  fi

  echo "$ ${CMD[*]}"
  if "${CMD[@]}"; then
    FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "${QUERY_ID},PASS,${QUERY_OUTPUT_ROOT},${STARTED_AT},${FINISHED_AT},${LOG_FILE}" >> "${STATUS_FILE}"
  else
    OVERALL_STATUS=1
    FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "${QUERY_ID},FAIL,${QUERY_OUTPUT_ROOT},${STARTED_AT},${FINISHED_AT},${LOG_FILE}" >> "${STATUS_FILE}"
  fi

  echo
  echo "============================================================"
  echo "Completed one-query C2 profiling runtime-only benchmark"
  echo "Status file: ${STATUS_FILE}"
  echo "Output root: ${QUERY_OUTPUT_ROOT}"
  echo "Log file:    ${LOG_FILE}"
  echo "============================================================"
} 2>&1 | tee "${LOG_FILE}"

exit "${OVERALL_STATUS}"


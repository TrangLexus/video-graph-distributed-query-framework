#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# VideoGraphDB Profiling Benchmark - Multiple queries
#
# Purpose:
#   Run diagnostic/profiling benchmark for selected queries.
#   This script continues with the next query if one query fails.
#
# Important:
#   Profiling output is NOT official paper runtime. It enables extra
#   count/materialization actions to expose phase-level costs and
#   intermediate cardinalities.
#
# Usage:
#   chmod +x run_multi_queries_profiling.sh
#   ./run_multi_queries_profiling.sh
# ============================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_BASE="${PROJECT_DIR}/benchmark_outputs_official"
RUN_ID="$(date +%Y%m%d_%H%M%S)"

PROFILING_BASE="${OUTPUT_BASE}/profiling/${RUN_ID}"
LOG_DIR="${OUTPUT_BASE}/logs"
STATUS_DIR="${OUTPUT_BASE}/status"

mkdir -p "${PROFILING_BASE}" "${LOG_DIR}" "${STATUS_DIR}"

LOG_FILE="${LOG_DIR}/run_multi_queries_profiling_${RUN_ID}.log"
STATUS_FILE="${STATUS_DIR}/run_multi_queries_profiling_status_${RUN_ID}.csv"

QUERIES=(
  #"Q1.1"
  #"Q1.2"
  #"Q1.3"
  #"Q2.1"
  #"Q2.2"
 "Q2.3"
 "Q3.1"
 # "Q3.2"
  #"Q3.3"
  #"Q4.1"
  #"Q4.2"
  #"Q4.3"
)

DATASETS="${DATASETS:-dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m}"
PROFILING_REPEATS="${PROFILING_REPEATS:-3}"

SPARK_MASTER="${SPARK_MASTER:-spark://master:7077}"
DRIVER_MEMORY="${DRIVER_MEMORY:-2G}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-5G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
CORES_MAX="${CORES_MAX:-8}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-16}"

# Profiling default keeps NEXT_TW bound small to avoid turning diagnostics into
# a memory-heavy stress test. Override only when needed.
MAX_NEXTTW_HOPS="${MAX_NEXTTW_HOPS:-3}"
EPSILON_TIME_SECONDS="${EPSILON_TIME_SECONDS:-120}"
QUICK_EXIT_SECONDS="${QUICK_EXIT_SECONDS:-120}"

Q14_EXIT_PERSON_ROLE="${Q14_EXIT_PERSON_ROLE:-P1}"
Q14_MIN_PARTITION_COUNT="${Q14_MIN_PARTITION_COUNT:-1}"
Q14_QUICK_EXIT_REFERENCE="${Q14_QUICK_EXIT_REFERENCE:-CARRY2}"

cd "${PROJECT_DIR}"

echo "query_id,status,output_root,started_at,finished_at" > "${STATUS_FILE}"

OVERALL_STATUS=0
FAILED_QUERIES=()

{
  echo "============================================================"
  echo "VideoGraphDB multi-query profiling benchmark"
  echo "Run ID:       ${RUN_ID}"
  echo "Project dir:  ${PROJECT_DIR}"
  echo "Output dir:   ${PROFILING_BASE}"
  echo "Queries:      ${QUERIES[*]}"
  echo "Datasets:     ${DATASETS}"
  echo "Repeats:      ${PROFILING_REPEATS}"
  echo "Mode:         profiling"
  echo "Note:         diagnostic counts/materialization enabled; not official runtime"
  echo "============================================================"

  echo
  echo "Global Step - Python compile check"
  python3 -m py_compile \
    run_experiment.py \
    baseline.py \
    proposed_distributed.py \
    queries.py \
    graph_io.py \
    config.py

  echo
  echo "Global Step - Supported query registry"
  python3 - <<'PY'
from queries import QUERY_REGISTRY
print("Supported queries:", sorted(QUERY_REGISTRY.keys()))
PY

  for QUERY_ID in "${QUERIES[@]}"; do
    QUERY_TAG="${QUERY_ID//./_}"
    QUERY_OUTPUT_ROOT="${PROFILING_BASE}/query_outputs_profiling_${QUERY_TAG}"
    mkdir -p "${QUERY_OUTPUT_ROOT}"

    STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"

    echo
    echo "============================================================"
    echo "Profiling benchmark for query: ${QUERY_ID}"
    echo "Output root: ${QUERY_OUTPUT_ROOT}"
    echo "Started at:  ${STARTED_AT}"
    echo "============================================================"

    if QUERY_ID="${QUERY_ID}" python3 - <<'PY'
import os
from queries import QUERY_REGISTRY
query_id = os.environ["QUERY_ID"]
if query_id not in QUERY_REGISTRY:
    raise SystemExit(f"Query is not registered: {query_id}")
spec = QUERY_REGISTRY[query_id]
print("Selected query:", query_id)
print("Query name:", spec.query_name)
print("Logical type:", spec.logical_query_type)
print("Physical plan:", spec.physical_plan)
PY
    then
      echo "Query registry PASS: ${QUERY_ID}"
    else
      FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "${QUERY_ID},FAIL_REGISTRY,${QUERY_OUTPUT_ROOT},${STARTED_AT},${FINISHED_AT}" >> "${STATUS_FILE}"
      echo "Query registry FAIL: ${QUERY_ID}"
      echo "Continue with next query..."
      OVERALL_STATUS=1
      FAILED_QUERIES+=("${QUERY_ID}")
      continue
    fi

    if python3 run_experiment.py \
      --mode profiling \
      --datasets "${DATASETS}" \
      --query-id "${QUERY_ID}" \
      --repeats "${PROFILING_REPEATS}" \
      --output-root "${QUERY_OUTPUT_ROOT}" \
      --spark-master "${SPARK_MASTER}" \
      --driver-memory "${DRIVER_MEMORY}" \
      --executor-memory "${EXECUTOR_MEMORY}" \
      --executor-cores "${EXECUTOR_CORES}" \
      --cores-max "${CORES_MAX}" \
      --shuffle-partitions "${SHUFFLE_PARTITIONS}" \
      --max-nexttw-hops "${MAX_NEXTTW_HOPS}" \
      --epsilon-time-seconds "${EPSILON_TIME_SECONDS}" \
      --quick-exit-seconds "${QUICK_EXIT_SECONDS}" \
      --q14-exit-person-role "${Q14_EXIT_PERSON_ROLE}" \
      --q14-min-partition-count "${Q14_MIN_PARTITION_COUNT}" \
      --q14-quick-exit-reference "${Q14_QUICK_EXIT_REFERENCE}"; then

      FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "${QUERY_ID},PASS,${QUERY_OUTPUT_ROOT},${STARTED_AT},${FINISHED_AT}" >> "${STATUS_FILE}"
      echo "Profiling PASS: ${QUERY_ID}"

    else
      FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "${QUERY_ID},FAIL,${QUERY_OUTPUT_ROOT},${STARTED_AT},${FINISHED_AT}" >> "${STATUS_FILE}"
      echo "Profiling FAIL: ${QUERY_ID}"
      echo "Status file: ${STATUS_FILE}"
      echo "Continue with next query..."
      OVERALL_STATUS=1
      FAILED_QUERIES+=("${QUERY_ID}")
      continue
    fi
  done

  echo
  echo "============================================================"
  if [[ "${OVERALL_STATUS}" -eq 0 ]]; then
    echo "All profiling benchmarks completed successfully"
  else
    echo "Profiling benchmark completed with failed queries"
    echo "Failed queries: ${FAILED_QUERIES[*]}"
  fi
  echo "Run ID:      ${RUN_ID}"
  echo "Output dir:  ${PROFILING_BASE}"
  echo "Status file: ${STATUS_FILE}"
  echo "Log file:    ${LOG_FILE}"
  echo "============================================================"

  exit "${OVERALL_STATUS}"

} 2>&1 | tee "${LOG_FILE}"


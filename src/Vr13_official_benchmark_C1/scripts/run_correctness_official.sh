#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# VideoGraphDB C1 - Official Correctness Validation
#
# Purpose:
#   Run correctness validation for multiple queries over all
#   configured datasets. This script DOES NOT run official runtime
#   measurement and DOES NOT run profiling.
#
# Usage:
#   chmod +x scripts/run_correctness_official.sh
#   DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
#   ./scripts/run_correctness_official.sh
#
# Output:
#   ${PROJECT_DIR}/benchmark_outputs_official/correctness/<RUN_ID>/...
#   ${PROJECT_DIR}/benchmark_outputs_official/logs/...
#   ${PROJECT_DIR}/benchmark_outputs_official/status/...
# ============================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_BASE="${PROJECT_DIR}/benchmark_outputs_official"
RUN_ID="$(date +%Y%m%d_%H%M%S)"

CORRECTNESS_BASE="${OUTPUT_BASE}/correctness/${RUN_ID}"
LOG_DIR="${OUTPUT_BASE}/logs"
STATUS_DIR="${OUTPUT_BASE}/status"

mkdir -p "${CORRECTNESS_BASE}" "${LOG_DIR}" "${STATUS_DIR}"

LOG_FILE="${LOG_DIR}/run_correctness_official_${RUN_ID}.log"
STATUS_FILE="${STATUS_DIR}/run_correctness_official_status_${RUN_ID}.csv"

# ------------------------------------------------------------
# Query list for correctness validation
# Default: all 12 Vr09 workload queries.
# ------------------------------------------------------------
QUERIES=(
  "Q1.1"
  "Q1.2"
  "Q1.3"
  "Q2.1"
  "Q2.2"
  "Q2.3"
  "Q3.1"
  "Q3.2"
  "Q3.3"
  "Q4.1"
  "Q4.2"
  "Q4.3"
)

# ------------------------------------------------------------
# Dataset list
# ------------------------------------------------------------
DATASETS="${DATASETS:-dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m}"

# If true, correctness mode fails when both Baseline and Proposed return zero witnesses.
# Recommended false for all-query correctness because some query/dataset pairs may legitimately return zero.
REQUIRE_NONZERO="${REQUIRE_NONZERO:-false}"

# ------------------------------------------------------------
# Spark configuration
# ------------------------------------------------------------
SPARK_MASTER="${SPARK_MASTER:-spark://master:7077}"
DRIVER_MEMORY="${DRIVER_MEMORY:-2G}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-5G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
CORES_MAX="${CORES_MAX:-8}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-16}"

# ------------------------------------------------------------
# Query parameters
# ------------------------------------------------------------
MAX_NEXTTW_HOPS="${MAX_NEXTTW_HOPS:-120}"
EPSILON_TIME_SECONDS="${EPSILON_TIME_SECONDS:-120}"
QUICK_EXIT_SECONDS="${QUICK_EXIT_SECONDS:-120}"

Q14_EXIT_PERSON_ROLE="${Q14_EXIT_PERSON_ROLE:-P1}"
Q14_MIN_PARTITION_COUNT="${Q14_MIN_PARTITION_COUNT:-1}"
Q14_QUICK_EXIT_REFERENCE="${Q14_QUICK_EXIT_REFERENCE:-CARRY2}"

cd "${PROJECT_DIR}"

echo "query_id,status,output_root,started_at,finished_at" > "${STATUS_FILE}"

# Continue running remaining queries even if one query fails.
# At the end, the script exits with 1 if at least one query failed.
OVERALL_STATUS=0
FAILED_QUERIES=()

{
  echo "============================================================"
  echo "VideoGraphDB official correctness validation"
  echo "Run ID:       ${RUN_ID}"
  echo "Project dir:  ${PROJECT_DIR}"
  echo "Output base:  ${OUTPUT_BASE}"
  echo "Output dir:   ${CORRECTNESS_BASE}"
  echo "Queries:      ${QUERIES[*]}"
  echo "Datasets:     ${DATASETS}"
  echo "Require nonzero: ${REQUIRE_NONZERO}"
  echo "Mode:         correctness"
  echo "============================================================"

  echo
  echo "============================================================"
  echo "Global Step - Python compile check"
  echo "============================================================"

  python3 -m py_compile \
    run_experiment.py \
    baseline.py \
    proposed_distributed.py \
    queries.py \
    graph_io.py \
    config.py \
    query_specs/q23.py \
    query_specs/q31.py

  echo
  echo "============================================================"
  echo "Global Step - Supported query registry"
  echo "============================================================"

  python3 - <<'PY'
from queries import QUERY_REGISTRY
print("Supported queries:", sorted(QUERY_REGISTRY.keys()))
PY

  for QUERY_ID in "${QUERIES[@]}"; do
    QUERY_TAG="${QUERY_ID//./_}"
    QUERY_OUTPUT_ROOT="${CORRECTNESS_BASE}/query_outputs_correctness_${QUERY_TAG}"
    mkdir -p "${QUERY_OUTPUT_ROOT}"

    STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"

    echo
    echo "============================================================"
    echo "Correctness check for query: ${QUERY_ID}"
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
print("Witness type:", spec.witness_type)
print("Compare columns:", spec.compare_columns)
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

    CORRECTNESS_ARGS=()
    if [[ "${REQUIRE_NONZERO}" == "true" ]]; then
      CORRECTNESS_ARGS+=(--require-nonzero)
    fi

    if python3 run_experiment.py \
      --mode correctness \
      --datasets "${DATASETS}" \
      --query-id "${QUERY_ID}" \
      --repeats 1 \
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
      --q14-quick-exit-reference "${Q14_QUICK_EXIT_REFERENCE}" \
      "${CORRECTNESS_ARGS[@]}"; then

      FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "${QUERY_ID},PASS,${QUERY_OUTPUT_ROOT},${STARTED_AT},${FINISHED_AT}" >> "${STATUS_FILE}"
      echo "Correctness PASS: ${QUERY_ID}"

    else
      FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "${QUERY_ID},FAIL,${QUERY_OUTPUT_ROOT},${STARTED_AT},${FINISHED_AT}" >> "${STATUS_FILE}"
      echo "Correctness FAIL: ${QUERY_ID}"
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
    echo "All correctness checks completed successfully"
  else
    echo "Correctness checks completed with failed queries"
    echo "Failed queries: ${FAILED_QUERIES[*]}"
  fi
  echo "Run ID:      ${RUN_ID}"
  echo "Output dir:  ${CORRECTNESS_BASE}"
  echo "Status file: ${STATUS_FILE}"
  echo "Log file:    ${LOG_FILE}"
  echo "============================================================"

  exit "${OVERALL_STATUS}"

} 2>&1 | tee "${LOG_FILE}"


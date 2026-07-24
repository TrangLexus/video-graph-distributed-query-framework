#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# VideoGraphDB C1 - Official Runtime-Only Benchmark - Multiple queries
#
# Purpose:
#   Run the official runtime-only lazy/end-to-end benchmark for selected
#   VideoGraphDB queries over the configured HDFS datasets.
#
# Important:
#   This script is for OFFICIAL paper/luận án runtime measurement without full witnesses_csv export.
#   It calls run_experiment.py with --mode official_runtime_only.
#   It must NOT use --mode profiling or profiling-only diagnostic
#   materialization/count actions. It also does NOT write full witnesses_csv.
#
# Behavior:
#   - Continue with the next query if one query fails.
#   - Write one status CSV for PASS/FAIL tracking.
#   - Write one global log for the whole multi-query run.
#
# Usage:
#   chmod +x scripts/run_multi_queries_official_runtime_only_runtime_only.sh
#   DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
#   OFFICIAL_RUNTIME_ONLY_REPEATS=3 \
#   MAX_NEXTTW_HOPS=120 \
#   EPSILON_TIME_SECONDS=120 \
#   QUICK_EXIT_SECONDS=120 \
#   ./scripts/run_multi_queries_official_runtime_only_runtime_only.sh
# ============================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_BASE="${PROJECT_DIR}/benchmark_outputs_official"
RUN_ID="$(date +%Y%m%d_%H%M%S)"

RUNTIME_ONLY_BASE="${OUTPUT_BASE}/official_runtime_only/${RUN_ID}"
LOG_DIR="${OUTPUT_BASE}/logs"
STATUS_DIR="${OUTPUT_BASE}/status"

mkdir -p "${RUNTIME_ONLY_BASE}" "${LOG_DIR}" "${STATUS_DIR}"

LOG_FILE="${LOG_DIR}/run_multi_queries_official_runtime_only_${RUN_ID}.log"
STATUS_FILE="${STATUS_DIR}/run_multi_queries_official_runtime_only_status_${RUN_ID}.csv"

# ------------------------------------------------------------
# Query list for official benchmark
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

# Official benchmark repetitions.
# TIMING_REPEATS is accepted only as a backward-compatible alias.
OFFICIAL_RUNTIME_ONLY_REPEATS="${OFFICIAL_RUNTIME_ONLY_REPEATS:-${OFFICIAL_REPEATS:-${TIMING_REPEATS:-3}}}"

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
# Official benchmark keeps the configured temporal bound.
# Use MAX_NEXTTW_HOPS=3 only for debugging/profiling, not for
# the official result table, unless explicitly justified.
# ------------------------------------------------------------
MAX_NEXTTW_HOPS="${MAX_NEXTTW_HOPS:-120}"
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
  echo "VideoGraphDB official runtime-only multi-query benchmark"
  echo "Run ID:       ${RUN_ID}"
  echo "Project dir:  ${PROJECT_DIR}"
  echo "Output base:  ${OUTPUT_BASE}"
  echo "Output dir:   ${RUNTIME_ONLY_BASE}"
  echo "Queries:      ${QUERIES[*]}"
  echo "Datasets:     ${DATASETS}"
  echo "Repeats:      ${OFFICIAL_RUNTIME_ONLY_REPEATS}"
  echo "Mode:         official_runtime_only"
  echo "Benchmark:    official runtime-only paper runtime without witnesses_csv export"
  echo "Spark master: ${SPARK_MASTER}"
  echo "Driver mem:   ${DRIVER_MEMORY}"
  echo "Executor mem: ${EXECUTOR_MEMORY}"
  echo "Exec cores:   ${EXECUTOR_CORES}"
  echo "Cores max:    ${CORES_MAX}"
  echo "Shuffle part: ${SHUFFLE_PARTITIONS}"
  echo "MAX_NEXTTW_HOPS:       ${MAX_NEXTTW_HOPS}"
  echo "EPSILON_TIME_SECONDS:  ${EPSILON_TIME_SECONDS}"
  echo "QUICK_EXIT_SECONDS:    ${QUICK_EXIT_SECONDS}"
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

  echo
  echo "============================================================"
  echo "Global Step - HDFS/Spark preflight notes"
  echo "============================================================"
  echo "Please ensure before running this script:"
  echo "  hdfs dfsadmin -safemode get  -> Safe mode is OFF"
  echo "  hdfs dfsadmin -report        -> Live datanodes are available"
  echo "  hdfs dfs -ls -d /spark-logs  -> /spark-logs exists and is writable"
  echo "  Spark Standalone master/workers are running"
  echo "============================================================"

  for QUERY_ID in "${QUERIES[@]}"; do
    QUERY_TAG="${QUERY_ID//./_}"
    QUERY_OUTPUT_ROOT="${RUNTIME_ONLY_BASE}/query_outputs_official_runtime_only_${QUERY_TAG}"
    mkdir -p "${QUERY_OUTPUT_ROOT}"

    STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"

    echo
    echo "============================================================"
    echo "Official runtime-only benchmark for query: ${QUERY_ID}"
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

    if python3 run_experiment.py \
      --mode official_runtime_only \
      --datasets "${DATASETS}" \
      --query-id "${QUERY_ID}" \
      --repeats "${OFFICIAL_RUNTIME_ONLY_REPEATS}" \
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
      echo "Official runtime-only benchmark PASS: ${QUERY_ID}"

    else
      FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "${QUERY_ID},FAIL,${QUERY_OUTPUT_ROOT},${STARTED_AT},${FINISHED_AT}" >> "${STATUS_FILE}"
      echo "Official runtime-only benchmark FAIL: ${QUERY_ID}"
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
    echo "All official runtime-only benchmarks completed successfully"
  else
    echo "Official runtime-only benchmark completed with failed queries"
    echo "Failed queries: ${FAILED_QUERIES[*]}"
  fi
  echo "Run ID:      ${RUN_ID}"
  echo "Output dir:  ${RUNTIME_ONLY_BASE}"
  echo "Status file: ${STATUS_FILE}"
  echo "Log file:    ${LOG_FILE}"
  echo "============================================================"

  exit "${OVERALL_STATUS}"

} 2>&1 | tee "${LOG_FILE}"



#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <project_root> <parent_dir_name> <new_dir_name>"
  exit 2
fi

PROJECT_ROOT="$1"
PARENT_NAME="$2"
NEW_NAME="$3"
PARENT_DIR="${PROJECT_ROOT}/src/${PARENT_NAME}"
NEW_DIR="${PROJECT_ROOT}/src/${NEW_NAME}"

[[ -d "${PARENT_DIR}" ]] || { echo "[FAIL] Parent not found: ${PARENT_DIR}"; exit 1; }
[[ ! -e "${NEW_DIR}" ]] || { echo "[FAIL] New directory already exists: ${NEW_DIR}"; exit 1; }

mkdir -p "${NEW_DIR}"

if command -v rsync >/dev/null 2>&1; then
  rsync -a \
    --exclude='query_outputs*' \
    --exclude='logs' \
    --exclude='audit' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    "${PARENT_DIR}/" "${NEW_DIR}/"
else
  cp -a "${PARENT_DIR}/." "${NEW_DIR}/"
  find "${NEW_DIR}" -type d \( -name 'query_outputs*' -o -name 'logs' -o -name 'audit' -o -name '__pycache__' \) -prune -exec rm -rf {} +
  find "${NEW_DIR}" -type f -name '*.pyc' -delete
fi

mkdir -p "${NEW_DIR}/audit"/{environment,diff,static,correctness_representative,correctness_all_queries,fairness,timing_scope,benchmark,readiness}

cp "${PROJECT_ROOT}/governance/templates/VERSION_PASSPORT_TEMPLATE.md" "${NEW_DIR}/VERSION_PASSPORT.md"
cp "${PROJECT_ROOT}/governance/templates/CHANGE_IMPACT_MATRIX_TEMPLATE.csv" "${NEW_DIR}/CHANGE_IMPACT_MATRIX.csv"
cp "${PROJECT_ROOT}/governance/templates/DECISION_LOG_TEMPLATE.md" "${NEW_DIR}/DECISION_LOG.md"
cp "${PROJECT_ROOT}/governance/templates/AUDIT_REPORT_TEMPLATE.md" "${NEW_DIR}/AUDIT_REPORT.md"

sed -i "s/<VERSION>/${NEW_NAME}/g" "${NEW_DIR}/VERSION_PASSPORT.md" "${NEW_DIR}/AUDIT_REPORT.md"
sed -i "s/- Parent version:/- Parent version: ${PARENT_NAME}/" "${NEW_DIR}/VERSION_PASSPORT.md"

echo "NOT_AUDITED" > "${NEW_DIR}/audit/readiness/current_status.txt"
echo "[PASS] Created ${NEW_DIR}"
echo "[IMPORTANT] The new version starts at NOT_AUDITED."

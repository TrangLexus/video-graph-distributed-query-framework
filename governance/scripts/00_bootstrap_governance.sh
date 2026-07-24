#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${1:-$HOME/VideoGraphDB_Project}"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "${PROJECT_ROOT}/governance"/{specifications,templates,scripts,config,audit_registry}

cp -n "${SOURCE_DIR}/specifications/"* "${PROJECT_ROOT}/governance/specifications/" || true
cp -n "${SOURCE_DIR}/templates/"* "${PROJECT_ROOT}/governance/templates/" || true
cp -n "${SOURCE_DIR}/scripts/"* "${PROJECT_ROOT}/governance/scripts/" || true
cp -n "${SOURCE_DIR}/config/"* "${PROJECT_ROOT}/governance/config/" || true

echo "[PASS] Governance structure initialized at: ${PROJECT_ROOT}/governance"
echo "[NEXT] Review and complete specifications before approving G0."

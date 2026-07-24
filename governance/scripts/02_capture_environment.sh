#!/usr/bin/env bash
set -euo pipefail

VERSION_DIR="${1:-.}"
OUT_DIR="${2:-${VERSION_DIR}/audit/environment}"
mkdir -p "${OUT_DIR}"

date --iso-8601=seconds > "${OUT_DIR}/captured_at.txt"
hostname > "${OUT_DIR}/hostname.txt"
uname -a > "${OUT_DIR}/uname.txt"
python3 --version > "${OUT_DIR}/python_version.txt" 2>&1 || true
java -version > "${OUT_DIR}/java_version.txt" 2>&1 || true
spark-submit --version > "${OUT_DIR}/spark_version.txt" 2>&1 || true
python3 -m pip freeze > "${OUT_DIR}/pip_freeze.txt" 2>&1 || true

if git -C "${VERSION_DIR}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git -C "${VERSION_DIR}" rev-parse HEAD > "${OUT_DIR}/git_commit.txt"
  git -C "${VERSION_DIR}" branch --show-current > "${OUT_DIR}/git_branch.txt"
  git -C "${VERSION_DIR}" status --short > "${OUT_DIR}/git_status.txt"
fi

find "${VERSION_DIR}" -maxdepth 3 -type f \
  \( -name '*.yaml' -o -name '*.yml' -o -name '*.json' -o -name '*.py' \) \
  -print0 | sort -z | xargs -0 sha256sum > "${OUT_DIR}/important_files_sha256.txt" 2>/dev/null || true

echo "[PASS] Environment captured: ${OUT_DIR}"

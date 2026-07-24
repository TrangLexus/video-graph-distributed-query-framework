#!/usr/bin/env bash
set -euo pipefail

VERSION_DIR="${1:-.}"
OUT_DIR="${2:-${VERSION_DIR}/audit/static}"
mkdir -p "${OUT_DIR}"

set +e
python3 -m compileall -q "${VERSION_DIR}" > "${OUT_DIR}/compileall.stdout.log" 2> "${OUT_DIR}/compileall.stderr.log"
RC=$?
set -e

python3 - <<'PY' "${OUT_DIR}/static_gate.json" "${RC}"
import json, sys, datetime
out, rc = sys.argv[1], int(sys.argv[2])
payload = {
    "gate": "static",
    "status": "PASS" if rc == 0 else "FAIL",
    "exit_code": rc,
    "generated_at": datetime.datetime.now().astimezone().isoformat()
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
PY

if [[ "${RC}" -ne 0 ]]; then
  echo "[FAIL] Static checks failed. See ${OUT_DIR}"
  exit "${RC}"
fi

echo "[PASS] Static checks passed."

#!/usr/bin/env python3
from __future__ import annotations
import argparse
import datetime as dt
import json
from pathlib import Path

DEFAULT_FILES = {
    "static": "static/static_gate.json",
    "all_query_correctness": "all_query_correctness/all_query_correctness_gate.json",
    "fairness": "fairness/fairness_gate.json",
    "timing_scope": "timing_scope/timing_scope_gate.json",
}

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit-dir", required=True)
    ap.add_argument("--output", default="")
    ap.add_argument("--gate-file", action="append", default=[],
                    help="stage=relative/path.json")
    args = ap.parse_args()

    audit_dir = Path(args.audit_dir).resolve()
    files = dict(DEFAULT_FILES)
    for item in args.gate_file:
        stage, rel = item.split("=", 1)
        files[stage] = rel

    results = {}
    failures = []
    for stage, rel in files.items():
        path = audit_dir / rel
        if not path.exists():
            results[stage] = {"status": "MISSING", "path": str(path)}
            failures.append(stage)
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            status = payload.get("status", "UNKNOWN")
        except Exception as exc:
            status = "INVALID"
            payload = {"error": str(exc)}
        results[stage] = {"status": status, "path": str(path), "payload": payload}
        if status != "PASS":
            failures.append(stage)

    final_status = "BENCHMARK_READY" if not failures else "NOT_READY"
    out = Path(args.output).resolve() if args.output else audit_dir / "readiness/readiness_gate.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "gate": "benchmark_readiness",
        "status": final_status,
        "failed_or_missing_stages": failures,
        "stages": results,
        "generated_at": dt.datetime.now().astimezone().isoformat()
    }
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{final_status}] {out}")
    return 0 if final_status == "BENCHMARK_READY" else 1

if __name__ == "__main__":
    raise SystemExit(main())

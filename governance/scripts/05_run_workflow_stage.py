#!/usr/bin/env python3
from __future__ import annotations
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--stage", required=True,
                        choices=["representative_correctness", "all_query_correctness",
                                 "fairness", "timing_scope", "full_benchmark"])
    args = parser.parse_args()

    cfg_path = Path(args.config).resolve()
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    version_dir = Path(cfg["version_dir"]).resolve()
    audit_dir = version_dir / cfg.get("audit_dir", "audit")
    command = cfg.get("commands", {}).get(args.stage, "").strip()
    stage_dir = audit_dir / args.stage
    stage_dir.mkdir(parents=True, exist_ok=True)

    gate_path = stage_dir / f"{args.stage}_gate.json"
    stdout_path = stage_dir / "stdout.log"
    stderr_path = stage_dir / "stderr.log"

    if not command:
        payload = {
            "gate": args.stage,
            "status": "NOT_CONFIGURED",
            "exit_code": None,
            "command": "",
            "generated_at": dt.datetime.now().astimezone().isoformat()
        }
        gate_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[NOT_CONFIGURED] Set commands.{args.stage} in {cfg_path}")
        return 3

    started = dt.datetime.now().astimezone()
    with stdout_path.open("w", encoding="utf-8") as out, stderr_path.open("w", encoding="utf-8") as err:
        proc = subprocess.run(command, cwd=version_dir, shell=True, stdout=out, stderr=err)
    ended = dt.datetime.now().astimezone()

    payload = {
        "gate": args.stage,
        "status": "PASS" if proc.returncode == 0 else "FAIL",
        "exit_code": proc.returncode,
        "command": command,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_seconds": (ended - started).total_seconds(),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path)
    }
    gate_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{payload['status']}] {args.stage}: {gate_path}")
    return proc.returncode

if __name__ == "__main__":
    raise SystemExit(main())

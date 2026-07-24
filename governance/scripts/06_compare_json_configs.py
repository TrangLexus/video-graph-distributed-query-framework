#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import Any

def flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            p = f"{prefix}.{key}" if prefix else str(key)
            out.update(flatten(value, p))
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            p = f"{prefix}[{i}]"
            out.update(flatten(value, p))
    else:
        out[prefix] = obj
    return out

def allowed(path: str, allowlist: list[str]) -> bool:
    return any(path == item or path.startswith(item + ".") or path.startswith(item + "[")
               for item in allowlist)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--proposed", required=True)
    ap.add_argument("--allow", action="append", default=[])
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    b = flatten(json.loads(Path(args.baseline).read_text(encoding="utf-8")))
    p = flatten(json.loads(Path(args.proposed).read_text(encoding="utf-8")))
    keys = sorted(set(b) | set(p))
    differences = []
    disallowed = []

    for key in keys:
        if b.get(key) != p.get(key):
            item = {"path": key, "baseline": b.get(key), "proposed": p.get(key),
                    "allowed": allowed(key, args.allow)}
            differences.append(item)
            if not item["allowed"]:
                disallowed.append(item)

    payload = {
        "gate": "fairness",
        "status": "PASS" if not disallowed else "FAIL",
        "allowed_paths": args.allow,
        "differences": differences,
        "disallowed_differences": disallowed
    }
    Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{payload['status']}] fairness differences: {len(differences)}, disallowed: {len(disallowed)}")
    return 0 if not disallowed else 1

if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-dir", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--include", action="append", default=["*.csv", "*.json", "*.log", "*.md", "*.yaml", "*.yml"])
    args = ap.parse_args()

    version_dir = Path(args.version_dir).resolve()
    files = {}
    seen = set()
    for pattern in args.include:
        for path in version_dir.rglob(pattern):
            if path.is_file() and path not in seen:
                seen.add(path)
                rel = str(path.relative_to(version_dir))
                files[rel] = {"size": path.stat().st_size, "sha256": sha256(path)}

    payload = {
        "version_dir": str(version_dir),
        "generated_at": dt.datetime.now().astimezone().isoformat(),
        "file_count": len(files),
        "files": files
    }
    out = Path(args.output).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[PASS] Manifest created: {out} ({len(files)} files)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

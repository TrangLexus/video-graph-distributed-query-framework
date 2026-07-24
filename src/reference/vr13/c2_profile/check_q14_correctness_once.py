#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_q14_correctness_once.py

Mục đích:
    Kiểm tra một lần duy nhất xem 2 thuật toán:
      1) baseline_global_bounded_erpq
      2) proposed_distributed_boundary_erpq
    có chạy ra witness Q14 và có khớp normalized witness set hay không.

Cách dùng:
    Đặt file này cùng thư mục với:
      baseline.py
      proposed_distributed.py
      run_experiment.py
      config.py
      graph_io.py
      queries.py

    Chạy:
      python3 check_q14_correctness_once.py

    Hoặc chỉ định dataset:
      python3 check_q14_correctness_once.py --dataset dataset_1m
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_cmd(cmd: list[str], env: dict[str, str]) -> int:
    print("\n" + "=" * 100)
    print("$ " + " ".join(cmd))
    print("=" * 100)
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
    return proc.wait()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-shot correctness check for Q14 Baseline vs Proposed Distributed."
    )
    parser.add_argument("--dataset", default="dataset_1m", help="Dataset name under HDFS root, e.g. dataset_1m")
    parser.add_argument("--hdfs-root", default="hdfs:///data/videographdb")
    parser.add_argument("--output-root", default="./query_outputs_correctness_once")
    parser.add_argument("--spark-master", default=os.getenv("SPARK_MASTER", "spark://master:7077"))
    parser.add_argument("--spark-submit", default=os.getenv("SPARK_SUBMIT", "spark-submit"))
    parser.add_argument("--shuffle-partitions", default=os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
    parser.add_argument("--driver-memory", default=os.getenv("SPARK_DRIVER_MEMORY", "2G"))
    parser.add_argument("--executor-memory", default=os.getenv("SPARK_EXECUTOR_MEMORY", "5G"))
    parser.add_argument("--executor-cores", default=os.getenv("SPARK_EXECUTOR_CORES", "2"))
    parser.add_argument("--cores-max", default=os.getenv("SPARK_CORES_MAX", "8"))

    # Q14 correctness constraints. Defaults are intentionally relaxed enough to
    # verify code conversion first; stricter paper-specific settings can be passed
    # explicitly.
    parser.add_argument("--max-nexttw-hops", default="120")
    parser.add_argument("--epsilon-time-seconds", default="120")
    parser.add_argument("--quick-exit-seconds", default="120")
    parser.add_argument("--q14-exit-person-role", choices=["P1", "P2"], default="P1")
    parser.add_argument("--q14-min-partition-count", default="1")
    parser.add_argument("--q14-quick-exit-reference", choices=["INTERACT", "CARRY2"], default="CARRY2")
    parser.add_argument(
        "--require-different-location",
        action="store_true",
        default=False,
        help="Enable the different-location constraint. Disabled by default for validated Q14 timing/correctness.",
    )
    parser.add_argument(
        "--no-different-location",
        action="store_true",
        help="Backward-compatible no-op: different-location is already disabled by default.",
    )
    parser.add_argument(
        "--allow-zero",
        action="store_true",
        help="Allow both methods to return zero witnesses. Default is fail on both-zero.",
    )
    parser.add_argument(
        "--enable-q14-diagnostics",
        action="store_true",
        help="Enable Q14 stage-by-stage diagnostic counts.",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    required_files = [
        "baseline.py",
        "proposed_distributed.py",
        "run_experiment.py",
        "config.py",
        "graph_io.py",
        "queries.py",
    ]
    missing = [f for f in required_files if not (script_dir / f).exists()]
    if missing:
        print("[ERROR] Missing required files in current directory:", ", ".join(missing))
        print("Please place this script in the same directory as the Vr09 Python runner files.")
        sys.exit(2)

    env = os.environ.copy()

    # Important for spark-submit mode, avoid Jupyter driver variables.
    env.pop("PYSPARK_DRIVER_PYTHON", None)
    env.pop("PYSPARK_DRIVER_PYTHON_OPTS", None)
    env["PYSPARK_PYTHON"] = "python3"
    env["PYSPARK_DRIVER_PYTHON"] = "python3"

    # Correctness mode: save final witnesses and force final action.
    # Explicitly control this flag so a stale shell variable cannot silently
    # over-constrain Q14 when --no-different-location is used.
    env["Q14_REQUIRE_DIFFERENT_LOCATION"] = "true" if args.require_different_location else "false"
    env["SAVE_FINAL_WITNESSES"] = "true"
    env["SAVE_RUNTIME_SUMMARY"] = "true"
    env["SAVE_CANDIDATE_OUTPUTS"] = "false"
    env["SAVE_INTERMEDIATE_OUTPUTS"] = "false"
    env["BENCHMARK_FORCE_FINAL_ACTION"] = "true"
    env["BENCHMARK_TIMING_ONLY"] = "true"
    env["BENCHMARK_VALIDATE_RESULT"] = "true"

    # Disable expensive benchmark-only counts, but keep final witness count.
    env["BENCHMARK_COUNT_DATASET_SIZE"] = "false"
    env["BENCHMARK_COUNT_INTERMEDIATE"] = "false"
    env["BENCHMARK_MATERIALIZE_STEP_BOUNDARIES"] = "false"
    env["ENABLE_LOCAL_COMPLETE_DETECTION"] = os.getenv("ENABLE_LOCAL_COMPLETE_DETECTION", "false")

    # Disable confidence filtering if queries.py has been changed as advised:
    # _q14_confidence_pass(...) returns F.lit(True).
    # This env var is kept only as documentation for the run log.
    env["Q14_CONFIDENCE_FILTER_DISABLED_EXPECTED"] = "true"

    cmd = [
        sys.executable,
        str(script_dir / "run_experiment.py"),
        "--mode", "correctness",
        "--datasets", args.dataset,
        "--query-id", "Q14",
        "--hdfs-root", args.hdfs_root,
        "--output-root", args.output_root,
        "--spark-master", args.spark_master,
        "--spark-submit", args.spark_submit,
        "--shuffle-partitions", str(args.shuffle_partitions),
        "--driver-memory", str(args.driver_memory),
        "--executor-memory", str(args.executor_memory),
        "--executor-cores", str(args.executor_cores),
        "--cores-max", str(args.cores_max),
        "--max-nexttw-hops", str(args.max_nexttw_hops),
        "--epsilon-time-seconds", str(args.epsilon_time_seconds),
        "--quick-exit-seconds", str(args.quick_exit_seconds),
        "--q14-exit-person-role", args.q14_exit_person_role,
        "--q14-min-partition-count", str(args.q14_min_partition_count),
        "--q14-quick-exit-reference", args.q14_quick_exit_reference,
    ]

    if args.require_different_location:
        cmd.append("--q14-require-different-location")

    if args.enable_q14_diagnostics:
        cmd.append("--enable-q14-diagnostics")

    if not args.allow_zero:
        cmd.append("--require-nonzero")

    ret = run_cmd(cmd, env)

    print("\n" + "#" * 100)
    if ret == 0:
        print("[PASS] Q14 correctness check passed.")
        print("Meaning:")
        print("  - Baseline produced final output.")
        print("  - Proposed Distributed produced final output.")
        print("  - Normalized witness sets match exactly.")
        if not args.allow_zero:
            print("  - At least one non-zero witness was required and satisfied.")
        print("\nCheck outputs under:")
        print(f"  {Path(args.output_root).resolve() / args.dataset}")
        print("#" * 100)
        sys.exit(0)

    print("[FAIL] Q14 correctness check failed.")
    print("Most common causes:")
    print("  1) Both algorithms returned zero witnesses while --require-nonzero is enabled.")
    print("  2) Baseline and Proposed witnesses differ.")
    print("  3) HDFS dataset path or by_partition structure is incorrect.")
    print("  4) Q14 constraints are too strict for this dataset.")
    print("\nInspect:")
    print(f"  {Path(args.output_root).resolve() / args.dataset / 'comparison' / 'Q14'}")
    print("  only_baseline.csv")
    print("  only_proposed.csv")
    print("#" * 100)
    sys.exit(ret)


if __name__ == "__main__":
    main()

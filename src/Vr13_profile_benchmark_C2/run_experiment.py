#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run Baseline/Proposed Distributed on HDFS datasets and compare witnesses.

This runner does not contain algorithm or query logic. It only orchestrates:
  1. spark-submit baseline.py
  2. spark-submit proposed_distributed.py
  3. normalized witness-set comparison using the query registry.
"""
from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from queries import QUERY_REGISTRY


DATASETS = ["dataset_1m", "dataset_2m", "dataset_4m", "dataset_6m", "dataset_8m", "dataset_10m"]

BASELINE_METHOD_DIR = "baseline_global_bounded_erpq"
PROPOSED_METHOD_DIR = "proposed_distributed_boundary_erpq"


def stream_run_cmd(cmd: list[str], env: dict[str, str], log_file: Path) -> None:
    print("\n$", " ".join(cmd), flush=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", encoding="utf-8", errors="replace") as f:
        f.write("$ " + " ".join(cmd) + "\n\n")
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
            f.write(line)
        return_code = proc.wait()
    if return_code != 0:
        raise SystemExit(f"Command failed with return code {return_code}. See log: {log_file}")


def read_witnesses(dataset_root: Path, method_dir: str, query_id: str, compare_columns: list[str]) -> pd.DataFrame:
    pattern = dataset_root / method_dir / query_id / "witnesses_csv" / "part-*.csv"
    files = sorted(glob.glob(str(pattern)))
    if not files:
        return pd.DataFrame(columns=compare_columns)
    frames = [pd.read_csv(path, dtype=str).fillna("") for path in files]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=compare_columns)
    for col in compare_columns:
        if col not in df.columns:
            df[col] = ""
    return df[compare_columns].drop_duplicates().sort_values(compare_columns).reset_index(drop=True)


def compare_outputs(output_root: Path, dataset: str, query_id: str) -> dict[str, object]:
    spec = QUERY_REGISTRY[query_id]
    compare_columns = list(spec.compare_columns)
    dataset_root = output_root / dataset

    baseline = read_witnesses(dataset_root, BASELINE_METHOD_DIR, query_id, compare_columns)
    proposed = read_witnesses(dataset_root, PROPOSED_METHOD_DIR, query_id, compare_columns)

    merged = baseline.merge(proposed, on=compare_columns, how="outer", indicator=True)
    only_baseline = merged[merged["_merge"] == "left_only"][compare_columns]
    only_proposed = merged[merged["_merge"] == "right_only"][compare_columns]

    diff_dir = dataset_root / "comparison" / query_id
    diff_dir.mkdir(parents=True, exist_ok=True)
    only_baseline.to_csv(diff_dir / "only_baseline.csv", index=False)
    only_proposed.to_csv(diff_dir / "only_proposed.csv", index=False)

    baseline_count = len(baseline)
    proposed_count = len(proposed)
    both_zero = baseline_count == 0 and proposed_count == 0
    outputs_match = len(only_baseline) == 0 and len(only_proposed) == 0

    return {
        "dataset": dataset,
        "query_id": query_id,
        "query_name": spec.query_name,
        "logical_query_type": spec.logical_query_type,
        "physical_plan": spec.physical_plan,
        "baseline_witnesses": baseline_count,
        "proposed_witnesses": proposed_count,
        "only_baseline": len(only_baseline),
        "only_proposed": len(only_proposed),
        "outputs_match": outputs_match,
        "both_zero": both_zero,
        "comparison_dir": str(diff_dir),
    }



def _read_runtime_row(dataset_root: Path, method_dir: str, query_id: str) -> dict[str, object]:
    path = dataset_root / method_dir / "runtime_summary_all_queries.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
        if "query_id" in df.columns:
            df = df[df["query_id"].astype(str) == str(query_id)]
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as exc:
        print(f"[WARN] Could not read runtime summary {path}: {exc!r}")
        return {}


def _num(row: dict[str, object], key: str, default: float = 0.0) -> float:
    try:
        value = row.get(key, default)
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def build_runtime_comparison_table(
    output_root: Path,
    datasets: list[str],
    query_id: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for dataset in datasets:
        dataset_root = output_root / dataset
        b = _read_runtime_row(dataset_root, BASELINE_METHOD_DIR, query_id)
        p = _read_runtime_row(dataset_root, PROPOSED_METHOD_DIR, query_id)
        if not b and not p:
            continue

        b_alg = _num(
            b,
            "time_query_algorithm_wall_seconds",
            _num(b, "time_algorithm_total_seconds", 0.0),
        )
        p_alg_wall = _num(
            p,
            "time_query_algorithm_wall_seconds",
            _num(p, "time_algorithm_total_seconds", 0.0),
        )
        p_alg_parallel = _num(
            p,
            "time_query_algorithm_parallel_estimate_seconds",
            _num(p, "time_algorithm_total_seconds", 0.0),
        )
        b_e2e = _num(b, "time_end_to_end_total_seconds", 0.0)
        p_e2e = _num(p, "time_end_to_end_total_seconds", 0.0)

        # Phase-level timing extracted from the method runtime summaries.
        # These fields already exist in baseline.py and proposed_distributed.py;
        # this runner only preserves them in the per-repeat and aggregate CSVs.
        b_load = _num(b, "time_load_data_seconds", 0.0)
        p_load = _num(p, "time_load_data_seconds", 0.0)

        b_step1 = _num(
            b,
            "time_step1_global_merge_nexttw_seconds",
            _num(b, "time_step1_graph_stitching_seconds", 0.0),
        )
        b_step2 = _num(
            b,
            "time_step2_query_validation_seconds",
            _num(b, "time_step2_bounded_erpq_validation_seconds", 0.0),
        )

        p_global_lookup = _num(
            p,
            "time_global_lookup_seconds",
            0.0,
        )
        p_step1_wall = _num(
            p,
            "time_step1_localeval_boundary_wall_seconds",
            _num(p, "time_step1_localeval_boundary_seconds", 0.0),
        )
        p_step1_parallel = _num(
            p,
            "time_step1_localeval_boundary_parallel_estimate_seconds",
            _num(p, "time_step1_localeval_boundary_seconds", 0.0),
        )
        p_step1_sum = _num(
            p,
            "time_step1_localeval_boundary_sum_partition_seconds",
            p_step1_wall,
        )
        p_step2 = _num(
            p,
            "time_step2_stitching_validation_seconds",
            0.0,
        )
        p_implementation_gap = _num(
            p,
            "time_implementation_gap_seconds",
            p_step1_wall - p_step1_parallel,
        )

        rows.append({
            # Identity / workload
            "Dataset": dataset,
            "Query": query_id,
            "Query_name": b.get("query_name", p.get("query_name", "")),
            "Logical_query_type": b.get(
                "logical_query_type",
                p.get("logical_query_type", ""),
            ),
            "Physical_plan": b.get(
                "physical_plan",
                p.get("physical_plan", ""),
            ),
            "Num_partitions": _num(
                b,
                "num_partitions",
                _num(p, "num_partitions", 0.0),
            ),

            # Result/cardinality metrics already emitted by the two methods.
            # A value of -1 means the corresponding benchmark count was disabled.
            "Num_global_vertices": _num(
                b,
                "num_global_vertices",
                _num(p, "num_global_vertices", -1.0),
            ),
            "Num_base_global_edges": _num(
                b,
                "num_base_global_edges",
                _num(p, "num_base_global_edges", -1.0),
            ),
            "Baseline_num_candidates": _num(
                b,
                "num_candidates",
                -1.0,
            ),
            "Proposed_num_candidates": _num(
                p,
                "num_candidates",
                -1.0,
            ),
            "Baseline_num_valid_witnesses": _num(
                b,
                "num_valid_witnesses",
                -1.0,
            ),
            "Proposed_num_valid_witnesses": _num(
                p,
                "num_valid_witnesses",
                -1.0,
            ),
            "Proposed_num_local_fragments": _num(
                p,
                "num_local_fragments",
                0.0,
            ),
            "Proposed_num_boundary_fragments": _num(
                p,
                "num_boundary_fragments",
                -1.0,
            ),
            "Proposed_num_localeval_fragments_total": _num(
                p,
                "num_localeval_fragments_total",
                -1.0,
            ),
            "Proposed_num_candidate_upper_bound_without_continuity": _num(
                p,
                "num_candidate_upper_bound_without_continuity",
                -1.0,
            ),

            # Data loading
            "Baseline_load_data_time_s": b_load,
            "Proposed_load_data_time_s": p_load,

            # Baseline phase timing
            "Baseline_step1_combine_union_s": b_step1,
            "Baseline_step2_global_query_s": b_step2,

            # Proposed phase timing
            "Proposed_global_lookup_s": p_global_lookup,
            "Proposed_step1_localeval_wall_s": p_step1_wall,
            "Proposed_step1_localeval_parallel_estimate_s": p_step1_parallel,
            "Proposed_step1_localeval_sum_partition_s": p_step1_sum,
            "Proposed_step2_globaleval_s": p_step2,
            "Proposed_implementation_gap_s": p_implementation_gap,

            # Existing algorithm totals and speedups
            "Baseline_query_algorithm_time_s": b_alg,
            "Proposed_query_algorithm_parallel_estimate_s": p_alg_parallel,
            "Proposed_query_algorithm_wall_time_s": p_alg_wall,
            "Algorithm_speedup_parallel_estimate": (
                b_alg / p_alg_parallel if p_alg_parallel > 0 else None
            ),
            "Algorithm_speedup_wall": (
                b_alg / p_alg_wall if p_alg_wall > 0 else None
            ),
            "Algorithm_time_reduction_wall_pct": (
                ((b_alg - p_alg_wall) / b_alg) * 100.0
                if b_alg > 0 else None
            ),

            # End-to-end timing
            "Baseline_end_to_end_time_s": b_e2e,
            "Proposed_end_to_end_time_s": p_e2e,
            "Baseline_unattributed_overhead_s": _num(
                b,
                "time_unattributed_overhead_seconds",
                b_e2e - b_load - b_alg,
            ),
            "Proposed_unattributed_overhead_s": _num(
                p,
                "time_unattributed_overhead_seconds",
                p_e2e - p_load - p_alg_wall,
            ),
            "End_to_end_speedup": (
                b_e2e / p_e2e if p_e2e > 0 else None
            ),
            "End_to_end_time_reduction_pct": (
                ((b_e2e - p_e2e) / b_e2e) * 100.0
                if b_e2e > 0 else None
            ),

            # Internal consistency checks.
            "Baseline_phase_sum_s": b_step1 + b_step2,
            "Baseline_algorithm_minus_phase_sum_s": (
                b_alg - b_step1 - b_step2
            ),
            "Proposed_wall_phase_sum_s": (
                p_global_lookup + p_step1_wall + p_step2
            ),
            "Proposed_algorithm_minus_wall_phase_sum_s": (
                p_alg_wall - p_global_lookup - p_step1_wall - p_step2
            ),
            "Proposed_parallel_phase_sum_s": (
                p_step1_parallel + p_step2
            ),
            "Proposed_parallel_algorithm_minus_phase_sum_s": (
                p_alg_parallel - p_step1_parallel - p_step2
            ),
        })
    return pd.DataFrame(rows)


def build_env(args: argparse.Namespace) -> dict[str, str]:
    """Build the environment passed to baseline.py/proposed_distributed.py.

    Modes are intentionally separated:
      - correctness: compare normalized witnesses; not intended for paper runtime.
      - official: official lazy/end-to-end runtime with final witness export;
        useful for auditability/demo and application-oriented end-to-end measurement.
      - official_runtime_only: official runtime without writing full witnesses_csv;
        still forces a final Spark action and is recommended for paper runtime tables
        after correctness has already been validated.
      - timing: backward-compatible alias of official.
      - profiling: diagnostic timing; materializes/counts step boundaries so phase
        costs and intermediate cardinalities can be inspected. Profiling results
        must not be reported as official runtime without this caveat.
      - profiling_runtime_only: C2 profiling for algorithm/phase runtime only;
        materializes/counts step boundaries but does not write full witnesses_csv.
    """
    env = os.environ.copy()
    env.pop("PYSPARK_DRIVER_PYTHON", None)
    env.pop("PYSPARK_DRIVER_PYTHON_OPTS", None)
    env["PYSPARK_PYTHON"] = "python3"
    env["PYSPARK_DRIVER_PYTHON"] = "python3"

    is_correctness = args.mode == "correctness"
    is_profiling = args.mode in {"profiling", "profiling_runtime_only"}
    is_runtime_only = args.mode in {"official_runtime_only", "profiling_runtime_only"}

    env.update(
        {
            "QUERY_ID": args.query_id,
            "BENCHMARK_MODE": args.mode,

            # Correctness and official-with-export write final witnesses for
            # auditability/comparison/demo. Runtime-only skips full witnesses_csv
            # but still relies on BENCHMARK_FORCE_FINAL_ACTION for execution.
            "SAVE_FINAL_WITNESSES": "false" if is_runtime_only else "true",
            "SAVE_RUNTIME_SUMMARY": "true",

            # Never save large candidate/intermediate outputs by default. In
            # profiling mode we count them, but do not persist them as CSV.
            "SAVE_CANDIDATE_OUTPUTS": "false",
            "SAVE_INTERMEDIATE_OUTPUTS": "false",

            # Force a final Spark action so lazy plans are actually executed.
            "BENCHMARK_FORCE_FINAL_ACTION": "true",

            # Official timing keeps additional count/materialization actions off.
            # Profiling turns them on intentionally to expose step boundaries and
            # intermediate cardinalities.
            "BENCHMARK_COUNT_INTERMEDIATE": "true" if is_profiling else "false",
            "BENCHMARK_COUNT_DATASET_SIZE": "true" if is_profiling else "false",
            "BENCHMARK_MATERIALIZE_STEP_BOUNDARIES": "true" if is_profiling else "false",

            # Correctness validates outputs; timing/profiling are benchmark modes.
            "BENCHMARK_TIMING_ONLY": "false" if is_correctness else "true",
            "BENCHMARK_VALIDATE_RESULT": "true" if is_correctness else "false",

            # Keep local-complete detection disabled unless explicitly enabled
            # from the shell environment. The main Proposed path is LocalEval
            # boundary fragments -> GlobalEval/Stitching -> Hard Validation.
            "ENABLE_LOCAL_COMPLETE_DETECTION": env.get("ENABLE_LOCAL_COMPLETE_DETECTION", "false"),
            "STAGE2_PERSIST_INTERMEDIATES": env.get("STAGE2_PERSIST_INTERMEDIATES", "true"),

            "SPARK_MASTER": args.spark_master,
            "SPARK_DRIVER_MEMORY": args.driver_memory,
            "SPARK_EXECUTOR_MEMORY": args.executor_memory,
            "SPARK_EXECUTOR_CORES": str(args.executor_cores),
            "SPARK_CORES_MAX": str(args.cores_max),
            "SPARK_SHUFFLE_PARTITIONS": str(args.shuffle_partitions),
            "MAX_NEXTTW_HOPS": str(args.max_nexttw_hops),
            "EPSILON_TIME_SECONDS": str(args.epsilon_time_seconds),
            "QUICK_EXIT_SECONDS": str(args.quick_exit_seconds),
            "Q14_EXIT_PERSON_ROLE": args.q14_exit_person_role,
            "Q14_MIN_PARTITION_COUNT": str(args.q14_min_partition_count),
            "Q14_REQUIRE_DIFFERENT_LOCATION": "true" if args.q14_require_different_location else "false",
            "Q14_QUICK_EXIT_REFERENCE": args.q14_quick_exit_reference,
            "Q14_DIAGNOSTICS_ENABLED": "true" if args.enable_q14_diagnostics else env.get("Q14_DIAGNOSTICS_ENABLED", "false"),
            "SPARK_LOG_LEVEL": env.get("SPARK_LOG_LEVEL", "WARN"),
            "SPARK_AUTO_BROADCAST_JOIN_THRESHOLD": str(args.auto_broadcast_join_threshold),
            "SPARK_FILES_MAX_PARTITION_BYTES": str(args.files_max_partition_bytes),
        }
    )
    return env


def main() -> None:
    parser = argparse.ArgumentParser(description="Run and compare VideoGraphDB Vr09 Baseline vs Proposed Distributed.")
    parser.add_argument("--hdfs-root", default="hdfs:///data/videographdb")
    parser.add_argument("--output-root", default="./query_outputs_hdfs")
    parser.add_argument("--query-id", default=os.getenv("QUERY_ID", "Q14"), help="Query ID registered in queries.py, for example Q1.1 or Q4.3")
    parser.add_argument("--datasets", default=",".join(DATASETS))
    parser.add_argument(
        "--mode",
        choices=["correctness", "official", "official_runtime_only", "timing", "profiling", "profiling_runtime_only"],
        default="correctness",
        help=(
            "Execution mode: correctness validates Baseline/Proposed witnesses; "
            "official measures runtime with final witness export; "
            "official_runtime_only measures runtime without writing full witnesses_csv; "
            "timing is a backward-compatible alias of official; "
            "profiling enables diagnostic counts/materialization; "
            "profiling_runtime_only enables C2 phase profiling without full witnesses_csv export."
        ),
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="Number of independent Baseline/Proposed repetitions for each dataset.",
    )
    parser.add_argument("--skip-run", action="store_true", help="Only compare existing outputs")
    parser.add_argument("--require-nonzero", action="store_true", help="Fail if both algorithms return zero witnesses")
    parser.add_argument("--spark-master", default=os.getenv("SPARK_MASTER", "spark://master:7077"))
    parser.add_argument("--spark-submit", default=os.getenv("SPARK_SUBMIT", "spark-submit"))
    parser.add_argument("--driver-memory", default=os.getenv("SPARK_DRIVER_MEMORY", "3G"))
    parser.add_argument("--executor-memory", default=os.getenv("SPARK_EXECUTOR_MEMORY", "3G"))
    parser.add_argument("--executor-cores", default=os.getenv("SPARK_EXECUTOR_CORES", "1"))
    parser.add_argument("--cores-max", default=os.getenv("SPARK_CORES_MAX", "4"))
    parser.add_argument("--shuffle-partitions", default=os.getenv("SPARK_SHUFFLE_PARTITIONS", "6"))
    parser.add_argument("--max-nexttw-hops", default=os.getenv("MAX_NEXTTW_HOPS", "2"))
    parser.add_argument("--epsilon-time-seconds", default=os.getenv("EPSILON_TIME_SECONDS", "120"))
    parser.add_argument("--quick-exit-seconds", default=os.getenv("QUICK_EXIT_SECONDS", "30"))
    parser.add_argument("--q14-exit-person-role", choices=["P1", "P2"], default=os.getenv("Q14_EXIT_PERSON_ROLE", "P1"))
    parser.add_argument("--q14-min-partition-count", "--min-partition-count", dest="q14_min_partition_count", default=os.getenv("Q14_MIN_PARTITION_COUNT", os.getenv("MIN_PARTITION_COUNT_Q14", "1")))
    # Keep different-location disabled by default for correctness checks.
    # It must only be enabled explicitly because existing shell environment
    # variables can otherwise silently over-constrain Q14.
    parser.add_argument("--q14-require-different-location", action="store_true", default=False)
    parser.add_argument("--q14-quick-exit-reference", choices=["INTERACT", "CARRY2"], default=os.getenv("Q14_QUICK_EXIT_REFERENCE", "CARRY2"))
    parser.add_argument("--enable-q14-diagnostics", action="store_true", default=os.getenv("Q14_DIAGNOSTICS_ENABLED", "false").lower() in {"1","true","yes","y","on"})
    parser.add_argument("--auto-broadcast-join-threshold", default=os.getenv("SPARK_AUTO_BROADCAST_JOIN_THRESHOLD", "2097152"))
    parser.add_argument("--files-max-partition-bytes", default=os.getenv("SPARK_FILES_MAX_PARTITION_BYTES", "67108864"))
    args = parser.parse_args()

    raw_mode = args.mode
    if args.mode == "timing":
        print(
            "[WARN] --mode timing is deprecated; use --mode official "
            "for official paper runtime.",
            flush=True,
        )
        args.mode = "official"

    if args.repeats < 1:
        parser.error("--repeats must be >= 1")

    if args.query_id not in QUERY_REGISTRY:
        raise SystemExit(f"Unsupported query-id {args.query_id}. Supported: {sorted(QUERY_REGISTRY.keys())}")

    script_dir = Path(__file__).resolve().parent
    for required_file in ["baseline.py", "proposed_distributed.py", "config.py", "graph_io.py", "queries.py"]:
        if not (script_dir / required_file).exists():
            raise SystemExit(f"Missing required file: {script_dir / required_file}")

    output_root = Path(args.output_root).resolve()
    logs_root = output_root / "logs"
    output_root.mkdir(parents=True, exist_ok=True)
    logs_root.mkdir(parents=True, exist_ok=True)

    datasets = [x.strip() for x in str(args.datasets).split(",") if x.strip()]
    env = build_env(args)
    query_tag = str(args.query_id).replace(".", "_")

    rows: list[dict[str, object]] = []
    runtime_rows: list[dict[str, object]] = []
    skip_witness_comparison = args.mode in {"official_runtime_only", "profiling_runtime_only"}

    for dataset in datasets:
        dataset_hdfs = f"{args.hdfs_root.rstrip('/')}/{dataset}"
        dataset_out = output_root / dataset

        if args.skip_run:
            runtime_df = build_runtime_comparison_table(
                output_root,
                [dataset],
                args.query_id,
            )
            if not runtime_df.empty:
                runtime_row = runtime_df.iloc[0].to_dict()
                runtime_row.update({
                    "Repeat": 0,
                    "Run_stamp": "existing_output",
                    "Benchmark_mode": args.mode,
                    "Requested_mode": raw_mode,
                })
                runtime_rows.append(runtime_row)

            if skip_witness_comparison:
                spec = QUERY_REGISTRY[args.query_id]
                rows.append({
                    "dataset": dataset,
                    "query_id": args.query_id,
                    "query_name": spec.query_name,
                    "logical_query_type": spec.logical_query_type,
                    "physical_plan": spec.physical_plan,
                    "baseline_witnesses": None,
                    "proposed_witnesses": None,
                    "only_baseline": None,
                    "only_proposed": None,
                    "outputs_match": None,
                    "both_zero": None,
                    "comparison_dir": "",
                    "comparison_mode": "skipped_runtime_only_no_witness_csv",
                    "repeat_id": 0,
                    "run_stamp": "existing_output",
                    "benchmark_mode": args.mode,
                    "requested_mode": raw_mode,
                })
            else:
                comparison = compare_outputs(output_root, dataset, args.query_id)
                comparison.update({
                    "repeat_id": 0,
                    "run_stamp": "existing_output",
                    "benchmark_mode": args.mode,
                    "requested_mode": raw_mode,
                    "comparison_mode": "witness_set_csv",
                })
                rows.append(comparison)
            continue

        for repeat_idx in range(1, args.repeats + 1):
            run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            run_id = f"run{repeat_idx:02d}"

            print("\n" + "=" * 100)
            print(
                f"Dataset={dataset} | Query={args.query_id} | "
                f"Repeat={repeat_idx}/{args.repeats} | "
                f"Run stamp={run_stamp}"
            )
            print("=" * 100)

            common = [
                "--dataset-root", dataset_hdfs,
                "--by-partition-dir", f"{dataset_hdfs}/by_partition",
                "--query-id", args.query_id,
                "--output-root", str(dataset_out),
                "--spark-master", args.spark_master,
                "--shuffle-partitions", str(args.shuffle_partitions),
                "--max-nexttw-hops", str(args.max_nexttw_hops),
                "--epsilon-time-seconds", str(args.epsilon_time_seconds),
                "--quick-exit-seconds", str(args.quick_exit_seconds),
                "--q14-exit-person-role", args.q14_exit_person_role,
                "--q14-min-partition-count", str(args.q14_min_partition_count),
                "--q14-quick-exit-reference", args.q14_quick_exit_reference,
            ]
            if args.q14_require_different_location:
                common.append("--q14-require-different-location")
            if args.enable_q14_diagnostics:
                common.append("--enable-q14-diagnostics")

            spark_submit_prefix = [
                args.spark_submit,
                "--master", args.spark_master,
                "--driver-memory", str(args.driver_memory),
                "--executor-memory", str(args.executor_memory),
                "--conf", f"spark.executor.cores={args.executor_cores}",
                "--conf", f"spark.cores.max={args.cores_max}",
                "--conf", f"spark.sql.shuffle.partitions={args.shuffle_partitions}",
                "--conf", "spark.sql.adaptive.enabled=false",
                "--conf", "spark.sql.adaptive.coalescePartitions.enabled=false",
                "--conf", "spark.sql.adaptive.skewJoin.enabled=false",
                "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                "--conf", "spark.kryoserializer.buffer.max=512m",
                "--conf", (
                    "spark.sql.autoBroadcastJoinThreshold="
                    f"{args.auto_broadcast_join_threshold}"
                ),
                "--conf", (
                    "spark.sql.files.maxPartitionBytes="
                    f"{args.files_max_partition_bytes}"
                ),
            ]

            baseline_log = logs_root / (
                f"{run_stamp}_{run_id}_{args.mode}_{dataset}_{query_tag}_"
                "baseline_global_first_bounded_erpq.log"
            )
            proposed_log = logs_root / (
                f"{run_stamp}_{run_id}_{args.mode}_{dataset}_{query_tag}_"
                "proposed_fragment_first_bounded_erpq.log"
            )

            run_env = env.copy()
            run_env["BENCHMARK_REPEAT_ID"] = str(repeat_idx)
            run_env["BENCHMARK_RUN_STAMP"] = run_stamp

            stream_run_cmd(
                [
                    *spark_submit_prefix,
                    str(script_dir / "baseline.py"),
                    *common,
                ],
                run_env,
                baseline_log,
            )
            stream_run_cmd(
                [
                    *spark_submit_prefix,
                    str(script_dir / "proposed_distributed.py"),
                    *common,
                ],
                run_env,
                proposed_log,
            )

            # Capture runtime before the next repetition overwrites summaries.
            runtime_df = build_runtime_comparison_table(
                output_root,
                [dataset],
                args.query_id,
            )
            runtime_row = None
            if not runtime_df.empty:
                runtime_row = runtime_df.iloc[0].to_dict()
                runtime_row.update({
                    "Repeat": repeat_idx,
                    "Run_stamp": run_stamp,
                    "Benchmark_mode": args.mode,
                    "Requested_mode": raw_mode,
                    "Baseline_log": str(baseline_log),
                    "Proposed_log": str(proposed_log),
                })
                runtime_rows.append(runtime_row)

            # Correctness comparison needs full witnesses_csv. In runtime-only
            # official mode, witnesses_csv is intentionally disabled, so this
            # comparison is skipped to avoid a misleading both-zero match.
            if skip_witness_comparison:
                spec = QUERY_REGISTRY[args.query_id]
                rows.append({
                    "dataset": dataset,
                    "query_id": args.query_id,
                    "query_name": spec.query_name,
                    "logical_query_type": spec.logical_query_type,
                    "physical_plan": spec.physical_plan,
                    "baseline_witnesses": (
                        runtime_row.get("Baseline_num_valid_witnesses")
                        if runtime_row else None
                    ),
                    "proposed_witnesses": (
                        runtime_row.get("Proposed_num_valid_witnesses")
                        if runtime_row else None
                    ),
                    "only_baseline": None,
                    "only_proposed": None,
                    "outputs_match": None,
                    "both_zero": None,
                    "comparison_dir": "",
                    "comparison_mode": "skipped_runtime_only_no_witness_csv",
                    "repeat_id": repeat_idx,
                    "run_stamp": run_stamp,
                    "benchmark_mode": args.mode,
                    "requested_mode": raw_mode,
                    "baseline_log": str(baseline_log),
                    "proposed_log": str(proposed_log),
                })
            else:
                # Capture correctness before the next repetition overwrites outputs.
                comparison = compare_outputs(
                    output_root,
                    dataset,
                    args.query_id,
                )
                comparison.update({
                    "repeat_id": repeat_idx,
                    "run_stamp": run_stamp,
                    "benchmark_mode": args.mode,
                    "requested_mode": raw_mode,
                    "comparison_mode": "witness_set_csv",
                    "baseline_log": str(baseline_log),
                    "proposed_log": str(proposed_log),
                })
                rows.append(comparison)

    report = pd.DataFrame(rows)
    report_path = output_root / f"baseline_vs_proposed_comparison_{args.query_id}.csv"
    report.to_csv(report_path, index=False)

    print("\nComparison report:")
    print(report.to_string(index=False))
    print(f"\n[Write] {report_path}")

    runtime_table = pd.DataFrame(runtime_rows)
    if not runtime_table.empty:
        runtime_table_path = (
            output_root
            / f"runtime_algorithm_end_to_end_summary_{args.query_id}.csv"
        )
        runtime_table.to_csv(runtime_table_path, index=False)
        print("\nRuntime algorithm/end-to-end summary by repetition:")
        print(runtime_table.to_string(index=False))
        print(f"\n[Write] {runtime_table_path}")

        numeric_metrics = [
            # Dataset/result cardinalities
            "Num_partitions",
            "Num_global_vertices",
            "Num_base_global_edges",
            "Baseline_num_candidates",
            "Proposed_num_candidates",
            "Baseline_num_valid_witnesses",
            "Proposed_num_valid_witnesses",
            "Proposed_num_local_fragments",
            "Proposed_num_boundary_fragments",
            "Proposed_num_localeval_fragments_total",
            "Proposed_num_candidate_upper_bound_without_continuity",

            # Load and phase timings
            "Baseline_load_data_time_s",
            "Proposed_load_data_time_s",
            "Baseline_step1_combine_union_s",
            "Baseline_step2_global_query_s",
            "Proposed_global_lookup_s",
            "Proposed_step1_localeval_wall_s",
            "Proposed_step1_localeval_parallel_estimate_s",
            "Proposed_step1_localeval_sum_partition_s",
            "Proposed_step2_globaleval_s",
            "Proposed_implementation_gap_s",

            # Algorithm and end-to-end totals
            "Baseline_query_algorithm_time_s",
            "Proposed_query_algorithm_parallel_estimate_s",
            "Proposed_query_algorithm_wall_time_s",
            "Algorithm_speedup_parallel_estimate",
            "Algorithm_speedup_wall",
            "Algorithm_time_reduction_wall_pct",
            "Baseline_end_to_end_time_s",
            "Proposed_end_to_end_time_s",
            "Baseline_unattributed_overhead_s",
            "Proposed_unattributed_overhead_s",
            "End_to_end_speedup",
            "End_to_end_time_reduction_pct",

            # Consistency checks
            "Baseline_phase_sum_s",
            "Baseline_algorithm_minus_phase_sum_s",
            "Proposed_wall_phase_sum_s",
            "Proposed_algorithm_minus_wall_phase_sum_s",
            "Proposed_parallel_phase_sum_s",
            "Proposed_parallel_algorithm_minus_phase_sum_s",
        ]
        available_metrics = [
            col for col in numeric_metrics
            if col in runtime_table.columns
        ]
        if available_metrics:
            aggregate = (
                runtime_table
                .groupby(["Dataset", "Query"], dropna=False)[available_metrics]
                .agg(["mean", "std", "median", "min", "max"])
                .reset_index()
            )
            aggregate.columns = [
                "_".join(
                    str(part)
                    for part in col
                    if str(part) != ""
                ).rstrip("_")
                if isinstance(col, tuple)
                else str(col)
                for col in aggregate.columns
            ]
            aggregate_path = (
                output_root
                / f"runtime_aggregate_repeats_{args.query_id}.csv"
            )
            aggregate.to_csv(aggregate_path, index=False)
            print("\nRuntime aggregate across repetitions:")
            print(aggregate.to_string(index=False))
            print(f"\n[Write] {aggregate_path}")

    print(f"[Logs]  {logs_root}")

    if skip_witness_comparison:
        print(
            "[INFO] Witness-set comparison skipped because this runtime-only mode "
            "does not write full witnesses_csv. Use --mode correctness for correctness validation.",
            flush=True,
        )
    else:
        if not bool(report["outputs_match"].all()):
            raise SystemExit("Baseline and Proposed Distributed outputs differ. See comparison/only_*.csv files.")

        if args.require_nonzero and bool(report["both_zero"].any()):
            zero_datasets = report.loc[report["both_zero"], "dataset"].tolist()
            raise SystemExit(
                "Both Baseline and Proposed Distributed returned zero witnesses for: "
                + ", ".join(zero_datasets)
                + ". This may be logically correct, but should not be accepted as a correctness pass without inspecting local fragments/candidates."
            )


if __name__ == "__main__":
    main()

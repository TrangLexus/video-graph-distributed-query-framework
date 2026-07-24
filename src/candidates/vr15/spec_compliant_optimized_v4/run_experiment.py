#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Runner Vr15 cho correctness, profiling và benchmark Baseline/Proposed."""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import math
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from queries import QUERY_REGISTRY

DATASETS = [
    "dataset_1m",
    "dataset_2m",
    "dataset_4m",
    "dataset_6m",
    "dataset_8m",
    "dataset_10m",
]
BASELINE_METHOD_DIR = "baseline_global_bounded_erpq"
PROPOSED_METHOD_DIR = "proposed_distributed_boundary_erpq"
ALL_QUERY_IDS = tuple(sorted(QUERY_REGISTRY))


def stream_run_cmd(cmd: list[str], env: dict[str, str], log_file: Path) -> None:
    print("\n$", " ".join(cmd), flush=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", encoding="utf-8", errors="replace") as handle:
        handle.write("$ " + " ".join(cmd) + "\n\n")
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
            handle.write(line)
        code = proc.wait()
    if code != 0:
        raise SystemExit(f"Lệnh lỗi {code}. Xem log: {log_file}")


def parse_query_ids(raw: str) -> list[str]:
    tokens = [x.strip() for x in raw.split(",") if x.strip()]
    if len(tokens) == 1 and tokens[0].upper() in {"ALL", "*"}:
        return list(ALL_QUERY_IDS)
    invalid = [q for q in tokens if q not in QUERY_REGISTRY]
    if invalid:
        raise SystemExit(
            f"Query không hỗ trợ: {invalid}; hợp lệ: {list(ALL_QUERY_IDS)}"
        )
    if not tokens:
        raise SystemExit("Danh sách query rỗng")
    return tokens


def read_output_artifact(
    dataset_root: Path,
    method_dir: str,
    query_id: str,
    artifact_dir: str,
) -> pd.DataFrame:
    pattern = dataset_root / method_dir / query_id / artifact_dir / "part-*.csv"
    files = sorted(glob.glob(str(pattern)))
    if not files:
        return pd.DataFrame()
    frames = [pd.read_csv(path, dtype=str).fillna("") for path in files]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def normalize_set_frame(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    cols = list(columns)
    out = df.copy()
    for column in cols:
        if column not in out.columns:
            out[column] = ""
    if not cols:
        raise ValueError("Không có cột ngữ nghĩa để chuẩn hóa output set")
    out = out[cols].fillna("").astype(str).drop_duplicates()
    return out.sort_values(cols, kind="mergesort").reset_index(drop=True)


def stable_set_hash(df: pd.DataFrame, columns: list[str]) -> str:
    digest = hashlib.sha256()
    for row in df[columns].itertuples(index=False, name=None):
        encoded = "\x1f".join(str(value) for value in row).encode("utf-8")
        digest.update(encoded)
        digest.update(b"\n")
    return digest.hexdigest()


def semantic_candidate_columns(spec, available_columns: Iterable[str]) -> list[str]:
    available = list(available_columns)
    if not available:
        # Candidate set rỗng vẫn phải có schema logic ổn định để so sánh.
        return list(spec.correctness_compare_columns)
    resolver = getattr(spec, "resolve_dedup_columns", None)
    if callable(resolver):
        return list(resolver("candidate", available))

    configured = [
        c for c in (getattr(spec, "candidate_dedup_columns", ()) or ())
        if c in available and c != "candidate_id"
    ]
    if configured:
        return configured
    semantic = [c for c in spec.correctness_compare_columns if c in available]
    if semantic:
        return semantic
    if "candidate_id" in available:
        return ["candidate_id"]
    raise ValueError(f"{spec.query_id}: không tìm được khóa so sánh candidate")


def compare_one_artifact(
    *,
    dataset_root: Path,
    query_id: str,
    artifact_dir: str,
    columns: list[str],
    diff_dir: Path,
    prefix: str,
) -> dict[str, object]:
    baseline_raw = read_output_artifact(
        dataset_root, BASELINE_METHOD_DIR, query_id, artifact_dir
    )
    proposed_raw = read_output_artifact(
        dataset_root, PROPOSED_METHOD_DIR, query_id, artifact_dir
    )
    baseline = normalize_set_frame(baseline_raw, columns)
    proposed = normalize_set_frame(proposed_raw, columns)

    merged = baseline.merge(proposed, on=columns, how="outer", indicator=True)
    only_baseline = merged[merged["_merge"] == "left_only"][columns]
    only_proposed = merged[merged["_merge"] == "right_only"][columns]
    diff_dir.mkdir(parents=True, exist_ok=True)
    only_baseline.to_csv(diff_dir / f"{prefix}_only_baseline.csv", index=False)
    only_proposed.to_csv(diff_dir / f"{prefix}_only_proposed.csv", index=False)

    return {
        f"{prefix}_compare_columns": columns,
        f"baseline_{prefix}_count": len(baseline),
        f"proposed_{prefix}_count": len(proposed),
        f"{prefix}_count_match": len(baseline) == len(proposed),
        f"{prefix}_only_baseline": len(only_baseline),
        f"{prefix}_only_proposed": len(only_proposed),
        f"{prefix}_set_match": len(only_baseline) == 0
        and len(only_proposed) == 0,
        f"baseline_{prefix}_set_hash": stable_set_hash(baseline, columns),
        f"proposed_{prefix}_set_hash": stable_set_hash(proposed, columns),
    }


def compare_outputs(output_root: Path, dataset: str, query_id: str) -> dict[str, object]:
    spec = QUERY_REGISTRY[query_id]
    dataset_root = output_root / dataset
    diff_dir = dataset_root / "comparison" / query_id

    baseline_candidates = read_output_artifact(
        dataset_root, BASELINE_METHOD_DIR, query_id, "candidates_csv"
    )
    proposed_candidates = read_output_artifact(
        dataset_root, PROPOSED_METHOD_DIR, query_id, "candidates_csv"
    )
    candidate_available = sorted(
        set(baseline_candidates.columns).union(proposed_candidates.columns)
    )
    candidate_columns = semantic_candidate_columns(spec, candidate_available)

    candidate_result = compare_one_artifact(
        dataset_root=dataset_root,
        query_id=query_id,
        artifact_dir="candidates_csv",
        columns=candidate_columns,
        diff_dir=diff_dir,
        prefix="candidate",
    )
    witness_columns = list(spec.correctness_compare_columns)
    witness_result = compare_one_artifact(
        dataset_root=dataset_root,
        query_id=query_id,
        artifact_dir="witnesses_csv",
        columns=witness_columns,
        diff_dir=diff_dir,
        prefix="witness",
    )

    return {
        "dataset": dataset,
        "query_id": query_id,
        "query_name": spec.query_name,
        "logical_query_type": spec.logical_query_type,
        **candidate_result,
        **witness_result,
        # Correctness gate chính thức dựa trên normalized final witness set.
        "outputs_match": bool(witness_result["witness_set_match"]),
        "both_zero": (
            int(witness_result["baseline_witness_count"]) == 0
            and int(witness_result["proposed_witness_count"]) == 0
        ),
        "comparison_dir": str(diff_dir),
    }


def read_runtime_row(
    dataset_root: Path, method_dir: str, query_id: str
) -> dict[str, object]:
    path = dataset_root / method_dir / "runtime_summary_all_queries.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if "query_id" in df.columns:
        df = df[df["query_id"].astype(str) == query_id]
    return {} if df.empty else df.iloc[-1].to_dict()


def num(row: dict[str, object], key: str, default: float = -1.0) -> float:
    try:
        value = row.get(key, default)
        if value is None or pd.isna(value) or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def num_first(
    row: dict[str, object], keys: Iterable[str], default: float = -1.0
) -> float:
    for key in keys:
        value = num(row, key, default)
        if value != float(default):
            return value
    return float(default)


def validate_timing_invariants(
    b: dict[str, object],
    p: dict[str, object],
    tolerance: float = 0.25,
) -> list[str]:
    errors: list[str] = []
    if not b:
        errors.append("Thiếu runtime summary Baseline")
        return errors
    if not p:
        errors.append("Thiếu runtime summary Proposed")
        return errors


    # Cùng input, tài nguyên và tham số logic.
    exact_equal_fields = [
        "dataset_root",
        "query_id",
        "spark_master",
        "spark_executor_memory",
        "spark_executor_cores",
        "spark_cores_max",
        "spark_shuffle_partitions",
        "spark_scheduler_mode",
        "query_time_scope",
        "query_start_time",
        "query_end_time",
        "quick_exit_max_seconds",
        "exit_person_role",
        "minimum_partition_count",
        "require_different_location",
        "quick_exit_reference_role",
    ]
    for field in exact_equal_fields:
        left = str(b.get(field, ""))
        right = str(p.get(field, ""))
        if left != right:
            errors.append(
                f"Baseline/Proposed khác {field}: {left!r} vs {right!r}"
            )

    for field in (
        "max_time_gap_seconds",
        "time_window_duration_seconds",
        "candidate_pruning_max_time_window_gap",
    ):
        left = num(b, field)
        right = num(p, field)
        if min(left, right) >= 0 and abs(left - right) > 1e-9:
            errors.append(
                f"Baseline/Proposed khác {field}: {left} vs {right}"
            )

    max_gap = num(p, "max_time_gap_seconds")
    tw_duration = num(p, "time_window_duration_seconds")
    observed_k = num(p, "candidate_pruning_max_time_window_gap")
    if max_gap >= 0 and tw_duration > 0 and observed_k >= 0:
        expected_k = math.ceil(max_gap / tw_duration)
        if abs(observed_k - expected_k) > 1e-9:
            errors.append(
                f"K_prune sai: observed={observed_k}, expected={expected_k}"
            )

    for name, row in (("Baseline", b), ("Proposed", p)):
        n_parts = num(row, "num_source_graph_partitions")
        if n_parts >= 0 and n_parts != 5:
            errors.append(f"{name} phải đọc đúng 5 graph partition, nhận {n_parts}")
        if num(row, "num_physical_next_tw_edges") != 0:
            errors.append(f"{name} đã materialize physical NEXT_TW edges")

    b_alg = num(b, "time_algorithm_total_seconds")
    b_sum = num(b, "time_step1_global_merge_seconds", 0) + num(
        b, "time_step2_global_query_validation_seconds", 0
    )
    b_gap = num(b, "time_algorithm_phase_gap_seconds", 0)
    if b_alg >= 0 and abs(b_alg - (b_sum + b_gap)) > tolerance:
        errors.append(
            "Baseline algorithm != phase sum + gap: "
            f"{b_alg:.6f} vs {(b_sum + b_gap):.6f}"
        )

    p_alg = num(p, "time_algorithm_total_seconds")
    p_sum = (
        num(p, "time_step1_boundary_index_preparation_seconds", 0)
        + num(p, "time_step1_localeval_parallel_seconds", 0)
        + num(p, "time_step1_fragment_classification_seconds", 0)
        + num(p, "time_step2_globaleval_seconds", 0)
    )
    p_gap = num(p, "time_algorithm_phase_gap_seconds", 0)
    if p_alg >= 0 and abs(p_alg - (p_sum + p_gap)) > tolerance:
        errors.append(
            "Proposed algorithm != phase sum + gap: "
            f"{p_alg:.6f} vs {(p_sum + p_gap):.6f}"
        )

    try:
        timings = json.loads(str(p.get("partition_localeval_timings", "[]")))
        if timings:
            completion_key = (
                "partition_completion_offset_seconds"
                if "partition_completion_offset_seconds" in timings[0]
                else "completion_offset_seconds"
            )
            expected = max(float(x[completion_key]) for x in timings)
            observed = num(p, "time_step1_localeval_parallel_seconds")
            if abs(expected - observed) > 0.02:
                errors.append(
                    "Proposed LocalEval != max completion offset: "
                    f"{observed:.6f} vs {expected:.6f}"
                )
            failed = [
                x for x in timings if str(x.get("status", "")) != "SUCCESS"
            ]
            if failed:
                errors.append(f"Có partition LocalEval không SUCCESS: {failed}")
    except Exception as exc:
        errors.append(f"Không đọc được partition_localeval_timings: {exc!r}")

    for name, row in [("Baseline", b), ("Proposed", p)]:
        e2e = num(row, "time_end_to_end_total_seconds")
        inp = num(row, "time_input_discovery_plan_seconds", 0)
        alg = num(row, "time_algorithm_total_seconds", 0)
        overhead = num(row, "time_non_algorithm_overhead_seconds", 0)
        # Theo đặc tả Vr15: dataset validation thuộc non-algorithm overhead.
        expected_e2e = inp + alg + overhead
        if e2e >= 0 and abs(e2e - expected_e2e) > tolerance:
            errors.append(
                f"{name} E2E decomposition sai: {e2e:.6f} vs "
                f"{expected_e2e:.6f}"
            )
        if overhead < -0.05:
            errors.append(f"{name} có overhead âm lớn")

    # Bất biến phân loại fragment.
    f_total = num(p, "num_localeval_fragments_total")
    f_complete = num(p, "num_complete_local_fragments")
    f_boundary = num(p, "num_true_boundary_fragments")
    f_complete_only = num(p, "num_complete_local_only_fragments")
    f_complete_boundary = num(p, "num_complete_and_boundary_fragments")
    f_incomplete_boundary = num(p, "num_incomplete_boundary_fragments")
    f_dead = num(p, "num_dead_end_fragments")
    if min(
        f_total, f_complete, f_boundary, f_complete_only,
        f_complete_boundary, f_incomplete_boundary, f_dead
    ) >= 0:
        class_sum = (
            f_complete_only + f_complete_boundary
            + f_incomplete_boundary + f_dead
        )
        if abs(f_total - class_sum) > 0.1:
            errors.append(
                f"Fragment partition invariant sai: total={f_total}, classes={class_sum}"
            )
        if abs(f_complete - (f_complete_only + f_complete_boundary)) > 0.1:
            errors.append("num_complete_local_fragments không khớp các lớp complete")
        if abs(f_boundary - (f_complete_boundary + f_incomplete_boundary)) > 0.1:
            errors.append("num_true_boundary_fragments không khớp các lớp boundary")

    atomic_f = num(p, "num_query_relevant_atomic_evidence")
    stitch_input_f = num(p, "num_stitch_input_fragments")
    support_f = num(p, "num_boundary_support_fragments")
    if min(f_total, atomic_f, f_complete) >= 0 and abs(
        f_total - (atomic_f + f_complete)
    ) > 0.1:
        errors.append(
            "Tổng fragment phải bằng atomic evidence + complete local fragments"
        )
    if min(stitch_input_f, f_boundary) >= 0 and stitch_input_f < f_boundary:
        errors.append("Stitch input nhỏ hơn true-boundary set")
    if min(stitch_input_f, f_boundary, support_f) >= 0 and abs(
        support_f - (stitch_input_f - f_boundary)
    ) > 0.1:
        errors.append("Boundary support metric không khớp stitch input - true boundary")

    # Bất biến candidate/witness.
    local_c = num(p, "num_local_complete_candidates")
    stitched_c = num(p, "num_stitched_candidates")
    merged_c = num(p, "num_merged_candidates")
    valid_c = num(p, "num_valid_witnesses_before_normalization")
    final_w = num(p, "num_final_valid_witnesses")
    if min(local_c, f_complete) >= 0 and abs(local_c - f_complete) > 0.1:
        errors.append("C_L count không bằng số complete local fragments")
    if min(local_c, stitched_c, merged_c) >= 0:
        if merged_c > local_c + stitched_c + 0.1:
            errors.append("Merged candidates lớn hơn local + stitched")
        if merged_c + 0.1 < max(local_c, stitched_c):
            errors.append("Merged candidates nhỏ hơn một tập đầu vào")
    if min(merged_c, valid_c) >= 0 and valid_c > merged_c + 0.1:
        errors.append("Valid-before-normalization lớn hơn merged candidates")
    if min(valid_c, final_w) >= 0 and final_w > valid_c + 0.1:
        errors.append("Final witnesses lớn hơn valid-before-normalization")

    b_merged = num(b, "num_merged_candidates")
    b_valid = num(b, "num_valid_witnesses_before_normalization")
    b_final = num(b, "num_final_valid_witnesses")
    if min(b_merged, b_valid) >= 0 and b_valid > b_merged + 0.1:
        errors.append("Baseline valid-before-normalization lớn hơn candidates")
    if min(b_valid, b_final) >= 0 and b_final > b_valid + 0.1:
        errors.append("Baseline final witnesses lớn hơn valid-before-normalization")

    mode = str(p.get("final_witness_construction_mode", ""))
    if mode != "MERGED_CANDIDATES_HARD_VALIDATE_THEN_NORMALIZE":
        errors.append(f"Final witness construction mode không hợp lệ: {mode}")

    if p_gap < -0.05:
        errors.append(f"Proposed phase gap âm lớn: {p_gap:.6f}")
    return errors


def runtime_comparison_row(
    dataset: str,
    query_id: str,
    repeat_id: int,
    b: dict,
    p: dict,
    *,
    compute_speedup: bool,
) -> dict[str, object]:
    b_alg = num(b, "time_algorithm_total_seconds", 0)
    p_alg = num(p, "time_algorithm_total_seconds", 0)
    b_e2e = num(b, "time_end_to_end_total_seconds", 0)
    p_e2e = num(p, "time_end_to_end_total_seconds", 0)
    return {
        "Dataset": dataset,
        "Query": query_id,
        "Repeat": repeat_id,
        "Baseline_dataset_integrity_validation_s": num(
            b, "time_dataset_integrity_validation_seconds", 0
        ),
        "Baseline_step1_global_merge_s": num(
            b, "time_step1_global_merge_seconds"
        ),
        "Baseline_step2_global_query_validation_s": num(
            b, "time_step2_global_query_validation_seconds"
        ),
        "Baseline_algorithm_total_s": b_alg,
        "Baseline_end_to_end_s": b_e2e,
        "Proposed_boundary_index_s": num(
            p, "time_step1_boundary_index_preparation_seconds"
        ),
        "Proposed_localeval_parallel_max_s": num(
            p, "time_step1_localeval_parallel_seconds"
        ),
        "Proposed_localeval_wall_s": num(
            p, "time_step1_localeval_wall_seconds"
        ),
        "Proposed_localeval_partition_action_elapsed_max_s": num_first(
            p,
            [
                "time_step1_localeval_partition_action_elapsed_max_seconds",
                "time_step1_localeval_active_max_seconds",
            ],
        ),
        "Proposed_localeval_partition_work_sum_s": num(
            p, "time_step1_localeval_partition_work_sum_seconds"
        ),
        "Proposed_fragment_classification_s": num(
            p, "time_step1_fragment_classification_seconds"
        ),
        "Proposed_step2_globaleval_s": num(
            p, "time_step2_globaleval_seconds"
        ),
        "Proposed_algorithm_total_s": p_alg,
        "Proposed_end_to_end_s": p_e2e,
        "Algorithm_speedup": (
            (b_alg / p_alg) if compute_speedup and p_alg > 0 else None
        ),
        "End_to_end_speedup": (
            (b_e2e / p_e2e) if compute_speedup and p_e2e > 0 else None
        ),
        "Baseline_candidates": num(b, "num_merged_candidates"),
        "Proposed_complete_local_fragments": num(p, "num_complete_local_fragments"),
        "Proposed_local_complete_candidates": num(p, "num_local_complete_candidates"),
        "Proposed_true_boundary_fragments": num(
            p, "num_true_boundary_fragments"
        ),
        "Proposed_dead_end_fragments": num(p, "num_dead_end_fragments"),
        "Proposed_stitch_input_fragments": num(p, "num_stitch_input_fragments"),
        "Proposed_boundary_support_fragments": num(p, "num_boundary_support_fragments"),
        "Proposed_stitched_candidates": num(p, "num_stitched_candidates"),
        "Proposed_merged_candidates": num(p, "num_merged_candidates"),
        "Baseline_valid_before_normalization": num(
            b, "num_valid_witnesses_before_normalization"
        ),
        "Proposed_valid_before_normalization": num(
            p, "num_valid_witnesses_before_normalization"
        ),
        "Baseline_final_valid_witnesses": num(
            b, "num_final_valid_witnesses"
        ),
        "Proposed_final_valid_witnesses": num(
            p, "num_final_valid_witnesses"
        ),
        "Proposed_final_witness_construction_mode": p.get(
            "final_witness_construction_mode", ""
        ),
    }


def build_env(
    args,
    dataset_root: str,
    dataset_output: Path,
    query_id: str,
) -> dict[str, str]:
    env = os.environ.copy()
    correctness = args.mode == "correctness"
    profiling = args.mode in {"profiling", "profiling_runtime_only"}
    runtime_only = args.mode == "profiling_runtime_only"

    # Đây là runner C2: các phase boundaries được materialize cho correctness,
    # profiling và benchmark C2. Runtime-only chỉ tắt action diagnostic/output,
    # không đổi thuật toán hoặc ranh giới phase.
    materialize_step_boundaries = True
    persist_intermediates = True
    count_intermediate = correctness or (profiling and not runtime_only)

    env.update(
        {
            "DATASET_ROOT": dataset_root,
            "BY_PARTITION_DIR": dataset_root.rstrip("/") + "/by_partition",
            "QUERY_ID": query_id,
            "QUERY_OUTPUT_ROOT": str(dataset_output),
            "BENCHMARK_FORCE_FINAL_ACTION": "true",
            "BENCHMARK_COUNT_INTERMEDIATE": (
                "true" if count_intermediate else "false"
            ),
            "BENCHMARK_COUNT_DATASET_SIZE": (
                "true" if count_intermediate else "false"
            ),
            "BENCHMARK_MATERIALIZE_STEP_BOUNDARIES": (
                "true" if materialize_step_boundaries else "false"
            ),
            "BENCHMARK_TIMING_ONLY": "false" if correctness else "true",
            "BENCHMARK_VALIDATE_RESULT": "true" if correctness else "false",
            "SAVE_RUNTIME_SUMMARY": "true",
            "SAVE_CANDIDATE_OUTPUTS": "true" if correctness else "false",
            "SAVE_FINAL_WITNESSES": "false" if runtime_only else "true",
            "SAVE_INTERMEDIATE_OUTPUTS": "true" if correctness else "false",
            "STAGE2_PERSIST_INTERMEDIATES": (
                "true" if persist_intermediates else "false"
            ),
            "SPARK_MASTER": args.spark_master,
            "SPARK_DRIVER_MEMORY": args.driver_memory,
            "SPARK_EXECUTOR_MEMORY": args.executor_memory,
            "SPARK_EXECUTOR_CORES": str(args.executor_cores),
            "SPARK_CORES_MAX": str(args.cores_max),
            "SPARK_SHUFFLE_PARTITIONS": str(args.shuffle_partitions),
            "SPARK_SCHEDULER_MODE": "FAIR",
            "MAX_TIME_GAP_SECONDS": str(args.max_time_gap_seconds),
            "QUICK_EXIT_MAX_SECONDS": str(args.quick_exit_max_seconds),
            "TIME_WINDOW_DURATION_SECONDS": str(
                args.time_window_duration_seconds
            ),
            "EXIT_PERSON_ROLE": args.exit_person_role,
            "MINIMUM_PARTITION_COUNT": str(args.minimum_partition_count),
            "REQUIRE_DIFFERENT_LOCATION": (
                "true" if args.require_different_location else "false"
            ),
            "QUICK_EXIT_REFERENCE_ROLE": args.quick_exit_reference_role,
            "QUERY_TIME_SCOPE": args.query_time_scope,
            "QUERY_START_TIME": args.query_start_time or "",
            "QUERY_END_TIME": args.query_end_time or "",
            "QUERY_DIAGNOSTICS_ENABLED": (
                "true" if args.enable_query_diagnostics else "false"
            ),
            "SPARK_LOG_LEVEL": args.spark_log_level,
            "SPARK_AUTO_BROADCAST_JOIN_THRESHOLD": str(
                args.auto_broadcast_join_threshold
            ),
            "SPARK_FILES_MAX_PARTITION_BYTES": str(
                args.files_max_partition_bytes
            ),
        }
    )
    # Tránh Python interactive shell vô tình được kế thừa vào spark-submit.
    env.pop("PYSPARK_DRIVER_PYTHON", None)
    env.pop("PYSPARK_DRIVER_PYTHON_OPTS", None)
    env.setdefault("PYSPARK_PYTHON", "/usr/bin/python3")
    return env


def write_runtime_outputs(
    output_root: Path, runtime_rows: list[dict], *, compute_speedup: bool
) -> None:
    runtime_df = pd.DataFrame(runtime_rows)
    if runtime_df.empty:
        return
    runtime_df.to_csv(output_root / "runtime_vr15_all_queries.csv", index=False)
    for query_id, query_df in runtime_df.groupby("Query", dropna=False):
        query_file = output_root / f"runtime_vr15_{query_id}.csv"
        query_df.to_csv(query_file, index=False)
        numeric = query_df.select_dtypes(include="number").columns.tolist()
        group_cols = ["Dataset", "Query"]
        metrics = [c for c in numeric if c != "Repeat"]
        if metrics:
            aggregate = (
                query_df.groupby(group_cols, dropna=False)[metrics]
                .agg(["mean", "std", "median", "min", "max"])
                .reset_index()
            )
            aggregate.columns = [
                "_".join(str(x) for x in c if str(x))
                if isinstance(c, tuple)
                else str(c)
                for c in aggregate.columns
            ]
            aggregate.to_csv(
                output_root / f"runtime_vr15_aggregate_{query_id}.csv",
                index=False,
            )

        if not compute_speedup:
            continue

        # Speedup đại diện = tỷ số hai median runtime, không phải median/mean
        # của các speedup từng lần.
        representative_rows = []
        for (dataset, qid), group in query_df.groupby(
            ["Dataset", "Query"], dropna=False
        ):
            b_med = group["Baseline_algorithm_total_s"].median()
            p_med = group["Proposed_algorithm_total_s"].median()
            representative_rows.append({
                "Dataset": dataset,
                "Query": qid,
                "Baseline_algorithm_median_s": b_med,
                "Proposed_algorithm_median_s": p_med,
                "Algorithm_speedup_ratio_of_medians": (b_med / p_med) if p_med > 0 else None,
                "Algorithm_time_reduction_pct_from_medians": ((b_med - p_med) / b_med * 100.0) if b_med > 0 else None,
            })
        pd.DataFrame(representative_rows).to_csv(
            output_root / f"runtime_vr15_representative_{query_id}.csv",
            index=False,
        )


def source_bundle_hash(script_dir: Path) -> str:
    digest = hashlib.sha256()
    paths = [
        script_dir / "baseline.py",
        script_dir / "proposed_distributed.py",
        script_dir / "run_experiment.py",
        *sorted((script_dir / "query_specs").glob("*.py")),
    ]
    for path in paths:
        digest.update(str(path.relative_to(script_dir)).encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def parameter_signature(args, query_id: str) -> dict[str, object]:
    spec = QUERY_REGISTRY[query_id]
    signature: dict[str, object] = {
        "hdfs_root": args.hdfs_root.rstrip("/"),
        "spark_master": args.spark_master,
        "driver_memory": args.driver_memory,
        "executor_memory": args.executor_memory,
        "executor_cores": args.executor_cores,
        "cores_max": args.cores_max,
        "shuffle_partitions": args.shuffle_partitions,
        "max_time_gap_seconds": args.max_time_gap_seconds,
        "time_window_duration_seconds": args.time_window_duration_seconds,
        "candidate_pruning_max_time_window_gap": math.ceil(
            args.max_time_gap_seconds / args.time_window_duration_seconds
        ),
        "query_time_scope": args.query_time_scope,
        "query_start_time": args.query_start_time,
        "query_end_time": args.query_end_time,
    }
    optional = {
        "quick_exit_max_seconds": args.quick_exit_max_seconds,
        "exit_person_role": args.exit_person_role,
        "minimum_partition_count": args.minimum_partition_count,
        "require_different_location": args.require_different_location,
        "quick_exit_reference_role": args.quick_exit_reference_role,
    }
    for name, value in optional.items():
        if name in spec.runtime_parameter_schema or name in spec.hard_constraint_names:
            signature[name] = value
    return signature


def _find_correctness_gate(root: Path, query_id: str) -> Path | None:
    direct = root / f"correctness_gate_{query_id}.json"
    if direct.exists():
        return direct
    matches = sorted(root.rglob(f"correctness_gate_{query_id}.json"))
    return matches[-1] if matches else None


def validate_correctness_gates(
    root: Path,
    query_ids: list[str],
    *,
    expected_source_hash: str,
    expected_signatures: dict[str, dict[str, object]],
) -> None:
    errors: list[str] = []
    for query_id in query_ids:
        gate = _find_correctness_gate(root, query_id)
        if gate is None:
            errors.append(f"{query_id}: không tìm thấy correctness gate")
            continue
        try:
            payload = json.loads(gate.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"{query_id}: không đọc được {gate}: {exc!r}")
            continue
        if payload.get("status") != "PASS":
            errors.append(f"{query_id}: gate không PASS: {gate}")
        if payload.get("normalized_witness_set_match") is not True:
            errors.append(f"{query_id}: normalized witness set chưa match: {gate}")
        if payload.get("source_bundle_hash") != expected_source_hash:
            errors.append(f"{query_id}: source hash của gate khác code hiện tại: {gate}")
        if payload.get("parameter_signature") != expected_signatures[query_id]:
            errors.append(f"{query_id}: tham số correctness khác profiling: {gate}")
    if errors:
        raise SystemExit(
            "Không được tính speedup trước correctness PASS:\n"
            + "\n".join(errors)
        )

def main() -> None:
    parser = argparse.ArgumentParser(
        description="VideoGraphDB Vr15 Baseline vs Proposed"
    )
    parser.add_argument(
        "--hdfs-root", default="hdfs:///data/videographdb_balanced"
    )
    parser.add_argument(
        "--output-root", default="./query_outputs_vr15_balanced"
    )
    parser.add_argument(
        "--query-id",
        default=os.getenv("QUERY_ID"),
        required=os.getenv("QUERY_ID") is None,
        help="Một query, danh sách phân tách dấu phẩy, hoặc ALL",
    )
    parser.add_argument("--datasets", default=",".join(DATASETS))
    parser.add_argument(
        "--mode",
        choices=[
            "correctness",
            "profiling",
            "profiling_runtime_only",
        ],
        default="correctness",
    )
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--require-nonzero", action="store_true")
    parser.add_argument("--spark-master", default="spark://master:7077")
    parser.add_argument("--driver-memory", default="2G")
    parser.add_argument("--executor-memory", default="5G")
    parser.add_argument("--executor-cores", type=int, default=2)
    parser.add_argument("--cores-max", type=int, default=8)
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--spark-log-level", default="WARN")
    parser.add_argument(
        "--auto-broadcast-join-threshold", default="2097152"
    )
    parser.add_argument("--files-max-partition-bytes", default="67108864")
    parser.add_argument("--max-time-gap-seconds", type=float, default=120.0)
    parser.add_argument("--quick-exit-max-seconds", type=float, default=120.0)
    parser.add_argument(
        "--time-window-duration-seconds", type=float, default=10.0
    )
    parser.add_argument("--exit-person-role", choices=["P1", "P2"], default="P1")
    parser.add_argument("--minimum-partition-count", type=int, default=1)
    parser.add_argument("--require-different-location", action="store_true")
    parser.add_argument(
        "--quick-exit-reference-role",
        choices=["INTERACT", "CARRY2"],
        default="CARRY2",
    )
    parser.add_argument("--enable-query-diagnostics", action="store_true")
    parser.add_argument(
        "--query-time-scope",
        choices=["FULL_DATASET", "EXPLICIT_INTERVAL"],
        default="FULL_DATASET",
    )
    parser.add_argument("--query-start-time")
    parser.add_argument("--query-end-time")
    parser.add_argument(
        "--correctness-gate-root",
        help="Thư mục chứa correctness_gate_<query>.json đã PASS; bắt buộc cho profiling.",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    current_source_hash = source_bundle_hash(script_dir)
    query_ids = parse_query_ids(args.query_id)
    if args.query_time_scope == "EXPLICIT_INTERVAL":
        if not args.query_start_time or not args.query_end_time:
            raise SystemExit(
                "EXPLICIT_INTERVAL yêu cầu đồng thời --query-start-time và --query-end-time"
            )
    elif args.query_start_time or args.query_end_time:
        raise SystemExit(
            "FULL_DATASET không nhận query_start_time/query_end_time; hãy chọn EXPLICIT_INTERVAL"
        )

    requires_correctness_gate = args.mode in {"profiling", "profiling_runtime_only"}
    compute_speedup = args.mode == "profiling_runtime_only"
    if args.mode == "profiling_runtime_only" and args.enable_query_diagnostics:
        raise SystemExit(
            "profiling_runtime_only không cho phép --enable-query-diagnostics"
        )
    if args.require_nonzero and args.mode != "correctness":
        raise SystemExit("--require-nonzero chỉ dùng trong mode correctness")
    if requires_correctness_gate:
        if not args.correctness_gate_root:
            raise SystemExit(
                "Profiling yêu cầu --correctness-gate-root để chứng minh correctness PASS trước khi tính speedup."
            )
        validate_correctness_gates(
            Path(args.correctness_gate_root).resolve(),
            query_ids,
            expected_source_hash=current_source_hash,
            expected_signatures={
                q: parameter_signature(args, q) for q in query_ids
            },
        )

    datasets = [x.strip() for x in args.datasets.split(",") if x.strip()]
    invalid_datasets = [d for d in datasets if d not in DATASETS]
    if invalid_datasets:
        raise SystemExit(
            f"Dataset không thuộc bộ Vr15: {invalid_datasets}; hợp lệ={DATASETS}"
        )

    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    logs_root = output_root / "logs"
    # Chỉ correctness lưu đủ candidate/witness để so sánh tập kết quả.
    skip_comparison = args.mode != "correctness"

    comparison_rows: list[dict[str, object]] = []
    runtime_rows: list[dict[str, object]] = []
    timing_errors: list[str] = []

    for repeat_id in range(1, args.repeats + 1):
        for query_id in query_ids:
            for dataset in datasets:
                run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                dataset_hdfs = args.hdfs_root.rstrip("/") + "/" + dataset
                dataset_output = output_root / dataset
                env = build_env(
                    args, dataset_hdfs, dataset_output, query_id
                )
                common = [
                    "--dataset-root",
                    dataset_hdfs,
                    "--by-partition-dir",
                    dataset_hdfs + "/by_partition",
                    "--query-id",
                    query_id,
                    "--output-root",
                    str(dataset_output),
                    "--max-time-gap-seconds",
                    str(args.max_time_gap_seconds),
                    "--quick-exit-max-seconds",
                    str(args.quick_exit_max_seconds),
                    "--time-window-duration-seconds",
                    str(args.time_window_duration_seconds),
                    "--exit-person-role",
                    args.exit_person_role,
                    "--minimum-partition-count",
                    str(args.minimum_partition_count),
                    "--quick-exit-reference-role",
                    args.quick_exit_reference_role,
                ]
                if args.require_different_location:
                    common.append("--require-different-location")
                if args.enable_query_diagnostics:
                    common.append("--enable-query-diagnostics")

                prefix = [
                    os.getenv("SPARK_SUBMIT", "spark-submit"),
                    "--master",
                    args.spark_master,
                    "--driver-memory",
                    args.driver_memory,
                    "--executor-memory",
                    args.executor_memory,
                    "--executor-cores",
                    str(args.executor_cores),
                    "--conf",
                    f"spark.cores.max={args.cores_max}",
                    "--conf",
                    f"spark.sql.shuffle.partitions={args.shuffle_partitions}",
                    "--conf",
                    "spark.scheduler.mode=FAIR",
                    "--conf",
                    "spark.sql.adaptive.enabled=false",
                    "--conf",
                    "spark.sql.adaptive.coalescePartitions.enabled=false",
                    "--conf",
                    "spark.sql.adaptive.skewJoin.enabled=false",
                    "--conf",
                    "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                    "--conf",
                    "spark.kryoserializer.buffer.max=512m",
                ]
                tag = query_id.replace(".", "_")
                b_log = logs_root / (
                    f"{run_stamp}_r{repeat_id}_{dataset}_{tag}_baseline.log"
                )
                p_log = logs_root / (
                    f"{run_stamp}_r{repeat_id}_{dataset}_{tag}_proposed.log"
                )
                stream_run_cmd(
                    [*prefix, str(script_dir / "baseline.py"), *common],
                    env,
                    b_log,
                )
                stream_run_cmd(
                    [
                        *prefix,
                        str(script_dir / "proposed_distributed.py"),
                        *common,
                    ],
                    env,
                    p_log,
                )

                b = read_runtime_row(
                    dataset_output, BASELINE_METHOD_DIR, query_id
                )
                p = read_runtime_row(
                    dataset_output, PROPOSED_METHOD_DIR, query_id
                )
                errs = validate_timing_invariants(b, p)
                timing_errors.extend(
                    [f"{query_id}/{dataset}/r{repeat_id}: {e}" for e in errs]
                )
                runtime_rows.append(
                    runtime_comparison_row(
                        dataset, query_id, repeat_id, b, p,
                        compute_speedup=compute_speedup,
                    )
                )

                if not skip_comparison:
                    comp = compare_outputs(output_root, dataset, query_id)
                    comp.update(
                        {
                            "repeat_id": repeat_id,
                            "mode": args.mode,
                            "baseline_log": str(b_log),
                            "proposed_log": str(p_log),
                            "baseline_num_candidates_runtime": num(
                                b, "num_merged_candidates"
                            ),
                            "proposed_num_candidates_runtime": num(
                                p, "num_merged_candidates"
                            ),
                            "baseline_num_final_witnesses_runtime": num(
                                b, "num_final_valid_witnesses"
                            ),
                            "proposed_num_final_witnesses_runtime": num(
                                p, "num_final_valid_witnesses"
                            ),
                            "final_witness_construction_mode": p.get(
                                "final_witness_construction_mode", ""
                            ),
                        }
                    )
                    comparison_rows.append(comp)

    write_runtime_outputs(
        output_root, runtime_rows, compute_speedup=compute_speedup
    )

    if timing_errors:
        error_file = output_root / "timing_invariant_errors.txt"
        error_file.write_text("\n".join(timing_errors), encoding="utf-8")
        raise SystemExit(
            "Timing invariant FAIL:\n" + "\n".join(timing_errors)
        )

    if not skip_comparison:
        report = pd.DataFrame(comparison_rows)
        report.to_csv(output_root / "correctness_vr15_all_queries.csv", index=False)

        gate_errors: list[str] = []
        for query_id in query_ids:
            q_report = report[report["query_id"] == query_id].copy()
            q_report.to_csv(
                output_root / f"correctness_vr15_{query_id}.csv", index=False
            )
            if not bool(q_report["outputs_match"].all()):
                gate_errors.append(
                    f"{query_id}: Baseline và Proposed khác normalized witness set"
                )
            if args.require_nonzero and bool(q_report["both_zero"].any()):
                gate_errors.append(
                    f"{query_id}: có dataset cả hai cùng zero witness"
                )

            modes = sorted(
                set(
                    str(x)
                    for x in q_report["final_witness_construction_mode"]
                    if str(x)
                )
            )
            if modes and modes != ["MERGED_CANDIDATES_HARD_VALIDATE_THEN_NORMALIZE"]:
                gate_errors.append(
                    f"{query_id}: final witness chưa được tạo trực tiếp từ "
                    f"merged candidates: {modes}"
                )

            gate_status = "PASS" if not any(
                e.startswith(f"{query_id}:") for e in gate_errors
            ) else "FAIL"
            gate_payload = {
                "status": gate_status,
                "code_version": "Vr15-spec-v4",
                "query_id": query_id,
                "query_name": QUERY_REGISTRY[query_id].query_name,
                "datasets": datasets,
                "repeats": args.repeats,
                "source_bundle_hash": current_source_hash,
                "parameter_signature": parameter_signature(args, query_id),
                "correctness_compare_columns": list(
                    QUERY_REGISTRY[query_id].correctness_compare_columns
                ),
                "normalized_witness_set_match": bool(
                    q_report["witness_set_match"].all()
                ),
                "witness_count_match": bool(
                    q_report["witness_count_match"].all()
                ),
                "candidate_set_match": bool(
                    q_report["candidate_set_match"].all()
                ),
                "candidate_count_match": bool(
                    q_report["candidate_count_match"].all()
                ),
                "baseline_only_total": int(
                    q_report["witness_only_baseline"].sum()
                ),
                "proposed_only_total": int(
                    q_report["witness_only_proposed"].sum()
                ),
                "comparisons": json.loads(
                    q_report.to_json(orient="records", force_ascii=False)
                ),
                "created_at": datetime.now().isoformat(),
            }
            gate = output_root / f"correctness_gate_{query_id}.json"
            gate.write_text(
                json.dumps(gate_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"[Correctness] {gate_status}: {gate}")

        if gate_errors:
            raise SystemExit("Correctness FAIL:\n" + "\n".join(gate_errors))

    print(f"[Write] {output_root / 'runtime_vr15_all_queries.csv'}")
    print(f"[Logs] {logs_root}")


if __name__ == "__main__":
    main()

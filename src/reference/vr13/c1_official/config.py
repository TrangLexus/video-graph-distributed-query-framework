#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared configuration for VideoGraphDB Vr09 experiments.

This module intentionally contains configuration only. It does not contain
query semantics or algorithm-specific logic.
"""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in TRUE_VALUES


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def parse_query_ids(value: str | None) -> List[str]:
    raw = value or os.getenv("SELECTED_QUERY_IDS", os.getenv("QUERY_ID", "Q14"))
    out = [q.strip() for q in raw.split(",") if q.strip()]
    return out or ["Q14"]


@dataclass(frozen=True)
class Stage2Config:
    # Input/output.
    dataset_root: str
    by_partition_dir: str
    query_ids: List[str]
    output_root: Path
    method: str
    app_name: str

    # Spark.
    spark_master: str
    spark_shuffle_partitions: int
    graphframes_package: str
    driver_memory: str
    executor_memory: str
    executor_cores: str
    cores_max: str
    spark_log_level: str
    auto_broadcast_join_threshold: str
    spark_files_max_partition_bytes: str

    # Query semantics / bounds.
    epsilon_time_seconds: float
    quick_exit_seconds: float
    max_nexttw_hops: int
    next_tw_require_consecutive: bool
    q14_exit_person_role: str  # "P1" by default to reproduce validated Q14 benchmark semantics.
    q14_min_partition_count: int  # Default 1 for semantic Q14; use 2 only for Q14-CROSS stress tests.
    q14_require_different_location: bool  # Default false; handover may occur in the same location.
    q14_quick_exit_reference: str  # "CARRY2" or "INTERACT"; default CARRY2 is less over-restrictive.
    q14_diagnostics_enabled: bool

    # Benchmark controls.
    benchmark_timing_only: bool
    benchmark_validate_result: bool
    benchmark_count_dataset_size: bool
    benchmark_count_intermediate: bool
    benchmark_force_final_action: bool
    benchmark_materialize_step_boundaries: bool

    # Proposed-algorithm profiling controls.
    enable_local_complete_detection: bool

    # Persist/write controls.
    persist_intermediates: bool
    spark_indexing_enabled: bool
    save_runtime_summary: bool
    save_candidate_outputs: bool
    save_final_witnesses: bool
    save_intermediate_outputs: bool


def build_arg_parser(default_app_name: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="VideoGraphDB Vr09 benchmark runner")
    parser.add_argument("--dataset-root", default=None, help="Dataset root, e.g. hdfs:///data/videographdb/dataset_1m")
    parser.add_argument("--by-partition-dir", default=None, help="Default: DATASET_ROOT/by_partition")
    parser.add_argument("--query-id", default=None, help="Single query ID, e.g. Q14 or Q4.3")
    parser.add_argument("--query-ids", default=None, help="Comma-separated query IDs")
    parser.add_argument("--output-root", default=None, help="Output root directory")
    parser.add_argument("--spark-master", default=None, help="Spark master URL")
    parser.add_argument("--shuffle-partitions", default=None, type=int)
    parser.add_argument("--app-name", default=None)
    parser.add_argument("--epsilon-time-seconds", default=None, type=float)
    parser.add_argument("--quick-exit-seconds", default=None, type=float)
    parser.add_argument("--max-nexttw-hops", default=None, type=int)
    parser.add_argument(
        "--q14-exit-person-role",
        choices=["P1", "P2"],
        default=None,
        help="Vehicle-use actor for Q14/Q4.3. Default P1 reproduces validated benchmark files.",
    )
    parser.add_argument(
        "--q14-min-partition-count",
        "--min-partition-count",
        dest="q14_min_partition_count",
        default=None,
        type=int,
        help="Minimum partitions required for Q14 validity. Default 1; use 2 for Q14-CROSS.",
    )
    parser.add_argument(
        "--q14-require-different-location",
        action="store_true",
        default=None,
        help="Require CARRY_1 and INTERACT to occur in different locations. Disabled by default.",
    )
    parser.add_argument(
        "--q14-quick-exit-reference",
        choices=["INTERACT", "CARRY2"],
        default=None,
        help="Reference event for quick_exit_delay. Default CARRY2.",
    )
    parser.add_argument(
        "--enable-q14-diagnostics",
        action="store_true",
        default=None,
        help="Enable extra Q14 diagnostics in logs/summary where supported.",
    )
    return parser


def load_config(method: str, default_app_name: str) -> Stage2Config:
    args, unknown = build_arg_parser(default_app_name).parse_known_args()
    if unknown:
        print(f"[WARN] Ignoring unknown arguments: {unknown}")

    dataset_root = (args.dataset_root or os.getenv("DATASET_ROOT", "hdfs:///data/videographdb/dataset_1m")).rstrip("/")
    by_partition_dir = (args.by_partition_dir or os.getenv("BY_PARTITION_DIR", f"{dataset_root}/by_partition")).rstrip("/")

    query_value = args.query_ids or args.query_id or os.getenv("QUERY_IDS") or os.getenv("QUERY_ID")
    query_ids = parse_query_ids(query_value)

    epsilon_time = args.epsilon_time_seconds
    if epsilon_time is None:
        # Q14 semantic benchmark: this is a general inter-role tolerance/diagnostic bound,
        # not the quick-exit threshold. Keep it looser than quick_exit by default.
        epsilon_time = env_float("EPSILON_TIME_SECONDS", env_float("Q14_EPSILON_TIME_SECONDS", env_float("MAX_TIME_GAP_SECONDS", 120.0)))

    quick_exit = args.quick_exit_seconds
    if quick_exit is None:
        quick_exit = env_float("QUICK_EXIT_SECONDS", env_float("Q14_QUICK_EXIT_SECONDS", 30.0))

    max_hops = args.max_nexttw_hops
    if max_hops is None:
        max_hops = env_int("MAX_NEXTTW_HOPS", env_int("ERPQ_MAX_NEXT_TW_HOPS", env_int("EPSILON_TW", 2)))

    if int(max_hops) > 3:
        print(
            f"[WARN] max_nexttw_hops={max_hops} may be expensive on 16GB RAM. "
            "Use 1 for official 16GB timing; use 3 only for debugging/relaxed correctness checks."
        )

    output_root = Path(args.output_root or os.getenv("QUERY_OUTPUT_ROOT", "./query_outputs"))
    output_root.mkdir(parents=True, exist_ok=True)

    q14_exit_role = (args.q14_exit_person_role or os.getenv("Q14_EXIT_PERSON_ROLE", "P1")).strip().upper()
    if q14_exit_role not in {"P1", "P2"}:
        q14_exit_role = "P1"

    q14_min_partition_count = args.q14_min_partition_count
    if q14_min_partition_count is None:
        q14_min_partition_count = env_int("Q14_MIN_PARTITION_COUNT", env_int("MIN_PARTITION_COUNT_Q14", 1))
    q14_min_partition_count = max(1, int(q14_min_partition_count))

    if args.q14_require_different_location is None:
        q14_require_different_location = env_bool("Q14_REQUIRE_DIFFERENT_LOCATION", "false")
    else:
        q14_require_different_location = bool(args.q14_require_different_location)

    q14_quick_exit_reference = (args.q14_quick_exit_reference or os.getenv("Q14_QUICK_EXIT_REFERENCE", "CARRY2")).strip().upper()
    if q14_quick_exit_reference not in {"INTERACT", "CARRY2"}:
        q14_quick_exit_reference = "CARRY2"

    q14_diagnostics_enabled = (
        bool(args.enable_q14_diagnostics)
        if args.enable_q14_diagnostics is not None
        else env_bool("Q14_DIAGNOSTICS_ENABLED", "false")
    )

    return Stage2Config(
        dataset_root=dataset_root,
        by_partition_dir=by_partition_dir,
        query_ids=query_ids,
        output_root=output_root,
        method=method,
        app_name=args.app_name or os.getenv("SPARK_APP_NAME", default_app_name),
        spark_master=args.spark_master or os.getenv("SPARK_MASTER", "spark://master:7077"),
        spark_shuffle_partitions=int(args.shuffle_partitions or env_int("SPARK_SHUFFLE_PARTITIONS", 6)),
        graphframes_package=os.getenv("SPARK_GRAPHFRAMES_PACKAGE", "graphframes:graphframes:0.8.2-spark3.2-s_2.12"),
        driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "3G"),
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "3G"),
        executor_cores=os.getenv("SPARK_EXECUTOR_CORES", "1"),
        cores_max=os.getenv("SPARK_CORES_MAX", "4"),
        spark_log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        auto_broadcast_join_threshold=os.getenv("SPARK_AUTO_BROADCAST_JOIN_THRESHOLD", "2097152"),
        spark_files_max_partition_bytes=os.getenv("SPARK_FILES_MAX_PARTITION_BYTES", "67108864"),
        epsilon_time_seconds=float(epsilon_time),
        quick_exit_seconds=float(quick_exit),
        max_nexttw_hops=int(max_hops),
        next_tw_require_consecutive=env_bool("NEXT_TW_REQUIRE_CONSECUTIVE", "false"),
        q14_exit_person_role=q14_exit_role,
        q14_min_partition_count=q14_min_partition_count,
        q14_require_different_location=q14_require_different_location,
        q14_quick_exit_reference=q14_quick_exit_reference,
        q14_diagnostics_enabled=q14_diagnostics_enabled,
        benchmark_timing_only=env_bool("BENCHMARK_TIMING_ONLY", "true"),
        benchmark_validate_result=env_bool("BENCHMARK_VALIDATE_RESULT", "false"),
        benchmark_count_dataset_size=env_bool("BENCHMARK_COUNT_DATASET_SIZE", "false"),
        benchmark_count_intermediate=env_bool("BENCHMARK_COUNT_INTERMEDIATE", "false"),
        benchmark_force_final_action=env_bool("BENCHMARK_FORCE_FINAL_ACTION", "true"),
        benchmark_materialize_step_boundaries=env_bool("BENCHMARK_MATERIALIZE_STEP_BOUNDARIES", "false"),
        enable_local_complete_detection=env_bool("ENABLE_LOCAL_COMPLETE_DETECTION", "false"),
        persist_intermediates=env_bool("STAGE2_PERSIST_INTERMEDIATES", "true"),
        spark_indexing_enabled=env_bool("STAGE2_SPARK_INDEXING_ENABLED", "true"),
        save_runtime_summary=env_bool("SAVE_RUNTIME_SUMMARY", "true"),
        save_candidate_outputs=env_bool("SAVE_CANDIDATE_OUTPUTS", "false"),
        save_final_witnesses=env_bool("SAVE_FINAL_WITNESSES", "true"),
        save_intermediate_outputs=env_bool("SAVE_INTERMEDIATE_OUTPUTS", "false"),
    )


def ensure_supported_query_ids(query_ids: Iterable[str], registry: dict) -> None:
    unsupported = [q for q in query_ids if q not in registry]
    if unsupported:
        raise ValueError(f"Unsupported query IDs: {unsupported}. Supported: {sorted(registry.keys())}")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Cấu hình dùng chung cho VideoGraphDB Vr15.

Vr15 tách rõ:
- cấu hình hệ thống/Spark;
- ràng buộc thời gian dùng chung;
- tham số runtime do từng QuerySpec khai báo sử dụng;
- chế độ correctness và benchmark.

Module này không chứa logic đánh giá truy vấn.
"""
from __future__ import annotations

import argparse
import math
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
    raw = value or os.getenv("SELECTED_QUERY_IDS") or os.getenv("QUERY_IDS") or os.getenv("QUERY_ID")
    if raw is None or not raw.strip():
        raise ValueError("Phải chỉ định ít nhất một query bằng --query-id, --query-ids hoặc QUERY_ID.")
    out = [q.strip() for q in raw.split(",") if q.strip()]
    if not out:
        raise ValueError("Danh sách query rỗng.")
    return out


@dataclass(frozen=True)
class Stage2Config:
    # Input/output.
    dataset_root: str
    by_partition_dir: str
    query_ids: List[str]
    output_root: Path
    method: str
    app_name: str

    # Spark/system.
    spark_master: str
    spark_shuffle_partitions: int
    graphframes_package: str
    driver_memory: str
    executor_memory: str
    executor_cores: int
    cores_max: int
    spark_log_level: str
    spark_scheduler_mode: str
    auto_broadcast_join_threshold: str
    spark_files_max_partition_bytes: str
    local_eval_submission_workers: int  # 0 => bằng số graph partition được phát hiện.

    # Temporal/query runtime constraints.
    max_time_gap_seconds: float
    quick_exit_max_seconds: float
    time_window_duration_seconds: float
    time_window_origin_policy: str
    time_window_first_index: int
    time_window_overlap_seconds: float
    candidate_pruning_max_time_window_gap: int
    continuity_execution_mode: str
    temporal_order_policy: str
    continuity_identity_policy: str

    # Query-specific runtime parameters, chỉ query có khai báo mới sử dụng.
    exit_person_role: str
    minimum_partition_count: int
    require_different_location: bool
    quick_exit_reference_role: str
    query_diagnostics_enabled: bool

    # Benchmark/correctness controls.
    benchmark_timing_only: bool
    benchmark_validate_result: bool
    benchmark_count_dataset_size: bool
    benchmark_count_intermediate: bool
    benchmark_force_final_action: bool
    benchmark_materialize_step_boundaries: bool

    # Persist/write controls.
    persist_intermediates: bool
    spark_indexing_enabled: bool
    save_runtime_summary: bool
    save_candidate_outputs: bool
    save_final_witnesses: bool
    save_intermediate_outputs: bool

    @property
    def enable_local_complete_detection(self) -> bool:
        return True

    @property
    def next_tw_require_consecutive(self) -> bool:
        return False

    @property
    def spark_available_task_slots(self) -> int:
        return max(1, int(self.cores_max))


def build_arg_parser(default_app_name: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="VideoGraphDB Vr15 runner")
    parser.add_argument("--dataset-root", default=None)
    parser.add_argument("--by-partition-dir", default=None)
    parser.add_argument("--query-id", default=None)
    parser.add_argument("--query-ids", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--spark-master", default=None)
    parser.add_argument("--shuffle-partitions", default=None, type=int)
    parser.add_argument("--app-name", default=None)

    parser.add_argument("--max-time-gap-seconds", default=None, type=float)
    # Alias đầu vào tạm thời để lệnh cũ không hỏng; không xuất dưới tên cũ.
    parser.add_argument("--epsilon-time-seconds", dest="deprecated_epsilon_time_seconds", default=None, type=float,
                        help=argparse.SUPPRESS)
    parser.add_argument("--quick-exit-max-seconds", default=None, type=float)
    parser.add_argument("--quick-exit-seconds", dest="deprecated_quick_exit_seconds", default=None, type=float,
                        help=argparse.SUPPRESS)
    parser.add_argument("--time-window-duration-seconds", default=None, type=float)

    parser.add_argument("--exit-person-role", choices=["P1", "P2"], default=None)
    parser.add_argument("--minimum-partition-count", "--min-partition-count", dest="minimum_partition_count", type=int, default=None)
    parser.add_argument("--require-different-location", action="store_true", default=None)
    parser.add_argument("--quick-exit-reference-role", choices=["INTERACT", "CARRY2"], default=None)
    parser.add_argument("--enable-query-diagnostics", action="store_true", default=None)
    return parser


def load_config(method: str, default_app_name: str) -> Stage2Config:
    args, unknown = build_arg_parser(default_app_name).parse_known_args()
    if unknown:
        print(f"[WARN] Bỏ qua đối số không nhận diện: {unknown}")

    dataset_root = (args.dataset_root or os.getenv("DATASET_ROOT", "hdfs:///data/videographdb/dataset_1m")).rstrip("/")
    by_partition_dir = (args.by_partition_dir or os.getenv("BY_PARTITION_DIR", f"{dataset_root}/by_partition")).rstrip("/")
    query_ids = parse_query_ids(args.query_ids or args.query_id)

    max_time_gap = args.max_time_gap_seconds
    if max_time_gap is None:
        max_time_gap = args.deprecated_epsilon_time_seconds
    if max_time_gap is None:
        max_time_gap = env_float("MAX_TIME_GAP_SECONDS", env_float("EPSILON_TIME_SECONDS", 120.0))
    if max_time_gap < 0:
        raise ValueError("MAX_TIME_GAP_SECONDS phải >= 0")

    quick_exit = args.quick_exit_max_seconds
    if quick_exit is None:
        quick_exit = args.deprecated_quick_exit_seconds
    if quick_exit is None:
        quick_exit = env_float("QUICK_EXIT_MAX_SECONDS", env_float("QUICK_EXIT_SECONDS", 120.0))
    if quick_exit < 0:
        raise ValueError("QUICK_EXIT_MAX_SECONDS phải >= 0")

    tw_duration = args.time_window_duration_seconds
    if tw_duration is None:
        tw_duration = env_float("TIME_WINDOW_DURATION_SECONDS", 10.0)
    if tw_duration <= 0:
        raise ValueError("TIME_WINDOW_DURATION_SECONDS phải > 0")

    pruning_gap = int(math.ceil(float(max_time_gap) / float(tw_duration)))

    output_root = Path(args.output_root or os.getenv("QUERY_OUTPUT_ROOT", "./query_outputs"))
    output_root.mkdir(parents=True, exist_ok=True)

    exit_role = (args.exit_person_role or os.getenv("EXIT_PERSON_ROLE", "P1")).strip().upper()
    if exit_role not in {"P1", "P2"}:
        exit_role = "P1"

    min_partition_count = args.minimum_partition_count
    if min_partition_count is None:
        min_partition_count = env_int("MINIMUM_PARTITION_COUNT", 1)
    min_partition_count = max(1, int(min_partition_count))

    require_different_location = (
        env_bool("REQUIRE_DIFFERENT_LOCATION", "false")
        if args.require_different_location is None
        else bool(args.require_different_location)
    )

    quick_exit_reference = (
        args.quick_exit_reference_role or os.getenv("QUICK_EXIT_REFERENCE_ROLE", "CARRY2")
    ).strip().upper()
    if quick_exit_reference not in {"INTERACT", "CARRY2"}:
        quick_exit_reference = "CARRY2"

    diagnostics = (
        env_bool("QUERY_DIAGNOSTICS_ENABLED", "false")
        if args.enable_query_diagnostics is None
        else bool(args.enable_query_diagnostics)
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
        executor_cores=env_int("SPARK_EXECUTOR_CORES", 1),
        cores_max=env_int("SPARK_CORES_MAX", 4),
        spark_log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        spark_scheduler_mode=os.getenv("SPARK_SCHEDULER_MODE", "FAIR").strip().upper(),
        auto_broadcast_join_threshold=os.getenv("SPARK_AUTO_BROADCAST_JOIN_THRESHOLD", "2097152"),
        spark_files_max_partition_bytes=os.getenv("SPARK_FILES_MAX_PARTITION_BYTES", "67108864"),
        local_eval_submission_workers=max(0, env_int("LOCAL_EVAL_SUBMISSION_WORKERS", 0)),
        max_time_gap_seconds=float(max_time_gap),
        quick_exit_max_seconds=float(quick_exit),
        time_window_duration_seconds=float(tw_duration),
        time_window_origin_policy=os.getenv("TIME_WINDOW_ORIGIN_POLICY", "DAILY_MIDNIGHT"),
        time_window_first_index=env_int("TIME_WINDOW_FIRST_INDEX", 1),
        time_window_overlap_seconds=env_float("TIME_WINDOW_OVERLAP_SECONDS", 0.0),
        candidate_pruning_max_time_window_gap=pruning_gap,
        continuity_execution_mode=os.getenv("CONTINUITY_EXECUTION_MODE", "BOUNDED_TEMPORAL_JOIN"),
        temporal_order_policy=os.getenv("TEMPORAL_ORDER_POLICY", "NON_NEGATIVE_GAP"),
        continuity_identity_policy=os.getenv("CONTINUITY_IDENTITY_POLICY", "SAME_GLOBAL_ENTITY_AND_TYPE"),
        exit_person_role=exit_role,
        minimum_partition_count=min_partition_count,
        require_different_location=require_different_location,
        quick_exit_reference_role=quick_exit_reference,
        query_diagnostics_enabled=diagnostics,
        benchmark_timing_only=env_bool("BENCHMARK_TIMING_ONLY", "true"),
        benchmark_validate_result=env_bool("BENCHMARK_VALIDATE_RESULT", "false"),
        benchmark_count_dataset_size=env_bool("BENCHMARK_COUNT_DATASET_SIZE", "false"),
        benchmark_count_intermediate=env_bool("BENCHMARK_COUNT_INTERMEDIATE", "false"),
        benchmark_force_final_action=env_bool("BENCHMARK_FORCE_FINAL_ACTION", "true"),
        benchmark_materialize_step_boundaries=env_bool("BENCHMARK_MATERIALIZE_STEP_BOUNDARIES", "false"),
        persist_intermediates=env_bool("STAGE2_PERSIST_INTERMEDIATES", "true"),
        spark_indexing_enabled=env_bool("STAGE2_SPARK_INDEXING_ENABLED", "true"),
        save_runtime_summary=env_bool("SAVE_RUNTIME_SUMMARY", "true"),
        save_candidate_outputs=env_bool("SAVE_CANDIDATE_OUTPUTS", "false"),
        save_final_witnesses=env_bool("SAVE_FINAL_WITNESSES", "false"),
        save_intermediate_outputs=env_bool("SAVE_INTERMEDIATE_OUTPUTS", "false"),
    )


def ensure_supported_query_ids(query_ids: Iterable[str], registry: dict) -> None:
    unsupported = [q for q in query_ids if q not in registry]
    if unsupported:
        raise ValueError(f"Query không hỗ trợ: {unsupported}. Query hợp lệ: {sorted(registry.keys())}")

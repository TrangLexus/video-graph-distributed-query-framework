#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Baseline Vr15 C2: Global Merge -> Query -> Hard Validation -> Normalize."""
from __future__ import annotations

import time

from pyspark.sql import functions as F

from config import ensure_supported_query_ids, load_config
from graph_io import (
    basename_uri,
    create_spark,
    load_partition_tables,
    materialize_df,
    persist_df,
    print_config,
    union_partition_graph,
    unpersist_all,
    validate_temporal_instance_context,
    write_runtime_summary,
    write_spark_csv,
)
from queries import QUERY_REGISTRY
from query_specs.common import (
    apply_query_time_scope,
    resolve_query_time_scope,
    validate_query_time_scope_config,
)

METHOD_NAME = "baseline_global_bounded_erpq"


def _query_parameter(spec, name: str, value):
    return value if name in spec.runtime_parameter_schema or name in spec.hard_constraint_names else None


def main() -> None:
    config = load_config(METHOD_NAME, "videographdb_vr15_baseline")
    ensure_supported_query_ids(config.query_ids, QUERY_REGISTRY)
    validate_query_time_scope_config(config)
    if len(config.query_ids) != 1:
        raise ValueError("Vr15 đo thời gian theo một query trên mỗi Spark application.")
    query_id = config.query_ids[0]
    spec = QUERY_REGISTRY[query_id]
    spec.validate_contract()
    query_time_scope, query_start_time, query_end_time = resolve_query_time_scope(config)

    spark = create_spark(config)
    print_config(config, spark)
    e2e_start = time.perf_counter()

    input_start = time.perf_counter()
    partition_tables = load_partition_tables(
        spark, config.dataset_root, config.by_partition_dir
    )
    time_input_discovery_plan = time.perf_counter() - input_start

    # Correctness-only dataset validation: metric riêng, ngoài algorithm timer.
    time_dataset_integrity_validation = 0.0
    if config.benchmark_validate_result:
        validation_start = time.perf_counter()
        validation_vertices, validation_edges = union_partition_graph(
            partition_tables
        )
        validate_temporal_instance_context(
            validation_vertices, validation_edges, enabled=True
        )
        time_dataset_integrity_validation = (
            time.perf_counter() - validation_start
        )

    algorithm_start = time.perf_counter()

    # Step 1: Global Merge + materialization. Không tạo NEXT_TW vật lý.
    step1_start = time.perf_counter()
    global_vertices, global_base_edges = union_partition_graph(partition_tables)
    global_base_edges = apply_query_time_scope(global_base_edges, config)
    global_vertices = persist_df(
        global_vertices.dropDuplicates(["id"]),
        config,
        "baseline.global_vertices",
    )
    global_base_edges = persist_df(
        global_base_edges.dropDuplicates(),
        config,
        "baseline.global_base_edges",
    )
    num_global_vertices, _ = materialize_df(
        global_vertices,
        "baseline.global_vertices",
        enabled=True,
        required=True,
    )
    num_global_base_edges, _ = materialize_df(
        global_base_edges,
        "baseline.global_base_edges",
        enabled=True,
        required=True,
    )
    time_step1_global_merge = time.perf_counter() - step1_start

    # Step 2: candidate generation -> Hard Validation -> Normalization -> final action.
    step2_start = time.perf_counter()
    candidates_raw, _unused_evaluator_witnesses = spec.baseline_evaluator(
        vertices=global_vertices,
        base_edges=global_base_edges,
        next_tw_edges=None,
        config=config,
        query_id=query_id,
    )
    candidates = persist_df(
        candidates_raw.dropDuplicates(
            list(spec.resolve_dedup_columns("candidate", candidates_raw.columns))
        ),
        config,
        f"baseline.{query_id}.candidates",
    )
    valid_before_normalization_raw, final_witnesses_raw = (
        spec.finalize_merged_candidates(candidates=candidates)
    )
    valid_before_normalization = persist_df(
        valid_before_normalization_raw,
        config,
        f"baseline.{query_id}.valid_before_normalization",
    )
    final_valid_witnesses = persist_df(
        final_witnesses_raw,
        config,
        f"baseline.{query_id}.final_valid_witnesses",
    )

    # Một action duy nhất materialize toàn bộ Step 2 và lấy metric bắt buộc.
    stats = (
        candidates.agg(F.count(F.lit(1)).alias("n_candidates"))
        .crossJoin(
            valid_before_normalization.agg(
                F.count(F.lit(1)).alias("n_valid_before_norm")
            )
        )
        .crossJoin(
            final_valid_witnesses.agg(
                F.count(F.lit(1)).alias("n_final")
            )
        )
        .collect()[0]
    )
    num_candidates = int(stats["n_candidates"] or 0)
    num_valid_witnesses_before_normalization = int(
        stats["n_valid_before_norm"] or 0
    )
    num_final_valid_witnesses = int(stats["n_final"] or 0)
    time_step2_global_query_validation = time.perf_counter() - step2_start

    time_algorithm_total = time.perf_counter() - algorithm_start
    time_algorithm_phase_gap = (
        time_algorithm_total
        - time_step1_global_merge
        - time_step2_global_query_validation
    )
    time_end_to_end_total = time.perf_counter() - e2e_start
    # Theo đặc tả: validation correctness nằm trong non-algorithm overhead.
    time_non_algorithm_overhead = (
        time_end_to_end_total
        - time_input_discovery_plan
        - time_algorithm_total
    )
    if time_non_algorithm_overhead < -0.05:
        raise RuntimeError(
            f"TIMING_SCOPE_ERROR baseline overhead={time_non_algorithm_overhead:.6f}s"
        )

    row = {
        "code_version": "Vr15-spec-v4",
        "method": METHOD_NAME,
        "method_family": "baseline_global_first_bounded_erpq",
        "dataset_name": basename_uri(config.dataset_root),
        "dataset_root": config.dataset_root,
        "query_id": query_id,
        "query_group": spec.query_group,
        "query_name": spec.query_name,
        "logical_query_type": spec.logical_query_type,
        "logical_query_description": spec.logical_query_description,
        "rpq_expression": spec.rpq_expression,
        "baseline_physical_plan": spec.baseline_physical_plan,
        "witness_type": spec.witness_type,
        "evaluation_strategy": (
            "Global Merge/Union -> Global Query -> Hard Validation -> "
            "Witness Normalization"
        ),
        "execution_parallelism_mode": (
            "SPARK_TASK_PARALLELISM_OVER_SINGLE_GLOBAL_QUERY_PLAN"
        ),
        "algorithm_runtime_aggregation": "OBSERVED_GLOBAL_PLAN_WALL_TIME",
        "spark_master": spark.sparkContext.master,
        "spark_app_id": spark.sparkContext.applicationId,
        "spark_executor_memory": config.executor_memory,
        "spark_executor_cores": config.executor_cores,
        "spark_cores_max": config.cores_max,
        "spark_available_task_slots": config.spark_available_task_slots,
        "spark_shuffle_partitions": config.spark_shuffle_partitions,
        "spark_scheduler_mode": config.spark_scheduler_mode,
        "num_source_graph_partitions": len(partition_tables),
        "num_global_vertices": num_global_vertices,
        "num_global_base_edges": num_global_base_edges,
        "num_physical_next_tw_edges": 0,
        "num_candidates": num_candidates,
        "num_merged_candidates": num_candidates,
        "num_valid_witnesses_before_normalization": (
            num_valid_witnesses_before_normalization
        ),
        "num_final_valid_witnesses": num_final_valid_witnesses,
        "max_time_gap_seconds": config.max_time_gap_seconds,
        "candidate_pruning_max_time_window_gap": (
            config.candidate_pruning_max_time_window_gap
        ),
        "time_window_duration_seconds": config.time_window_duration_seconds,
        "query_time_scope": query_time_scope,
        "query_start_time": query_start_time,
        "query_end_time": query_end_time,
        "quick_exit_max_seconds": _query_parameter(
            spec, "quick_exit_max_seconds", config.quick_exit_max_seconds
        ),
        "exit_person_role": _query_parameter(
            spec, "exit_person_role", config.exit_person_role
        ),
        "minimum_partition_count": _query_parameter(
            spec, "minimum_partition_count", config.minimum_partition_count
        ),
        "require_different_location": _query_parameter(
            spec, "require_different_location", config.require_different_location
        ),
        "quick_exit_reference_role": _query_parameter(
            spec, "quick_exit_reference_role", config.quick_exit_reference_role
        ),
        "next_tw_execution_mode": (
            "LOGICAL_BOUNDED_TEMPORAL_PREDICATE_NO_PHYSICAL_EDGE_MATERIALIZATION"
        ),
        "final_witness_construction_mode": (
            "MERGED_CANDIDATES_HARD_VALIDATE_THEN_NORMALIZE"
        ),
        "hard_validation_mode": (
            "QUERY_SPECIFIC_IS_VALID_PREDICATE_REAPPLIED_AFTER_CANDIDATE_MERGE"
        ),
        "time_input_discovery_plan_seconds": time_input_discovery_plan,
        "time_dataset_integrity_validation_seconds": (
            time_dataset_integrity_validation
        ),
        "time_step1_global_merge_seconds": time_step1_global_merge,
        "time_step2_global_query_validation_seconds": (
            time_step2_global_query_validation
        ),
        "time_algorithm_total_seconds": time_algorithm_total,
        "time_algorithm_phase_gap_seconds": time_algorithm_phase_gap,
        "time_end_to_end_total_seconds": time_end_to_end_total,
        "time_non_algorithm_overhead_seconds": time_non_algorithm_overhead,
        "end_to_end_scope": (
            "input_discovery_start_to_final_valid_witness_action_end"
        ),
    }

    print("=" * 96)
    for key, value in row.items():
        print(f"{key:62s}: {value}")
    print("=" * 96)

    out_dir = config.output_root / METHOD_NAME / query_id
    out_dir.mkdir(parents=True, exist_ok=True)
    write_spark_csv(
        candidates,
        out_dir,
        "candidates_csv",
        config.save_candidate_outputs,
    )
    write_spark_csv(
        final_valid_witnesses,
        out_dir,
        "witnesses_csv",
        config.save_final_witnesses and num_final_valid_witnesses > 0,
    )
    write_runtime_summary(
        [row],
        config.output_root / METHOD_NAME,
        enabled=config.save_runtime_summary,
    )

    unpersist_all(
        candidates,
        valid_before_normalization,
        final_valid_witnesses,
        global_vertices,
        global_base_edges,
    )
    spark.stop()


if __name__ == "__main__":
    main()

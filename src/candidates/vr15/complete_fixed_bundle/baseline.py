#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Baseline Vr15: Global Merge/Union -> Global Query -> Hard Validation."""
from __future__ import annotations

import time

from config import ensure_supported_query_ids, load_config
from graph_io import (
    basename_uri,
    create_spark,
    load_partition_tables,
    materialize_df,
    persist_df,
    print_config,
    safe_count,
    union_partition_graph,
    unpersist_all,
    validate_temporal_instance_context,
    write_runtime_summary,
    write_spark_csv,
)
from queries import QUERY_REGISTRY

METHOD_NAME = "baseline_global_bounded_erpq"


def main() -> None:
    config = load_config(METHOD_NAME, "videographdb_vr15_baseline")
    ensure_supported_query_ids(config.query_ids, QUERY_REGISTRY)
    if len(config.query_ids) != 1:
        raise ValueError("Vr15 đo thời gian theo một query trên mỗi Spark application.")
    query_id = config.query_ids[0]
    spec = QUERY_REGISTRY[query_id]

    e2e_start = time.perf_counter()
    spark = create_spark(config)
    print_config(config, spark)

    t_input = time.perf_counter()
    partition_tables = load_partition_tables(spark, config.dataset_root, config.by_partition_dir)
    time_input_discovery_plan = time.perf_counter() - t_input

    # Dataset integrity validation là kiểm tra correctness, không phải một
    # phase của thuật toán Baseline. Trong correctness, tạo logical union plan,
    # chạy validation và báo cáo thời gian riêng. Trong benchmark, validation
    # bị tắt và union plan vẫn được tạo bên trong timer thuật toán.
    time_dataset_integrity_validation = 0.0
    if config.benchmark_validate_result:
        # Dùng logical plan riêng cho kiểm tra dữ liệu. Không tái sử dụng plan
        # validation trong thuật toán để tránh đưa công việc chuẩn bị Step 1 ra
        # ngoài timer hoặc làm sai phạm vi đo.
        validation_start = time.perf_counter()
        validation_vertices, validation_edges = union_partition_graph(
            partition_tables
        )
        validate_temporal_instance_context(
            validation_vertices,
            validation_edges,
            enabled=True,
        )
        time_dataset_integrity_validation = (
            time.perf_counter() - validation_start
        )

    algorithm_start = time.perf_counter()

    # Step 1: tạo và materialize global logical graph. Không tạo cạnh NEXT_TW vật lý.
    step1_start = time.perf_counter()
    global_vertices, global_base_edges = union_partition_graph(
        partition_tables
    )

    global_vertices = persist_df(global_vertices.dropDuplicates(["id"]), config, "baseline.global_vertices")
    global_base_edges = persist_df(global_base_edges.dropDuplicates(), config, "baseline.global_base_edges")

    num_global_vertices, _ = materialize_df(
        global_vertices, "baseline.global_vertices", enabled=True, required=True
    )
    num_base_global_edges, _ = materialize_df(
        global_base_edges, "baseline.global_base_edges", enabled=True, required=True
    )
    time_step1_global_merge = time.perf_counter() - step1_start

    # Step 2: query toàn cục, hard validation, normalization và final action.
    step2_start = time.perf_counter()
    candidates, witnesses = spec.baseline_evaluator(
        vertices=global_vertices,
        base_edges=global_base_edges,
        next_tw_edges=None,
        config=config,
        query_id=query_id,
    )
    candidates = persist_df(candidates, config, f"baseline.{query_id}.candidates")
    witnesses = persist_df(witnesses, config, f"baseline.{query_id}.witnesses")

    num_candidates = (
        safe_count(candidates, f"baseline.{query_id}.num_candidates", required=True)
        if config.benchmark_count_intermediate
        else -1
    )
    num_final_valid_witnesses = (
        safe_count(witnesses, f"baseline.{query_id}.num_final_valid_witnesses", required=True)
        if config.benchmark_force_final_action
        else -1
    )
    time_step2_global_query_validation = time.perf_counter() - step2_start

    time_algorithm_total = time.perf_counter() - algorithm_start
    time_algorithm_phase_gap = (
        time_algorithm_total
        - time_step1_global_merge
        - time_step2_global_query_validation
    )
    time_end_to_end_total = time.perf_counter() - e2e_start
    time_non_algorithm_overhead = (
        time_end_to_end_total
        - time_input_discovery_plan
        - time_dataset_integrity_validation
        - time_algorithm_total
    )
    if time_non_algorithm_overhead < -0.05:
        raise RuntimeError(
            f"TIMING_SCOPE_ERROR baseline overhead={time_non_algorithm_overhead:.6f}s"
        )

    row = {
        "code_version": "Vr15",
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
        "evaluation_strategy": "Global Merge/Union -> Global Query -> Hard Validation -> Witness Normalization",
        "execution_parallelism_mode": "SPARK_TASK_PARALLELISM_OVER_SINGLE_GLOBAL_QUERY_PLAN",
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
        "num_global_base_edges": num_base_global_edges,
        "num_physical_next_tw_edges": 0,
        "num_candidates": num_candidates,
        "num_merged_candidates": num_candidates,
        "num_final_valid_witnesses": num_final_valid_witnesses,
        "num_valid_witnesses": num_final_valid_witnesses,
        "max_time_gap_seconds": config.max_time_gap_seconds,
        "candidate_pruning_max_time_window_gap": config.candidate_pruning_max_time_window_gap,
        "time_window_duration_seconds": config.time_window_duration_seconds,
        "quick_exit_max_seconds": config.quick_exit_max_seconds,
        "exit_person_role": config.exit_person_role,
        "minimum_partition_count": config.minimum_partition_count,
        "require_different_location": config.require_different_location,
        "quick_exit_reference_role": config.quick_exit_reference_role,
        "next_tw_execution_mode": "LOGICAL_BOUNDED_TEMPORAL_PREDICATE_NO_PHYSICAL_EDGE_MATERIALIZATION",
        "time_input_discovery_plan_seconds": time_input_discovery_plan,
        "time_dataset_integrity_validation_seconds": time_dataset_integrity_validation,
        "time_step1_global_merge_seconds": time_step1_global_merge,
        "time_step2_global_query_validation_seconds": time_step2_global_query_validation,
        "time_algorithm_total_seconds": time_algorithm_total,
        "time_algorithm_phase_gap_seconds": time_algorithm_phase_gap,
        "time_end_to_end_total_seconds": time_end_to_end_total,
        "time_non_algorithm_overhead_seconds": time_non_algorithm_overhead,
        "end_to_end_scope": "application_start_before_spark_session_to_final_valid_witness_action_end",
    }

    print("=" * 96)
    for key, value in row.items():
        print(f"{key:55s}: {value}")
    print("=" * 96)

    out_dir = config.output_root / METHOD_NAME / query_id
    out_dir.mkdir(parents=True, exist_ok=True)
    write_spark_csv(candidates, out_dir, "candidates_csv", config.save_candidate_outputs)
    write_spark_csv(
        witnesses,
        out_dir,
        "witnesses_csv",
        config.save_final_witnesses and num_final_valid_witnesses > 0,
    )
    write_runtime_summary([row], config.output_root / METHOD_NAME, enabled=config.save_runtime_summary)

    unpersist_all(candidates, witnesses, global_vertices, global_base_edges)
    spark.stop()


if __name__ == "__main__":
    main()

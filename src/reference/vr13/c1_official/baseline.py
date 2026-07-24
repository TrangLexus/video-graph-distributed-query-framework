#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Baseline algorithm: global graph stitching first, then query.

Algorithm name:
    baseline_global_bounded_erpq

Evaluation strategy:
    HDFS by_partition -> Union/Merge global graph -> materialize global NEXT_TW_*
    -> run the same logical query spec as Proposed -> Hard Validation/Witness normalization.
"""
from __future__ import annotations

import time

from config import ensure_supported_query_ids, load_config
from graph_io import (
    basename_uri,
    build_next_tw_edges,
    create_spark,
    load_partition_tables,
    materialize_df,
    persist_df,
    print_config,
    safe_count,
    union_partition_graph,
    unpersist_all,
    write_runtime_summary,
    write_spark_csv,
)
from queries import QUERY_REGISTRY


METHOD_NAME = "baseline_global_bounded_erpq"


def main() -> None:
    config = load_config(METHOD_NAME, "videographdb_baseline_global_bounded_erpq")
    ensure_supported_query_ids(config.query_ids, QUERY_REGISTRY)

    spark = create_spark(config)
    print_config(config, spark)
    summary_rows: list[dict] = []
    t_total = time.perf_counter()

    t_load = time.perf_counter()
    partition_tables = load_partition_tables(spark, config.dataset_root, config.by_partition_dir)
    time_load_data = time.perf_counter() - t_load

    # Step 1: global merge/stitching. This is intentionally part of baseline cost.
    t_step1 = time.perf_counter()
    global_vertices, base_global_edges = union_partition_graph(partition_tables)

    global_vertices = persist_df(
        global_vertices.dropDuplicates(["id"]),
        config,
        "baseline.global_vertices",
        ["label", "global_id", "id"],
    )
    base_global_edges = persist_df(
        base_global_edges.dropDuplicates(),
        config,
        "baseline.base_global_edges",
        ["label", "src", "dst", "tw_id"],
    )
    global_nexttw_edges = persist_df(
        build_next_tw_edges(global_vertices, require_consecutive=config.next_tw_require_consecutive),
        config,
        "baseline.global_nexttw_edges",
        ["label", "src", "dst", "tw_id"],
    )

    global_graph_edges_for_step1 = persist_df(
        base_global_edges.unionByName(global_nexttw_edges, allowMissingColumns=True),
        config,
        "baseline.global_graph_edges_base_plus_NEXT_TW",
        ["label", "src", "dst", "tw_id"],
    )
    num_global_graph_edges, _ = materialize_df(
        global_graph_edges_for_step1,
        "baseline.step1.global_graph_edges_base_plus_NEXT_TW",
        enabled=config.benchmark_materialize_step_boundaries,
        required=True,
    )

    if config.benchmark_count_dataset_size:
        num_global_vertices = safe_count(global_vertices, "baseline.num_global_vertices")
        num_base_global_edges = safe_count(base_global_edges, "baseline.num_base_global_edges")
    else:
        num_global_vertices = -1
        num_base_global_edges = -1

    if config.benchmark_count_intermediate:
        num_global_nexttw_edges = safe_count(global_nexttw_edges, "baseline.num_global_nexttw_edges")
    else:
        num_global_nexttw_edges = -1

    time_step1_graph_stitching = time.perf_counter() - t_step1
    time_step1_global_merge_nexttw = time_step1_graph_stitching

    for query_id in config.query_ids:
        spec = QUERY_REGISTRY[query_id]
        t_step2 = time.perf_counter()

        candidates, witnesses = spec.baseline_evaluator(
            vertices=global_vertices,
            base_edges=base_global_edges,
            next_tw_edges=global_nexttw_edges,
            config=config,
            query_id=query_id,
        )
        candidates = persist_df(
            candidates,
            config,
            f"baseline.{query_id}.candidates",
            ["person_id", "other_person_id", "thing_id", "vehicle_id"],
        )
        witnesses = persist_df(
            witnesses,
            config,
            f"baseline.{query_id}.witnesses",
            ["person_id", "other_person_id", "thing_id", "vehicle_id"],
        )

        if config.benchmark_count_intermediate:
            num_candidates = safe_count(candidates, f"baseline.{query_id}.num_candidates")
        else:
            num_candidates = -1
            print(f"[Skip count] baseline.{query_id}.num_candidates")

        if config.benchmark_force_final_action:
            num_valid_witnesses = safe_count(witnesses, f"baseline.{query_id}.num_valid_witnesses", required=True)
        else:
            num_valid_witnesses = -1
            print(f"[Skip count] baseline.{query_id}.num_valid_witnesses")

        time_step2_query_validation = time.perf_counter() - t_step2
        time_step2_bounded_erpq_validation = time_step2_query_validation
        time_query_algorithm_wall = time_step1_global_merge_nexttw + time_step2_bounded_erpq_validation
        time_algorithm_total = time_query_algorithm_wall
        time_end_to_end_total = time.perf_counter() - t_total
        time_unattributed_overhead = time_end_to_end_total - time_load_data - time_query_algorithm_wall

        row = {
            "method": METHOD_NAME,
            "dataset_name": basename_uri(config.dataset_root),
            "dataset_root": config.dataset_root,
            "method_family": "baseline_global_first_bounded_erpq",
            "spark_master": spark.sparkContext.master,
            "spark_app_id": spark.sparkContext.applicationId,
            "spark_executor_memory": config.executor_memory,
            "spark_executor_cores": config.executor_cores,
            "spark_cores_max": config.cores_max,
            "spark_shuffle_partitions": config.spark_shuffle_partitions,
            "evaluation_strategy": "Baseline: Merge/Union -> Query -> Hard Validation",
            "execution_strategy": "global_union_then_global_NEXT_TW_then_query_spec_evaluator",
            "query_id": query_id,
            "query_group": spec.query_group,
            "query_name": spec.query_name,
            "logical_query_type": spec.logical_query_type,
            "physical_plan": spec.physical_plan,
            "rpq_expression": spec.rpq_expression,
            "witness_type": spec.witness_type,
            "num_partitions": len(partition_tables),
            "num_global_vertices": num_global_vertices,
            "num_base_global_edges": num_base_global_edges,
            "num_global_nexttw_edges": num_global_nexttw_edges,
            "num_global_graph_edges": num_global_graph_edges,
            "num_candidates": num_candidates,
            "num_valid_witnesses": num_valid_witnesses,
            "epsilon_time_seconds": config.epsilon_time_seconds,
            "quick_exit_seconds": config.quick_exit_seconds,
            "max_nexttw_hops": config.max_nexttw_hops,
            "q14_exit_person_role": config.q14_exit_person_role,
            "q14_min_partition_count": config.q14_min_partition_count,
            "q14_require_different_location": config.q14_require_different_location,
            "q14_quick_exit_reference": config.q14_quick_exit_reference,
            "q14_diagnostics_enabled": config.q14_diagnostics_enabled,
            "time_load_data_seconds": time_load_data,
            "time_step1_graph_stitching_seconds": time_step1_graph_stitching,
            "time_step1_global_merge_nexttw_seconds": time_step1_global_merge_nexttw,
            "time_step2_query_validation_seconds": time_step2_query_validation,
            "time_step2_bounded_erpq_validation_seconds": time_step2_bounded_erpq_validation,
            "time_algorithm_total_seconds": time_algorithm_total,
            "time_query_algorithm_wall_seconds": time_query_algorithm_wall,
            "time_query_algorithm_parallel_estimate_seconds": time_query_algorithm_wall,
            "time_end_to_end_total_seconds": time_end_to_end_total,
            "time_unattributed_overhead_seconds": time_unattributed_overhead,
            "step_timing_note": (
                "Step boundary materialized."
                if config.benchmark_materialize_step_boundaries
                else "Official lazy timing: step boundary materialization skipped; end-to-end total is the primary runtime."
            ),
        }
        summary_rows.append(row)

        print("=" * 96)
        for k, v in row.items():
            print(f"{k:45s}: {v}")
        print("=" * 96)

        out_dir = config.output_root / METHOD_NAME / query_id
        out_dir.mkdir(parents=True, exist_ok=True)
        write_spark_csv(candidates, out_dir, "candidates_csv", config.save_candidate_outputs)
        write_spark_csv(witnesses, out_dir, "witnesses_csv", config.save_final_witnesses and num_valid_witnesses > 0)
        unpersist_all(candidates, witnesses)

    write_runtime_summary(summary_rows, config.output_root / METHOD_NAME, enabled=config.save_runtime_summary)
    unpersist_all(global_vertices, base_global_edges, global_nexttw_edges, global_graph_edges_for_step1)
    spark.stop()


if __name__ == "__main__":
    main()

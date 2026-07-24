#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Proposed distributed algorithm: LocalEval -> Stitching/GlobalEval -> Validation.

Algorithm name:
    proposed_distributed_boundary_erpq

Evaluation strategy:
    HDFS by_partition -> per-partition LocalEval fragments -> merge/stitch only
    query-relevant fragments -> run the same logical query spec as Baseline
    -> Hard Validation/Witness normalization.
"""
from __future__ import annotations

import time
from dataclasses import replace

from pyspark.sql import functions as F

from config import ensure_supported_query_ids, load_config
from graph_io import (
    basename_uri,
    create_spark,
    get_spark_session,
    load_partition_tables,
    normalize_base_edges,
    normalize_vertices,
    persist_df,
    print_config,
    safe_count,
    union_dataframes,
    union_partition_graph,
    unpersist_all,
    write_runtime_summary,
    write_spark_csv,
)
from queries import QUERY_REGISTRY, empty_candidates_like, empty_witnesses_like


METHOD_NAME = "proposed_distributed_boundary_erpq"


def main() -> None:
    config = load_config(METHOD_NAME, "videographdb_proposed_distributed_boundary_erpq")
    ensure_supported_query_ids(config.query_ids, QUERY_REGISTRY)

    spark = create_spark(config)
    print_config(config, spark)
    summary_rows: list[dict] = []
    t_total = time.perf_counter()

    t_load = time.perf_counter()
    partition_tables = load_partition_tables(spark, config.dataset_root, config.by_partition_dir)
    time_load_data = time.perf_counter() - t_load

    # Global lookup only. Proposed must not use this to pre-materialize all NEXT_TW
    # before LocalEval. This is measured separately because it is part of
    # prototype implementation overhead / lookup preparation, not LocalEval.
    t_global_lookup = time.perf_counter()
    global_vertices, global_base_edges = union_partition_graph(partition_tables)
    global_vertices = persist_df(
        global_vertices.dropDuplicates(["id"]),
        config,
        "proposed.global_vertices_lookup",
        ["label", "global_id", "id"],
    )
    global_base_edges = persist_df(
        global_base_edges.dropDuplicates(),
        config,
        "proposed.global_base_edges_lookup",
        ["label", "src", "dst", "tw_id"],
    )

    time_global_lookup_seconds = time.perf_counter() - t_global_lookup

    if config.benchmark_count_dataset_size:
        num_global_vertices = safe_count(global_vertices, "proposed.num_global_vertices")
        num_base_global_edges = safe_count(global_base_edges, "proposed.num_base_global_edges")
    else:
        num_global_vertices = -1
        num_base_global_edges = -1

    for query_id in config.query_ids:
        spec = QUERY_REGISTRY[query_id]

        # Step 1: LocalEval on each partition using only base edges.
        # LocalEval now explicitly produces two outputs:
        #   (1) local complete witnesses/candidates: the query is already complete inside one partition;
        #   (2) boundary fragments: high-recall query-relevant evidence that may need GlobalEval/Stitching.
        # Boundary fragments are the main input of the proposed GlobalEval stage.
        boundary_event_dfs = []
        local_complete_candidate_dfs = []
        local_complete_witness_dfs = []
        partition_query_timings = []

        # Local-complete detection must not require cross-partition validity. It checks whether the
        # whole Q14 pattern can already be assembled inside a single partition. Diagnostics are
        # disabled inside per-partition local-complete checks to avoid excessive actions.
        local_complete_config = replace(
            config,
            q14_min_partition_count=1,
            q14_diagnostics_enabled=False,
        )

        t_step1_wall = time.perf_counter()

        for tables in partition_tables:
            pname = tables.get("partition_name", "partition")
            if "rels" not in tables:
                continue

            v_i = normalize_vertices(tables)
            e_i = normalize_base_edges(tables["rels"])

            t_partition_eval = time.perf_counter()

            # 1) High-recall LocalEval evidence: keep any fragment containing at least one query relation.
            # These are boundary fragments for GlobalEval. Do not require a full Q14 chain here.
            boundary_i = persist_df(
                spec.local_event_extractor(v_i, e_i, config, query_id)
                    .withColumn("source_partition_name", F.lit(pname))
                    .withColumn("fragment_type", F.lit("BOUNDARY_FRAGMENT"))
                    .withColumn("boundary_fragment", F.lit(True))
                    .withColumn("local_complete_fragment", F.lit(False)),
                config,
                f"proposed.{query_id}.{pname}.boundary_fragments",
                ["fragment_role", "relation_type", "person_id", "other_person_id", "thing_id", "vehicle_id", "tw_id"],
            )
            if config.benchmark_count_intermediate:
                num_boundary_i = safe_count(
                    boundary_i,
                    f"proposed.{query_id}.{pname}.num_boundary_fragments",
                    required=True,
                )
            else:
                num_boundary_i = -1
                print(f"[Skip count] proposed.{query_id}.{pname}.num_boundary_fragments")

            # 2) Optional Local-complete fragments. In official timing mode, this can be
            # disabled because GlobalEval over boundary_events can still assemble local and
            # cross-partition witnesses from the same query-relevant evidence. Enable it in
            # profiling/correctness diagnostics when local-complete fragment counts are needed.
            if config.enable_local_complete_detection:
                # Optimized execution: NEXT_TW_* remains a logical continuity
                # relation in the query model, but Q14 continuity is evaluated
                # by bounded joins over same entity id and tw_num distance.
                # Do not physically materialize selected NEXT_TW_* in the
                # official/profiling execution path.
                local_candidates_i, local_witnesses_i = spec.distributed_evaluator(
                    vertices=v_i,
                    atomic_events=boundary_i,
                    next_tw_edges=None,
                    config=local_complete_config,
                    query_id=query_id,
                )
                local_candidates_i = persist_df(
                    local_candidates_i.withColumn("source_partition_name", F.lit(pname)).withColumn("fragment_type", F.lit("LOCAL_COMPLETE_FRAGMENT")),
                    config,
                    f"proposed.{query_id}.{pname}.local_complete_candidates",
                    ["person_id", "other_person_id", "thing_id", "vehicle_id"],
                )
                local_witnesses_i = persist_df(
                    local_witnesses_i.withColumn("source_partition_name", F.lit(pname)).withColumn("fragment_type", F.lit("LOCAL_COMPLETE_FRAGMENT")),
                    config,
                    f"proposed.{query_id}.{pname}.local_complete_witnesses",
                    ["person_id", "other_person_id", "thing_id", "vehicle_id"],
                )
                if config.benchmark_count_intermediate:
                    num_local_complete_i = safe_count(
                        local_witnesses_i,
                        f"proposed.{query_id}.{pname}.num_local_complete_fragments",
                        required=True,
                    )
                else:
                    num_local_complete_i = -1
                    print(f"[Skip count] proposed.{query_id}.{pname}.num_local_complete_fragments")
            else:
                spark_for_empty_i = get_spark_session(boundary_i)
                local_candidates_i = empty_candidates_like(spark_for_empty_i).withColumn("source_partition_name", F.lit(pname)).withColumn("fragment_type", F.lit("LOCAL_COMPLETE_FRAGMENT"))
                local_witnesses_i = empty_witnesses_like(spark_for_empty_i).withColumn("source_partition_name", F.lit(pname)).withColumn("fragment_type", F.lit("LOCAL_COMPLETE_FRAGMENT"))
                num_local_complete_i = 0

            partition_eval_seconds = time.perf_counter() - t_partition_eval
            partition_query_timings.append({
                "partition_name": pname,
                "num_boundary_fragments": num_boundary_i,
                "num_local_complete_fragments": num_local_complete_i,
                "time_evaluation_seconds": partition_eval_seconds,
            })
            boundary_event_dfs.append(boundary_i)
            local_complete_candidate_dfs.append(local_candidates_i)
            local_complete_witness_dfs.append(local_witnesses_i)

        if not boundary_event_dfs:
            raise RuntimeError(f"No boundary fragments generated for query {query_id}. Check schema/labels.")

        time_step1_localeval_boundary_wall_seconds = time.perf_counter() - t_step1_wall
        time_step1_localeval_boundary_parallel_estimate_seconds = max(
            (row["time_evaluation_seconds"] for row in partition_query_timings),
            default=0.0,
        )
        time_step1_localeval_boundary_sum_partition_seconds = sum(
            row["time_evaluation_seconds"] for row in partition_query_timings
        )
        # Backward-compatible name: this is the parallel-stage estimate, not wall-clock.
        time_step1_localeval_boundary = time_step1_localeval_boundary_parallel_estimate_seconds

        # Step 2: merge/stitch query-relevant boundary fragments and run the query-specific GlobalEval.
        t_step2 = time.perf_counter()

        boundary_events = persist_df(
            union_dataframes(boundary_event_dfs).dropDuplicates(),
            config,
            f"proposed.{query_id}.merged_boundary_fragments",
            ["fragment_role", "relation_type", "person_id", "other_person_id", "thing_id", "vehicle_id", "tw_id"],
        )
        if config.benchmark_count_intermediate:
            num_boundary_fragments = safe_count(
                boundary_events,
                f"proposed.{query_id}.num_merged_boundary_fragments",
                required=True,
            )
        else:
            num_boundary_fragments = -1
            print(f"[Skip count] proposed.{query_id}.num_merged_boundary_fragments")

        local_complete_candidates_all = persist_df(
            union_dataframes(local_complete_candidate_dfs).dropDuplicates(),
            config,
            f"proposed.{query_id}.merged_local_complete_candidates",
            ["person_id", "other_person_id", "thing_id", "vehicle_id"],
        )
        local_complete_witnesses_all = persist_df(
            union_dataframes(local_complete_witness_dfs).dropDuplicates(),
            config,
            f"proposed.{query_id}.merged_local_complete_witnesses",
            ["person_id", "other_person_id", "thing_id", "vehicle_id"],
        )
        if config.benchmark_count_intermediate:
            num_local_complete_fragments = safe_count(
                local_complete_witnesses_all,
                f"proposed.{query_id}.num_merged_local_complete_fragments",
                required=True,
            )
        else:
            num_local_complete_fragments = -1 if config.enable_local_complete_detection else 0
            print(f"[Skip count] proposed.{query_id}.num_merged_local_complete_fragments")
        num_localeval_fragments_total = (
            num_boundary_fragments + num_local_complete_fragments
            if num_boundary_fragments >= 0 and num_local_complete_fragments >= 0
            else -1
        )

        num_candidate_upper_bound = -1
        if spec.candidate_upper_bound is not None:
            candidate_upper_bound = persist_df(
                spec.candidate_upper_bound(boundary_events, config, query_id),
                config,
                f"proposed.{query_id}.candidate_upper_bound_without_continuity",
                ["person_id", "other_person_id", "thing_id", "vehicle_id"],
            )
            if config.benchmark_count_intermediate:
                num_candidate_upper_bound = safe_count(
                    candidate_upper_bound,
                    f"proposed.{query_id}.num_candidate_upper_bound_without_continuity",
                    required=True,
                )
            else:
                num_candidate_upper_bound = -1
                print(f"[Skip count] proposed.{query_id}.num_candidate_upper_bound_without_continuity")
        else:
            candidate_upper_bound = None

        spark_for_empty = get_spark_session(boundary_events)
        candidates = empty_candidates_like(spark_for_empty)
        witnesses = empty_witnesses_like(spark_for_empty)
        num_candidates = -1
        num_valid_witnesses = -1

        # Optimized execution: keep NEXT_TW_* as a logical continuity relation
        # in the ERPQ/RPQ expression, but do not physically materialize selected
        # NEXT_TW_* during query execution. The executable continuity predicate
        # is implemented inside queries.py as bounded joins:
        #   same entity id AND 0 <= tw_num_delta <= max_nexttw_hops
        # over query-relevant fragments only.
        selected_nexttw_edges = None
        time_selected_nexttw_seconds = 0.0

        # Do NOT shortcut when the optional pre-stitch evidence summary is empty.
        # The proposed algorithm is a two-step boundary-fragment method:
        #   LocalEval  : keep any fragment containing at least one query relation.
        #   GlobalEval : assemble/sort/bind fragments and then enforce hard constraints.
        # Therefore, only GlobalEval/Hard Validation is allowed to decide witnesses.
        if config.benchmark_force_final_action:
            candidates, witnesses = spec.distributed_evaluator(
                vertices=global_vertices,
                atomic_events=boundary_events,
                next_tw_edges=None,
                config=config,
                query_id=query_id,
            )
            candidates = persist_df(
                candidates,
                config,
                f"proposed.{query_id}.candidates",
                ["person_id", "other_person_id", "thing_id", "vehicle_id"],
            )
            witnesses = persist_df(
                witnesses,
                config,
                f"proposed.{query_id}.witnesses",
                ["person_id", "other_person_id", "thing_id", "vehicle_id"],
            )
            if config.benchmark_count_intermediate:
                num_candidates = safe_count(candidates, f"proposed.{query_id}.num_candidates")
            else:
                print(f"[Skip count] proposed.{query_id}.num_candidates")
            num_valid_witnesses = safe_count(witnesses, f"proposed.{query_id}.num_valid_witnesses", required=True)
        else:
            print(f"[Proposed local metric mode] Skipping final GlobalEval for query {query_id}.")

        time_step2_stitching_validation = time.perf_counter() - t_step2
        time_query_algorithm_parallel_estimate = (
            time_step1_localeval_boundary_parallel_estimate_seconds
            + time_selected_nexttw_seconds
            + time_step2_stitching_validation
        )
        time_query_algorithm_wall = (
            time_global_lookup_seconds
            + time_step1_localeval_boundary_wall_seconds
            + time_selected_nexttw_seconds
            + time_step2_stitching_validation
        )
        time_algorithm_total = time_query_algorithm_wall
        time_end_to_end_total = time.perf_counter() - t_total
        time_implementation_gap = (
            time_step1_localeval_boundary_wall_seconds
            - time_step1_localeval_boundary_parallel_estimate_seconds
        )
        time_unattributed_overhead = time_end_to_end_total - time_load_data - time_query_algorithm_wall

        row = {
            "method": METHOD_NAME,
            "dataset_name": basename_uri(config.dataset_root),
            "dataset_root": config.dataset_root,
            "method_family": "proposed_fragment_first_bounded_erpq",
            "spark_master": spark.sparkContext.master,
            "spark_app_id": spark.sparkContext.applicationId,
            "spark_executor_memory": config.executor_memory,
            "spark_executor_cores": config.executor_cores,
            "spark_cores_max": config.cores_max,
            "spark_shuffle_partitions": config.spark_shuffle_partitions,
            "enable_local_complete_detection": config.enable_local_complete_detection,
            "evaluation_strategy": "Proposed Distributed: LocalEval -> Stitching -> Hard Validation",
            "execution_strategy": "partition_localeval_then_query_relevant_stitching_then_query_spec_evaluator",
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
            "num_local_fragments": num_local_complete_fragments,
            "num_boundary_fragments": num_boundary_fragments,
            "num_localeval_fragments_total": num_localeval_fragments_total,
            "num_candidate_upper_bound_without_continuity": num_candidate_upper_bound,
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
            "time_global_lookup_seconds": time_global_lookup_seconds,
            "time_step1_localeval_boundary_seconds": time_step1_localeval_boundary,
            "time_step1_localeval_boundary_parallel_estimate_seconds": time_step1_localeval_boundary_parallel_estimate_seconds,
            "time_step1_localeval_boundary_wall_seconds": time_step1_localeval_boundary_wall_seconds,
            "time_step1_localeval_boundary_sum_partition_seconds": time_step1_localeval_boundary_sum_partition_seconds,
            "time_selected_nexttw_seconds": time_selected_nexttw_seconds,
            "nexttw_execution_mode": "logical_only_bounded_join_no_physical_nexttw_materialization",
            "time_step2_stitching_validation_seconds": time_step2_stitching_validation,
            "time_algorithm_total_seconds": time_algorithm_total,
            "time_query_algorithm_wall_seconds": time_query_algorithm_wall,
            "time_query_algorithm_parallel_estimate_seconds": time_query_algorithm_parallel_estimate,
            "time_end_to_end_total_seconds": time_end_to_end_total,
            "time_implementation_gap_seconds": time_implementation_gap,
            "time_unattributed_overhead_seconds": time_unattributed_overhead,
            "partition_query_timings": partition_query_timings,
            "step_timing_note": "NEXT_TW_* is kept as a logical continuity relation; execution uses optimized bounded joins over entity id and tw_num on query-relevant fragments, without physical selected NEXT_TW materialization.",
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
        unpersist_all(boundary_events, local_complete_candidates_all, local_complete_witnesses_all, candidate_upper_bound, candidates, witnesses)

    write_runtime_summary(summary_rows, config.output_root / METHOD_NAME, enabled=config.save_runtime_summary)
    unpersist_all(global_vertices, global_base_edges)
    spark.stop()


if __name__ == "__main__":
    main()

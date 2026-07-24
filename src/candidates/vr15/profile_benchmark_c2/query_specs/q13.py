#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q1.3 — Multi-location Movement Detection."""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q11 import q11_local_event_extractor

COMPARE_COLUMNS_Q13 = [
    "query_id",
    "entity_type",
    "entity_id",
    "location_trace",
    "partition_trace",
]


def q13_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    """Extract DETECTED_IN-based location segments for Q1.3.

    Q1.3 is an aggregate ERPQ-style query. The local fragment is the observation
    of one entity in one time-window/video/location context. The global/build
    phase groups fragments by entity and validates whether the entity appears in
    at least ``Q13_MIN_DISTINCT_LOCATIONS`` distinct locations.
    """
    return (
        q11_local_event_extractor(vertices, base_edges, config, query_id)
        .withColumn("query_group", F.lit("G1"))
        .withColumn("logical_query_type", F.lit("ERPQ with aggregate constraint"))
        .withColumn("fragment_role", F.lit("LOCATION_SEGMENT"))
        .withColumn("stitch_mode", F.lit("multi_location_path"))
        .withColumn(
            "constraint_tags",
            F.lit("same_entity,min_distinct_locations,temporal_order,bounded_continuity"),
        )
    )


def _build(events: DataFrame, config: Stage2Config, query_id: str):
    min_locations = int(os.getenv("Q13_MIN_DISTINCT_LOCATIONS", "2"))
    k = int(config.candidate_pruning_max_time_window_gap)
    eps = float(config.max_time_gap_seconds)

    w = Window.partitionBy("entity_type", "entity_id").orderBy(
        F.col("tw_num").asc_nulls_last(),
        F.col("start_epoch").asc_nulls_last(),
        F.col("edge_id").asc_nulls_last(),
    )

    ordered = (
        events
        .filter(F.col("location_id").isNotNull())
        .withColumn("event_order", F.row_number().over(w))
        .withColumn("prev_tw_num", F.lag("tw_num").over(w))
        .withColumn("prev_end_epoch", F.lag("end_epoch").over(w))
        .withColumn(
            "tw_gap",
            F.when(F.col("prev_tw_num").isNull(), F.lit(0))
            .otherwise(F.col("tw_num") - F.col("prev_tw_num")),
        )
        .withColumn(
            "time_gap_seconds",
            F.when(F.col("prev_end_epoch").isNull(), F.lit(0.0))
            .otherwise((F.col("start_epoch") - F.col("prev_end_epoch")).cast("double")),
        )
    )

    grouped = ordered.groupBy("entity_type", "entity_id").agg(
        F.sort_array(F.collect_list(F.struct("event_order", "location_id"))).alias("_locations"),
        F.sort_array(F.collect_list(F.struct("event_order", "camera_id"))).alias("_cameras"),
        F.min("start_ts").alias("ts_start"),
        F.max("end_ts").alias("ts_end"),
        F.count(F.lit(1)).alias("num_fragments"),
        F.countDistinct("location_id").alias("num_distinct_locations"),
        F.countDistinct("partition_id").alias("num_partitions"),
        F.max("tw_gap").cast("int").alias("max_tw_gap"),
        F.max("time_gap_seconds").cast("double").alias("max_time_gap_seconds"),
        F.concat_ws(",", F.sort_array(F.collect_set("partition_id"))).alias("partition_trace"),
    )

    candidates = (
        grouped
        .withColumn("location_trace", F.transform(F.col("_locations"), lambda x: x["location_id"]))
        .withColumn("camera_trace", F.transform(F.col("_cameras"), lambda x: x["camera_id"]))
        .drop("_locations", "_cameras")
        .withColumn(
            "is_valid",
            (F.col("num_fragments") >= F.lit(2))
            & (F.col("num_distinct_locations") >= F.lit(min_locations))
            & (F.col("max_tw_gap") >= F.lit(0))
            & (F.col("max_tw_gap") <= F.lit(k))
            & (F.col("max_time_gap_seconds") >= F.lit(0.0))
            & (F.col("max_time_gap_seconds") <= F.lit(eps)),
        )
        # IMPORTANT: F.when() requires a Column condition, not the string "is_valid".
        .withColumn(
            "validation_status",
            F.when(F.col("is_valid"), F.lit("VALID")).otherwise(F.lit("INVALID")),
        )
        .withColumn(
            "validation_reason",
            F.concat(
                F.lit("distinct_locations="), F.col("num_distinct_locations"),
                F.lit(",max_tw_gap="), F.col("max_tw_gap"),
                F.lit(",max_time_gap_seconds="), F.col("max_time_gap_seconds"),
            ),
        )
        .withColumn("query_id", F.lit(query_id))
        .withColumn("query_group", F.lit("G1"))
        .withColumn("logical_query_type", F.lit("ERPQ with aggregate constraint"))
        .withColumn("rpq_expression", F.lit("(DETECTED_IN·RECORDED_BY·LOCATED_AT)·NEXT_TW_*"))
        # Vr15 boundary classification requires the same four nullable
        # identity columns on every candidate/witness schema.
        .withColumn(
            "person_id",
            F.when(F.col("entity_type") == F.lit("PERSON"), F.col("entity_id")).cast("string"),
        )
        .withColumn("other_person_id", F.lit(None).cast("string"))
        .withColumn(
            "thing_id",
            F.when(F.col("entity_type") == F.lit("THING"), F.col("entity_id")).cast("string"),
        )
        .withColumn(
            "vehicle_id",
            F.when(F.col("entity_type") == F.lit("VEHICLE"), F.col("entity_id")).cast("string"),
        )
        .withColumn("constraint_tags", F.lit("same_entity,min_distinct_locations,temporal_order,bounded_continuity"))
        .withColumn(
            "candidate_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("entity_type"),
                    F.col("entity_id"),
                    F.to_json(F.col("location_trace")),
                    F.col("partition_trace"),
                ),
                256,
            ),
        )
    )

    witnesses = (
        candidates
        .filter(F.col("is_valid"))
        .withColumn(
            "witness_id",
            F.sha2(F.concat_ws("|", F.col("candidate_id"), F.lit(query_id)), 256),
        )
        .withColumn("witness_type", F.lit("multi_location_trajectory"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q13_complete_local_candidates(candidates: DataFrame, config: Stage2Config) -> DataFrame:
    """Pattern completeness needs a local sequence, not final location validity."""
    del config
    return candidates.filter(F.col("num_fragments") >= F.lit(2))


def q13_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(
        q13_local_event_extractor(vertices, base_edges, config, query_id),
        config,
        query_id,
    )


def q13_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(atomic_events, config, query_id)


Q13_SPEC = QuerySpec(
    query_id="Q1.3", query_group="G1", query_name="Multi-location Movement Detection",
    logical_query_type="ERPQ with aggregate constraint",
    rpq_expression="(DETECTED_IN·RECORDED_BY·LOCATED_AT)·NEXT_TW_ENTITY*",
    logical_query_description="Phát hiện cùng thực thể xuất hiện theo thứ tự tại nhiều location.",
    baseline_physical_plan="GLOBAL_MERGE_THEN_LOCATION_AGGREGATION",
    proposed_physical_plan="PARTITION_LOCATION_LOCALEVAL_THEN_TRAJECTORY_STITCHING",
    witness_type="multi_location_trajectory", correctness_compare_columns=COMPARE_COLUMNS_Q13,
    local_event_extractor=q13_local_event_extractor, baseline_evaluator=q13_baseline_global_evaluator,
    distributed_evaluator=q13_distributed_evaluator,
    local_candidate_completion_filter=q13_complete_local_candidates,
    automaton_transitions=(("q0","LOCATION_SEGMENT","q1"),("q1","LOCATION_SEGMENT","q1")), automaton_accepting_states=("q1",),
    required_fragment_roles=("LOCATION_SEGMENT",), boundary_eligible_roles=("LOCATION_SEGMENT",),
    stitch_mode="SAME_ENTITY_MULTI_LOCATION", stitch_key_fields=("entity_type","entity_id"),
    stitch_role_order=("LOCATION_SEGMENT",),
    boundary_interfaces=(("LOCATION_SEGMENT","LOCATION_SEGMENT",("entity_type","entity_id"),("entity_type","entity_id"),"FORWARD","q1","q1"),),
    hard_constraint_names=("same_entity","temporal_order","bounded_continuity","minimum_distinct_locations"),
    candidate_dedup_columns=("candidate_id",),
    witness_dedup_columns=tuple(COMPARE_COLUMNS_Q13),
    runtime_parameter_schema={"max_time_gap_seconds":"float", "min_distinct_locations":"int"},
)

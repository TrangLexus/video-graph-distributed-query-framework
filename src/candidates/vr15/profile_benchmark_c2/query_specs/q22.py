#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q2.2 — Temporal Continuity Checking."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q11 import q11_local_event_extractor

COMPARE_COLUMNS_Q22 = [
    "query_id",
    "entity_type",
    "entity_id",
    "tw_trace",
    "max_tw_gap",
    "partition_trace",
]


def q22_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    """Extract ordered observation fragments for temporal continuity checking."""
    return (
        q11_local_event_extractor(vertices, base_edges, config, query_id)
        .withColumn("query_group", F.lit("G2"))
        .withColumn("logical_query_type", F.lit("Bounded ERPQ"))
        .withColumn("fragment_role", F.lit("CONTINUITY_SEGMENT"))
        .withColumn("stitch_mode", F.lit("temporal_continuity"))
        .withColumn("constraint_tags", F.lit("same_entity,continuity,epsilon_time"))
    )


def _build(events: DataFrame, config: Stage2Config, query_id: str):
    k = int(config.candidate_pruning_max_time_window_gap)
    eps = float(config.max_time_gap_seconds)

    w = Window.partitionBy("entity_type", "entity_id").orderBy(
        F.col("tw_num").asc_nulls_last(),
        F.col("start_epoch").asc_nulls_last(),
        F.col("edge_id").asc_nulls_last(),
    )

    x = (
        events
        .filter(F.col("tw_num").isNotNull())
        .withColumn("event_order", F.row_number().over(w))
        .withColumn("prev_tw", F.lag("tw_num").over(w))
        .withColumn("prev_end", F.lag("end_epoch").over(w))
        .withColumn(
            "tw_gap",
            F.when(F.col("prev_tw").isNull(), F.lit(0))
             .otherwise(F.col("tw_num") - F.col("prev_tw")),
        )
        .withColumn(
            "time_gap",
            F.when(F.col("prev_end").isNull(), F.lit(0.0))
             .otherwise((F.col("start_epoch") - F.col("prev_end")).cast("double")),
        )
    )

    grouped = x.groupBy("entity_type", "entity_id").agg(
        F.sort_array(F.collect_list(F.struct("event_order", "tw_id"))).alias("_tw"),
        F.max("tw_gap").alias("max_tw_gap"),
        F.max("time_gap").alias("max_time_gap_seconds"),
        F.count(F.lit(1)).alias("num_fragments"),
        F.countDistinct("partition_id").alias("num_partitions"),
        F.concat_ws(",", F.sort_array(F.collect_set("partition_id"))).alias("partition_trace"),
        F.min("start_ts").alias("ts_start"),
        F.max("end_ts").alias("ts_end"),
    )

    candidates = (
        grouped
        .withColumn("tw_trace", F.transform(F.col("_tw"), lambda z: z["tw_id"]))
        .drop("_tw")
        .withColumn(
            "is_valid",
            (F.col("num_fragments") >= F.lit(2))
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
            F.concat_ws(
                ";",
                F.concat(F.lit("max_tw_gap="), F.col("max_tw_gap")),
                F.concat(F.lit("max_time_gap="), F.col("max_time_gap_seconds")),
            ),
        )
        .withColumn("query_id", F.lit(query_id))
        .withColumn("query_group", F.lit("G2"))
        .withColumn("logical_query_type", F.lit("Bounded ERPQ"))
        .withColumn("rpq_expression", F.lit("DETECTED_IN·(NEXT_TW_*·DETECTED_IN){m,n}"))
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
        .withColumn("constraint_tags", F.lit("same_entity,continuity,epsilon_time"))
        .withColumn(
            "candidate_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("entity_type"),
                    F.col("entity_id"),
                    F.to_json(F.col("tw_trace")),
                    F.col("max_tw_gap").cast("string"),
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
        .withColumn("witness_type", F.lit("temporal_continuity"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q22_complete_local_candidates(candidates: DataFrame, config: Stage2Config) -> DataFrame:
    """Temporal continuity candidate needs at least two observations."""
    del config
    return candidates.filter(F.col("num_fragments") >= F.lit(2))


def q22_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(
        q22_local_event_extractor(vertices, base_edges, config, query_id),
        config,
        query_id,
    )


def q22_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(atomic_events, config, query_id)


Q22_SPEC = QuerySpec(
    query_id="Q2.2", query_group="G2", query_name="Temporal Continuity Checking",
    logical_query_type="Bounded ERPQ",
    rpq_expression="DETECTED_IN·(NEXT_TW_ENTITY·DETECTED_IN){m,n}",
    logical_query_description="Kiểm tra continuity theo chế độ bounded gap, không mặc định yêu cầu TimeWindow liên tiếp tuyệt đối.",
    baseline_physical_plan="GLOBAL_MERGE_THEN_BOUNDED_GAP_VALIDATION",
    proposed_physical_plan="PARTITION_CONTINUITY_LOCALEVAL_THEN_BOUNDARY_STITCHING",
    witness_type="temporal_continuity", correctness_compare_columns=COMPARE_COLUMNS_Q22,
    local_event_extractor=q22_local_event_extractor, baseline_evaluator=q22_baseline_global_evaluator,
    distributed_evaluator=q22_distributed_evaluator,
    local_candidate_completion_filter=q22_complete_local_candidates,
    automaton_transitions=(("q0","CONTINUITY_SEGMENT","q1"),("q1","CONTINUITY_SEGMENT","q1")), automaton_accepting_states=("q1",),
    required_fragment_roles=("CONTINUITY_SEGMENT",), boundary_eligible_roles=("CONTINUITY_SEGMENT",),
    stitch_mode="BOUNDED_TEMPORAL_CONTINUITY", stitch_key_fields=("entity_type","entity_id"),
    stitch_role_order=("CONTINUITY_SEGMENT",),
    boundary_interfaces=(("CONTINUITY_SEGMENT","CONTINUITY_SEGMENT",("entity_type","entity_id"),("entity_type","entity_id"),"FORWARD","q1","q1"),),
    hard_constraint_names=("same_entity","bounded_time_window_gap","max_time_gap_seconds","temporal_order"),
    candidate_dedup_columns=("candidate_id",),
    witness_dedup_columns=tuple(COMPARE_COLUMNS_Q22),
    runtime_parameter_schema={"max_time_gap_seconds":"float", "continuity_mode":"BOUNDED_GAP"},
)

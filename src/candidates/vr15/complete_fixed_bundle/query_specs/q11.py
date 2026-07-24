#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q1.1 — reconstruct the global trajectory of an entity."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec, prepared_edges, extract_time_window_number

# ---------------------------------------------------------------------------
# Q1.1 — Reconstruct Global Trajectory of an Entity
# ---------------------------------------------------------------------------

COMPARE_COLUMNS_Q11 = [
    "query_id",
    "entity_type",
    "entity_id",
    "entity_tw_trace",
    "tw_trace",
    "video_trace",
    "camera_trace",
    "location_trace",
    "partition_trace",
]


def _q11_entity_vertices(vertices: DataFrame) -> DataFrame:
    """Normalize Person_TW, Thing_TW and Vehicle_TW into one entity view."""
    frames = []
    for label, entity_type in [
        ("Person_TW", "PERSON"),
        ("Thing_TW", "THING"),
        ("Vehicle_TW", "VEHICLE"),
    ]:
        frames.append(
            vertices
            .filter(F.col("label") == F.lit(label))
            .select(
                F.col("id").cast("string").alias("entity_tw_id"),
                F.col("global_id").cast("string").alias("entity_id"),
                F.lit(entity_type).alias("entity_type"),
                F.col("tw_id").cast("string").alias("entity_vertex_tw_id"),
                F.col("partition_id").cast("string").alias("entity_vertex_partition_id"),
            )
        )

    return (
        frames[0]
        .unionByName(frames[1], allowMissingColumns=True)
        .unionByName(frames[2], allowMissingColumns=True)
        .filter(F.col("entity_tw_id").isNotNull() & F.col("entity_id").isNotNull())
        .dropDuplicates(["entity_tw_id"])
    )


def _q11_observation_events(vertices: DataFrame, base_edges: DataFrame) -> DataFrame:
    """Extract query-relevant observation records for Q1.1.

    The logical query is EntityTW -DETECTED_IN-> Video -RECORDED_BY->
    Camera -LOCATED_AT-> Location. Current generated benchmark data already
    carries video_id/camera_id/location_id as edge context on DETECTED_IN.
    Therefore physical execution reads that context directly and remains robust
    to either DETECTED_IN edge orientation by checking both src and dst.
    """
    entities = _q11_entity_vertices(vertices)
    edges = prepared_edges(base_edges).filter(F.col("label") == F.lit("DETECTED_IN"))

    def orient(entity_col: str, context_col: str) -> DataFrame:
        return (
            entities.alias("v")
            .join(edges.alias("e"), F.col("v.entity_tw_id") == F.col(f"e.{entity_col}"), "inner")
            .select(
                F.col("v.entity_type"),
                F.col("v.entity_id"),
                F.col("v.entity_tw_id"),
                F.col(f"e.{entity_col}").cast("string").alias("src"),
                F.col(f"e.{context_col}").cast("string").alias("dst"),
                F.lit("DETECTED_IN").alias("relation_type"),
                F.col("e.start_time"), F.col("e.end_time"),
                F.col("e.start_ts"), F.col("e.end_ts"),
                F.col("e.start_epoch"), F.col("e.end_epoch"),
                F.coalesce(F.col("e.tw_id"), F.col("v.entity_vertex_tw_id")).cast("string").alias("tw_id"),
                extract_time_window_number(
                    F.coalesce(F.col("e.tw_id"), F.col("v.entity_vertex_tw_id"))
                ).alias("tw_num"),
                F.coalesce(F.col("e.partition_id"), F.col("v.entity_vertex_partition_id")).cast("string").alias("partition_id"),
                F.coalesce(F.col("e.video_id"), F.col(f"e.{context_col}").cast("string")).alias("video_id"),
                F.col("e.camera_id").cast("string").alias("camera_id"),
                F.col("e.location_id").cast("string").alias("location_id"),
                F.col("e.confidence").cast("double").alias("confidence"),
                F.col("e.edge_id").cast("string").alias("edge_id"),
            )
        )

    # The first branch is the expected schema (EntityTW as src). The reverse
    # branch makes the query tolerant to imported datasets with reversed edges.
    forward = orient("src", "dst")
    reverse = orient("dst", "src")

    return (
        forward
        .unionByName(reverse, allowMissingColumns=True)
        .filter(F.col("tw_num").isNotNull())
        .dropDuplicates(["entity_type", "entity_id", "entity_tw_id", "edge_id", "tw_id", "partition_id"])
    )


def q11_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    """LocalEval for Q1.1: emit high-recall TRAJECTORY_SEGMENT fragments."""
    obs = _q11_observation_events(vertices, base_edges)
    return (
        obs
        .select(
            F.lit(query_id).alias("query_id"),
            F.lit("G1").alias("query_group"),
            F.lit("ERPQ").alias("logical_query_type"),
            F.lit("TRAJECTORY_SEGMENT").alias("fragment_role"),
            F.lit("BOUNDARY_OR_LOCAL_EVIDENCE").alias("fragment_scope"),
            "src", "dst", "relation_type",
            "entity_type", "entity_id", "entity_tw_id",
            F.when(F.col("entity_type") == "PERSON", F.col("entity_id")).cast("string").alias("person_id"),
            F.lit(None).cast("string").alias("other_person_id"),
            F.when(F.col("entity_type") == "THING", F.col("entity_id")).cast("string").alias("thing_id"),
            F.when(F.col("entity_type") == "VEHICLE", F.col("entity_id")).cast("string").alias("vehicle_id"),
            "start_time", "end_time", "start_ts", "end_ts", "start_epoch", "end_epoch",
            "tw_id", "tw_num", "partition_id", "location_id", "camera_id", "video_id",
            "confidence", "edge_id",
            F.col("entity_id").alias("stitch_key"),
            F.lit("same_entity_trajectory").alias("stitch_mode"),
            F.concat_ws(":", F.col("entity_id"), F.col("tw_id")).alias("prev_key"),
            F.concat_ws(":", F.col("entity_id"), (F.col("tw_num") + F.lit(1)).cast("string")).alias("next_key"),
            F.lit("same_entity,temporal_order,bounded_tw_continuity").alias("constraint_tags"),
        )
        .dropDuplicates()
    )


def _build_q11_trajectories(
    observations: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Stitch ordered Q1.1 trajectory fragments and hard-validate continuity."""
    k = int(config.candidate_pruning_max_time_window_gap)
    order_window = (
        Window
        .partitionBy("entity_type", "entity_id")
        .orderBy(
            F.col("tw_num").asc_nulls_last(),
            F.col("start_epoch").asc_nulls_last(),
            F.col("entity_tw_id"),
            F.col("edge_id"),
        )
    )

    ordered = (
        observations
        .filter(F.col("entity_id").isNotNull() & F.col("tw_num").isNotNull())
        .withColumn("event_order", F.row_number().over(order_window))
        .withColumn("prev_tw_num", F.lag("tw_num").over(order_window))
        .withColumn("prev_end_epoch", F.lag("end_epoch").over(order_window))
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

    trace_struct = F.struct(
        F.col("event_order").alias("ord"),
        F.col("entity_tw_id").alias("entity_tw_id"),
        F.col("tw_id").alias("tw_id"),
        F.col("video_id").alias("video_id"),
        F.col("camera_id").alias("camera_id"),
        F.col("location_id").alias("location_id"),
        F.col("partition_id").alias("partition_id"),
        F.col("start_epoch").alias("start_epoch"),
        F.col("end_epoch").alias("end_epoch"),
        F.col("edge_id").alias("edge_id"),
    )

    grouped = (
        ordered
        .groupBy("entity_type", "entity_id")
        .agg(
            F.sort_array(F.collect_list(trace_struct)).alias("_trace"),
            F.min("start_ts").alias("ts_start"),
            F.max("end_ts").alias("ts_end"),
            F.count(F.lit(1)).cast("int").alias("num_fragments"),
            F.countDistinct("partition_id").cast("int").alias("num_partitions"),
            F.max("tw_gap").cast("int").alias("max_tw_gap"),
            F.max("time_gap_seconds").cast("double").alias("max_time_gap_seconds"),
        )
        .withColumn("entity_tw_trace", F.transform("_trace", lambda x: x["entity_tw_id"]))
        .withColumn("tw_trace", F.transform("_trace", lambda x: x["tw_id"]))
        .withColumn("video_trace", F.transform("_trace", lambda x: x["video_id"]))
        .withColumn("camera_trace", F.transform("_trace", lambda x: x["camera_id"]))
        .withColumn("location_trace", F.transform("_trace", lambda x: x["location_id"]))
        .withColumn("partition_trace", F.array_distinct(F.transform("_trace", lambda x: x["partition_id"])))
        .withColumn(
            "is_valid",
            (F.col("num_fragments") >= F.lit(2)) &
            (F.col("max_tw_gap") >= F.lit(0)) &
            (F.col("max_tw_gap") <= F.lit(k)) &
            (F.col("max_time_gap_seconds") >= F.lit(0.0)) &
            (F.col("max_time_gap_seconds") <= F.lit(float(config.max_time_gap_seconds))),
        )
        .withColumn("validation_status", F.when(F.col("is_valid"), "PASS").otherwise("FAIL"))
        .withColumn(
            "validation_reason",
            F.when(F.col("num_fragments") < 2, "INSUFFICIENT_TRAJECTORY_LENGTH")
             .when(F.col("max_tw_gap") > F.lit(k), "TW_CONTINUITY_EXCEEDED")
             .otherwise("PASS"),
        )
        .withColumn(
            "candidate_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(query_id),
                    F.col("entity_type"),
                    F.col("entity_id"),
                    F.concat_ws(",", F.col("entity_tw_trace")),
                    F.concat_ws(",", F.col("tw_trace")),
                    F.concat_ws(",", F.col("partition_trace")),
                ),
                256,
            ),
        )
        .withColumn(
            "explanation_trace",
            F.concat(
                F.lit("ENTITY="), F.col("entity_id"),
                F.lit(" | TW="), F.concat_ws("->", F.col("tw_trace")),
                F.lit(" | CAMERA="), F.concat_ws("->", F.col("camera_trace")),
                F.lit(" | LOCATION="), F.concat_ws("->", F.col("location_trace")),
            ),
        )
        .drop("_trace")
    )

    candidates = grouped.select(
        F.lit(query_id).alias("query_id"),
        F.lit("G1").alias("query_group"),
        F.lit("ERPQ").alias("logical_query_type"),
        F.lit(
            "(DETECTED_IN · RECORDED_BY · LOCATED_AT) · NEXT_TW_* · "
            "(DETECTED_IN · RECORDED_BY · LOCATED_AT)*"
        ).alias("rpq_expression"),
        "candidate_id", "entity_type", "entity_id",
        F.when(F.col("entity_type") == "PERSON", F.col("entity_id")).cast("string").alias("person_id"),
        F.lit(None).cast("string").alias("other_person_id"),
        F.when(F.col("entity_type") == "THING", F.col("entity_id")).cast("string").alias("thing_id"),
        F.when(F.col("entity_type") == "VEHICLE", F.col("entity_id")).cast("string").alias("vehicle_id"),
        "entity_tw_trace", "tw_trace", "video_trace", "camera_trace", "location_trace",
        "ts_start", "ts_end", "num_fragments", "num_partitions", "partition_trace",
        "max_tw_gap", "max_time_gap_seconds", "explanation_trace",
        F.lit("same_entity,temporal_order,bounded_tw_continuity,partition_trace").alias("constraint_tags"),
        "validation_status", "validation_reason", "is_valid",
    )

    witnesses = (
        candidates
        .filter(F.col("is_valid") == F.lit(True))
        .withColumn(
            "witness_id",
            F.sha2(F.concat_ws("|", F.col("candidate_id"), F.col("entity_type"), F.col("entity_id")), 256),
        )
        .withColumn("witness_type", F.lit("trajectory"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q11_complete_local_candidates(candidates: DataFrame, config: Stage2Config) -> DataFrame:
    """Q1.1 chỉ hoàn thành cục bộ khi có ít nhất hai observation."""
    return candidates.filter(F.col("num_fragments") >= F.lit(2))


def q11_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Baseline Q1.1: evaluate trajectory reconstruction on the merged graph."""
    observations = q11_local_event_extractor(vertices, base_edges, config, query_id)
    return _build_q11_trajectories(observations, config, query_id)


def q11_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame | None,
    config: Stage2Config,
    query_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Proposed Q1.1: stitch LocalEval trajectory segments and validate globally."""
    return _build_q11_trajectories(atomic_events, config, query_id)


Q11_SPEC = QuerySpec(
    query_id="Q1.1", query_group="G1",
    query_name="Reconstruct Global Trajectory of an Entity",
    logical_query_type="ERPQ",
    rpq_expression="(DETECTED_IN · RECORDED_BY · LOCATED_AT) · NEXT_TW_ENTITY*",
    logical_query_description="Khôi phục chuỗi quan sát không gian-thời gian có thứ tự của cùng một thực thể.",
    baseline_physical_plan="GLOBAL_MERGE_THEN_SAME_ENTITY_BOUNDED_TRAJECTORY_EVALUATION",
    proposed_physical_plan="PARTITION_LOCALEVAL_THEN_SAME_ENTITY_FRAGMENT_STITCHING",
    witness_type="trajectory",
    correctness_compare_columns=COMPARE_COLUMNS_Q11,
    local_event_extractor=q11_local_event_extractor,
    baseline_evaluator=q11_baseline_global_evaluator,
    distributed_evaluator=q11_distributed_evaluator,
    candidate_upper_bound=None,
    local_candidate_completion_filter=q11_complete_local_candidates,
    automaton_start_state="q0", automaton_accepting_states=("q1",),
    automaton_transitions=(("q0", "TRAJECTORY_SEGMENT", "q1"),),
    required_fragment_roles=("TRAJECTORY_SEGMENT",),
    boundary_eligible_roles=("TRAJECTORY_SEGMENT",),
    stitch_mode="SAME_ENTITY_TRAJECTORY",
    stitch_key_fields=("entity_type", "entity_id"),
    stitch_role_order=("TRAJECTORY_SEGMENT",),
    hard_constraint_names=("same_entity", "temporal_order", "bounded_continuity", "query_time_scope"),
    runtime_parameter_schema={"max_time_gap_seconds":"float", "time_window_duration_seconds":"float"},
)

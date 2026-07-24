#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q2.1 — Camera Sequence Reconstruction."""
from __future__ import annotations
from pyspark.sql import Window
from pyspark.sql import functions as F
from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q11 import q11_local_event_extractor

COMPARE_COLUMNS_Q21 = ["query_id", "entity_type", "entity_id", "camera_trace", "tw_trace", "partition_trace"]

def q21_local_event_extractor(vertices, base_edges, config, query_id):
    return (
        q11_local_event_extractor(vertices, base_edges, config, query_id)
        .withColumn("query_group", F.lit("G2"))
        .withColumn("logical_query_type", F.lit("ERPQ"))
        .withColumn("fragment_role", F.lit("CAMERA_OBSERVATION"))
        .withColumn("stitch_mode", F.lit("temporal_sequence"))
        .withColumn("constraint_tags", F.lit("same_entity,temporal_order,bounded_continuity,no_spurious_merge"))
    )

def _build(events, config, query_id):
    k = int(config.candidate_pruning_max_time_window_gap)
    eps = float(config.max_time_gap_seconds)
    w = Window.partitionBy("entity_type", "entity_id").orderBy(
        F.col("tw_num").asc_nulls_last(),
        F.col("start_epoch").asc_nulls_last(),
        F.col("edge_id").asc_nulls_last(),
    )
    ordered = (
        events.filter(F.col("camera_id").isNotNull())
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
    g = ordered.groupBy("entity_type", "entity_id").agg(
        F.sort_array(F.collect_list(F.struct("event_order", "camera_id"))).alias("_camera"),
        F.sort_array(F.collect_list(F.struct("event_order", "tw_id"))).alias("_tw"),
        F.min("start_ts").alias("ts_start"),
        F.max("end_ts").alias("ts_end"),
        F.count("*").cast("int").alias("num_fragments"),
        F.countDistinct("partition_id").cast("int").alias("num_partitions"),
        F.max("tw_gap").cast("int").alias("max_tw_gap"),
        F.max("time_gap_seconds").cast("double").alias("max_time_gap_seconds"),
        F.concat_ws(",", F.sort_array(F.collect_set("partition_id"))).alias("partition_trace"),
    )
    candidates = (
        g.withColumn("camera_trace", F.transform("_camera", lambda x: x["camera_id"]))
        .withColumn("tw_trace", F.transform("_tw", lambda x: x["tw_id"]))
        .drop("_camera", "_tw")
        .withColumn(
            "is_valid",
            (F.col("num_fragments") >= F.lit(1))
            & (F.col("max_tw_gap") >= F.lit(0))
            & (F.col("max_tw_gap") <= F.lit(k))
            & (F.col("max_time_gap_seconds") >= F.lit(0.0))
            & (F.col("max_time_gap_seconds") <= F.lit(eps)),
        )
        .withColumn(
            "validation_status",
            F.when(F.col("is_valid"), F.lit("VALID")).otherwise(F.lit("INVALID")),
        )
        .withColumn(
            "validation_reason",
            F.concat(
                F.lit("max_tw_gap="), F.col("max_tw_gap"),
                F.lit(",max_time_gap_seconds="), F.col("max_time_gap_seconds"),
            ),
        )
        .withColumn("query_id", F.lit(query_id))
        .withColumn("query_group", F.lit("G2"))
        .withColumn("logical_query_type", F.lit("ERPQ"))
        .withColumn("rpq_expression", F.lit("(DETECTED_IN·RECORDED_BY)(NEXT_TW_*·DETECTED_IN·RECORDED_BY)*"))
        .withColumn("person_id", F.when(F.col("entity_type") == "PERSON", F.col("entity_id")).cast("string"))
        .withColumn("other_person_id", F.lit(None).cast("string"))
        .withColumn("thing_id", F.when(F.col("entity_type") == "THING", F.col("entity_id")).cast("string"))
        .withColumn("vehicle_id", F.when(F.col("entity_type") == "VEHICLE", F.col("entity_id")).cast("string"))
        .withColumn("constraint_tags", F.lit("same_entity,temporal_order,bounded_continuity,no_spurious_merge"))
        .withColumn(
            "candidate_id",
            F.sha2(
                F.concat_ws(
                    "|", "entity_type", "entity_id",
                    F.to_json("camera_trace"), F.to_json("tw_trace"),
                    F.col("partition_trace"),
                ),
                256,
            ),
        )
    )
    witnesses = (
        candidates.filter(F.col("is_valid"))
        .withColumn("witness_id", F.sha2(F.concat_ws("|", "candidate_id", F.lit("Q2.1")), 256))
        .withColumn("witness_type", F.lit("camera_sequence"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses

def q21_baseline_global_evaluator(vertices, base_edges, next_tw_edges, config, query_id):
    return _build(q21_local_event_extractor(vertices, base_edges, config, query_id), config, query_id)
def q21_distributed_evaluator(vertices, atomic_events, next_tw_edges, config, query_id):
    return _build(atomic_events, config, query_id)

Q21_SPEC = QuerySpec(
    query_id="Q2.1", query_group="G2", query_name="Camera Sequence Reconstruction",
    logical_query_type="ERPQ",
    rpq_expression="(DETECTED_IN·RECORDED_BY)(NEXT_TW_ENTITY*·DETECTED_IN·RECORDED_BY)*",
    logical_query_description="Khôi phục chuỗi camera theo thời gian của cùng một thực thể.",
    baseline_physical_plan="GLOBAL_MERGE_THEN_CAMERA_OBSERVATION_ORDERING",
    proposed_physical_plan="PARTITION_CAMERA_LOCALEVAL_THEN_SEQUENCE_STITCHING",
    witness_type="camera_sequence", correctness_compare_columns=COMPARE_COLUMNS_Q21,
    local_event_extractor=q21_local_event_extractor, baseline_evaluator=q21_baseline_global_evaluator,
    distributed_evaluator=q21_distributed_evaluator,
    automaton_transitions=(("q0","CAMERA_OBSERVATION","q1"),("q1","CAMERA_OBSERVATION","q1")), automaton_accepting_states=("q1",),
    required_fragment_roles=("CAMERA_OBSERVATION",), boundary_eligible_roles=("CAMERA_OBSERVATION",),
    stitch_mode="SAME_ENTITY_CAMERA_SEQUENCE", stitch_key_fields=("entity_type","entity_id"),
    stitch_role_order=("CAMERA_OBSERVATION",),
    boundary_interfaces=(("CAMERA_OBSERVATION","CAMERA_OBSERVATION",("entity_type","entity_id"),("entity_type","entity_id"),"FORWARD","q1","q1"),),
    hard_constraint_names=("same_entity","temporal_order","bounded_continuity","no_spurious_merge"),
    candidate_dedup_columns=("candidate_id",),
    witness_dedup_columns=tuple(COMPARE_COLUMNS_Q21),
    runtime_parameter_schema={"max_time_gap_seconds":"float"},
)

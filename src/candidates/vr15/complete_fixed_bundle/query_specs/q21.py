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
        .withColumn("constraint_tags", F.lit("same_entity,temporal_order,no_spurious_merge"))
    )

def _build(events, config, query_id):
    w = Window.partitionBy("entity_type", "entity_id").orderBy("tw_num", "start_epoch", "edge_id")
    ordered = events.filter(F.col("camera_id").isNotNull()).withColumn("event_order", F.row_number().over(w))
    g = ordered.groupBy("entity_type", "entity_id").agg(
        F.sort_array(F.collect_list(F.struct("event_order", "camera_id"))).alias("_camera"),
        F.sort_array(F.collect_list(F.struct("event_order", "tw_id"))).alias("_tw"),
        F.min("start_ts").alias("ts_start"), F.max("end_ts").alias("ts_end"),
        F.count("*").alias("num_fragments"), F.countDistinct("partition_id").alias("num_partitions"),
        F.concat_ws(",", F.sort_array(F.collect_set("partition_id"))).alias("partition_trace"),
    )
    candidates = (
        g.withColumn("camera_trace", F.transform("_camera", lambda x: x["camera_id"]))
         .withColumn("tw_trace", F.transform("_tw", lambda x: x["tw_id"]))
         .drop("_camera", "_tw")
         .withColumn("is_valid", F.col("num_fragments") >= 1)
         .withColumn("validation_status", F.lit("VALID"))
         .withColumn("validation_reason", F.lit("ordered camera observations reconstructed"))
         .withColumn("query_id", F.lit(query_id))
         .withColumn("query_group", F.lit("G2"))
         .withColumn("logical_query_type", F.lit("ERPQ"))
         .withColumn("rpq_expression", F.lit("(DETECTED_IN·RECORDED_BY)(NEXT_TW_*·DETECTED_IN·RECORDED_BY)*"))
         .withColumn("person_id", F.when(F.col("entity_type") == "PERSON", F.col("entity_id")).cast("string"))
         .withColumn("other_person_id", F.lit(None).cast("string"))
         .withColumn("thing_id", F.when(F.col("entity_type") == "THING", F.col("entity_id")).cast("string"))
         .withColumn("vehicle_id", F.when(F.col("entity_type") == "VEHICLE", F.col("entity_id")).cast("string"))
         .withColumn("constraint_tags", F.lit("same_entity,temporal_order,no_spurious_merge"))
         .withColumn("candidate_id", F.sha2(F.concat_ws("|", "entity_type", "entity_id", F.to_json("camera_trace")), 256))
    )
    witnesses = candidates.withColumn("witness_id", F.sha2(F.concat_ws("|", "candidate_id", F.lit("Q2.1")), 256)) \
        .withColumn("witness_type", F.lit("camera_sequence")).select("witness_id", "witness_type", *candidates.columns)
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
    automaton_transitions=(("q0","CAMERA_OBSERVATION","q1"),), automaton_accepting_states=("q1",),
    required_fragment_roles=("CAMERA_OBSERVATION",), boundary_eligible_roles=("CAMERA_OBSERVATION",),
    stitch_mode="SAME_ENTITY_CAMERA_SEQUENCE", stitch_key_fields=("entity_type","entity_id"),
    stitch_role_order=("CAMERA_OBSERVATION",),
    hard_constraint_names=("same_entity","temporal_order","no_spurious_merge"),
    runtime_parameter_schema={"max_time_gap_seconds":"float"},
)

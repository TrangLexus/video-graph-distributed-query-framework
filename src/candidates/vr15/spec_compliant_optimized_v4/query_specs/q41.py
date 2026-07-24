#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q4.1 — Multi-factor Suspicious Behavior Detection.

Executable Vr09-aligned instance:
    A person satisfies a composite pattern consisting of:
      (i) CARRIES(P,T),
      (ii) INTERACTS_WITH(P,P2),
      (iii) observations in at least Q41_MIN_DISTINCT_LOCATIONS locations.

This is a composite ERPQ / multi-relation pattern query. The implementation
uses bounded temporal compatibility for CARRY–INTERACT to avoid Cartesian
explosion while preserving the intended multi-factor behavior semantics.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q11 import q11_local_event_extractor
from query_specs.q43 import q43_local_event_extractor

COMPARE_COLUMNS_Q41 = [
    "query_id",
    "person_id",
    "thing_id",
    "other_person_id",
    "location_trace",
    "partition_trace",
]


def q41_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    """LocalEval for Q4.1: emit CARRY, INTERACT, and LOCATION fragments."""
    behavior = (
        q43_local_event_extractor(vertices, base_edges, config, query_id)
        .filter(F.col("fragment_role").isin("CARRY", "INTERACT"))
        .withColumn("query_group", F.lit("G4"))
        .withColumn("logical_query_type", F.lit("Composite ERPQ"))
        .withColumn("stitch_mode", F.lit("conjunctive_composite_pattern"))
        .withColumn("constraint_tags", F.lit("factor_coverage,same_person,min_distinct_locations,temporal_compatibility"))
    )

    location = (
        q11_local_event_extractor(vertices, base_edges, config, query_id)
        .filter(F.col("entity_type") == F.lit("PERSON"))
        .select(
            F.lit(query_id).alias("query_id"),
            F.lit("G4").alias("query_group"),
            F.lit("Composite ERPQ").alias("logical_query_type"),
            F.lit("LOCATION").alias("fragment_role"),
            F.col("src"),
            F.col("dst"),
            F.lit("DETECTED_IN").alias("relation_type"),
            F.col("fragment_scope"),
            F.col("person_id"),
            F.lit(None).cast("string").alias("other_person_id"),
            F.lit(None).cast("string").alias("thing_id"),
            F.lit(None).cast("string").alias("vehicle_id"),
            F.col("start_time"),
            F.col("end_time"),
            F.col("start_ts"),
            F.col("end_ts"),
            F.col("start_epoch"),
            F.col("end_epoch"),
            F.col("tw_id"),
            F.col("tw_num"),
            F.col("partition_id"),
            F.col("location_id"),
            F.col("camera_id"),
            F.col("video_id"),
            F.col("confidence"),
            F.col("edge_id"),
            F.col("person_id").alias("stitch_key"),
            F.lit("conjunctive_composite_pattern").alias("stitch_mode"),
            F.lit(None).cast("string").alias("prev_key"),
            F.lit(None).cast("string").alias("next_key"),
            F.lit("factor_coverage,same_person,min_distinct_locations").alias("constraint_tags"),
        )
    )

    return behavior.unionByName(location, allowMissingColumns=True).dropDuplicates()


def _build(events: DataFrame, config: Stage2Config, query_id: str):
    min_locations = int(os.getenv("Q41_MIN_DISTINCT_LOCATIONS", "2"))
    k = int(config.candidate_pruning_max_time_window_gap)
    eps = float(config.max_time_gap_seconds)

    c = (
        events
        .filter(F.col("fragment_role") == F.lit("CARRY"))
        .filter(F.col("person_id").isNotNull() & F.col("thing_id").isNotNull())
        .dropDuplicates(["person_id", "thing_id", "tw_id", "start_epoch", "end_epoch", "edge_id"])
        .alias("c")
    )
    i = (
        events
        .filter(F.col("fragment_role") == F.lit("INTERACT"))
        .filter(F.col("person_id").isNotNull() & F.col("other_person_id").isNotNull())
        .dropDuplicates(["person_id", "other_person_id", "tw_id", "start_epoch", "end_epoch", "edge_id"])
        .alias("i")
    )
    loc = (
        events
        .filter(F.col("fragment_role") == F.lit("LOCATION"))
        .filter(F.col("person_id").isNotNull() & F.col("location_id").isNotNull())
        .groupBy("person_id")
        .agg(
            F.concat_ws("->", F.sort_array(F.collect_set(F.col("location_id").cast("string")))).alias("location_trace"),
            F.concat_ws(",", F.sort_array(F.collect_set(F.col("partition_id").cast("string")))).alias("location_partition_trace"),
            F.countDistinct("location_id").cast("int").alias("num_distinct_locations"),
        )
        .alias("l")
    )

    raw = (
        c.join(
            i,
            (F.col("c.person_id") == F.col("i.person_id")) &
            (F.abs(F.col("i.tw_num") - F.col("c.tw_num")) <= F.lit(k)) &
            (F.abs((F.col("i.start_epoch") - F.col("c.start_epoch")).cast("double")) <= F.lit(eps)),
            "inner",
        )
        .join(loc, F.col("c.person_id") == F.col("l.person_id"), "inner")
    )

    candidates = (
        raw.select(
            F.lit(query_id).alias("query_id"),
            F.lit("G4").alias("query_group"),
            F.lit("Composite ERPQ").alias("logical_query_type"),
            F.lit("CARRIES(P,T) ∧ INTERACTS_WITH(P,P2) ∧ multi-location(P)").alias("rpq_expression"),
            F.col("c.person_id").alias("person_id"),
            F.col("c.thing_id").alias("thing_id"),
            F.col("i.other_person_id").alias("other_person_id"),
            F.lit(None).cast("string").alias("vehicle_id"),
            F.col("l.location_trace").alias("location_trace"),
            F.col("l.location_partition_trace").alias("partition_trace"),
            F.col("l.num_distinct_locations"),
            F.least(F.col("c.start_ts"), F.col("i.start_ts")).alias("ts_start"),
            F.greatest(F.col("c.end_ts"), F.col("i.end_ts")).alias("ts_end"),
            F.lit(3).cast("int").alias("num_fragments"),
            F.size(F.split(F.col("l.location_partition_trace"), ",")).cast("int").alias("num_partitions"),
            F.abs((F.col("i.start_epoch") - F.col("c.start_epoch")).cast("double")).alias("behavior_time_gap_seconds"),
        )
        .withColumn("is_valid", F.col("num_distinct_locations") >= F.lit(min_locations))
        .withColumn(
            "validation_status",
            F.when(F.col("is_valid"), F.lit("VALID")).otherwise(F.lit("INVALID")),
        )
        .withColumn(
            "validation_reason",
            F.when(F.col("is_valid"), F.lit("carry+interact+multi-location factor coverage"))
             .otherwise(F.lit("insufficient distinct locations")),
        )
        .withColumn("constraint_tags", F.lit("factor_coverage,same_person,min_distinct_locations,temporal_compatibility"))
        .dropDuplicates(COMPARE_COLUMNS_Q41[1:])
        .withColumn(
            "candidate_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("person_id"),
                    F.col("thing_id"),
                    F.col("other_person_id"),
                    F.col("location_trace"),
                    F.col("partition_trace"),
                ),
                256,
            ),
        )
    )

    witnesses = (
        candidates
        .filter(F.col("is_valid"))
        .withColumn("witness_id", F.sha2(F.concat_ws("|", F.col("candidate_id"), F.lit(query_id)), 256))
        .withColumn("witness_type", F.lit("multi_factor_behavior"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q41_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(q41_local_event_extractor(vertices, base_edges, config, query_id), config, query_id)


def q41_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(atomic_events, config, query_id)


Q41_SPEC = QuerySpec(
    query_id="Q4.1", query_group="G4", query_name="Multi-factor Suspicious Behavior Detection",
    logical_query_type="Composite ERPQ",
    rpq_expression="CARRIES(P,T) ∧ INTERACTS_WITH(P,P2) ∧ multi-location(P)",
    logical_query_description="Mẫu kết hợp carry, interaction và chuyển động nhiều location của cùng người.",
    baseline_physical_plan="GLOBAL_MERGE_THEN_CONJUNCTIVE_FACTOR_JOIN",
    proposed_physical_plan="PARTITION_FACTOR_LOCALEVAL_THEN_CONJUNCTIVE_STITCHING",
    witness_type="multi_factor_behavior", correctness_compare_columns=COMPARE_COLUMNS_Q41,
    local_event_extractor=q41_local_event_extractor, baseline_evaluator=q41_baseline_global_evaluator,
    distributed_evaluator=q41_distributed_evaluator,
    automaton_transitions=(("q0","CARRY","q1"),("q1","INTERACT","q2"),("q2","LOCATION","q3")),
    automaton_accepting_states=("q3",), required_fragment_roles=("CARRY","INTERACT","LOCATION"),
    boundary_eligible_roles=("CARRY","INTERACT","LOCATION"), stitch_mode="CONJUNCTIVE_MULTI_FACTOR",
    stitch_key_fields=("person_id",), stitch_role_order=("CARRY","INTERACT","LOCATION"),
    boundary_interfaces=(("CARRY","INTERACT",("person_id",),("person_id",),"ABSOLUTE","q1","q1"),("CARRY","LOCATION",("person_id",),("person_id",),"ABSOLUTE","q1","q2"),("INTERACT","LOCATION",("person_id",),("person_id",),"ABSOLUTE","q2","q2")),
    hard_constraint_names=("factor_coverage","same_person","temporal_compatibility","minimum_distinct_locations"),
    candidate_dedup_columns=("candidate_id",),
    witness_dedup_columns=tuple(COMPARE_COLUMNS_Q41),
    runtime_parameter_schema={"max_time_gap_seconds":"float", "minimum_distinct_locations":"int"},
)

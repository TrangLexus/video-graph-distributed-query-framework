#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q3.3 — Group Interaction Chain Across Locations.

Vr09 logical intent:
    Group interaction chain across locations, i.e., INTERACTS_WITH+ combined
    with observation/location context and a minimum distinct-location constraint.

Physical implementation:
    Bounded two-step endpoint-compatible interaction chain. The final witness is
    existence-level per group_members/location_trace to avoid reporting all
    duplicate edge-level combinations.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q43 import q43_local_event_extractor

COMPARE_COLUMNS_Q33 = [
    "query_id",
    "group_members",
    "location_trace",
    "partition_trace",
]


def _interaction_events(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
    group: str,
    role: str = "GROUP_INTERACTION_STEP",
) -> DataFrame:
    return (
        q43_local_event_extractor(vertices, base_edges, config, query_id)
        .filter(F.col("fragment_role") == F.lit("INTERACT"))
        .filter(F.col("person_id").isNotNull() & F.col("other_person_id").isNotNull())
        .withColumn("query_group", F.lit(group))
        .withColumn("logical_query_type", F.lit("ERPQ"))
        .withColumn("fragment_role", F.lit(role))
        .withColumn("stitch_mode", F.lit("recursive_group_chain"))
        .withColumn("prev_key", F.col("person_id"))
        .withColumn("next_key", F.col("other_person_id"))
        .withColumn("stitch_key", F.col("person_id"))
        .withColumn("constraint_tags", F.lit("group_binding,endpoint_compatibility,temporal_order,min_distinct_locations"))
        .dropDuplicates(["person_id", "other_person_id", "tw_id", "start_epoch", "end_epoch", "location_id", "edge_id"])
    )


def _two_step_chain(events: DataFrame, config: Stage2Config) -> DataFrame:
    k = int(config.max_nexttw_hops)
    eps = float(config.epsilon_time_seconds)

    a = events.alias("a")
    b = events.alias("b")
    return (
        a.join(
            b,
            (F.col("a.other_person_id") == F.col("b.person_id")) &
            (F.col("b.start_epoch") >= F.col("a.end_epoch")) &
            ((F.col("b.tw_num") - F.col("a.tw_num")).between(F.lit(0), F.lit(k))) &
            (((F.col("b.start_epoch") - F.col("a.end_epoch")).cast("double")) <= F.lit(eps)),
            "inner",
        )
        .filter(F.col("a.person_id") != F.col("b.other_person_id"))
        .filter(F.coalesce(F.col("a.edge_id"), F.lit("")) != F.coalesce(F.col("b.edge_id"), F.lit("__different__")))
    )


def q33_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    return _interaction_events(vertices, base_edges, config, query_id, "G3", "GROUP_INTERACTION_STEP")


def _build(events: DataFrame, config: Stage2Config, query_id: str):
    min_locations = int(os.getenv("Q33_MIN_DISTINCT_LOCATIONS", "2"))
    raw = _two_step_chain(events, config)

    base = raw.select(
        F.concat_ws("->", F.col("a.person_id"), F.col("a.other_person_id"), F.col("b.other_person_id")).alias("group_members"),
        F.concat_ws("->", F.col("a.location_id"), F.col("b.location_id")).alias("location_trace"),
        F.concat_ws("->", F.col("a.partition_id"), F.col("b.partition_id")).alias("partition_trace"),
        F.col("a.person_id").alias("person_id"),
        F.col("b.other_person_id").alias("other_person_id"),
        F.least(F.col("a.start_ts"), F.col("b.start_ts")).alias("ts_start"),
        F.greatest(F.col("a.end_ts"), F.col("b.end_ts")).alias("ts_end"),
        F.array_distinct(F.array(F.col("a.location_id"), F.col("b.location_id"))).alias("_distinct_locations"),
        F.size(F.array_distinct(F.array(F.col("a.partition_id"), F.col("b.partition_id")))).cast("int").alias("num_partitions"),
        ((F.col("b.start_epoch") - F.col("a.end_epoch")).cast("double")).alias("max_time_gap_seconds"),
        F.concat_ws("->", F.col("a.edge_id"), F.col("b.edge_id")).alias("edge_trace"),
    )

    candidates = (
        base
        .groupBy("group_members", "location_trace", "partition_trace")
        .agg(
            F.first("person_id", ignorenulls=True).alias("person_id"),
            F.first("other_person_id", ignorenulls=True).alias("other_person_id"),
            F.min("ts_start").alias("ts_start"),
            F.max("ts_end").alias("ts_end"),
            F.max(F.size(F.col("_distinct_locations"))).cast("int").alias("num_distinct_locations"),
            F.max("num_partitions").cast("int").alias("num_partitions"),
            F.min("max_time_gap_seconds").alias("max_time_gap_seconds"),
            F.first("edge_trace", ignorenulls=True).alias("edge_trace"),
        )
        .select(
            F.lit(query_id).alias("query_id"),
            F.lit("G3").alias("query_group"),
            F.lit("ERPQ").alias("logical_query_type"),
            F.lit("INTERACTS_WITH+ · observation_context+").alias("rpq_expression"),
            "group_members",
            "location_trace",
            "partition_trace",
            "person_id",
            "other_person_id",
            "ts_start",
            "ts_end",
            F.lit(2).cast("int").alias("num_fragments"),
            "num_partitions",
            "num_distinct_locations",
            "max_time_gap_seconds",
            "edge_trace",
        )
        .withColumn("is_valid", F.col("num_distinct_locations") >= F.lit(min_locations))
        .withColumn(
            "validation_status",
            F.when(F.col("is_valid"), F.lit("VALID")).otherwise(F.lit("INVALID")),
        )
        .withColumn(
            "validation_reason",
            F.concat(F.lit("distinct_locations="), F.col("num_distinct_locations").cast("string")),
        )
        .withColumn("constraint_tags", F.lit("group_binding,endpoint_compatibility,min_distinct_locations"))
        .withColumn(
            "candidate_id",
            F.sha2(F.concat_ws("|", F.col("group_members"), F.col("location_trace"), F.col("partition_trace")), 256),
        )
        .dropDuplicates(["candidate_id"])
    )

    witnesses = (
        candidates
        .filter(F.col("is_valid"))
        .withColumn("witness_id", F.sha2(F.concat_ws("|", F.col("candidate_id"), F.lit(query_id)), 256))
        .withColumn("witness_type", F.lit("group_interaction_chain"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q33_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(q33_local_event_extractor(vertices, base_edges, config, query_id), config, query_id)


def q33_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(atomic_events, config, query_id)


Q33_SPEC = QuerySpec(
    query_id="Q3.3",
    aliases=(),
    query_group="G3",
    query_name="Group Interaction Chain Across Locations",
    logical_query_type="ERPQ",
    rpq_expression="INTERACTS_WITH+ · observation_context+",
    physical_plan="bounded_interaction_chain_with_location_aggregation",
    witness_type="group_interaction_chain",
    compare_columns=COMPARE_COLUMNS_Q33,
    local_event_extractor=q33_local_event_extractor,
    baseline_evaluator=q33_baseline_global_evaluator,
    distributed_evaluator=q33_distributed_evaluator,
)

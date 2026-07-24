#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q4.2 — Ordered Behavior Chain.

Vr09 logical intent:
    CARRIES(P,T) · NEXT_TW* · INTERACTS_WITH(P,P2)
      · NEXT_TW* · USES(P,VH)

Physical implementation:
    Bounded role joins over LocalEval fragments. Both time-window distance and
    epsilon_time_seconds are used to prevent candidate explosion while preserving
    ordered ERPQ semantics.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q43 import q43_local_event_extractor

COMPARE_COLUMNS_Q42 = [
    "query_id",
    "person_id",
    "thing_id",
    "other_person_id",
    "vehicle_id",
    "carry_tw_id",
    "interact_tw_id",
    "use_tw_id",
]


def q42_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    """LocalEval for Q4.2: emit CARRY, INTERACT, USE role fragments."""
    return (
        q43_local_event_extractor(vertices, base_edges, config, query_id)
        .filter(F.col("fragment_role").isin("CARRY", "INTERACT", "USE"))
        .withColumn("query_group", F.lit("G4"))
        .withColumn("logical_query_type", F.lit("Composite ERPQ"))
        .withColumn("stitch_mode", F.lit("ordered_behavior_chain"))
        .withColumn("constraint_tags", F.lit("event_order,binding_consistency,bounded_tw_continuity,epsilon_time,pattern_coverage"))
    )


def _build(events: DataFrame, config: Stage2Config, query_id: str):
    k = int(config.max_nexttw_hops)
    eps = float(config.epsilon_time_seconds)

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
    u = (
        events
        .filter(F.col("fragment_role") == F.lit("USE"))
        .filter(F.col("person_id").isNotNull() & F.col("vehicle_id").isNotNull())
        .dropDuplicates(["person_id", "vehicle_id", "tw_id", "start_epoch", "end_epoch", "edge_id"])
        .alias("u")
    )

    raw = (
        c.join(
            i,
            (F.col("c.person_id") == F.col("i.person_id")) &
            (F.col("i.start_epoch") >= F.col("c.end_epoch")) &
            ((F.col("i.tw_num") - F.col("c.tw_num")).between(F.lit(0), F.lit(k))) &
            (((F.col("i.start_epoch") - F.col("c.end_epoch")).cast("double")) <= F.lit(eps)),
            "inner",
        )
        .join(
            u,
            (F.col("i.person_id") == F.col("u.person_id")) &
            (F.col("u.start_epoch") >= F.col("i.end_epoch")) &
            ((F.col("u.tw_num") - F.col("i.tw_num")).between(F.lit(0), F.lit(k))) &
            (((F.col("u.start_epoch") - F.col("i.end_epoch")).cast("double")) <= F.lit(eps)),
            "inner",
        )
    )

    partition_array = F.array(F.col("c.partition_id"), F.col("i.partition_id"), F.col("u.partition_id"))

    candidates = (
        raw.select(
            F.lit(query_id).alias("query_id"),
            F.lit("G4").alias("query_group"),
            F.lit("Composite ERPQ").alias("logical_query_type"),
            F.lit("CARRIES · NEXT_TW* · INTERACTS_WITH · NEXT_TW* · USES").alias("rpq_expression"),
            F.col("c.person_id").alias("person_id"),
            F.col("c.thing_id").alias("thing_id"),
            F.col("i.other_person_id").alias("other_person_id"),
            F.col("u.vehicle_id").alias("vehicle_id"),
            F.col("c.tw_id").alias("carry_tw_id"),
            F.col("i.tw_id").alias("interact_tw_id"),
            F.col("u.tw_id").alias("use_tw_id"),
            F.least(F.col("c.start_ts"), F.col("i.start_ts"), F.col("u.start_ts")).alias("ts_start"),
            F.greatest(F.col("c.end_ts"), F.col("i.end_ts"), F.col("u.end_ts")).alias("ts_end"),
            F.concat_ws(",", F.array_sort(F.array_distinct(partition_array))).alias("partition_trace"),
            F.lit(3).cast("int").alias("num_fragments"),
            F.size(F.array_distinct(partition_array)).cast("int").alias("num_partitions"),
            ((F.col("i.start_epoch") - F.col("c.end_epoch")).cast("double")).alias("gap_carry_to_interact_seconds"),
            ((F.col("u.start_epoch") - F.col("i.end_epoch")).cast("double")).alias("gap_interact_to_use_seconds"),
            F.lit("event_order,binding_consistency,bounded_tw_continuity,epsilon_time,pattern_coverage").alias("constraint_tags"),
            F.lit(True).alias("is_valid"),
            F.lit("VALID").alias("validation_status"),
            F.lit("required ordered behavior roles matched within temporal bounds").alias("validation_reason"),
        )
        .dropDuplicates(COMPARE_COLUMNS_Q42[1:])
        .withColumn(
            "candidate_id",
            F.sha2(F.concat_ws("|", *[F.col(x).cast("string") for x in COMPARE_COLUMNS_Q42[1:]]), 256),
        )
    )

    witnesses = (
        candidates
        .withColumn("witness_id", F.sha2(F.concat_ws("|", F.col("candidate_id"), F.lit(query_id)), 256))
        .withColumn("witness_type", F.lit("ordered_behavior_chain"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q42_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(q42_local_event_extractor(vertices, base_edges, config, query_id), config, query_id)


def q42_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(atomic_events, config, query_id)


Q42_SPEC = QuerySpec(
    query_id="Q4.2",
    aliases=(),
    query_group="G4",
    query_name="Ordered Behavior Chain",
    logical_query_type="Composite ERPQ",
    rpq_expression="CARRIES · NEXT_TW* · INTERACTS_WITH · NEXT_TW* · USES",
    physical_plan="ordered_role_join_with_bounded_temporal_compatibility",
    witness_type="ordered_behavior_chain",
    compare_columns=COMPARE_COLUMNS_Q42,
    local_event_extractor=q42_local_event_extractor,
    baseline_evaluator=q42_baseline_global_evaluator,
    distributed_evaluator=q42_distributed_evaluator,
)

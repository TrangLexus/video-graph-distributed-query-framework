#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q3.1 — Multi-step Interaction Chain.

Vr09 logical intent:
    Bounded ERPQ / reachability over INTERACTS_WITH relations with endpoint
    compatibility and temporal order.

Physical implementation:
    Two-step endpoint-compatible interaction chain, implemented with bounded
    DataFrame joins. The output is existence-level per
    (source, intermediate, target), not all duplicate edge combinations, to avoid
    candidate/witness explosion.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q43 import q43_local_event_extractor

# Correctness is evaluated at the semantic endpoint-chain level.
# edge_trace/partition_trace are diagnostic columns and may legitimately differ
# between Baseline and Proposed because each strategy can choose a different
# representative edge pair for the same existence-level chain.
COMPARE_COLUMNS_Q31 = [
    "query_id",
    "source_person_id",
    "intermediate_person_id",
    "target_person_id",
]


def _interaction_events(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
    group: str,
    role: str = "INTERACTION_STEP",
) -> DataFrame:
    """Extract directed interaction-step fragments for bounded reachability."""
    return (
        q43_local_event_extractor(vertices, base_edges, config, query_id)
        .filter(F.col("fragment_role") == F.lit("INTERACT"))
        .filter(F.col("person_id").isNotNull() & F.col("other_person_id").isNotNull())
        .withColumn("query_group", F.lit(group))
        .withColumn("logical_query_type", F.lit("Bounded ERPQ / Reachability"))
        .withColumn("fragment_role", F.lit(role))
        .withColumn("stitch_mode", F.lit("endpoint_chain"))
        .withColumn("prev_key", F.col("person_id"))
        .withColumn("next_key", F.col("other_person_id"))
        .withColumn("stitch_key", F.col("person_id"))
        .withColumn("constraint_tags", F.lit("endpoint_compatibility,temporal_order,bounded_depth"))
        .dropDuplicates(["person_id", "other_person_id", "tw_id", "start_epoch", "end_epoch", "edge_id"])
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


def q31_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    return _interaction_events(vertices, base_edges, config, query_id, "G3")


def _build(events: DataFrame, config: Stage2Config, query_id: str):
    raw = _two_step_chain(events, config)

    path_rows = raw.select(
        F.col("a.person_id").alias("source_person_id"),
        F.col("a.other_person_id").alias("intermediate_person_id"),
        F.col("b.other_person_id").alias("target_person_id"),
        F.concat_ws("->", F.col("a.edge_id"), F.col("b.edge_id")).alias("edge_trace"),
        F.concat_ws("->", F.col("a.partition_id"), F.col("b.partition_id")).alias("partition_trace"),
        F.least(F.col("a.start_ts"), F.col("b.start_ts")).alias("ts_start"),
        F.greatest(F.col("a.end_ts"), F.col("b.end_ts")).alias("ts_end"),
        ((F.col("b.start_epoch") - F.col("a.end_epoch")).cast("double")).alias("max_time_gap_seconds"),
        F.size(F.array_distinct(F.array(F.col("a.partition_id"), F.col("b.partition_id")))).cast("int").alias("num_partitions"),
    )

    # Existence-level witness per endpoint chain. This avoids reporting millions
    # of equivalent edge-level witnesses for the same logical reachability fact.
    # Representative edge/partition traces are kept only for explanation and are
    # chosen deterministically; they are not part of the correctness key.
    candidates = (
        path_rows
        .groupBy("source_person_id", "intermediate_person_id", "target_person_id")
        .agg(
            F.min("edge_trace").alias("edge_trace"),
            F.min("partition_trace").alias("partition_trace"),
            F.min("ts_start").alias("ts_start"),
            F.max("ts_end").alias("ts_end"),
            F.min("max_time_gap_seconds").alias("max_time_gap_seconds"),
            F.max("num_partitions").cast("int").alias("num_partitions"),
        )
        .select(
            F.lit(query_id).alias("query_id"),
            F.lit("G3").alias("query_group"),
            F.lit("Bounded ERPQ / Reachability").alias("logical_query_type"),
            F.lit("INTERACTS_WITH · INTERACTS_WITH{1,max_depth-1}").alias("rpq_expression"),
            "source_person_id",
            "intermediate_person_id",
            "target_person_id",
            F.col("source_person_id").alias("person_id"),
            F.col("target_person_id").alias("other_person_id"),
            "edge_trace",
            "partition_trace",
            "ts_start",
            "ts_end",
            F.lit(2).cast("int").alias("num_fragments"),
            "num_partitions",
            "max_time_gap_seconds",
            F.lit("endpoint_compatibility,temporal_order,bounded_depth,no_cycle").alias("constraint_tags"),
            F.lit(True).alias("is_valid"),
            F.lit("VALID").alias("validation_status"),
            F.lit("two-step endpoint-compatible interaction chain").alias("validation_reason"),
        )
        .withColumn(
            "candidate_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(query_id),
                    F.col("source_person_id"),
                    F.col("intermediate_person_id"),
                    F.col("target_person_id"),
                ),
                256,
            ),
        )
    )

    witnesses = (
        candidates
        .withColumn("witness_id", F.col("candidate_id"))
        .withColumn("witness_type", F.lit("interaction_chain"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q31_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(q31_local_event_extractor(vertices, base_edges, config, query_id), config, query_id)


def q31_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(atomic_events, config, query_id)


Q31_SPEC = QuerySpec(
    query_id="Q3.1",
    aliases=(),
    query_group="G3",
    query_name="Multi-step Interaction Chain",
    logical_query_type="Bounded ERPQ / Reachability",
    rpq_expression="INTERACTS_WITH · INTERACTS_WITH{1,max_depth-1}",
    physical_plan="bounded_endpoint_compatible_self_join_with_endpoint_level_dedup",
    witness_type="interaction_chain",
    compare_columns=COMPARE_COLUMNS_Q31,
    local_event_extractor=q31_local_event_extractor,
    baseline_evaluator=q31_baseline_global_evaluator,
    distributed_evaluator=q31_distributed_evaluator,
)

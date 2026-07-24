#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q2.3 — Ordered Location Sequence Detection.

Vr09 logical intent:
    Detect an entity whose observation-context path follows a configured
    ordered location sequence under temporal order constraints.

The query remains an ERPQ logically. The physical implementation uses
DataFrame window ordering and a higher-order array scan to test ordered
subsequence membership without exploding into self-joins.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q11 import q11_local_event_extractor

COMPARE_COLUMNS_Q23 = [
    "query_id",
    "entity_type",
    "entity_id",
    "matched_location_sequence",
    "partition_trace",
]


def _target_sequence() -> list[str]:
    return [x.strip() for x in os.getenv("Q23_LOCATION_SEQUENCE", "L001,L003,L004").split(",") if x.strip()]


def q23_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    """LocalEval for Q2.3: emit ordered-location observation fragments."""
    return (
        q11_local_event_extractor(vertices, base_edges, config, query_id)
        .withColumn("query_group", F.lit("G2"))
        .withColumn("logical_query_type", F.lit("ERPQ"))
        .withColumn("fragment_role", F.lit("ORDERED_LOCATION"))
        .withColumn("stitch_mode", F.lit("ordered_path_sequence"))
        .withColumn("constraint_tags", F.lit("same_entity,ordered_locations,temporal_order,bounded_continuity"))
    )


def _ordered_subsequence_expr() -> F.Column:
    """Return Column expression checking matched_location_sequence ⊑ location_trace.

    The expression scans target locations left-to-right and advances the current
    position in location_trace. Unlike a naive array_position chain, it correctly
    handles repeated locations because every next search starts after the previous
    matched position. The accumulator position is explicitly typed as BIGINT
    because Spark's array_position returns BIGINT, while slice start is cast back
    to INT for SQL type compatibility.
    """
    return F.expr(
        """
        aggregate(
          matched_location_sequence,
          named_struct('pos', CAST(0 AS BIGINT), 'ok', true),
          (acc, x) ->
            named_struct(
              'pos',
              CASE
                WHEN acc.ok AND array_position(
                  slice(location_trace, CAST(acc.pos + 1 AS INT), size(location_trace)),
                  x
                ) > 0
                THEN acc.pos + array_position(
                  slice(location_trace, CAST(acc.pos + 1 AS INT), size(location_trace)),
                  x
                )
                ELSE acc.pos
              END,
              'ok',
              acc.ok AND array_position(
                slice(location_trace, CAST(acc.pos + 1 AS INT), size(location_trace)),
                x
              ) > 0
            ),
          acc -> acc.ok
        )
        """
    )


def _build(events: DataFrame, config: Stage2Config, query_id: str):
    target = _target_sequence()
    if not target:
        raise ValueError("Q23_LOCATION_SEQUENCE must contain at least one location.")
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
        .filter(F.col("entity_id").isNotNull())
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
        F.sort_array(
            F.collect_list(
                F.struct(
                    F.col("event_order").alias("event_order"),
                    F.col("location_id").cast("string").alias("location_id"),
                    F.col("partition_id").cast("string").alias("partition_id"),
                    F.col("tw_id").cast("string").alias("tw_id"),
                    F.col("camera_id").cast("string").alias("camera_id"),
                )
            )
        ).alias("_trace"),
        F.min("start_ts").alias("ts_start"),
        F.max("end_ts").alias("ts_end"),
        F.count(F.lit(1)).cast("int").alias("num_fragments"),
        F.countDistinct("partition_id").cast("int").alias("num_partitions"),
        F.max("tw_gap").cast("int").alias("max_tw_gap"),
        F.max("time_gap_seconds").cast("double").alias("max_time_gap_seconds"),
        F.concat_ws(",", F.sort_array(F.collect_set(F.col("partition_id").cast("string")))).alias("partition_trace"),
    )

    candidates = (
        grouped
        .withColumn("location_trace", F.transform(F.col("_trace"), lambda z: z["location_id"]))
        .withColumn("tw_trace", F.transform(F.col("_trace"), lambda z: z["tw_id"]))
        .withColumn("camera_trace", F.transform(F.col("_trace"), lambda z: z["camera_id"]))
        .withColumn("matched_location_sequence", F.array(*[F.lit(x) for x in target]))
        .withColumn(
            "is_valid",
            _ordered_subsequence_expr()
            & (F.col("max_tw_gap") >= F.lit(0))
            & (F.col("max_tw_gap") <= F.lit(k))
            & (F.col("max_time_gap_seconds") >= F.lit(0.0))
            & (F.col("max_time_gap_seconds") <= F.lit(eps)),
        )
        .drop("_trace")
        .withColumn(
            "validation_status",
            F.when(F.col("is_valid"), F.lit("VALID")).otherwise(F.lit("INVALID")),
        )
        .withColumn(
            "validation_reason",
            F.when(
                F.col("is_valid"),
                F.concat(
                    F.lit("ordered sequence found; max_tw_gap="), F.col("max_tw_gap"),
                    F.lit(",max_time_gap_seconds="), F.col("max_time_gap_seconds"),
                ),
            ).otherwise(F.lit("sequence/order/continuity constraint failed")),
        )
        .withColumn("query_id", F.lit(query_id))
        .withColumn("query_group", F.lit("G2"))
        .withColumn("logical_query_type", F.lit("ERPQ"))
        .withColumn(
            "rpq_expression",
            F.lit("observation_context · NEXT_TW_* with ordered location constraint"),
        )
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
        .withColumn("constraint_tags", F.lit("same_entity,ordered_locations,temporal_order,bounded_continuity"))
        .withColumn(
            "candidate_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("entity_type").cast("string"),
                    F.col("entity_id").cast("string"),
                    F.to_json(F.col("matched_location_sequence")),
                    F.to_json(F.col("location_trace")),
                    F.to_json(F.col("tw_trace")),
                    F.col("partition_trace").cast("string"),
                ),
                256,
            ),
        )
    )

    witnesses = (
        candidates
        .filter(F.col("is_valid"))
        .withColumn("witness_id", F.sha2(F.concat_ws("|", F.col("candidate_id"), F.lit(query_id)), 256))
        .withColumn("witness_type", F.lit("ordered_location_sequence"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q23_complete_local_candidates(candidates: DataFrame, config: Stage2Config) -> DataFrame:
    """A local fragment can cover the target only with enough observations."""
    del config
    return candidates.filter(F.col("num_fragments") >= F.lit(len(_target_sequence())))


def q23_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(q23_local_event_extractor(vertices, base_edges, config, query_id), config, query_id)


def q23_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(atomic_events, config, query_id)


Q23_SPEC = QuerySpec(
    query_id="Q2.3", query_group="G2", query_name="Ordered Location Sequence Detection",
    logical_query_type="ERPQ", rpq_expression="observation_context · NEXT_TW_ENTITY* with ordered location constraint",
    logical_query_description="Phát hiện một dãy location mục tiêu theo đúng thứ tự thời gian.",
    baseline_physical_plan="GLOBAL_MERGE_THEN_ORDERED_LOCATION_SUBSEQUENCE_SCAN",
    proposed_physical_plan="PARTITION_LOCATION_LOCALEVAL_THEN_ORDERED_SUBSEQUENCE_STITCHING",
    witness_type="ordered_location_sequence", correctness_compare_columns=COMPARE_COLUMNS_Q23,
    local_event_extractor=q23_local_event_extractor, baseline_evaluator=q23_baseline_global_evaluator,
    distributed_evaluator=q23_distributed_evaluator,
    local_candidate_completion_filter=q23_complete_local_candidates,
    automaton_transitions=(("q0","ORDERED_LOCATION","q1"),("q1","ORDERED_LOCATION","q1")), automaton_accepting_states=("q1",),
    required_fragment_roles=("ORDERED_LOCATION",), boundary_eligible_roles=("ORDERED_LOCATION",),
    stitch_mode="ORDERED_LOCATION_SEQUENCE", stitch_key_fields=("entity_type","entity_id"),
    stitch_role_order=("ORDERED_LOCATION",),
    boundary_interfaces=(("ORDERED_LOCATION","ORDERED_LOCATION",("entity_type","entity_id"),("entity_type","entity_id"),"FORWARD","q1","q1"),),
    hard_constraint_names=("same_entity","target_location_order","temporal_order","bounded_continuity"),
    candidate_dedup_columns=("candidate_id",),
    witness_dedup_columns=tuple(COMPARE_COLUMNS_Q23),
    runtime_parameter_schema={"target_location_sequence":"list[str]", "max_time_gap_seconds":"float"},
)

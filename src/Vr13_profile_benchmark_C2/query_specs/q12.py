#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q1.2 — Carry–Interact–Vehicle-use Sequence.

Vr09 logical pattern:
    APPEAR(P) · NEXT_TW_PERSON* · CARRIES(P,T)
      · NEXT_TW_PERSON* · INTERACTS_WITH(P,P2)
      · NEXT_TW_PERSON* · USES(P,VH)

Implementation policy:
- Keep the query as a composite ERPQ at the logical level.
- Implement it with bounded Spark DataFrame joins over LocalEval role fragments.
- Avoid APPEAR × CARRY explosion by selecting the nearest valid APPEAR for each
  CARRY event.
- Bound temporal joins by both max_nexttw_hops and epsilon_time_seconds.
- Preserve identical Baseline/Proposed semantics by using the same _build() logic.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec
from query_specs.q11 import q11_local_event_extractor
from query_specs.q43 import q43_local_event_extractor

COMPARE_COLUMNS_Q12 = [
    "query_id",
    "person_id",
    "other_person_id",
    "thing_id",
    "vehicle_id",
    "appear_tw_id",
    "carry_tw_id",
    "interact_tw_id",
    "use_tw_id",
]


def q12_local_event_extractor(
    vertices: DataFrame,
    base_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> DataFrame:
    """LocalEval for Q1.2.

    Emit APPEAR, CARRY, INTERACT, USE fragments. LocalEval is intentionally
    high-recall and does not decide complete pattern validity.
    """
    appear = (
        q11_local_event_extractor(vertices, base_edges, config, query_id)
        .filter(F.col("entity_type") == F.lit("PERSON"))
        .select(
            F.lit(query_id).alias("query_id"),
            F.lit("G1").alias("query_group"),
            F.lit("composite ERPQ").alias("logical_query_type"),
            F.lit("APPEAR").alias("fragment_role"),
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
            F.lit("composite_pattern").alias("stitch_mode"),
            F.lit("START").alias("prev_key"),
            F.lit("CARRY").alias("next_key"),
            F.lit("same_person,temporal_order,pattern_coverage").alias("constraint_tags"),
        )
    )

    semantic = (
        q43_local_event_extractor(vertices, base_edges, config, query_id)
        .filter(F.col("fragment_role").isin("CARRY", "INTERACT", "USE"))
        .withColumn("query_id", F.lit(query_id))
        .withColumn("query_group", F.lit("G1"))
        .withColumn("logical_query_type", F.lit("composite ERPQ"))
        .withColumn("stitch_key", F.col("person_id"))
        .withColumn("stitch_mode", F.lit("composite_pattern"))
        .withColumn(
            "prev_key",
            F.when(F.col("fragment_role") == F.lit("CARRY"), F.lit("APPEAR"))
             .when(F.col("fragment_role") == F.lit("INTERACT"), F.lit("CARRY"))
             .when(F.col("fragment_role") == F.lit("USE"), F.lit("INTERACT"))
             .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "next_key",
            F.when(F.col("fragment_role") == F.lit("CARRY"), F.lit("INTERACT"))
             .when(F.col("fragment_role") == F.lit("INTERACT"), F.lit("USE"))
             .when(F.col("fragment_role") == F.lit("USE"), F.lit("END"))
             .otherwise(F.lit(None).cast("string")),
        )
        .withColumn("constraint_tags", F.lit("same_person,temporal_order,pattern_coverage"))
    )

    return appear.unionByName(semantic, allowMissingColumns=True).dropDuplicates()


def _dedup_role_events(events: DataFrame) -> DataFrame:
    """Reduce duplicate role fragments without changing witness semantics."""
    role_cols = [
        "fragment_role", "person_id", "other_person_id", "thing_id", "vehicle_id",
        "tw_id", "tw_num", "start_epoch", "end_epoch", "partition_id",
        "location_id", "camera_id", "video_id", "edge_id",
    ]
    available = [c for c in role_cols if c in events.columns]
    return events.dropDuplicates(available)


def _build(events: DataFrame, config: Stage2Config, query_id: str):
    k = int(config.max_nexttw_hops)
    eps = float(config.epsilon_time_seconds)

    e = _dedup_role_events(events)

    a = e.filter(F.col("fragment_role") == F.lit("APPEAR")).alias("a")
    c = e.filter(F.col("fragment_role") == F.lit("CARRY")).alias("c")
    i = e.filter(F.col("fragment_role") == F.lit("INTERACT")).alias("i")
    u = e.filter(F.col("fragment_role") == F.lit("USE")).alias("u")

    # APPEAR is a binding/profile state. Choose one nearest APPEAR for each CARRY
    # instead of joining all historical appearances of P to all CARRY events.
    ac_candidates = (
        a.join(
            c,
            (F.col("a.person_id") == F.col("c.person_id")) &
            (F.col("a.start_epoch") <= F.col("c.start_epoch")) &
            ((F.col("c.tw_num") - F.col("a.tw_num")).between(F.lit(0), F.lit(k))),
            "inner",
        )
        .withColumn(
            "_appear_rank",
            F.row_number().over(
                Window.partitionBy(
                    F.col("c.person_id"),
                    F.col("c.thing_id"),
                    F.col("c.tw_id"),
                    F.col("c.edge_id"),
                ).orderBy(
                    F.col("a.tw_num").desc_nulls_last(),
                    F.col("a.start_epoch").desc_nulls_last(),
                    F.col("a.edge_id").asc_nulls_last(),
                )
            ),
        )
        .filter(F.col("_appear_rank") == F.lit(1))
        .drop("_appear_rank")
    )

    raw = (
        ac_candidates
        .join(
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
        .filter(F.col("i.other_person_id").isNotNull())
        .filter(F.col("u.vehicle_id").isNotNull())
    )

    partition_array = F.array(
        F.col("a.partition_id"),
        F.col("c.partition_id"),
        F.col("i.partition_id"),
        F.col("u.partition_id"),
    )

    candidates = (
        raw.select(
            F.lit(query_id).alias("query_id"),
            F.lit("G1").alias("query_group"),
            F.lit("composite ERPQ").alias("logical_query_type"),
            F.lit(
                "APPEAR(P)·NEXT_TW_PERSON*·CARRIES(P,T)"
                "·NEXT_TW_PERSON*·INTERACTS_WITH(P,P2)"
                "·NEXT_TW_PERSON*·USES(P,VH)"
            ).alias("rpq_expression"),
            F.col("c.person_id").alias("person_id"),
            F.col("i.other_person_id").alias("other_person_id"),
            F.col("c.thing_id").alias("thing_id"),
            F.col("u.vehicle_id").alias("vehicle_id"),
            F.col("a.tw_id").alias("appear_tw_id"),
            F.col("c.tw_id").alias("carry_tw_id"),
            F.col("i.tw_id").alias("interact_tw_id"),
            F.col("u.tw_id").alias("use_tw_id"),
            F.least(
                F.col("a.start_ts"),
                F.col("c.start_ts"),
                F.col("i.start_ts"),
                F.col("u.start_ts"),
            ).alias("ts_start"),
            F.greatest(
                F.col("a.end_ts"),
                F.col("c.end_ts"),
                F.col("i.end_ts"),
                F.col("u.end_ts"),
            ).alias("ts_end"),
            F.concat_ws(",", F.array_sort(F.array_distinct(partition_array))).alias("partition_trace"),
            F.lit(4).cast("int").alias("num_fragments"),
            F.size(F.array_distinct(partition_array)).cast("int").alias("num_partitions"),
            ((F.col("i.start_epoch") - F.col("c.end_epoch")).cast("double")).alias("gap_carry_to_interact_seconds"),
            ((F.col("u.start_epoch") - F.col("i.end_epoch")).cast("double")).alias("gap_interact_to_use_seconds"),
            F.lit("same_person,temporal_order,bounded_tw_continuity,epsilon_time,pattern_coverage").alias("constraint_tags"),
        )
        .withColumn("is_valid", F.lit(True))
        .withColumn("validation_status", F.lit("VALID"))
        .withColumn("validation_reason", F.lit("all required roles present in bounded temporal order"))
        .dropDuplicates(COMPARE_COLUMNS_Q12[1:])
        .withColumn(
            "candidate_id",
            F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in COMPARE_COLUMNS_Q12[1:]]), 256),
        )
    )

    witnesses = (
        candidates
        .withColumn("witness_id", F.sha2(F.concat_ws("|", F.col("candidate_id"), F.lit(query_id)), 256))
        .withColumn("witness_type", F.lit("composite_pattern"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q12_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(q12_local_event_extractor(vertices, base_edges, config, query_id), config, query_id)


def q12_distributed_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
):
    return _build(atomic_events, config, query_id)


Q12_SPEC = QuerySpec(
    query_id="Q1.2",
    aliases=(),
    query_group="G1",
    query_name="Carry–Interact–Vehicle-use Sequence",
    logical_query_type="Composite ERPQ",
    rpq_expression=(
        "APPEAR(P) · NEXT_TW_PERSON* · CARRIES(P,T) · "
        "NEXT_TW_PERSON* · INTERACTS_WITH(P,P2) · NEXT_TW_PERSON* · USES(P,VH)"
    ),
    physical_plan="role_fragment_extraction_then_bounded_temporal_binding_join",
    witness_type="composite_pattern",
    compare_columns=COMPARE_COLUMNS_Q12,
    local_event_extractor=q12_local_event_extractor,
    baseline_evaluator=q12_baseline_global_evaluator,
    distributed_evaluator=q12_distributed_evaluator,
    candidate_upper_bound=None,
)

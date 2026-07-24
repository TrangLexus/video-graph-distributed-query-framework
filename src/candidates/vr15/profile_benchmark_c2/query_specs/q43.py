#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Q4.3 — heterogeneous multi-entity behavior query."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import Stage2Config
from query_specs.common import QuerySpec, prepared_edges, typed_vertices


COMPARE_COLUMNS_Q43 = [
    "query_id",
    "person_id",
    "other_person_id",
    "thing_id",
    "vehicle_id",
    "carry1_src",
    "carry1_dst",
    "interact_src",
    "interact_dst",
    "carry2_src",
    "carry2_dst",
    "uses_src",
    "uses_dst",
    "carry1_start_time",
    "interact_start_time",
    "carry2_start_time",
    "uses_start_time",
]

# ---------------------------------------------------------------------------
# Shared Q4.3 preparation
# ---------------------------------------------------------------------------

_prepared_edges = prepared_edges
_vertices = typed_vertices


def _role_edges(vertices: DataFrame, edges: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    e = _prepared_edges(edges)
    persons = _vertices(vertices, "Person_TW", "person_tw_id", "person_id")
    things = _vertices(vertices, "Thing_TW", "thing_tw_id", "thing_id")
    vehicles = _vertices(vertices, "Vehicle_TW", "vehicle_tw_id", "vehicle_id")

    carries = (
        persons.alias("p")
        .join(e.filter(F.col("label") == "CARRIES").alias("e"), F.col("p.person_tw_id") == F.col("e.src"), "inner")
        .join(things.alias("t"), F.col("e.dst") == F.col("t.thing_tw_id"), "inner")
        .select(
            F.col("e.src"), F.col("e.dst"), F.col("p.person_id"), F.col("t.thing_id").alias("thing_id"),
            F.col("e.start_time"), F.col("e.end_time"), F.col("e.start_ts"), F.col("e.end_ts"),
            F.col("e.start_epoch"), F.col("e.end_epoch"), F.col("e.tw_id"), F.col("e.tw_num"),
            F.col("e.partition_id"), F.col("e.location_id"), F.col("e.camera_id"), F.col("e.video_id"),
            F.col("e.confidence"), F.col("e.edge_id"),
        )
        .dropDuplicates()
    )

    interacts_base = (
        persons.alias("p1")
        .join(e.filter(F.col("label") == "INTERACTS_WITH").alias("e"), F.col("p1.person_tw_id") == F.col("e.src"), "inner")
        .join(persons.alias("p2"), F.col("e.dst") == F.col("p2.person_tw_id"), "inner")
        .filter(F.col("p1.person_id") != F.col("p2.person_id"))
    )
    interacts_fwd = interacts_base.select(
        F.col("e.src"), F.col("e.dst"), F.col("p1.person_id"), F.col("p2.person_id").alias("other_person_id"),
        F.col("e.start_time"), F.col("e.end_time"), F.col("e.start_ts"), F.col("e.end_ts"),
        F.col("e.start_epoch"), F.col("e.end_epoch"), F.col("e.tw_id"), F.col("e.tw_num"),
        F.col("e.partition_id"), F.col("e.location_id"), F.col("e.camera_id"), F.col("e.video_id"),
        F.col("e.confidence"), F.col("e.edge_id"),
    )
    # Treat interactions as undirected for matching unless the data encodes both directions.
    interacts_rev = interacts_base.select(
        F.col("e.dst").alias("src"), F.col("e.src").alias("dst"),
        F.col("p2.person_id").alias("person_id"), F.col("p1.person_id").alias("other_person_id"),
        F.col("e.start_time"), F.col("e.end_time"), F.col("e.start_ts"), F.col("e.end_ts"),
        F.col("e.start_epoch"), F.col("e.end_epoch"), F.col("e.tw_id"), F.col("e.tw_num"),
        F.col("e.partition_id"), F.col("e.location_id"), F.col("e.camera_id"), F.col("e.video_id"),
        F.col("e.confidence"), F.col("e.edge_id"),
    )
    interacts = interacts_fwd.unionByName(interacts_rev, allowMissingColumns=True).dropDuplicates()

    uses = (
        persons.alias("p")
        .join(e.filter(F.col("label") == "USES").alias("e"), F.col("p.person_tw_id") == F.col("e.src"), "inner")
        .join(vehicles.alias("v"), F.col("e.dst") == F.col("v.vehicle_tw_id"), "inner")
        .select(
            F.col("e.src"), F.col("e.dst"), F.col("p.person_id"), F.col("v.vehicle_id"),
            F.col("e.start_time"), F.col("e.end_time"), F.col("e.start_ts"), F.col("e.end_ts"),
            F.col("e.start_epoch"), F.col("e.end_epoch"), F.col("e.tw_id"), F.col("e.tw_num"),
            F.col("e.partition_id"), F.col("e.location_id"), F.col("e.camera_id"), F.col("e.video_id"),
            F.col("e.confidence"), F.col("e.edge_id"),
        )
        .dropDuplicates()
    )
    return carries, interacts, uses


def _nexttw_reachability(vertices: DataFrame, next_tw_edges: DataFrame, vertex_label: str, edge_label: str, max_hops: int) -> DataFrame:
    nodes = (
        vertices
        .filter(F.col("label") == F.lit(vertex_label))
        .select(F.col("id").alias("rpq_src"), F.col("id").alias("rpq_dst"), F.col("global_id").alias("rpq_global_id"))
        .filter(F.col("rpq_src").isNotNull() & F.col("rpq_global_id").isNotNull())
        .withColumn("rpq_hops", F.lit(0).cast("int"))
    )
    result = nodes
    frontier = nodes
    edges = next_tw_edges.filter(F.col("label") == F.lit(edge_label)).select(F.col("src").alias("e_src"), F.col("dst").alias("e_dst"))
    id_global = vertices.select(F.col("id").alias("v_id"), F.col("global_id").alias("v_global_id")).dropDuplicates(["v_id"])

    for hop in range(1, int(max_hops) + 1):
        nxt = (
            frontier.alias("f")
            .join(edges.alias("e"), F.col("f.rpq_dst") == F.col("e.e_src"), "inner")
            .join(id_global.alias("g"), F.col("e.e_dst") == F.col("g.v_id"), "inner")
            .filter(F.col("f.rpq_global_id") == F.col("g.v_global_id"))
            .select(
                F.col("f.rpq_src"),
                F.col("e.e_dst").alias("rpq_dst"),
                F.col("f.rpq_global_id"),
                F.lit(hop).cast("int").alias("rpq_hops"),
            )
            .dropDuplicates(["rpq_src", "rpq_dst", "rpq_global_id"])
        )
        result = result.unionByName(nxt, allowMissingColumns=True).dropDuplicates(["rpq_src", "rpq_dst", "rpq_global_id"])
        frontier = nxt
    return result


def q43_local_event_extractor(vertices: DataFrame, base_edges: DataFrame, config: Stage2Config, query_id: str) -> DataFrame:
    """LocalEval: high-recall query-relevant fragment generation.

    LocalEval must NOT decide whether the complete Q4.3 pattern exists. For the
    two-step boundary-fragment algorithm, each partition only emits local
    evidence fragments that match at least one query relation: CARRIES,
    INTERACTS_WITH, or USES. Entity binding, temporal ordering, same-object
    checking, quick-exit validation, and cross-partition requirements are handled
    later by GlobalEval/Stitching and Hard Validation.
    """
    carries, interacts, uses = _role_edges(vertices, base_edges)

    carry_edges = carries.select(
        F.lit(query_id).alias("query_id"),
        F.lit("G4").alias("query_group"),
        F.lit("composite ERPQ").alias("logical_query_type"),
        F.lit("CARRY").alias("fragment_role"),
        "src", "dst", F.lit("CARRIES").alias("relation_type"),
        F.lit("BOUNDARY_OR_LOCAL_EVIDENCE").alias("fragment_scope"),
        "person_id", F.lit(None).cast("string").alias("other_person_id"), "thing_id",
        F.lit(None).cast("string").alias("vehicle_id"),
        "start_time", "end_time", "start_ts", "end_ts", "start_epoch", "end_epoch",
        "tw_id", "tw_num", "partition_id", "location_id", "camera_id", "video_id", "confidence", "edge_id",
    )
    interact_edges = interacts.select(
        F.lit(query_id).alias("query_id"),
        F.lit("G4").alias("query_group"),
        F.lit("composite ERPQ").alias("logical_query_type"),
        F.lit("INTERACT").alias("fragment_role"),
        "src", "dst", F.lit("INTERACTS_WITH").alias("relation_type"),
        F.lit("BOUNDARY_OR_LOCAL_EVIDENCE").alias("fragment_scope"),
        "person_id", "other_person_id", F.lit(None).cast("string").alias("thing_id"),
        F.lit(None).cast("string").alias("vehicle_id"),
        "start_time", "end_time", "start_ts", "end_ts", "start_epoch", "end_epoch",
        "tw_id", "tw_num", "partition_id", "location_id", "camera_id", "video_id", "confidence", "edge_id",
    )
    uses_edges = uses.select(
        F.lit(query_id).alias("query_id"),
        F.lit("G4").alias("query_group"),
        F.lit("composite ERPQ").alias("logical_query_type"),
        F.lit("USE").alias("fragment_role"),
        "src", "dst", F.lit("USES").alias("relation_type"),
        F.lit("BOUNDARY_OR_LOCAL_EVIDENCE").alias("fragment_scope"),
        "person_id", F.lit(None).cast("string").alias("other_person_id"),
        F.lit(None).cast("string").alias("thing_id"), "vehicle_id",
        "start_time", "end_time", "start_ts", "end_ts", "start_epoch", "end_epoch",
        "tw_id", "tw_num", "partition_id", "location_id", "camera_id", "video_id", "confidence", "edge_id",
    )
    return carry_edges.unionByName(interact_edges, allowMissingColumns=True).unionByName(uses_edges, allowMissingColumns=True).dropDuplicates()


def _q43_exit_role(config: Stage2Config) -> str:
    role = getattr(config, "exit_person_role", "P1")
    role = str(role or "P1").strip().upper()
    return role if role in {"P1", "P2"} else "P1"


def _finalize_q43_candidates(raw: DataFrame, config: Stage2Config, query_id: str) -> DataFrame:
    """Finalize Q4.3 candidates using the same hard-validation contract.

    Important alignment with the previously working benchmark files:
    - Default Q4.3 uses Vehicle by the original person P1.
    - P2 carries the same object after interaction.
    - Quick-exit delay is measured from interaction end to USE when exit_role=P1.
    - Baseline and Proposed call this same finalizer, so witness comparison is fair.
    """
    exit_role = _q43_exit_role(config)
    if exit_role == "P1":
        rpq_expression = (
            "CARRIES(P1,T) · NEXT_TW_PERSON* · INTERACTS_WITH(P1,P2) · "
            "NEXT_TW_PERSON* · CARRIES(P2,T) · NEXT_TW_PERSON* · USES(P1,VH)"
        )
        vehicle_user_ok = F.col("u.person_id") == F.col("c.person_id")
        # Default: measure quick exit after the handover is completed, approximated by CARRY_2.
        # Set QUICK_EXIT_REFERENCE_ROLE=INTERACT only when the experiment requires the stricter old variant.
        if getattr(config, "quick_exit_reference_role", "CARRY2") == "INTERACT":
            quick_exit_delay = (F.col("u.start_epoch") - F.col("i.end_epoch")).cast("double")
        else:
            quick_exit_delay = (F.col("u.start_epoch") - F.col("p2c.end_epoch")).cast("double")
    else:
        rpq_expression = (
            "CARRIES(P1,T) · NEXT_TW_PERSON* · INTERACTS_WITH(P1,P2) · "
            "NEXT_TW_PERSON* · CARRIES(P2,T) · NEXT_TW_PERSON* · USES(P2,VH)"
        )
        vehicle_user_ok = F.col("u.person_id") == F.col("i.other_person_id")
        quick_exit_delay = (F.col("u.start_epoch") - F.col("p2c.end_epoch")).cast("double")

    return (
        raw
        .withColumn("gap_carry1_to_interact_seconds", (F.col("i.start_epoch") - F.col("c.end_epoch")).cast("double"))
        .withColumn("gap_interact_to_carry2_seconds", (F.col("p2c.start_epoch") - F.col("i.end_epoch")).cast("double"))
        .withColumn("gap_carry2_to_uses_seconds", (F.col("u.start_epoch") - F.col("p2c.end_epoch")).cast("double"))
        .withColumn("quick_exit_delay_seconds", quick_exit_delay)
        .withColumn(
            "max_time_gap_seconds",
            F.greatest("gap_carry1_to_interact_seconds", "gap_interact_to_carry2_seconds", "gap_carry2_to_uses_seconds"),
        )
        .withColumn("same_thing_ok", F.col("c.thing_id") == F.col("p2c.thing_id"))
        .withColumn("distinct_persons_ok", F.col("c.person_id") != F.col("i.other_person_id"))
        .withColumn("handover_consistency_ok", F.col("p2c.person_id") == F.col("i.other_person_id"))
        .withColumn("vehicle_user_ok", vehicle_user_ok)
        .withColumn("temporal_order_ok",
            (F.col("gap_carry1_to_interact_seconds") >= F.lit(0.0)) &
            (F.col("gap_interact_to_carry2_seconds") >= F.lit(0.0)) &
            (F.col("gap_carry2_to_uses_seconds") >= F.lit(0.0))
        )
        .withColumn("quick_exit_ok", (F.col("quick_exit_delay_seconds") >= F.lit(0.0)) & (F.col("quick_exit_delay_seconds") <= F.lit(float(config.quick_exit_max_seconds))))
        .withColumn("max_time_gap_ok", F.col("max_time_gap_seconds") <= F.lit(float(config.max_time_gap_seconds)))
        .withColumn(
            "different_interaction_location_ok",
            F.when(
                F.lit(bool(getattr(config, "require_different_location", False))),
                F.when(
                    F.col("c.location_id").isNull() | F.col("i.location_id").isNull(),
                    F.lit(False),
                ).otherwise(F.col("c.location_id") != F.col("i.location_id")),
            ).otherwise(F.lit(True)),
        )
        .withColumn(
            "num_partitions",
            F.size(F.array_distinct(F.array("c.partition_id", "i.partition_id", "p2c.partition_id", "u.partition_id"))),
        )
        .withColumn("partition_count_ok", F.col("num_partitions") >= F.lit(int(getattr(config, "minimum_partition_count", 1))))
        .withColumn(
            "partition_trace",
            F.concat_ws("->", F.array_distinct(F.array("c.partition_id", "i.partition_id", "p2c.partition_id", "u.partition_id"))),
        )
        .withColumn("num_fragments", F.lit(4).cast("int"))
        .withColumn("pattern_coverage_ok", F.lit(True))
        .withColumn(
            "is_valid",
            F.col("same_thing_ok") &
            F.col("distinct_persons_ok") &
            F.col("handover_consistency_ok") &
            F.col("vehicle_user_ok") &
            F.col("temporal_order_ok") &
            F.col("max_time_gap_ok") &
            F.col("quick_exit_ok") &
            F.col("different_interaction_location_ok") &
            F.col("partition_count_ok") &
            F.col("pattern_coverage_ok"),
        )
        .withColumn("validation_status", F.when(F.col("is_valid"), F.lit("PASS")).otherwise(F.lit("FAIL")))
        .withColumn("validation_reason", F.when(F.col("is_valid"), F.lit("PASS")).otherwise(F.lit("FAILED_Q43_CONSTRAINTS")))
        .withColumn("constraint_tags", F.concat_ws(
            ",",
            F.lit("same_thing"),
            F.lit("distinct_persons"),
            F.lit("handover_consistency"),
            F.lit("temporal_order"),
            F.lit("quick_exit_delay"),
            F.lit("bounded_tw_continuity"),
            F.lit("pattern_coverage"),
            F.when(F.lit(bool(getattr(config, "require_different_location", False))), F.lit("different_interaction_location")),
            F.when(F.lit(int(getattr(config, "minimum_partition_count", 1)) > 1), F.lit("cross_partition")),
        ))
        .withColumn(
            "candidate_id",
            F.sha2(F.concat_ws("|", F.lit(query_id), F.col("c.src"), F.col("c.dst"), F.col("i.src"), F.col("i.dst"), F.col("p2c.src"), F.col("p2c.dst"), F.col("u.src"), F.col("u.dst")), 256),
        )
        .withColumn("ts_start", F.col("c.start_ts"))
        .withColumn("ts_end", F.col("u.end_ts"))
        .withColumn("_q43_tw_id_trace", F.concat_ws("->", F.col("c.tw_id"), F.col("i.tw_id"), F.col("p2c.tw_id"), F.col("u.tw_id")))
        .withColumn("_q43_location_id_trace", F.concat_ws("->", F.col("c.location_id"), F.col("i.location_id"), F.col("p2c.location_id"), F.col("u.location_id")))
        .withColumn("_q43_camera_id_trace", F.concat_ws("->", F.col("c.camera_id"), F.col("i.camera_id"), F.col("p2c.camera_id"), F.col("u.camera_id")))
        .withColumn("_q43_video_id_trace", F.concat_ws("->", F.col("c.video_id"), F.col("i.video_id"), F.col("p2c.video_id"), F.col("u.video_id")))
        .withColumn(
            "explanation_trace",
            F.concat_ws(
                " | ",
                F.concat(F.lit("CARRY_1:"), F.col("c.src"), F.lit("->"), F.col("c.dst")),
                F.concat(F.lit("INTERACT:"), F.col("i.src"), F.lit("->"), F.col("i.dst")),
                F.concat(F.lit("CARRY_2:"), F.col("p2c.src"), F.lit("->"), F.col("p2c.dst")),
                F.concat(F.lit("USE:"), F.col("u.src"), F.lit("->"), F.col("u.dst")),
            ),
        )
        .select(
            F.lit(query_id).alias("query_id"),
            F.lit("G4").alias("query_group"),
            F.lit("composite ERPQ / Q4.3").alias("logical_query_type"),
            F.lit(rpq_expression).alias("rpq_expression"),
            "candidate_id",
            F.col("c.person_id").alias("person_id"),
            F.col("i.other_person_id").alias("other_person_id"),
            F.col("c.thing_id").alias("thing_id"),
            F.col("u.vehicle_id").alias("vehicle_id"),
            F.col("c.src").alias("carry1_src"), F.col("c.dst").alias("carry1_dst"),
            F.col("i.src").alias("interact_src"), F.col("i.dst").alias("interact_dst"),
            F.col("p2c.src").alias("carry2_src"), F.col("p2c.dst").alias("carry2_dst"),
            F.col("u.src").alias("uses_src"), F.col("u.dst").alias("uses_dst"),
            F.col("c.start_time").alias("carry1_start_time"),
            F.col("i.start_time").alias("interact_start_time"),
            F.col("p2c.start_time").alias("carry2_start_time"),
            F.col("u.start_time").alias("uses_start_time"),
            "ts_start", "ts_end",
            F.col("_q43_tw_id_trace").alias("tw_id"),
            F.col("_q43_location_id_trace").alias("location_id"),
            F.col("_q43_camera_id_trace").alias("camera_id"),
            F.col("_q43_video_id_trace").alias("video_id"),
            "quick_exit_delay_seconds", "max_time_gap_seconds", "observed_max_time_window_gap",
            "num_fragments", "num_partitions", "partition_trace", "explanation_trace",
            "constraint_tags", "validation_status", "validation_reason", "is_valid",
        )
        .dropDuplicates(["candidate_id"])
    )



def _build_q43_raw_from_atomic_events(atomic_events: DataFrame, config: Stage2Config) -> DataFrame:
    """Build Q4.3 raw candidates from semantic role events.

    This version intentionally matches the previously validated benchmark behavior:
    - CARRY -> INTERACT -> P2_CARRY must satisfy temporal order.
    - USE is bound to the configured exit person, P1 by default.
    - Every stitched transition obeys temporal order and max_time_gap_seconds.
      The derived time-window gap is only a physical candidate-search bound.
    - quick_exit is measured from CARRY_2 end to USE by default, and USE must
      still occur after P2_CARRY. Set QUICK_EXIT_REFERENCE_ROLE=INTERACT for
      the stricter interaction-to-exit variant.
    """
    e = _prepared_edges(atomic_events)
    carries = e.filter(F.col("relation_type") == "CARRIES").dropDuplicates()
    interacts = e.filter(F.col("relation_type") == "INTERACTS_WITH").dropDuplicates()
    uses = e.filter(F.col("relation_type") == "USES").dropDuplicates()

    c = carries.alias("c")
    i = interacts.alias("i")
    p2c = carries.alias("p2c")
    u = uses.alias("u")
    k = int(config.candidate_pruning_max_time_window_gap)
    exit_role = _q43_exit_role(config)

    # Step 1-2: P1 carries T -> P1 interacts P2 -> P2 carries the same T.
    # Use temporal order and bounded TW continuity, not a hard 30-second gap between
    # all roles. The 30-second bound is reserved for quick exit.
    raw0 = (
        c
        .join(i, F.col("c.person_id") == F.col("i.person_id"), "inner")
        .filter(F.col("c.person_id") != F.col("i.other_person_id"))
        .filter(F.col("i.start_epoch") >= F.col("c.end_epoch"))
        .filter((F.col("i.start_epoch") - F.col("c.end_epoch")) <= F.lit(float(config.max_time_gap_seconds)))
        .filter((F.col("i.tw_num") - F.col("c.tw_num")).between(F.lit(0), F.lit(k)))
        .join(
            p2c,
            (F.col("p2c.person_id") == F.col("i.other_person_id")) &
            (F.col("p2c.thing_id") == F.col("c.thing_id")),
            "inner",
        )
        .filter(F.col("p2c.start_epoch") >= F.col("i.end_epoch"))
        .filter((F.col("p2c.start_epoch") - F.col("i.end_epoch")) <= F.lit(float(config.max_time_gap_seconds)))
        .filter((F.col("p2c.tw_num") - F.col("i.tw_num")).between(F.lit(0), F.lit(k)))
        .filter((F.col("p2c.tw_num") - F.col("c.tw_num")).between(F.lit(0), F.lit(k)))
    )

    if exit_role == "P1":
        raw = (
            raw0
            .join(u, F.col("u.person_id") == F.col("c.person_id"), "inner")
            .filter((F.col("u.tw_num") - F.col("p2c.tw_num")).between(F.lit(0), F.lit(k)))
            .filter(F.col("u.start_epoch") >= F.col("p2c.end_epoch"))
            .filter((F.col("u.start_epoch") - F.col("p2c.end_epoch")) <= F.lit(float(config.max_time_gap_seconds)))
            .filter(
                F.when(
                    F.lit(getattr(config, "quick_exit_reference_role", "CARRY2") == "INTERACT"),
                    (F.col("u.start_epoch") - F.col("i.end_epoch")).cast("double"),
                ).otherwise((F.col("u.start_epoch") - F.col("p2c.end_epoch")).cast("double"))
                <= F.lit(float(config.quick_exit_max_seconds))
            )
            .withColumn(
                "observed_max_time_window_gap",
                F.greatest(
                    (F.col("i.tw_num") - F.col("c.tw_num")).cast("int"),
                    (F.col("p2c.tw_num") - F.col("i.tw_num")).cast("int"),
                    (F.col("p2c.tw_num") - F.col("c.tw_num")).cast("int"),
                    (F.col("u.tw_num") - F.col("p2c.tw_num")).cast("int"),
                ),
            )
        )
    else:
        raw = (
            raw0
            .join(u, F.col("u.person_id") == F.col("i.other_person_id"), "inner")
            .filter((F.col("u.tw_num") - F.col("p2c.tw_num")).between(F.lit(0), F.lit(k)))
            .filter(F.col("u.start_epoch") >= F.col("p2c.end_epoch"))
            .filter((F.col("u.start_epoch") - F.col("p2c.end_epoch")) <= F.lit(float(config.max_time_gap_seconds)))
            .filter((F.col("u.start_epoch") - F.col("p2c.end_epoch")).cast("double") <= F.lit(float(config.quick_exit_max_seconds)))
            .withColumn(
                "observed_max_time_window_gap",
                F.greatest(
                    (F.col("i.tw_num") - F.col("c.tw_num")).cast("int"),
                    (F.col("p2c.tw_num") - F.col("i.tw_num")).cast("int"),
                    (F.col("p2c.tw_num") - F.col("c.tw_num")).cast("int"),
                    (F.col("u.tw_num") - F.col("p2c.tw_num")).cast("int"),
                ),
            )
        )
    return raw


def _events_from_global_graph(vertices: DataFrame, base_edges: DataFrame, config: Stage2Config, query_id: str) -> DataFrame:
    """Extract global semantic events for Baseline using the same LocalEval extractor.

    Baseline vẫn trả chi phí global merge/materialization của base graph trong
    baseline.py, nhưng NEXT_TW chỉ là predicate liên tục logic, không phải cạnh vật lý.
    Query dùng cùng role-event semantics với Proposed để bảo đảm fairness.
    """
    return q43_local_event_extractor(vertices, base_edges, config, query_id)



def _maybe_print_q43_diagnostics(atomic_events: DataFrame, config: Stage2Config, label_prefix: str = "q43") -> None:
    """Print Q4.3 stage-by-stage diagnostic counts when enabled.

    This is intentionally action-heavy and must be used only in correctness/debug mode.
    It helps identify whether candidates disappear at role extraction, binding,
    temporal ordering, location filtering, vehicle binding, or partition validation.
    """
    if not bool(getattr(config, "query_diagnostics_enabled", False)):
        return
    try:
        e = _prepared_edges(atomic_events)
        carry = e.filter(F.col("relation_type") == "CARRIES").dropDuplicates()
        inter = e.filter(F.col("relation_type") == "INTERACTS_WITH").dropDuplicates()
        uses = e.filter(F.col("relation_type") == "USES").dropDuplicates()
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_role_carries = {carry.count()}")
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_role_interacts = {inter.count()}")
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_role_uses = {uses.count()}")

        c = carry.alias("c")
        i = inter.alias("i")
        p2c = carry.alias("p2c")
        u = uses.alias("u")
        k = int(config.candidate_pruning_max_time_window_gap)

        ci_binding = (
            c.join(i, F.col("c.person_id") == F.col("i.person_id"), "inner")
             .filter(F.col("c.person_id") != F.col("i.other_person_id"))
        )
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_join_ci_binding = {ci_binding.count()}")

        ci_temporal = (
            ci_binding
            .filter(F.col("i.start_epoch") >= F.col("c.end_epoch"))
            .filter((F.col("i.tw_num") - F.col("c.tw_num")).between(F.lit(0), F.lit(k)))
        )
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_join_ci_temporal_tw = {ci_temporal.count()}")

        ci_spatial_diff = ci_temporal.filter(
            F.col("c.location_id").isNotNull() &
            F.col("i.location_id").isNotNull() &
            (F.col("c.location_id") != F.col("i.location_id"))
        )
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_join_ci_spatial_different_location = {ci_spatial_diff.count()}")

        cip = (
            ci_temporal
            .join(
                p2c,
                (F.col("p2c.person_id") == F.col("i.other_person_id")) &
                (F.col("p2c.thing_id") == F.col("c.thing_id")),
                "inner",
            )
            .filter(F.col("p2c.start_epoch") >= F.col("i.end_epoch"))
            .filter((F.col("p2c.tw_num") - F.col("i.tw_num")).between(F.lit(0), F.lit(k)))
            .filter((F.col("p2c.tw_num") - F.col("c.tw_num")).between(F.lit(0), F.lit(k)))
        )
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_join_cip = {cip.count()}")

        if _q43_exit_role(config) == "P1":
            full_person = cip.join(u, F.col("u.person_id") == F.col("c.person_id"), "inner")
        else:
            full_person = cip.join(u, F.col("u.person_id") == F.col("i.other_person_id"), "inner")
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_join_full_person = {full_person.count()}")

        if getattr(config, "quick_exit_reference_role", "CARRY2") == "INTERACT":
            quick_delay = (F.col("u.start_epoch") - F.col("i.end_epoch")).cast("double")
        else:
            quick_delay = (F.col("u.start_epoch") - F.col("p2c.end_epoch")).cast("double")
        full_temporal = (
            full_person
            .filter((F.col("u.tw_num") - F.col("p2c.tw_num")).between(F.lit(0), F.lit(k)))
            .filter(F.col("u.start_epoch") >= F.col("p2c.end_epoch"))
            .filter(quick_delay <= F.lit(float(config.quick_exit_max_seconds)))
        )
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_join_full_temporal_quick_exit = {full_temporal.count()}")

        full_partition = full_temporal.withColumn(
            "_num_partitions",
            F.size(F.array_distinct(F.array("c.partition_id", "i.partition_id", "p2c.partition_id", "u.partition_id"))),
        ).filter(F.col("_num_partitions") >= F.lit(int(getattr(config, "minimum_partition_count", 1))))
        print(f"[Q4.3-DIAG] {label_prefix}.num_q43_join_full_partition_valid = {full_partition.count()}")
    except Exception as exc:
        print(f"[Q4.3-DIAG] {label_prefix}.diagnostics_failed = {repr(exc)}")


def q43_baseline_global_evaluator(
    vertices: DataFrame,
    base_edges: DataFrame,
    next_tw_edges: DataFrame,
    config: Stage2Config,
    query_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Baseline evaluator: global graph first, same Q4.3 logic as Proposed.

    Baseline thực hiện Union/Merge và materialize global base graph trước khi
    evaluator được gọi; NEXT_TW chỉ là predicate thời gian logic. Evaluator dùng cùng
    bounded role-event query với Proposed để so sánh cùng một witness semantics.
    """
    global_events = _events_from_global_graph(vertices, base_edges, config, query_id)
    _maybe_print_q43_diagnostics(global_events, config, f"baseline.{query_id}")
    raw = _build_q43_raw_from_atomic_events(global_events, config)
    candidates = _finalize_q43_candidates(raw, config, query_id)
    witnesses = (
        candidates
        .filter(F.col("is_valid") == F.lit(True))
        .withColumn("witness_id", F.sha2(F.concat_ws("|", F.col("candidate_id"), F.col("person_id"), F.col("other_person_id"), F.col("thing_id"), F.col("vehicle_id")), 256))
        .withColumn("witness_type", F.lit("q43_handover_quick_exit"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q43_distributed_atomic_evaluator(
    vertices: DataFrame,
    atomic_events: DataFrame,
    next_tw_edges: DataFrame | None,
    config: Stage2Config,
    query_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Proposed evaluator: LocalEval fragments + same bounded Q4.3 logic."""
    _maybe_print_q43_diagnostics(atomic_events, config, f"proposed.{query_id}")
    raw = _build_q43_raw_from_atomic_events(atomic_events, config)
    candidates = _finalize_q43_candidates(raw, config, query_id)
    witnesses = (
        candidates
        .filter(F.col("is_valid") == F.lit(True))
        .withColumn("witness_id", F.sha2(F.concat_ws("|", F.col("candidate_id"), F.col("person_id"), F.col("other_person_id"), F.col("thing_id"), F.col("vehicle_id")), 256))
        .withColumn("witness_type", F.lit("q43_handover_quick_exit"))
        .select("witness_id", "witness_type", *candidates.columns)
    )
    return candidates, witnesses


def q43_candidate_upper_bound_without_nexttw(atomic_events: DataFrame, config: Stage2Config, query_id: str) -> DataFrame:
    """High-recall boundary-evidence summary before GlobalEval/Stitching.

    This function intentionally does NOT assemble the full Q4.3 pattern. It only
    summarizes query-relevant fragments that may participate in a global witness.
    The complete witness construction is performed by
    q43_distributed_atomic_evaluator() / _build_q43_raw_from_atomic_events().

    Rationale: in the two-step algorithm, LocalEval keeps fragments containing at
    least one query relation. A zero full-pattern join at this stage must never
    shortcut the final result because valid witnesses may require GlobalEval /
    stitching over boundary fragments from multiple partitions/time windows.
    """
    e = _prepared_edges(atomic_events)
    return (
        e.filter(F.col("relation_type").isin("CARRIES", "INTERACTS_WITH", "USES"))
        .select(
            F.col("person_id").cast("string").alias("person_id"),
            F.col("other_person_id").cast("string").alias("other_person_id"),
            F.col("thing_id").cast("string").alias("thing_id"),
            F.col("vehicle_id").cast("string").alias("vehicle_id"),
        )
        .dropDuplicates()
    )

Q43_SPEC = QuerySpec(
    query_id="Q4.3", query_group="G4",
    query_name="Cross-partition Object Handover Followed by Quick Exit",
    logical_query_type="Boundary-fragment composite ERPQ",
    rpq_expression="CARRIES(P1,T) · NEXT_TW_PERSON* · INTERACTS_WITH(P1,P2) · NEXT_TW_PERSON* · CARRIES(P2,T) · NEXT_TW_PERSON* · USES(P1,VH)",
    logical_query_description="Người P1 mang T, tương tác P2, P2 mang cùng T, sau đó P1 sử dụng phương tiện trong ngưỡng quick exit.",
    baseline_physical_plan="GLOBAL_MERGE_THEN_FOUR_ROLE_BOUNDED_JOIN_AND_VALIDATION",
    proposed_physical_plan="PARTITION_ROLE_LOCALEVAL_THEN_CONSTRAINED_BOUNDARY_STITCHING",
    witness_type="q43_handover_quick_exit", correctness_compare_columns=COMPARE_COLUMNS_Q43,
    local_event_extractor=q43_local_event_extractor,
    baseline_evaluator=q43_baseline_global_evaluator,
    distributed_evaluator=q43_distributed_atomic_evaluator,
    candidate_upper_bound=q43_candidate_upper_bound_without_nexttw,
    automaton_start_state="q0", automaton_accepting_states=("q4",),
    automaton_transitions=(("q0","CARRY_P1_THING","q1"),("q1","INTERACT_P1_P2","q2"),("q2","CARRY_P2_SAME_THING","q3"),("q3","USE_P1_VEHICLE","q4")),
    required_fragment_roles=("CARRY_P1_THING","INTERACT_P1_P2","CARRY_P2_SAME_THING","USE_P1_VEHICLE"),
    optional_fragment_roles=("SUPPORTING_CONTINUITY",),
    boundary_eligible_roles=("CARRY","INTERACT","USE","CARRY_P1_THING","INTERACT_P1_P2","CARRY_P2_SAME_THING","USE_P1_VEHICLE"),
    stitch_mode="HANDOVER_QUICK_EXIT", stitch_key_fields=("person_id","thing_id"),
    stitch_role_order=("CARRY_P1_THING","INTERACT_P1_P2","CARRY_P2_SAME_THING","USE_P1_VEHICLE"),
    boundary_interfaces=(("CARRY","INTERACT",("person_id",),("person_id",),"FORWARD","q1","q1"),("INTERACT","CARRY",("other_person_id",),("person_id",),"FORWARD","q2","q2"),("CARRY","USE",("person_id",),("person_id",),"FORWARD","q1","q3"),("INTERACT","USE",("person_id",),("person_id",),"FORWARD","q2","q3")),
    hard_constraint_names=("required_pattern_coverage","same_primary_person","same_thing","distinct_persons","temporal_order","max_time_gap_seconds","quick_exit","minimum_partition_count","optional_different_location","automaton_consistency"),
    candidate_dedup_columns=("candidate_id",),
    witness_dedup_columns=tuple(COMPARE_COLUMNS_Q43),
    runtime_parameter_schema={"max_time_gap_seconds":"float","quick_exit_max_seconds":"float","exit_person_role":"P1|P2","minimum_partition_count":"int","require_different_location":"bool","quick_exit_reference_role":"INTERACT|CARRY2"},
)

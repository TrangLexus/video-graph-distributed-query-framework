#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Proposed Vr15: Parallel LocalEval -> Fragment Stitching -> Hard Validation."""
from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import Stage2Config, ensure_supported_query_ids, load_config
from graph_io import (
    basename_uri,
    build_entity_partition_index,
    create_spark,
    get_spark_session,
    load_partition_tables,
    materialize_df,
    normalize_base_edges,
    normalize_vertices,
    persist_df,
    print_config,
    safe_count,
    union_dataframes,
    unpersist_all,
    write_runtime_summary,
    write_spark_csv,
)
from queries import QUERY_REGISTRY, empty_candidates_like, empty_witnesses_like
from query_specs.common import QuerySpec, add_missing_columns

METHOD_NAME = "proposed_distributed_boundary_erpq"


def _dedup_columns(spec: QuerySpec, kind: str, df: DataFrame) -> list[str]:
    """Lấy khóa dedup ngữ nghĩa đã được QuerySpec chuẩn hóa."""
    resolver = getattr(spec, "resolve_dedup_columns", None)
    if callable(resolver):
        return list(resolver(kind, df.columns))

    attr = f"{kind}_dedup_columns"
    configured = [
        name for name in (getattr(spec, attr, ()) or ())
        if name in df.columns
    ]
    if configured:
        return configured

    semantic = [
        name for name in spec.correctness_compare_columns
        if name in df.columns
    ]
    if semantic:
        return semantic

    fallback = "candidate_id" if kind == "candidate" else "witness_id"
    if fallback in df.columns:
        return [fallback]
    raise ValueError(
        f"QuerySpec {spec.query_id} không có khóa dedup hợp lệ cho {kind}"
    )


def _deduplicate(df: DataFrame, spec: QuerySpec, kind: str) -> DataFrame:
    return df.dropDuplicates(_dedup_columns(spec, kind, df))


def _finalize_merged_candidates_if_supported(
    *,
    spec: QuerySpec,
    vertices: DataFrame,
    merged_candidates: DataFrame,
    config: Stage2Config,
    query_id: str,
    fallback_witnesses: DataFrame,
) -> tuple[DataFrame, str]:
    """Tạo final witness từ merged candidates khi QuerySpec cung cấp finalizer.

    API mở rộng khuyến nghị cho query_specs/common.py:
      finalize_merged_candidates(vertices, candidates, config, query_id) -> witnesses

    Với QuerySpec Vr15 cũ chưa có API này, giữ đường tương thích cũ nhưng ghi
    rõ chế độ trong runtime summary để không tuyên bố sai rằng đã HardValidate
    trực tiếp trên merged candidates.
    """
    finalizer = getattr(spec, "finalize_merged_candidates", None)
    if callable(finalizer):
        witnesses = finalizer(
            vertices=vertices,
            candidates=merged_candidates,
            config=config,
            query_id=query_id,
        )
        return (
            _deduplicate(witnesses, spec, "witness"),
            "QUERY_SPEC_FINALIZE_MERGED_CANDIDATES",
        )

    return (
        _deduplicate(fallback_witnesses, spec, "witness"),
        "LEGACY_UNION_OF_LOCAL_AND_STITCH_VALIDATED_WITNESSES",
    )


@dataclass
class PartitionLocalEvalResult:
    partition_name: str
    events: DataFrame
    complete_candidates: DataFrame
    locally_validated_witnesses: DataFrame
    num_query_relevant_atomic_evidence: int
    num_complete_local_candidates: int
    num_locally_validated_witnesses: int
    submit_offset_seconds: float
    job_elapsed_seconds: float
    completion_offset_seconds: float
    status: str


def _fragment_state_maps(spec: QuerySpec) -> tuple[dict[str, str], dict[str, str]]:
    entry: dict[str, str] = {}
    exit_state: dict[str, str] = {}
    for q_in, role, q_out in spec.automaton_transitions:
        entry.setdefault(role, q_in)
        exit_state.setdefault(role, q_out)
    return entry, exit_state


def _canonicalize_events(df: DataFrame, spec: QuerySpec, partition_name: str) -> DataFrame:
    required = {
        "fragment_role": "string", "relation_type": "string",
        "entity_type": "string", "entity_id": "string",
        "entity_tw_id": "string",
        "person_id": "string", "other_person_id": "string",
        "thing_id": "string", "vehicle_id": "string",
        "src": "string", "dst": "string", "edge_id": "string",
        "start_time": "string", "end_time": "string",
        "start_ts": "timestamp", "end_ts": "timestamp",
        "start_epoch": "long", "end_epoch": "long",
        "tw_id": "string", "tw_num": "int", "partition_id": "string",
        "location_id": "string", "camera_id": "string", "video_id": "string",
        "stitch_key": "string", "prev_key": "string", "next_key": "string",
    }
    out = add_missing_columns(df, required)
    entry_map, exit_map = _fragment_state_maps(spec)
    entry_expr = F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in entry_map.items()), [])) if entry_map else None
    exit_expr = F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in exit_map.items()), [])) if exit_map else None
    out = (
        out
        .withColumn("source_partition_name", F.lit(partition_name))
        .withColumn("partition_id", F.coalesce(F.col("partition_id"), F.lit(partition_name)))
        .withColumn("entry_state", F.element_at(entry_expr, F.col("fragment_role")) if entry_expr is not None else F.lit(None).cast("string"))
        .withColumn("exit_state", F.element_at(exit_expr, F.col("fragment_role")) if exit_expr is not None else F.lit(None).cast("string"))
        .withColumn("endpoint_in", F.coalesce(F.col("prev_key"), F.col("src"), F.col("person_id")))
        .withColumn("endpoint_out", F.coalesce(F.col("next_key"), F.col("dst"), F.col("other_person_id"), F.col("person_id")))
        .withColumn("is_complete", F.lit(False))
        .withColumn("is_true_boundary", F.lit(False))
        .withColumn("requires_external_evidence", F.lit(False))
        .withColumn("fragment_class", F.lit("BOUNDARY_CANDIDATE"))
        .withColumn("boundary_type", F.lit(None).cast("string"))
        .withColumn("boundary_reason", F.lit(None).cast("string"))
        # logical_fragment_id không chứa partition. Vì vậy cùng một bằng
        # chứng bị replicate sang nhiều partition vẫn có cùng logical id và
        # không bị hiểu sai thành continuation xuyên partition.
        .withColumn(
            "logical_fragment_id",
            F.sha2(F.concat_ws(
                "|", F.lit(spec.query_id),
                F.coalesce(F.col("fragment_role"), F.lit("")),
                F.coalesce(F.col("relation_type"), F.lit("")),
                # Không dùng edge_id: replicated semantic evidence có thể được
                # gán edge_id vật lý khác nhau ở từng partition.
                F.coalesce(F.col("entity_type"), F.lit("")),
                F.coalesce(F.col("entity_id"), F.lit("")),
                F.coalesce(F.col("entity_tw_id"), F.lit("")),
                F.coalesce(F.col("person_id"), F.lit("")),
                F.coalesce(F.col("other_person_id"), F.lit("")),
                F.coalesce(F.col("thing_id"), F.lit("")),
                F.coalesce(F.col("vehicle_id"), F.lit("")),
                F.coalesce(F.col("src"), F.lit("")),
                F.coalesce(F.col("dst"), F.lit("")),
                F.coalesce(F.col("tw_id"), F.lit("")),
                F.coalesce(F.col("start_epoch").cast("string"), F.lit("")),
                F.coalesce(F.col("end_epoch").cast("string"), F.lit("")),
            ), 256),
        )
        .withColumn(
            "canonical_fragment_id",
            F.sha2(F.concat_ws(
                "|", F.col("logical_fragment_id"), F.lit(partition_name)
            ), 256),
        )
    )
    return out.dropDuplicates(["canonical_fragment_id"])


def _local_role_pairs(spec: QuerySpec) -> list[tuple[str, str]]:
    explicit = list(getattr(spec, "boundary_role_pairs", ()) or ())
    if explicit:
        return sorted(set(explicit))

    order = list(spec.stitch_role_order or spec.required_fragment_roles)
    pairs = [(order[i], order[i + 1]) for i in range(len(order) - 1)]
    if len(order) == 1:
        pairs.append((order[0], order[0]))
    return sorted(set(pairs))


def _boundary_identity_events(
    all_events: DataFrame,
    spec: QuerySpec,
) -> DataFrame:
    """Chuẩn hóa interface nối thành khóa đẳng trị có kiểu.

    Không dùng điều kiện OR giữa person_id/thing_id/vehicle_id trong join.
    Mỗi fragment được chiếu thành một hoặc nhiều khóa identity có kiểu, nhưng
    chỉ từ các trường được QuerySpec khai báo trong stitch_key_fields.
    """
    base_cols = [
        "canonical_fragment_id",
        "logical_fragment_id",
        "fragment_role",
        "source_partition_name",
        "entity_tw_id",
        "tw_num",
        "start_epoch",
        "end_epoch",
    ]
    requested = set(spec.stitch_key_fields or ())
    frames: list[DataFrame] = []

    # Các query trajectory/camera/location/continuity dùng khóa ghép
    # (entity_type, entity_id), tránh va chạm PERSON/THING/VEHICLE cùng id.
    if {"entity_type", "entity_id"}.issubset(requested) and {
        "entity_type", "entity_id"
    }.issubset(set(all_events.columns)):
        frames.append(
            all_events.select(
                *base_cols,
                F.concat_ws(
                    ":",
                    F.upper(F.col("entity_type").cast("string")),
                    F.col("entity_id").cast("string"),
                ).alias("boundary_identity_key"),
            ).filter(F.col("entity_id").isNotNull())
        )

    # Với chuỗi tương tác, person_id và other_person_id đều là endpoint người.
    if requested.intersection({"person_id", "other_person_id"}):
        for col_name in ("person_id", "other_person_id"):
            if col_name in all_events.columns:
                frames.append(
                    all_events.select(
                        *base_cols,
                        F.concat(
                            F.lit("PERSON:"),
                            F.col(col_name).cast("string"),
                        ).alias("boundary_identity_key"),
                    ).filter(F.col(col_name).isNotNull())
                )

    if "thing_id" in requested and "thing_id" in all_events.columns:
        frames.append(
            all_events.select(
                *base_cols,
                F.concat(
                    F.lit("THING:"),
                    F.col("thing_id").cast("string"),
                ).alias("boundary_identity_key"),
            ).filter(F.col("thing_id").isNotNull())
        )

    if "vehicle_id" in requested and "vehicle_id" in all_events.columns:
        frames.append(
            all_events.select(
                *base_cols,
                F.concat(
                    F.lit("VEHICLE:"),
                    F.col("vehicle_id").cast("string"),
                ).alias("boundary_identity_key"),
            ).filter(F.col("vehicle_id").isNotNull())
        )

    # Fallback chỉ dùng khi QuerySpec chưa khai báo được trường identity cụ thể.
    if not frames and "stitch_key" in all_events.columns:
        frames.append(
            all_events.select(
                *base_cols,
                F.concat(
                    F.lit("STITCH:"),
                    F.col("stitch_key").cast("string"),
                ).alias("boundary_identity_key"),
            ).filter(F.col("stitch_key").isNotNull())
        )

    if not frames:
        # Schema rỗng nhưng vẫn giữ đúng các cột mà classifier cần.
        return (
            all_events.limit(0)
            .select(*base_cols)
            .withColumn("boundary_identity_key", F.lit(None).cast("string"))
        )

    return (
        union_dataframes(frames)
        .filter(
            F.col("boundary_identity_key").isNotNull()
            & F.col("fragment_role").isNotNull()
            & F.col("source_partition_name").isNotNull()
        )
        .dropDuplicates(
            ["canonical_fragment_id", "boundary_identity_key"]
        )
    )


def _classify_true_boundary_fragments(
    all_events: DataFrame,
    spec: QuerySpec,
    config: Stage2Config,
) -> DataFrame:
    """Phân loại true-boundary bằng equi-join có chặn TimeWindow.

    Thay cho self-join toàn bộ fragments với điều kiện identity dạng OR.
    Physical plan mới:

      fragment -> typed identity key
               -> role-compatible source fragment
               -> explode tối đa K+1 TimeWindow đích
               -> equi-join trên
                  (right_role, identity_key, target_tw_num)
               -> kiểm tra khác partition và exact timestamp gap

    Số dòng nguồn tăng tối đa K+1 lần thay vì tạo tích Descartes N x N.
    """
    spark = get_spark_session(all_events)
    role_pairs = _local_role_pairs(spec)
    eligible_roles = tuple(spec.boundary_eligible_roles or ())

    if not role_pairs or not eligible_roles:
        return (
            all_events
            .withColumn("is_true_boundary", F.lit(False))
            .withColumn("requires_external_evidence", F.lit(False))
            .withColumn("fragment_class", F.lit("DEAD_END"))
            .withColumn("boundary_type", F.lit(None).cast("string"))
            .withColumn(
                "boundary_reason",
                F.lit("NO_QUERY_BOUNDARY_INTERFACE"),
            )
        )

    k = int(config.candidate_pruning_max_time_window_gap)
    pair_df = spark.createDataFrame(
        role_pairs,
        ["left_role", "right_role"],
    )

    identity_events = (
        _boundary_identity_events(all_events, spec)
        .filter(F.col("fragment_role").isin(*eligible_roles))
        .filter(F.col("tw_num").isNotNull())
    )

    # Mỗi source fragment chỉ phát sinh K+1 khóa TimeWindow đích. Với cấu hình
    # hiện tại K=12, 422 nghìn fragments tạo khoảng 5,5 triệu khóa, thay vì
    # self-join gần 178 tỷ cặp trong trường hợp xấu nhất.
    source = (
        identity_events.alias("e")
        .join(
            F.broadcast(pair_df).alias("p"),
            F.col("e.fragment_role") == F.col("p.left_role"),
            "inner",
        )
        .select(
            F.col("e.canonical_fragment_id").alias("left_fragment_id"),
            F.col("e.logical_fragment_id").alias("left_logical_fragment_id"),
            F.col("e.source_partition_name").alias("left_partition"),
            F.col("e.boundary_identity_key").alias("boundary_identity_key"),
            F.col("e.fragment_role").alias("left_role"),
            F.col("p.right_role").alias("right_role"),
            F.col("e.entity_tw_id").alias("left_entity_tw_id"),
            F.col("e.tw_num").alias("left_tw_num"),
            F.col("e.end_epoch").alias("left_end_epoch"),
            F.explode(
                F.sequence(
                    F.col("e.tw_num"),
                    F.col("e.tw_num") + F.lit(k),
                )
            ).alias("target_tw_num"),
        )
    )

    target = identity_events.select(
        F.col("canonical_fragment_id").alias("right_fragment_id"),
        F.col("logical_fragment_id").alias("right_logical_fragment_id"),
        F.col("source_partition_name").alias("right_partition"),
        F.col("boundary_identity_key"),
        F.col("fragment_role").alias("right_role"),
        F.col("entity_tw_id").alias("right_entity_tw_id"),
        F.col("tw_num").alias("target_tw_num"),
        F.col("start_epoch").alias("right_start_epoch"),
    )

    joined = source.join(
        target,
        ["boundary_identity_key", "right_role", "target_tw_num"],
        "inner",
    )

    exact_time_ok = (
        F.col("left_end_epoch").isNotNull()
        & F.col("right_start_epoch").isNotNull()
        & (F.col("right_start_epoch") >= F.col("left_end_epoch"))
        & (
            (
                F.col("right_start_epoch")
                - F.col("left_end_epoch")
            ).cast("double")
            <= F.lit(float(config.max_time_gap_seconds))
        )
    )
    # Hai bản sao vật lý của cùng một bằng chứng không phải là continuation.
    # Với cặp role giống nhau, cùng temporal instance cũng không được xem là
    # một bước NEXT_TW mới. Không dùng fallback khi thiếu timestamp vì true
    # boundary phải thỏa time gap thực tế.
    different_logical_fragment = (
        F.col("left_logical_fragment_id")
        != F.col("right_logical_fragment_id")
    )
    same_temporal_instance_on_same_role = (
        (F.col("left_role") == F.col("right_role"))
        & F.col("left_entity_tw_id").isNotNull()
        & F.col("right_entity_tw_id").isNotNull()
        & (F.col("left_entity_tw_id") == F.col("right_entity_tw_id"))
    )

    compatible = joined.filter(
        (F.col("left_partition") != F.col("right_partition"))
        & different_logical_fragment
        & (~same_temporal_instance_on_same_role)
        & exact_time_ok
    )

    boundary_ids = (
        compatible.select(
            F.col("left_fragment_id").alias("canonical_fragment_id")
        )
        .unionByName(
            compatible.select(
                F.col("right_fragment_id").alias("canonical_fragment_id")
            )
        )
        .dropDuplicates(["canonical_fragment_id"])
        .withColumn("_has_external_continuation", F.lit(True))
    )

    return (
        all_events.join(boundary_ids, "canonical_fragment_id", "left")
        .withColumn(
            "is_true_boundary",
            F.coalesce(
                F.col("_has_external_continuation"),
                F.lit(False),
            ),
        )
        .withColumn(
            "requires_external_evidence",
            F.col("is_true_boundary"),
        )
        .withColumn(
            "fragment_class",
            F.when(
                F.col("is_true_boundary"),
                F.lit("INCOMPLETE_BOUNDARY"),
            ).otherwise(F.lit("DEAD_END")),
        )
        .withColumn(
            "boundary_type",
            F.when(
                F.col("is_true_boundary"),
                F.lit("QUERY_ROLE_IDENTITY_TIMEWINDOW_INTERFACE"),
            ),
        )
        .withColumn(
            "boundary_reason",
            F.when(
                F.col("is_true_boundary"),
                F.lit(
                    "ROLE_IDENTITY_DIFFERENT_LOGICAL_FRAGMENT_AND_"
                    "EXACT_BOUNDED_TEMPORAL_CONTINUATION_IN_ANOTHER_PARTITION"
                ),
            ).otherwise(
                F.lit("NO_EXTERNAL_QUERY_COMPATIBLE_CONTINUATION")
            ),
        )
        .drop("_has_external_continuation")
    )


def _mark_complete_candidate_boundary(
    complete_candidates: DataFrame,
    true_boundary_fragments: DataFrame,
) -> DataFrame:
    """Đánh dấu complete candidate đồng thời có interface hướng ra partition khác."""
    candidate_ids = [c for c in ["person_id", "other_person_id", "thing_id", "vehicle_id"] if c in complete_candidates.columns]
    boundary_ids = [c for c in ["person_id", "other_person_id", "thing_id", "vehicle_id"] if c in true_boundary_fragments.columns]
    if not candidate_ids or not boundary_ids or "candidate_id" not in complete_candidates.columns:
        return (
            complete_candidates
            .withColumn("is_complete", F.lit(True))
            .withColumn("is_true_boundary", F.lit(False))
            .withColumn("requires_external_evidence", F.lit(False))
            .withColumn("fragment_class", F.lit("COMPLETE_LOCAL"))
        )

    c_arrays = [F.col(c).cast("string") for c in candidate_ids]
    b_arrays = [F.col(c).cast("string") for c in boundary_ids]
    c_long = (
        complete_candidates
        .select(
            "candidate_id",
            F.col("source_partition_name").cast("string").alias("source_partition_name"),
            F.explode(F.array(*c_arrays)).alias("entity_id"),
        )
        .filter(F.col("entity_id").isNotNull())
    )
    b_long = (
        true_boundary_fragments
        .select(
            F.col("source_partition_name").cast("string").alias("source_partition_name"),
            F.explode(F.array(*b_arrays)).alias("entity_id"),
        )
        .filter(F.col("entity_id").isNotNull())
        .dropDuplicates()
    )
    marked = (
        c_long.join(b_long, ["source_partition_name", "entity_id"], "inner")
        .select("candidate_id").dropDuplicates()
        .withColumn("_complete_has_boundary", F.lit(True))
    )
    return (
        complete_candidates.join(marked, "candidate_id", "left")
        .withColumn("is_complete", F.lit(True))
        .withColumn("is_true_boundary", F.coalesce(F.col("_complete_has_boundary"), F.lit(False)))
        .withColumn("requires_external_evidence", F.col("is_true_boundary"))
        .withColumn(
            "fragment_class",
            F.when(F.col("is_true_boundary"), F.lit("COMPLETE_AND_BOUNDARY"))
             .otherwise(F.lit("COMPLETE_LOCAL")),
        )
        .drop("_complete_has_boundary")
    )


def _run_partition_localeval(
    *,
    spark,
    tables: dict,
    spec: QuerySpec,
    config: Stage2Config,
    barrier: threading.Barrier,
    group_start_holder: list[float],
) -> PartitionLocalEvalResult:
    pname = str(tables.get("partition_name", "partition"))
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", f"localeval_{pname}")
    sc.setJobGroup(
        f"{spec.query_id}-PROPOSED-LOCALEVAL-{pname}",
        f"Vr15 LocalEval {spec.query_id} on {pname}",
        interruptOnCancel=True,
    )
    barrier.wait()
    submit_time = time.perf_counter()
    group_start = group_start_holder[0]
    try:
        if "rels" not in tables:
            raise RuntimeError(f"Partition {pname} thiếu rels.csv")
        v_i = normalize_vertices(tables)
        e_i = normalize_base_edges(tables["rels"])

        events = persist_df(
            _canonicalize_events(
                spec.local_event_extractor(v_i, e_i, config, spec.query_id),
                spec,
                pname,
            ),
            config,
            f"proposed.{spec.query_id}.{pname}.query_relevant_fragments",
        )

        local_config = replace(
            config,
            minimum_partition_count=1,
            query_diagnostics_enabled=False,
        )
        raw_local_candidates, local_witnesses = spec.distributed_evaluator(
            vertices=v_i,
            atomic_events=events,
            next_tw_edges=None,
            config=local_config,
            query_id=spec.query_id,
        )
        # Candidate và valid witness là hai tầng khác nhau. QuerySpec lọc
        # completeness cục bộ độc lập với hard validity.
        complete_raw_local_candidates = spec.select_complete_local_candidates(
            raw_local_candidates,
            local_config,
        )
        local_candidates = persist_df(
            _deduplicate(complete_raw_local_candidates, spec, "candidate")
            .withColumn("source_partition_name", F.lit(pname))
            .withColumn("fragment_class", F.lit("COMPLETE_LOCAL"))
            .withColumn("is_complete", F.lit(True))
            .withColumn("is_true_boundary", F.lit(False))
            .withColumn("requires_external_evidence", F.lit(False)),
            config,
            f"proposed.{spec.query_id}.{pname}.complete_local_candidates",
        )
        local_witnesses = persist_df(
            _deduplicate(local_witnesses, spec, "witness")
            .withColumn("source_partition_name", F.lit(pname))
            .withColumn("fragment_class", F.lit("COMPLETE_LOCAL"))
            .withColumn("is_complete", F.lit(True))
            .withColumn("is_true_boundary", F.lit(False))
            .withColumn("requires_external_evidence", F.lit(False)),
            config,
            f"proposed.{spec.query_id}.{pname}.locally_validated_witnesses",
        )

        # Một Spark action duy nhất materialize đồng thời ba đầu ra LocalEval.
        # Không chạy thêm count diagnostic sau mốc completion của partition.
        stats = (
            events.agg(
                F.count(F.lit(1)).cast("long").alias("n_events")
            )
            .crossJoin(
                local_candidates.agg(
                    F.count(F.lit(1)).cast("long").alias("n_candidates")
                )
            )
            .crossJoin(
                local_witnesses.agg(
                    F.count(F.lit(1)).cast("long").alias("n_witnesses")
                )
            )
            .collect()[0]
        )
        action_end = time.perf_counter()
        n_events = int(stats["n_events"] or 0)
        n_candidates = int(stats["n_candidates"] or 0)
        n_witnesses = int(stats["n_witnesses"] or 0)
        return PartitionLocalEvalResult(
            partition_name=pname,
            events=events,
            complete_candidates=local_candidates,
            locally_validated_witnesses=local_witnesses,
            num_query_relevant_atomic_evidence=n_events,
            num_complete_local_candidates=n_candidates,
            num_locally_validated_witnesses=n_witnesses,
            submit_offset_seconds=submit_time - group_start,
            job_elapsed_seconds=action_end - submit_time,
            completion_offset_seconds=action_end - group_start,
            status="SUCCESS",
        )
    finally:
        sc.setLocalProperty("spark.scheduler.pool", None)
        sc.setLocalProperty("spark.jobGroup.id", None)
        sc.setLocalProperty("spark.job.description", None)
        sc.setLocalProperty("spark.job.interruptOnCancel", None)


def main() -> None:
    config = load_config(METHOD_NAME, "videographdb_vr15_proposed")
    ensure_supported_query_ids(config.query_ids, QUERY_REGISTRY)
    if len(config.query_ids) != 1:
        raise ValueError("Vr15 đo thời gian theo một query trên mỗi Spark application.")
    query_id = config.query_ids[0]
    spec = QUERY_REGISTRY[query_id]

    e2e_start = time.perf_counter()
    spark = create_spark(config)
    print_config(config, spark)

    t_input = time.perf_counter()
    partition_tables = load_partition_tables(spark, config.dataset_root, config.by_partition_dir)
    time_input_discovery_plan = time.perf_counter() - t_input
    num_graph_partitions = len(partition_tables)
    if num_graph_partitions == 0:
        raise RuntimeError("Không phát hiện graph partition")

    algorithm_start = time.perf_counter()

    # Compact index: không union global base edges và không dựng global graph.
    index_start = time.perf_counter()
    entity_partition_index = persist_df(
        build_entity_partition_index(partition_tables),
        config,
        "proposed.entity_partition_index",
    )
    num_entity_partition_index_records, _ = materialize_df(
        entity_partition_index,
        "proposed.entity_partition_index",
        enabled=True,
        required=True,
    )
    time_step1_boundary_index_preparation = time.perf_counter() - index_start

    # LocalEval được submit đồng thời cho toàn bộ graph partition.
    workers = num_graph_partitions
    barrier = threading.Barrier(workers + 1)
    group_start_holder = [0.0]
    localeval_results: list[PartitionLocalEvalResult] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="localeval") as executor:
        futures = [
            executor.submit(
                _run_partition_localeval,
                spark=spark,
                tables=tables,
                spec=spec,
                config=config,
                barrier=barrier,
                group_start_holder=group_start_holder,
            )
            for tables in partition_tables
        ]
        group_start_holder[0] = time.perf_counter()
        barrier.wait()
        for future in as_completed(futures):
            localeval_results.append(future.result())
    time_step1_localeval_wall = time.perf_counter() - group_start_holder[0]
    time_step1_localeval_parallel = max(r.completion_offset_seconds for r in localeval_results)
    time_step1_localeval_partition_work_sum = sum(r.job_elapsed_seconds for r in localeval_results)
    time_step1_localeval_partition_action_elapsed_max = max(r.job_elapsed_seconds for r in localeval_results)
    time_step1_localeval_parallel_overhead = time_step1_localeval_wall - time_step1_localeval_parallel

    all_local_events = persist_df(
        union_dataframes([r.events for r in localeval_results]).dropDuplicates(["canonical_fragment_id"]),
        config,
        f"proposed.{query_id}.all_local_fragments",
    )
    complete_local_candidates = persist_df(
        _deduplicate(
            union_dataframes([r.complete_candidates for r in localeval_results]),
            spec,
            "candidate",
        ),
        config,
        f"proposed.{query_id}.complete_local_candidates",
    )
    locally_validated_witnesses = persist_df(
        _deduplicate(
            union_dataframes([r.locally_validated_witnesses for r in localeval_results]),
            spec,
            "witness",
        ),
        config,
        f"proposed.{query_id}.locally_validated_witnesses",
    )

    classification_start = time.perf_counter()
    classified_fragments = persist_df(
        _classify_true_boundary_fragments(all_local_events, spec, config),
        config,
        f"proposed.{query_id}.classified_fragments",
    )
    true_boundary_fragments = persist_df(
        classified_fragments.filter(F.col("is_true_boundary")),
        config,
        f"proposed.{query_id}.true_boundary_fragments",
    )
    dead_end_fragments = persist_df(
        classified_fragments.filter(F.col("fragment_class") == F.lit("DEAD_END")),
        config,
        f"proposed.{query_id}.dead_end_fragments",
    )
    complete_local_candidates = persist_df(
        _mark_complete_candidate_boundary(complete_local_candidates, true_boundary_fragments),
        config,
        f"proposed.{query_id}.classified_complete_local_candidates",
    )
    complete_and_boundary_fragments = persist_df(
        complete_local_candidates.filter(F.col("fragment_class") == F.lit("COMPLETE_AND_BOUNDARY")),
        config,
        f"proposed.{query_id}.complete_and_boundary_fragments",
    )

    # Materialize classified_fragments đúng một lần và lấy ba metric bằng
    # một aggregate action. Tránh chạy ba count job riêng trên cùng lineage.
    fragment_metrics = (
        classified_fragments
        .agg(
            F.count(F.lit(1)).cast("long").alias("num_atomic"),
            F.sum(
                F.when(F.col("is_true_boundary"), F.lit(1))
                 .otherwise(F.lit(0))
            ).cast("long").alias("num_boundary"),
            F.sum(
                F.when(
                    F.col("fragment_class") == F.lit("DEAD_END"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).cast("long").alias("num_dead_end"),
        )
        .collect()[0]
    )
    num_query_relevant_atomic_evidence = int(fragment_metrics["num_atomic"] or 0)
    num_true_boundary_fragments = int(fragment_metrics["num_boundary"] or 0)
    num_dead_end_fragments = int(fragment_metrics["num_dead_end"] or 0)

    # Materialize candidate classification một lần, đồng thời lấy số complete
    # và complete-and-boundary.
    complete_metrics = (
        complete_local_candidates
        .agg(
            F.count(F.lit(1)).cast("long").alias("num_complete"),
            F.sum(
                F.when(
                    F.col("fragment_class") == F.lit("COMPLETE_AND_BOUNDARY"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).cast("long").alias("num_complete_boundary"),
        )
        .collect()[0]
    )
    num_complete_local_candidates = int(complete_metrics["num_complete"] or 0)
    num_complete_and_boundary_candidates = int(
        complete_metrics["num_complete_boundary"] or 0
    )

    # Union witness đã deduplicate theo witness_id nên giữ một count chính xác.
    num_locally_validated_witnesses = safe_count(
        locally_validated_witnesses,
        f"proposed.{query_id}.num_locally_validated_witnesses",
        required=True,
    )
    time_step1_fragment_classification = time.perf_counter() - classification_start
    time_step1_total = (
        time_step1_boundary_index_preparation
        + time_step1_localeval_parallel
        + time_step1_fragment_classification
    )

    # Step 2: Stitch boundary -> MergeCandidates -> Hard Validation -> Normalize.
    step2_start = time.perf_counter()
    spark_for_empty = get_spark_session(true_boundary_fragments)
    stitched_candidates_only = empty_candidates_like(spark_for_empty)
    stitched_witnesses_only = empty_witnesses_like(spark_for_empty)

    stitched_candidates_only, stitched_witnesses_only = spec.distributed_evaluator(
        vertices=entity_partition_index,  # compact lookup; evaluator không được duyệt global graph.
        atomic_events=true_boundary_fragments,
        next_tw_edges=None,
        config=config,
        query_id=query_id,
    )
    stitched_candidates_only = persist_df(
        _deduplicate(stitched_candidates_only, spec, "candidate"),
        config,
        f"proposed.{query_id}.stitched_candidates_only",
    )
    stitched_witnesses_only = persist_df(
        _deduplicate(stitched_witnesses_only, spec, "witness"),
        config,
        f"proposed.{query_id}.stitched_witnesses_only",
    )

    merged_candidates = persist_df(
        _deduplicate(
            union_dataframes([complete_local_candidates, stitched_candidates_only]),
            spec,
            "candidate",
        ),
        config,
        f"proposed.{query_id}.merged_candidates",
    )

    local_witnesses_for_merge = locally_validated_witnesses
    if query_id == "Q4.3" and "num_partitions" in local_witnesses_for_merge.columns:
        local_witnesses_for_merge = local_witnesses_for_merge.filter(
            F.col("num_partitions") >= F.lit(config.minimum_partition_count)
        )
    fallback_validated_witnesses = union_dataframes(
        [local_witnesses_for_merge, stitched_witnesses_only]
    )
    final_valid_witnesses_raw, final_witness_construction_mode = (
        _finalize_merged_candidates_if_supported(
            spec=spec,
            vertices=entity_partition_index,
            merged_candidates=merged_candidates,
            config=config,
            query_id=query_id,
            fallback_witnesses=fallback_validated_witnesses,
        )
    )
    final_valid_witnesses = persist_df(
        final_valid_witnesses_raw,
        config,
        f"proposed.{query_id}.final_valid_witnesses",
    )

    num_stitched_candidates = (
        safe_count(stitched_candidates_only, f"proposed.{query_id}.num_stitched_candidates", required=True)
        if config.benchmark_count_intermediate else -1
    )
    num_stitched_valid_witnesses = (
        safe_count(stitched_witnesses_only, f"proposed.{query_id}.num_stitched_valid_witnesses", required=True)
        if config.benchmark_count_intermediate else -1
    )
    num_merged_candidates = (
        safe_count(merged_candidates, f"proposed.{query_id}.num_merged_candidates", required=True)
        if config.benchmark_count_intermediate else -1
    )
    num_final_valid_witnesses = safe_count(
        final_valid_witnesses,
        f"proposed.{query_id}.num_final_valid_witnesses",
        required=True,
    )
    time_step2_globaleval = time.perf_counter() - step2_start

    time_algorithm_total = time.perf_counter() - algorithm_start
    time_algorithm_phase_gap = (
        time_algorithm_total
        - time_step1_boundary_index_preparation
        - time_step1_localeval_parallel
        - time_step1_fragment_classification
        - time_step2_globaleval
    )
    time_end_to_end_total = time.perf_counter() - e2e_start
    time_non_algorithm_overhead = (
        time_end_to_end_total
        - time_input_discovery_plan
        - time_algorithm_total
    )
    if time_non_algorithm_overhead < -0.05:
        raise RuntimeError(
            f"TIMING_SCOPE_ERROR proposed overhead={time_non_algorithm_overhead:.6f}s"
        )

    partition_timings = [
        {
            "partition_name": r.partition_name,
            "partition_action_submit_offset_seconds": r.submit_offset_seconds,
            "partition_action_elapsed_seconds": r.job_elapsed_seconds,
            "partition_completion_offset_seconds": r.completion_offset_seconds,
            "submit_offset_seconds": r.submit_offset_seconds,
            "job_elapsed_seconds": r.job_elapsed_seconds,
            "completion_offset_seconds": r.completion_offset_seconds,
            "num_query_relevant_atomic_evidence": r.num_query_relevant_atomic_evidence,
            "num_complete_local_candidates": r.num_complete_local_candidates,
            "num_locally_validated_witnesses": r.num_locally_validated_witnesses,
            "status": r.status,
        }
        for r in sorted(localeval_results, key=lambda x: x.partition_name)
    ]

    row = {
        "code_version": "Vr15",
        "method": METHOD_NAME,
        "method_family": "proposed_fragment_first_bounded_erpq",
        "dataset_name": basename_uri(config.dataset_root),
        "dataset_root": config.dataset_root,
        "query_id": query_id,
        "query_group": spec.query_group,
        "query_name": spec.query_name,
        "logical_query_type": spec.logical_query_type,
        "logical_query_description": spec.logical_query_description,
        "rpq_expression": spec.rpq_expression,
        "proposed_physical_plan": spec.proposed_physical_plan,
        "witness_type": spec.witness_type,
        "candidate_dedup_columns": json.dumps(
            _dedup_columns(spec, "candidate", merged_candidates),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "witness_dedup_columns": json.dumps(
            _dedup_columns(spec, "witness", final_valid_witnesses),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "evaluation_strategy": "Parallel LocalEval -> True-boundary Stitching -> Candidate Merge -> Final Witness Construction",
        "fragment_classification_mode": "COMPLETE_LOCAL_COMPLETE_AND_BOUNDARY_INCOMPLETE_BOUNDARY_DEAD_END",
        "execution_parallelism_mode": "CONCURRENT_GRAPH_PARTITION_LOCALEVAL_JOBS_WITH_SPARK_TASK_PARALLELISM",
        "localeval_runtime_aggregation": "MAX_PARTITION_COMPLETION_OFFSET_FROM_COMMON_START",
        "spark_master": spark.sparkContext.master,
        "spark_app_id": spark.sparkContext.applicationId,
        "spark_executor_memory": config.executor_memory,
        "spark_executor_cores": config.executor_cores,
        "spark_cores_max": config.cores_max,
        "spark_available_task_slots": config.spark_available_task_slots,
        "spark_shuffle_partitions": config.spark_shuffle_partitions,
        "spark_scheduler_mode": config.spark_scheduler_mode,
        "num_source_graph_partitions": num_graph_partitions,
        "local_eval_submission_workers": workers,
        "num_entity_partition_index_records": num_entity_partition_index_records,
        "num_global_vertices": -1,
        "num_global_base_edges": -1,
        "num_physical_next_tw_edges": 0,
        "num_query_relevant_atomic_evidence": num_query_relevant_atomic_evidence,
        "num_complete_local_candidates": num_complete_local_candidates,
        "num_complete_local_fragments": num_complete_local_candidates,
        "num_true_boundary_fragments": num_true_boundary_fragments,
        "num_incomplete_boundary_fragments": num_true_boundary_fragments,
        "num_complete_and_boundary_candidates": num_complete_and_boundary_candidates,
        "num_complete_and_boundary_fragments": num_complete_and_boundary_candidates,
        "num_dead_end_fragments": num_dead_end_fragments,
        "num_localeval_atomic_fragments": num_query_relevant_atomic_evidence,
        "num_localeval_fragments_total": num_query_relevant_atomic_evidence,
        "num_locally_validated_witnesses": num_locally_validated_witnesses,
        "num_stitched_candidates": num_stitched_candidates,
        "num_stitched_valid_witnesses": num_stitched_valid_witnesses,
        "num_merged_candidates": num_merged_candidates,
        "num_candidates": num_merged_candidates,
        "num_final_valid_witnesses": num_final_valid_witnesses,
        "num_valid_witnesses": num_final_valid_witnesses,
        "max_time_gap_seconds": config.max_time_gap_seconds,
        "candidate_pruning_max_time_window_gap": config.candidate_pruning_max_time_window_gap,
        "time_window_duration_seconds": config.time_window_duration_seconds,
        "quick_exit_max_seconds": config.quick_exit_max_seconds,
        "exit_person_role": config.exit_person_role,
        "minimum_partition_count": config.minimum_partition_count,
        "require_different_location": config.require_different_location,
        "quick_exit_reference_role": config.quick_exit_reference_role,
        "next_tw_execution_mode": "LOGICAL_CONTINUITY_EVIDENCE_INFERRED_DURING_STITCHING",
        "final_witness_construction_mode": final_witness_construction_mode,
        "time_input_discovery_plan_seconds": time_input_discovery_plan,
        "time_step1_boundary_index_preparation_seconds": time_step1_boundary_index_preparation,
        "time_step1_localeval_parallel_seconds": time_step1_localeval_parallel,
        "time_step1_localeval_wall_seconds": time_step1_localeval_wall,
        "time_step1_localeval_partition_action_elapsed_max_seconds": time_step1_localeval_partition_action_elapsed_max,
        "time_step1_localeval_active_max_seconds": time_step1_localeval_partition_action_elapsed_max,
        "time_step1_localeval_partition_work_sum_seconds": time_step1_localeval_partition_work_sum,
        "time_step1_localeval_parallel_overhead_seconds": time_step1_localeval_parallel_overhead,
        "time_step1_fragment_classification_seconds": time_step1_fragment_classification,
        "time_step1_total_seconds": time_step1_total,
        "time_step2_globaleval_seconds": time_step2_globaleval,
        "time_algorithm_total_seconds": time_algorithm_total,
        "time_algorithm_phase_gap_seconds": time_algorithm_phase_gap,
        "time_end_to_end_total_seconds": time_end_to_end_total,
        "time_non_algorithm_overhead_seconds": time_non_algorithm_overhead,
        "partition_localeval_timings": json.dumps(partition_timings, ensure_ascii=False, separators=(",", ":")),
        "end_to_end_scope": "application_start_before_spark_session_to_final_valid_witness_action_end",
    }

    print("=" * 96)
    for key, value in row.items():
        print(f"{key:60s}: {value}")
    print("=" * 96)

    out_dir = config.output_root / METHOD_NAME / query_id
    out_dir.mkdir(parents=True, exist_ok=True)
    write_spark_csv(merged_candidates, out_dir, "candidates_csv", config.save_candidate_outputs)
    write_spark_csv(
        final_valid_witnesses,
        out_dir,
        "witnesses_csv",
        config.save_final_witnesses and num_final_valid_witnesses > 0,
    )
    write_spark_csv(
        classified_fragments,
        out_dir,
        "localeval_fragments_csv",
        config.save_intermediate_outputs,
    )
    write_runtime_summary([row], config.output_root / METHOD_NAME, enabled=config.save_runtime_summary)

    unpersist_all(
        entity_partition_index,
        all_local_events,
        classified_fragments,
        true_boundary_fragments,
        dead_end_fragments,
        complete_and_boundary_fragments,
        complete_local_candidates,
        locally_validated_witnesses,
        stitched_candidates_only,
        stitched_witnesses_only,
        merged_candidates,
        final_valid_witnesses,
        *[r.events for r in localeval_results],
        *[r.complete_candidates for r in localeval_results],
        *[r.locally_validated_witnesses for r in localeval_results],
    )
    spark.stop()


if __name__ == "__main__":
    main()

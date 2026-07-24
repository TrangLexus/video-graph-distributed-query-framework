#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Proposed Vr15 C2: Parallel LocalEval -> boundary fragment Stitching -> Validation.

Bất biến chính:
    C_M = Deduplicate(C_L union C_S)
    W_V = HardValidate(C_M)
    W_F = Deduplicate(Normalize(W_V))

True-boundary được phát hiện bằng equi-join query-specific, có chặn TimeWindow
và timestamp; không self-join OR trên nhiều identity.
"""
from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import Stage2Config, ensure_supported_query_ids, load_config
from graph_io import (
    basename_uri,
    create_spark,
    load_partition_tables,
    normalize_base_edges,
    normalize_vertices,
    persist_df,
    print_config,
    union_dataframes,
    unpersist_all,
    write_runtime_summary,
    write_spark_csv,
)
from queries import QUERY_REGISTRY
from query_specs.common import (
    QuerySpec,
    add_missing_columns,
    apply_query_time_scope,
    resolve_query_time_scope,
    validate_query_time_scope_config,
)

METHOD_NAME = "proposed_distributed_boundary_erpq"


def _deduplicate(df: DataFrame, spec: QuerySpec, kind: str) -> DataFrame:
    return df.dropDuplicates(list(spec.resolve_dedup_columns(kind, df.columns)))


@dataclass
class PartitionLocalEvalResult:
    partition_name: str
    events: DataFrame
    complete_candidates: DataFrame
    num_atomic_fragments: int
    num_complete_candidates: int
    submit_offset_seconds: float
    action_elapsed_seconds: float
    completion_offset_seconds: float
    status: str


def _canonicalize_events(
    df: DataFrame,
    spec: QuerySpec,
    partition_name: str,
) -> DataFrame:
    required = {
        "fragment_role": "string",
        "relation_type": "string",
        "entity_type": "string",
        "entity_id": "string",
        "entity_tw_id": "string",
        "person_id": "string",
        "other_person_id": "string",
        "thing_id": "string",
        "vehicle_id": "string",
        "src": "string",
        "dst": "string",
        "edge_id": "string",
        "start_time": "string",
        "end_time": "string",
        "start_ts": "timestamp",
        "end_ts": "timestamp",
        "start_epoch": "long",
        "end_epoch": "long",
        "tw_id": "string",
        "tw_num": "int",
        "partition_id": "string",
        "location_id": "string",
        "camera_id": "string",
        "video_id": "string",
        "stitch_key": "string",
        "prev_key": "string",
        "next_key": "string",
    }
    out = add_missing_columns(df, required)
    return (
        out
        .withColumn("source_partition_name", F.lit(partition_name))
        .withColumn(
            "partition_id",
            F.coalesce(F.col("partition_id"), F.lit(partition_name)),
        )
        .withColumn("fragment_kind", F.lit("ATOMIC_QUERY_EVIDENCE"))
        .withColumn("is_complete", F.lit(False))
        .withColumn("is_true_boundary", F.lit(False))
        .withColumn("requires_external_evidence", F.lit(False))
        .withColumn("fragment_class", F.lit("UNCLASSIFIED"))
        .withColumn("boundary_type", F.lit(None).cast("string"))
        .withColumn("boundary_reason", F.lit(None).cast("string"))
        .withColumn(
            "logical_fragment_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(spec.query_id),
                    F.coalesce(F.col("fragment_role"), F.lit("")),
                    F.coalesce(F.col("relation_type"), F.lit("")),
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
                ),
                256,
            ),
        )
        .withColumn(
            "canonical_fragment_id",
            F.sha2(
                F.concat_ws(
                    "|", F.col("logical_fragment_id"), F.lit(partition_name)
                ),
                256,
            ),
        )
        .dropDuplicates(["canonical_fragment_id"])
    )


def _key_condition(fields: tuple[str, ...]) -> F.Column:
    condition = F.lit(True)
    for field in fields:
        condition = condition & F.col(field).isNotNull()
    return condition


def _key_expr(fields: tuple[str, ...]) -> F.Column:
    # interface_id đã phân biệt kiểu interface; key chỉ chứa các giá trị binding
    # theo đúng thứ tự để other_person_id có thể nối với person_id.
    return F.concat_ws(
        "\u001e", *[F.col(field).cast("string") for field in fields]
    )


def _build_interface_views(
    events: DataFrame,
    spec: QuerySpec,
    k: int,
) -> tuple[DataFrame | None, DataFrame | None]:
    """Dựng hai projection nhỏ cho equi-join true-boundary.

    Mỗi source fragment được nhân tối đa K+1 (FORWARD) hoặc 2K+1
    (ABSOLUTE) target TimeWindow. Với K=12 và số interface I nhỏ, chi phí
    chuẩn bị là O(I*K*|F|), tránh self-join O(|F|^2).
    """
    source_frames: list[DataFrame] = []
    target_frames: list[DataFrame] = []
    columns = set(events.columns)

    for idx, interface in enumerate(spec.boundary_interfaces):
        (
            left_role,
            right_role,
            left_fields,
            right_fields,
            temporal_mode,
            left_exit_state,
            right_entry_state,
        ) = interface
        missing = [c for c in (*left_fields, *right_fields) if c not in columns]
        if missing:
            raise ValueError(
                f"{spec.query_id}: boundary interface thiếu cột {missing}"
            )
        interface_id = (
            f"I{idx}:{left_role}->{right_role}:"
            f"{left_exit_state}->{right_entry_state}:{temporal_mode}"
        )

        left = events.filter(
            (F.col("fragment_role") == F.lit(left_role))
            & _key_condition(left_fields)
            & F.col("tw_num").isNotNull()
        )
        if temporal_mode == "FORWARD":
            tw_sequence = F.sequence(
                F.col("tw_num"), F.col("tw_num") + F.lit(k)
            )
        else:
            tw_sequence = F.sequence(
                F.greatest(F.lit(0), F.col("tw_num") - F.lit(k)),
                F.col("tw_num") + F.lit(k),
            )
        source_frames.append(
            left.select(
                F.lit(interface_id).alias("interface_id"),
                F.lit(temporal_mode).alias("temporal_mode"),
                F.lit(left_exit_state).alias("left_exit_state"),
                F.lit(right_entry_state).alias("right_entry_state"),
                F.col("canonical_fragment_id").alias("left_fragment_id"),
                F.col("logical_fragment_id").alias("left_logical_id"),
                F.col("source_partition_name").alias("left_partition"),
                F.col("fragment_role").alias("left_role"),
                F.col("entity_tw_id").alias("left_entity_tw_id"),
                F.col("start_epoch").alias("left_start_epoch"),
                F.col("end_epoch").alias("left_end_epoch"),
                _key_expr(left_fields).alias("interface_key"),
                F.explode(tw_sequence).alias("target_tw_num"),
            ).dropDuplicates(
                [
                    "interface_id", "interface_key", "target_tw_num",
                    "left_fragment_id", "left_partition",
                ]
            )
        )

        right = events.filter(
            (F.col("fragment_role") == F.lit(right_role))
            & _key_condition(right_fields)
            & F.col("tw_num").isNotNull()
        )
        target_frames.append(
            right.select(
                F.lit(interface_id).alias("interface_id"),
                F.col("canonical_fragment_id").alias("right_fragment_id"),
                F.col("logical_fragment_id").alias("right_logical_id"),
                F.col("source_partition_name").alias("right_partition"),
                F.col("fragment_role").alias("right_role"),
                F.col("entity_tw_id").alias("right_entity_tw_id"),
                F.col("start_epoch").alias("right_start_epoch"),
                F.col("end_epoch").alias("right_end_epoch"),
                _key_expr(right_fields).alias("interface_key"),
                F.col("tw_num").alias("target_tw_num"),
            ).dropDuplicates(
                [
                    "interface_id", "interface_key", "target_tw_num",
                    "right_fragment_id", "right_partition",
                ]
            )
        )

    if not source_frames:
        return None, None
    return union_dataframes(source_frames), union_dataframes(target_frames)

def _typed_identity_events(events: DataFrame) -> DataFrame:
    """Long-form typed identity; tối đa năm record trên một atomic fragment."""
    frames: list[DataFrame] = []
    columns = set(events.columns)
    base = ["canonical_fragment_id"]

    if {"entity_type", "entity_id"}.issubset(columns):
        frames.append(
            events.select(
                *base,
                F.concat_ws(
                    ":",
                    F.upper(F.col("entity_type").cast("string")),
                    F.col("entity_id").cast("string"),
                ).alias("support_identity_key"),
            ).filter(
                F.col("entity_type").isNotNull()
                & F.col("entity_id").isNotNull()
            )
        )
    for field, prefix in (
        ("person_id", "PERSON"),
        ("other_person_id", "PERSON"),
        ("thing_id", "THING"),
        ("vehicle_id", "VEHICLE"),
    ):
        if field in columns:
            frames.append(
                events.select(
                    *base,
                    F.concat(
                        F.lit(prefix + ":"), F.col(field).cast("string")
                    ).alias("support_identity_key"),
                ).filter(F.col(field).isNotNull())
            )
    if not frames:
        return (
            events.limit(0)
            .select(*base)
            .withColumn("support_identity_key", F.lit(None).cast("string"))
        )
    return union_dataframes(frames).dropDuplicates(
        ["canonical_fragment_id", "support_identity_key"]
    )

def _interval_gap_seconds(
    left_start: F.Column,
    left_end: F.Column,
    right_start: F.Column,
    right_end: F.Column,
) -> F.Column:
    return F.greatest(
        F.lit(0.0),
        (right_start - left_end).cast("double"),
        (left_start - right_end).cast("double"),
    )


def _classify_atomic_fragments(
    events: DataFrame,
    spec: QuerySpec,
    config: Stage2Config,
) -> tuple[DataFrame, DataFrame]:
    """Phân loại true boundary và dựng input Stitching high-recall.

    - ``is_true_boundary`` chỉ gắn cho fragment có một đối tác ngoài partition
      thỏa interface role/state/endpoint, typed identity và hai ràng buộc thời gian.
    - Local support không bị gọi nhầm là true boundary. Support được lấy bằng
      typed-identity semi-join O(|F|+|B|), rồi truyền riêng vào Stitching để không
      mất partial match nằm cùng partition với boundary seed.
    """
    k = int(config.candidate_pruning_max_time_window_gap)
    eps = float(config.max_time_gap_seconds)
    source, target = _build_interface_views(events, spec, k)
    if source is None or target is None:
        classified = (
            events
            .withColumn("is_true_boundary", F.lit(False))
            .withColumn("requires_external_evidence", F.lit(False))
            .withColumn("fragment_class", F.lit("DEAD_END"))
            .withColumn("boundary_reason", F.lit("NO_QUERY_BOUNDARY_INTERFACE"))
            .withColumn("is_stitch_support", F.lit(False))
        )
        return classified, classified.limit(0)

    joined = source.join(
        target,
        ["interface_id", "interface_key", "target_tw_num"],
        "inner",
    )
    forward_ok = (
        F.col("right_start_epoch").isNotNull()
        & F.col("left_end_epoch").isNotNull()
        & (F.col("right_start_epoch") >= F.col("left_end_epoch"))
        & (
            (F.col("right_start_epoch") - F.col("left_end_epoch"))
            .cast("double")
            <= F.lit(eps)
        )
    )
    absolute_ok = (
        F.col("left_start_epoch").isNotNull()
        & F.col("left_end_epoch").isNotNull()
        & F.col("right_start_epoch").isNotNull()
        & F.col("right_end_epoch").isNotNull()
        & (
            _interval_gap_seconds(
                F.col("left_start_epoch"),
                F.col("left_end_epoch"),
                F.col("right_start_epoch"),
                F.col("right_end_epoch"),
            )
            <= F.lit(eps)
        )
    )
    temporal_ok = F.when(
        F.col("temporal_mode") == F.lit("FORWARD"), forward_ok
    ).otherwise(absolute_ok)
    same_temporal_instance_same_role = (
        (F.col("left_role") == F.col("right_role"))
        & F.col("left_entity_tw_id").isNotNull()
        & F.col("right_entity_tw_id").isNotNull()
        & (F.col("left_entity_tw_id") == F.col("right_entity_tw_id"))
    )
    compatible = joined.filter(
        (F.col("left_partition") != F.col("right_partition"))
        & (F.col("left_logical_id") != F.col("right_logical_id"))
        & (~same_temporal_instance_same_role)
        & temporal_ok
    )

    seed_ids = (
        compatible.select(
            F.col("left_fragment_id").alias("canonical_fragment_id"),
            "interface_id",
        )
        .unionByName(
            compatible.select(
                F.col("right_fragment_id").alias("canonical_fragment_id"),
                "interface_id",
            )
        )
        .groupBy("canonical_fragment_id")
        .agg(F.min("interface_id").alias("boundary_interface_id"))
        .withColumn("_boundary_seed", F.lit(True))
    )

    identities = _typed_identity_events(events)
    seed_identity_keys = (
        identities.join(
            seed_ids.select("canonical_fragment_id"),
            "canonical_fragment_id",
            "inner",
        )
        .select("support_identity_key")
        .dropDuplicates(["support_identity_key"])
    )
    support_ids = (
        identities.join(seed_identity_keys, "support_identity_key", "left_semi")
        .select("canonical_fragment_id")
        .dropDuplicates(["canonical_fragment_id"])
        .withColumn("_stitch_support", F.lit(True))
    )

    marks = seed_ids.join(support_ids, "canonical_fragment_id", "full")
    classified = (
        events.join(marks, "canonical_fragment_id", "left")
        .withColumn(
            "is_true_boundary",
            F.coalesce(F.col("_boundary_seed"), F.lit(False)),
        )
        .withColumn(
            "is_stitch_support",
            F.coalesce(F.col("_stitch_support"), F.lit(False)),
        )
        .withColumn("requires_external_evidence", F.col("is_true_boundary"))
        .withColumn(
            "fragment_class",
            F.when(
                F.col("is_true_boundary"), F.lit("INCOMPLETE_BOUNDARY")
            ).otherwise(F.lit("DEAD_END")),
        )
        .withColumn(
            "boundary_type",
            F.when(
                F.col("is_true_boundary"), F.lit("QUERY_INTERFACE_CROSS_PARTITION")
            ),
        )
        .withColumn(
            "boundary_reason",
            F.when(
                F.col("is_true_boundary"),
                F.concat(
                    F.lit("ROLE_STATE_ENDPOINT_IDENTITY_PARTITION_TIME:"),
                    F.col("boundary_interface_id"),
                ),
            ).when(
                F.col("is_stitch_support"),
                F.lit("TYPED_IDENTITY_SUPPORT_FOR_BOUNDARY_PARTIAL_MATCH"),
            ).otherwise(F.lit("NO_EXTERNAL_QUERY_COMPATIBLE_CONTINUATION")),
        )
        .drop("_boundary_seed", "_stitch_support")
    )
    stitch_input = classified.filter(F.col("is_stitch_support"))
    return classified, stitch_input

def _complete_candidate_fragments(
    candidates: DataFrame,
    spec: QuerySpec,
) -> DataFrame:
    keys = spec.resolve_dedup_columns("candidate", candidates.columns)
    id_parts = [
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in keys
    ]
    return (
        candidates
        .withColumn(
            "logical_fragment_id",
            F.sha2(F.concat_ws("|", *id_parts), 256),
        )
        .withColumn(
            "canonical_fragment_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("logical_fragment_id"),
                    F.col("source_partition_name"),
                ),
                256,
            ),
        )
        .withColumn("fragment_kind", F.lit("COMPLETE_LOCAL_PATTERN"))
        .withColumn("fragment_role", F.lit("COMPLETE_PATTERN"))
        .withColumn("is_complete", F.lit(True))
        .withColumn("is_true_boundary", F.lit(False))
        .withColumn("requires_external_evidence", F.lit(False))
        .withColumn("fragment_class", F.lit("COMPLETE_LOCAL"))
        .withColumn("boundary_type", F.lit(None).cast("string"))
        .withColumn("boundary_reason", F.lit("LOCAL_ACCEPTING_PATTERN"))
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
        vertices = normalize_vertices(tables)
        base_edges = apply_query_time_scope(
            normalize_base_edges(tables["rels"]), config
        )
        events = persist_df(
            _canonicalize_events(
                spec.local_event_extractor(
                    vertices, base_edges, config, spec.query_id
                ),
                spec,
                pname,
            ),
            config,
            f"proposed.{spec.query_id}.{pname}.atomic_fragments",
        )

        raw_candidates, _unused_local_witnesses = spec.distributed_evaluator(
            vertices=vertices,
            atomic_events=events,
            next_tw_edges=None,
            config=config,  # Không nới hard constraints trong LocalEval.
            query_id=spec.query_id,
        )
        complete_candidates = persist_df(
            _deduplicate(
                spec.select_complete_local_candidates(raw_candidates, config),
                spec,
                "candidate",
            ).withColumn("source_partition_name", F.lit(pname)),
            config,
            f"proposed.{spec.query_id}.{pname}.complete_candidates",
        )

        # Một action duy nhất materialize hai output cần cho ranh giới LocalEval.
        stats = (
            events.agg(F.count(F.lit(1)).alias("n_atomic"))
            .crossJoin(
                complete_candidates.agg(
                    F.count(F.lit(1)).alias("n_complete")
                )
            )
            .collect()[0]
        )
        action_end = time.perf_counter()
        return PartitionLocalEvalResult(
            partition_name=pname,
            events=events,
            complete_candidates=complete_candidates,
            num_atomic_fragments=int(stats["n_atomic"] or 0),
            num_complete_candidates=int(stats["n_complete"] or 0),
            submit_offset_seconds=submit_time - group_start,
            action_elapsed_seconds=action_end - submit_time,
            completion_offset_seconds=action_end - group_start,
            status="SUCCESS",
        )
    finally:
        sc.setLocalProperty("spark.scheduler.pool", None)
        sc.setLocalProperty("spark.jobGroup.id", None)
        sc.setLocalProperty("spark.job.description", None)
        sc.setLocalProperty("spark.job.interruptOnCancel", None)


def _query_parameter(spec: QuerySpec, name: str, value):
    return value if name in spec.runtime_parameter_schema or name in spec.hard_constraint_names else None


def main() -> None:
    config = load_config(METHOD_NAME, "videographdb_vr15_proposed")
    ensure_supported_query_ids(config.query_ids, QUERY_REGISTRY)
    validate_query_time_scope_config(config)
    if len(config.query_ids) != 1:
        raise ValueError("Vr15 đo thời gian theo một query trên mỗi Spark application.")
    query_id = config.query_ids[0]
    spec = QUERY_REGISTRY[query_id]
    spec.validate_contract()
    query_time_scope, query_start_time, query_end_time = resolve_query_time_scope(config)

    spark = create_spark(config)
    print_config(config, spark)
    e2e_start = time.perf_counter()

    input_start = time.perf_counter()
    partition_tables = load_partition_tables(
        spark, config.dataset_root, config.by_partition_dir
    )
    time_input_discovery_plan = time.perf_counter() - input_start
    num_graph_partitions = len(partition_tables)
    if num_graph_partitions == 0:
        raise RuntimeError("Không phát hiện graph partition")

    algorithm_start = time.perf_counter()

    # Vr15 hiện dùng query-scoped atomic fragments làm compact stitching index.
    # Không dựng thêm entity-partition index O(|V|), vì cả 12 distributed evaluator
    # chỉ đọc atomic_events. T_index = 0 là một trường hợp hợp lệ của công thức Vr15.
    entity_partition_index = None
    num_entity_partition_index_records = 0
    time_step1_boundary_index_preparation = 0.0

    workers = num_graph_partitions
    barrier = threading.Barrier(workers + 1)
    group_start_holder = [0.0]
    localeval_results: list[PartitionLocalEvalResult] = []
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="localeval"
    ) as executor:
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
    time_step1_localeval_parallel = max(
        r.completion_offset_seconds for r in localeval_results
    )
    time_step1_localeval_partition_work_sum = sum(
        r.action_elapsed_seconds for r in localeval_results
    )
    time_step1_localeval_partition_action_elapsed_max = max(
        r.action_elapsed_seconds for r in localeval_results
    )
    time_step1_localeval_parallel_overhead = (
        time_step1_localeval_wall - time_step1_localeval_parallel
    )

    all_local_events = persist_df(
        union_dataframes([r.events for r in localeval_results]).dropDuplicates(
            ["canonical_fragment_id"]
        ),
        config,
        f"proposed.{query_id}.all_atomic_fragments",
    )
    complete_local_candidates = persist_df(
        _deduplicate(
            union_dataframes(
                [r.complete_candidates for r in localeval_results]
            ),
            spec,
            "candidate",
        ),
        config,
        f"proposed.{query_id}.complete_local_candidates",
    )

    classification_start = time.perf_counter()
    classified_atomic_raw, stitch_input_raw = _classify_atomic_fragments(
        all_local_events, spec, config
    )
    classified_atomic = persist_df(
        classified_atomic_raw,
        config,
        f"proposed.{query_id}.classified_atomic_fragments",
    )
    stitch_input_fragments = persist_df(
        stitch_input_raw,
        config,
        f"proposed.{query_id}.stitch_input_fragments",
    )

    # C2 materialize đúng hai output Step 1 thực sự được tái sử dụng.
    # Complete-local fragment có quan hệ 1-1 với C_L; không union một DataFrame
    # wide chỉ để đếm vì điều đó làm tăng shuffle/memory mà không đổi thuật toán.
    fragment_stats = (
        classified_atomic.agg(
            F.count(F.lit(1)).cast("long").alias("n_atomic"),
            F.sum(F.when(F.col("is_true_boundary"), 1).otherwise(0))
            .cast("long").alias("n_boundary"),
            F.sum(
                F.when(~F.col("is_true_boundary"), 1).otherwise(0)
            ).cast("long").alias("n_dead"),
        )
        .crossJoin(
            complete_local_candidates.agg(
                F.count(F.lit(1)).cast("long").alias("n_complete")
            )
        )
        .crossJoin(
            stitch_input_fragments.agg(
                F.count(F.lit(1)).cast("long").alias("n_stitch_input")
            )
        )
        .collect()[0]
    )
    num_atomic_fragments_materialized = int(fragment_stats["n_atomic"] or 0)
    num_complete_local_fragments = int(fragment_stats["n_complete"] or 0)
    num_true_boundary_fragments = int(fragment_stats["n_boundary"] or 0)
    num_dead_end_fragments = int(fragment_stats["n_dead"] or 0)
    num_stitch_input_fragments = int(fragment_stats["n_stitch_input"] or 0)

    # Complete-local accepting units là terminal fragments trong physical plan này.
    # Việc mở rộng xuyên partition được thực hiện từ atomic boundary/support views,
    # nên complete local fragments không bị truyền lại như boundary carriers.
    num_complete_and_boundary_fragments = 0
    num_complete_local_only_fragments = num_complete_local_fragments
    num_incomplete_boundary_fragments = num_true_boundary_fragments
    num_boundary_support_fragments = max(
        0, num_stitch_input_fragments - num_true_boundary_fragments
    )
    num_localeval_fragments_total = (
        num_atomic_fragments_materialized + num_complete_local_fragments
    )
    fragment_partition_sum = (
        num_complete_local_only_fragments
        + num_complete_and_boundary_fragments
        + num_incomplete_boundary_fragments
        + num_dead_end_fragments
    )
    if fragment_partition_sum != num_localeval_fragments_total:
        raise RuntimeError(
            "FRAGMENT_INVARIANT_FAIL: "
            f"total={num_localeval_fragments_total}, classes={fragment_partition_sum}"
        )
    time_step1_fragment_classification = (
        time.perf_counter() - classification_start
    )
    time_step1_total = (
        time_step1_boundary_index_preparation
        + time_step1_localeval_parallel
        + time_step1_fragment_classification
    )

    # GlobalEval chỉ trên query-scoped boundary/support fragments và local candidates.
    step2_start = time.perf_counter()
    stitched_candidates = complete_local_candidates.limit(0)
    if num_stitch_input_fragments > 0:
        stitched_candidates, _unused_stitched_witnesses = spec.distributed_evaluator(
            vertices=entity_partition_index,
            atomic_events=stitch_input_fragments,
            next_tw_edges=None,
            config=config,
            query_id=query_id,
        )
    if "num_partitions" in stitched_candidates.columns:
        stitched_candidates = stitched_candidates.filter(
            F.col("num_partitions") >= F.lit(2)
        )
    stitched_candidates = persist_df(
        _deduplicate(stitched_candidates, spec, "candidate"),
        config,
        f"proposed.{query_id}.stitched_candidates",
    )

    merged_candidates = persist_df(
        _deduplicate(
            union_dataframes(
                [complete_local_candidates, stitched_candidates]
            ),
            spec,
            "candidate",
        ),
        config,
        f"proposed.{query_id}.merged_candidates",
    )
    valid_before_normalization_raw, final_witnesses_raw = (
        spec.finalize_merged_candidates(candidates=merged_candidates)
    )
    valid_before_normalization = persist_df(
        valid_before_normalization_raw,
        config,
        f"proposed.{query_id}.valid_before_normalization",
    )
    final_valid_witnesses = persist_df(
        final_witnesses_raw,
        config,
        f"proposed.{query_id}.final_valid_witnesses",
    )

    # Một final action materialize toàn bộ Step 2 và lấy các metric bắt buộc.
    step2_stats = (
        stitched_candidates.agg(F.count(F.lit(1)).alias("n_stitched"))
        .crossJoin(
            merged_candidates.agg(F.count(F.lit(1)).alias("n_merged"))
        )
        .crossJoin(
            valid_before_normalization.agg(
                F.count(F.lit(1)).alias("n_valid_before_norm")
            )
        )
        .crossJoin(
            final_valid_witnesses.agg(
                F.count(F.lit(1)).alias("n_final")
            )
        )
        .collect()[0]
    )
    num_stitched_candidates = int(step2_stats["n_stitched"] or 0)
    num_merged_candidates = int(step2_stats["n_merged"] or 0)
    num_valid_witnesses_before_normalization = int(
        step2_stats["n_valid_before_norm"] or 0
    )
    num_final_valid_witnesses = int(step2_stats["n_final"] or 0)
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

    num_atomic_fragments = sum(
        r.num_atomic_fragments for r in localeval_results
    )
    if num_atomic_fragments != num_atomic_fragments_materialized:
        raise RuntimeError(
            "LOCALEVAL_ATOMIC_COUNT_MISMATCH: "
            f"partition_sum={num_atomic_fragments}, "
            f"global_deduplicated={num_atomic_fragments_materialized}"
        )
    # C_L là tập candidate sau hợp nhất và loại trùng toàn cục.
    num_local_complete_candidates = num_complete_local_fragments
    partition_timings = [
        {
            "partition_name": r.partition_name,
            "partition_action_submit_offset_seconds": r.submit_offset_seconds,
            "partition_action_elapsed_seconds": r.action_elapsed_seconds,
            "partition_completion_offset_seconds": r.completion_offset_seconds,
            "num_atomic_fragments": r.num_atomic_fragments,
            "num_complete_candidates": r.num_complete_candidates,
            "status": r.status,
        }
        for r in sorted(localeval_results, key=lambda x: x.partition_name)
    ]

    row = {
        "code_version": "Vr15-spec-v4",
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
            list(spec.candidate_dedup_columns), ensure_ascii=False
        ),
        "witness_dedup_columns": json.dumps(
            list(spec.witness_dedup_columns), ensure_ascii=False
        ),
        "evaluation_strategy": (
            "Parallel LocalEval -> True-boundary/support Stitching -> "
            "Candidate Merge -> Hard Validation -> Witness Normalization"
        ),
        "fragment_classification_mode": (
            "COMPLETE_LOCAL_COMPLETE_AND_BOUNDARY_"
            "INCOMPLETE_BOUNDARY_DEAD_END"
        ),
        "complete_fragment_boundary_policy": (
            "TERMINAL_LOCAL_ACCEPTING_UNITS; CROSS_PARTITION_EXTENSION_USES_ATOMIC_BOUNDARY_SUPPORT"
        ),
        "execution_parallelism_mode": (
            "CONCURRENT_GRAPH_PARTITION_LOCALEVAL_JOBS_WITH_SPARK_TASK_PARALLELISM"
        ),
        "localeval_runtime_aggregation": (
            "MAX_PARTITION_COMPLETION_OFFSET_FROM_COMMON_START"
        ),
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
        "boundary_index_policy": "QUERY_SCOPED_INTERFACE_PROJECTIONS_NO_SEPARATE_ENTITY_INDEX",
        "num_global_vertices": -1,
        "num_global_base_edges": -1,
        "num_physical_next_tw_edges": 0,
        "num_query_relevant_atomic_evidence": num_atomic_fragments,
        "num_localeval_fragments_total": num_localeval_fragments_total,
        "num_complete_local_fragments": num_complete_local_fragments,
        "num_complete_local_only_fragments": num_complete_local_only_fragments,
        "num_true_boundary_fragments": num_true_boundary_fragments,
        "num_complete_and_boundary_fragments": num_complete_and_boundary_fragments,
        "num_incomplete_boundary_fragments": num_incomplete_boundary_fragments,
        "num_dead_end_fragments": num_dead_end_fragments,
        "num_stitch_input_fragments": num_stitch_input_fragments,
        "num_boundary_support_fragments": num_boundary_support_fragments,
        "num_local_complete_candidates": num_local_complete_candidates,
        "num_stitched_candidates": num_stitched_candidates,
        "num_merged_candidates": num_merged_candidates,
        "num_candidates": num_merged_candidates,
        "num_valid_witnesses_before_normalization": (
            num_valid_witnesses_before_normalization
        ),
        "num_final_valid_witnesses": num_final_valid_witnesses,
        "max_time_gap_seconds": config.max_time_gap_seconds,
        "candidate_pruning_max_time_window_gap": (
            config.candidate_pruning_max_time_window_gap
        ),
        "time_window_duration_seconds": config.time_window_duration_seconds,
        "query_time_scope": query_time_scope,
        "query_start_time": query_start_time,
        "query_end_time": query_end_time,
        "quick_exit_max_seconds": _query_parameter(
            spec, "quick_exit_max_seconds", config.quick_exit_max_seconds
        ),
        "exit_person_role": _query_parameter(
            spec, "exit_person_role", config.exit_person_role
        ),
        "minimum_partition_count": _query_parameter(
            spec, "minimum_partition_count", config.minimum_partition_count
        ),
        "require_different_location": _query_parameter(
            spec, "require_different_location", config.require_different_location
        ),
        "quick_exit_reference_role": _query_parameter(
            spec, "quick_exit_reference_role", config.quick_exit_reference_role
        ),
        "next_tw_execution_mode": (
            "LOGICAL_BOUNDED_TEMPORAL_PREDICATE_NO_PHYSICAL_EDGE_MATERIALIZATION"
        ),
        "final_witness_construction_mode": (
            "MERGED_CANDIDATES_HARD_VALIDATE_THEN_NORMALIZE"
        ),
        "hard_validation_mode": (
            "QUERY_SPECIFIC_IS_VALID_PREDICATE_REAPPLIED_AFTER_CANDIDATE_MERGE"
        ),
        "time_input_discovery_plan_seconds": time_input_discovery_plan,
        "time_dataset_integrity_validation_seconds": 0.0,
        "time_step1_boundary_index_preparation_seconds": (
            time_step1_boundary_index_preparation
        ),
        "time_step1_localeval_parallel_seconds": time_step1_localeval_parallel,
        "time_step1_localeval_wall_seconds": time_step1_localeval_wall,
        "time_step1_localeval_partition_action_elapsed_max_seconds": (
            time_step1_localeval_partition_action_elapsed_max
        ),
        "time_step1_localeval_partition_work_sum_seconds": (
            time_step1_localeval_partition_work_sum
        ),
        "time_step1_localeval_parallel_overhead_seconds": (
            time_step1_localeval_parallel_overhead
        ),
        "time_step1_fragment_classification_seconds": (
            time_step1_fragment_classification
        ),
        "time_step1_total_seconds": time_step1_total,
        "time_step2_globaleval_seconds": time_step2_globaleval,
        "time_algorithm_total_seconds": time_algorithm_total,
        "time_algorithm_phase_gap_seconds": time_algorithm_phase_gap,
        "time_end_to_end_total_seconds": time_end_to_end_total,
        "time_non_algorithm_overhead_seconds": time_non_algorithm_overhead,
        "partition_localeval_timings": json.dumps(
            partition_timings, ensure_ascii=False, separators=(",", ":")
        ),
        "end_to_end_scope": (
            "input_discovery_start_to_final_valid_witness_action_end"
        ),
    }

    print("=" * 96)
    for key, value in row.items():
        print(f"{key:62s}: {value}")
    print("=" * 96)

    out_dir = config.output_root / METHOD_NAME / query_id
    out_dir.mkdir(parents=True, exist_ok=True)
    write_spark_csv(
        merged_candidates,
        out_dir,
        "candidates_csv",
        config.save_candidate_outputs,
    )
    write_spark_csv(
        final_valid_witnesses,
        out_dir,
        "witnesses_csv",
        config.save_final_witnesses and num_final_valid_witnesses > 0,
    )
    if config.save_intermediate_outputs:
        classified_fragments_output = union_dataframes(
            [
                classified_atomic,
                _complete_candidate_fragments(complete_local_candidates, spec),
            ]
        )
        write_spark_csv(
            classified_fragments_output,
            out_dir,
            "localeval_fragments_csv",
            True,
        )
    write_runtime_summary(
        [row],
        config.output_root / METHOD_NAME,
        enabled=config.save_runtime_summary,
    )

    unpersist_all(
        all_local_events,
        complete_local_candidates,
        classified_atomic,
        stitch_input_fragments,
        stitched_candidates,
        merged_candidates,
        valid_before_normalization,
        final_valid_witnesses,
        *[r.events for r in localeval_results],
        *[r.complete_candidates for r in localeval_results],
    )
    spark.stop()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hợp đồng QuerySpec và helper dùng chung cho VideoGraphDB Vr15."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Mapping, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from graph_io import extract_time_window_number


@dataclass(frozen=True)
class QuerySpec:
    # Định danh và truy vấn logic.
    query_id: str
    query_group: str
    query_name: str
    logical_query_type: str
    rpq_expression: str
    logical_query_description: str

    # Hai kế hoạch thực thi vật lý cho cùng một truy vấn logic.
    baseline_physical_plan: str
    proposed_physical_plan: str

    # Output/correctness.
    witness_type: str
    correctness_compare_columns: Sequence[str]

    # Hàm thực thi query-specific.
    local_event_extractor: Callable[..., DataFrame]
    baseline_evaluator: Callable[..., tuple[DataFrame, DataFrame]]
    distributed_evaluator: Callable[..., tuple[DataFrame, DataFrame]]
    candidate_upper_bound: Callable[..., DataFrame] | None = None
    local_candidate_completion_filter: Callable[[DataFrame, object], DataFrame] | None = None

    # Đặc tả LocalEval–GlobalEval Vr05.
    automaton_start_state: str = "q0"
    automaton_accepting_states: tuple[str, ...] = ("q_accept",)
    automaton_transitions: tuple[tuple[str, str, str], ...] = ()
    required_fragment_roles: tuple[str, ...] = ()
    optional_fragment_roles: tuple[str, ...] = ()
    boundary_eligible_roles: tuple[str, ...] = ()
    stitch_mode: str = "QUERY_SPECIFIC"
    stitch_key_fields: tuple[str, ...] = ()
    stitch_role_order: tuple[str, ...] = ()
    boundary_role_pairs: tuple[tuple[str, str], ...] = ()
    hard_constraint_names: tuple[str, ...] = ()
    runtime_parameter_schema: Mapping[str, str] = field(default_factory=dict)
    candidate_dedup_columns: tuple[str, ...] = ("candidate_id",)
    witness_dedup_columns: tuple[str, ...] = ("witness_id",)

    def entry_exit_state(self, role: str) -> tuple[str | None, str | None]:
        for entry, transition_role, exit_state in self.automaton_transitions:
            if transition_role == role:
                return entry, exit_state
        return None, None

    def next_required_role(self, role: str) -> str | None:
        order = self.stitch_role_order or self.required_fragment_roles
        try:
            idx = order.index(role)
        except ValueError:
            return None
        return order[idx + 1] if idx + 1 < len(order) else None

    def select_complete_local_candidates(
        self,
        candidates: DataFrame,
        config,
    ) -> DataFrame:
        """Lọc các candidate đã hoàn thành pattern cục bộ.

        Completeness độc lập với hard validity. Query không cần quy tắc riêng
        giữ nguyên toàn bộ candidate do evaluator đã dựng đủ role/pattern.
        """
        if self.local_candidate_completion_filter is None:
            return candidates
        return self.local_candidate_completion_filter(candidates, config)

    def resolve_dedup_columns(
        self,
        kind: str,
        available_columns: Sequence[str],
    ) -> tuple[str, ...]:
        """Trả về khóa dedup ngữ nghĩa có thật trong DataFrame.

        Các QuerySpec Vr15 cũ mặc định dùng candidate_id/witness_id. Hai ID này
        có thể phụ thuộc physical plan, nên khi chưa khai báo khóa ngữ nghĩa riêng,
        ưu tiên correctness_compare_columns.
        """
        if kind not in {"candidate", "witness"}:
            raise ValueError(f"Loại dedup không hợp lệ: {kind}")

        configured = (
            self.candidate_dedup_columns
            if kind == "candidate"
            else self.witness_dedup_columns
        )
        id_only_defaults = {("candidate_id",), ("witness_id",)}
        preferred = (
            tuple(configured)
            if tuple(configured) not in id_only_defaults
            else tuple(self.correctness_compare_columns)
        )
        existing = tuple(c for c in preferred if c in available_columns)
        if existing:
            return existing

        semantic = tuple(
            c for c in self.correctness_compare_columns if c in available_columns
        )
        if semantic:
            return semantic

        fallback = "candidate_id" if kind == "candidate" else "witness_id"
        if fallback in available_columns:
            return (fallback,)
        raise ValueError(
            f"QuerySpec {self.query_id} không có khóa dedup {kind} hợp lệ; "
            f"available={list(available_columns)}"
        )

    def finalize_merged_candidates(
        self,
        *,
        vertices: DataFrame,
        candidates: DataFrame,
        config,
        query_id: str,
    ) -> DataFrame:
        """Hard Validation + Witness Normalization trên merged candidates.

        Các evaluator query-specific đã ghi kết quả hard constraints vào is_valid
        hoặc validation_status. Phương thức này áp dụng đúng bất biến:

            W_final = Normalize(HardValidate(Dedup(C_local ∪ C_stitched))).

        ``vertices`` được giữ trong giao diện để các query tương lai có thể thực
        hiện validation cần lookup; hiện tại các QuerySpec Vr15 chưa cần dùng.
        """
        del vertices, config
        candidate_keys = self.resolve_dedup_columns(
            "candidate", candidates.columns
        )
        normalized_candidates = candidates.dropDuplicates(list(candidate_keys))

        if "is_valid" in normalized_candidates.columns:
            valid = normalized_candidates.filter(F.col("is_valid") == F.lit(True))
        elif "validation_status" in normalized_candidates.columns:
            valid = normalized_candidates.filter(
                F.upper(F.col("validation_status").cast("string"))
                .isin("PASS", "VALID")
            )
        else:
            raise ValueError(
                f"QuerySpec {self.query_id}: merged candidates thiếu is_valid/"
                "validation_status để Hard Validation"
            )

        semantic_cols = [
            c for c in self.correctness_compare_columns if c in valid.columns
        ]
        id_parts = [F.lit(query_id), F.lit(self.witness_type)]
        if "candidate_id" in valid.columns:
            id_parts.append(F.coalesce(F.col("candidate_id").cast("string"), F.lit("")))
        else:
            id_parts.extend(
                F.coalesce(F.col(c).cast("string"), F.lit(""))
                for c in semantic_cols
            )

        witness_columns = [
            c for c in valid.columns if c not in {"witness_id", "witness_type"}
        ]
        witnesses = (
            valid
            .withColumn("witness_id", F.sha2(F.concat_ws("|", *id_parts), 256))
            .withColumn("witness_type", F.lit(self.witness_type))
            .select("witness_id", "witness_type", *witness_columns)
        )
        witness_keys = self.resolve_dedup_columns(
            "witness", witnesses.columns
        )
        return witnesses.dropDuplicates(list(witness_keys))


def _candidate_schema() -> T.StructType:
    return T.StructType([
        T.StructField("query_id", T.StringType()),
        T.StructField("query_group", T.StringType()),
        T.StructField("logical_query_type", T.StringType()),
        T.StructField("rpq_expression", T.StringType()),
        T.StructField("candidate_id", T.StringType()),
        T.StructField("person_id", T.StringType()),
        T.StructField("other_person_id", T.StringType()),
        T.StructField("thing_id", T.StringType()),
        T.StructField("vehicle_id", T.StringType()),
        T.StructField("carry1_src", T.StringType()),
        T.StructField("carry1_dst", T.StringType()),
        T.StructField("interact_src", T.StringType()),
        T.StructField("interact_dst", T.StringType()),
        T.StructField("carry2_src", T.StringType()),
        T.StructField("carry2_dst", T.StringType()),
        T.StructField("uses_src", T.StringType()),
        T.StructField("uses_dst", T.StringType()),
        T.StructField("carry1_start_time", T.StringType()),
        T.StructField("interact_start_time", T.StringType()),
        T.StructField("carry2_start_time", T.StringType()),
        T.StructField("uses_start_time", T.StringType()),
        T.StructField("ts_start", T.TimestampType()),
        T.StructField("ts_end", T.TimestampType()),
        T.StructField("tw_id", T.StringType()),
        T.StructField("location_id", T.StringType()),
        T.StructField("camera_id", T.StringType()),
        T.StructField("video_id", T.StringType()),
        T.StructField("quick_exit_delay_seconds", T.DoubleType()),
        T.StructField("max_time_gap_seconds", T.DoubleType()),
        T.StructField("observed_max_time_window_gap", T.IntegerType()),
        T.StructField("num_fragments", T.IntegerType()),
        T.StructField("num_partitions", T.IntegerType()),
        T.StructField("partition_trace", T.StringType()),
        T.StructField("explanation_trace", T.StringType()),
        T.StructField("constraint_tags", T.StringType()),
        T.StructField("validation_status", T.StringType()),
        T.StructField("validation_reason", T.StringType()),
        T.StructField("is_valid", T.BooleanType()),
    ])


def _witness_schema() -> T.StructType:
    return T.StructType([
        T.StructField("witness_id", T.StringType()),
        T.StructField("witness_type", T.StringType()),
        *_candidate_schema().fields,
    ])


def empty_candidates_like(spark) -> DataFrame:
    return spark.createDataFrame([], _candidate_schema())


def empty_witnesses_like(spark) -> DataFrame:
    return spark.createDataFrame([], _witness_schema())


def prepared_edges(edges: DataFrame) -> DataFrame:
    """Chuẩn hóa timestamp, epoch và chỉ số TimeWindow."""
    return (
        edges
        .withColumn("start_ts", F.to_timestamp("start_time"))
        .withColumn("end_ts", F.to_timestamp("end_time"))
        .withColumn(
            "start_epoch",
            F.coalesce(
                F.unix_timestamp("start_ts").cast("long"),
                F.col("start_time").cast("double").cast("long"),
            ),
        )
        .withColumn(
            "end_epoch",
            F.coalesce(
                F.unix_timestamp("end_ts").cast("long"),
                F.col("end_time").cast("double").cast("long"),
            ),
        )
        .withColumn("tw_num", extract_time_window_number(F.col("tw_id")))
    )


def typed_vertices(vertices: DataFrame, label: str, id_alias: str, gid_alias: str) -> DataFrame:
    return (
        vertices
        .filter(F.col("label") == F.lit(label))
        .select(
            F.col("id").cast("string").alias(id_alias),
            F.col("global_id").cast("string").alias(gid_alias),
            F.col("tw_id").cast("string").alias(f"{id_alias}_tw_id"),
            F.col("partition_id").cast("string").alias(f"{id_alias}_partition_id"),
            extract_time_window_number(F.col("tw_id")).alias(f"{id_alias}_tw_num"),
        )
        .dropDuplicates([id_alias])
    )


def add_missing_columns(df: DataFrame, schema: Mapping[str, str]) -> DataFrame:
    out = df
    for name, dtype in schema.items():
        if name not in out.columns:
            out = out.withColumn(name, F.lit(None).cast(dtype))
    return out

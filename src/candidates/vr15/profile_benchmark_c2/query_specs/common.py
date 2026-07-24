#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hợp đồng QuerySpec và helper dùng chung cho VideoGraphDB Vr15 C2.

Bất biến bắt buộc:
- C_M = Deduplicate(C_L union C_S)
- W_V = HardValidate(C_M, Q)
- W_F = Deduplicate(Normalize(W_V))
- correctness dùng khóa ngữ nghĩa, không dùng thứ tự vật lý/Spark partition.
"""
from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Mapping, Sequence, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from graph_io import extract_time_window_number

# (left_role, right_role, left_key_fields, right_key_fields, temporal_mode,
#  left_exit_state, right_entry_state)
# temporal_mode thuộc {FORWARD, ABSOLUTE}.
# State pair có thể là transition trực tiếp hoặc hai đầu của một automaton path
# có các role còn thiếu được Stitching bổ sung ở giữa.
BoundaryInterface = Tuple[
    str,
    str,
    Tuple[str, ...],
    Tuple[str, ...],
    str,
    str,
    str,
]


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

    # Đặc tả LocalEval-GlobalEval.
    automaton_start_state: str = "q0"
    automaton_accepting_states: tuple[str, ...] = ("q_accept",)
    automaton_transitions: tuple[tuple[str, str, str], ...] = ()
    required_fragment_roles: tuple[str, ...] = ()
    optional_fragment_roles: tuple[str, ...] = ()
    boundary_eligible_roles: tuple[str, ...] = ()
    stitch_mode: str = "QUERY_SPECIFIC"
    stitch_key_fields: tuple[str, ...] = ()
    stitch_role_order: tuple[str, ...] = ()
    boundary_interfaces: tuple[BoundaryInterface, ...] = ()
    hard_constraint_names: tuple[str, ...] = ()
    runtime_parameter_schema: Mapping[str, str] = field(default_factory=dict)

    # Candidate key phải giữ các biến thể evidence cho tới sau Hard Validation.
    # Witness key là phép chiếu ngữ nghĩa sau normalization.
    candidate_dedup_columns: tuple[str, ...] = ()
    witness_dedup_columns: tuple[str, ...] = ()

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

    def _state_path_exists(self, source_state: str, target_state: str) -> bool:
        if source_state == target_state:
            return True
        adjacency: dict[str, set[str]] = {}
        for entry, _role, exit_state in self.automaton_transitions:
            adjacency.setdefault(entry, set()).add(exit_state)
        queue: deque[str] = deque([source_state])
        visited = {source_state}
        while queue:
            state = queue.popleft()
            for nxt in adjacency.get(state, set()):
                if nxt == target_state:
                    return True
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append(nxt)
        return False

    def validate_contract(self) -> None:
        """Fail-fast khi QuerySpec không đủ thông tin để benchmark chính thức."""
        if not self.correctness_compare_columns:
            raise ValueError(f"{self.query_id}: correctness_compare_columns rỗng")
        if not self.candidate_dedup_columns:
            raise ValueError(f"{self.query_id}: candidate_dedup_columns rỗng")
        if not self.witness_dedup_columns:
            raise ValueError(f"{self.query_id}: witness_dedup_columns rỗng")
        if not self.boundary_interfaces:
            raise ValueError(f"{self.query_id}: boundary_interfaces rỗng")

        states = {self.automaton_start_state, *self.automaton_accepting_states}
        for entry, role, exit_state in self.automaton_transitions:
            if not role:
                raise ValueError(f"{self.query_id}: automaton role rỗng")
            states.update((entry, exit_state))

        seen_interface_ids: set[tuple[object, ...]] = set()
        for interface in self.boundary_interfaces:
            if len(interface) != 7:
                raise ValueError(
                    f"{self.query_id}: BoundaryInterface phải có 7 phần tử, nhận {interface!r}"
                )
            (
                left_role,
                right_role,
                left_fields,
                right_fields,
                temporal_mode,
                left_exit_state,
                right_entry_state,
            ) = interface
            if temporal_mode not in {"FORWARD", "ABSOLUTE"}:
                raise ValueError(
                    f"{self.query_id}: temporal_mode không hợp lệ: {temporal_mode}"
                )
            if not left_role or not right_role or not left_fields or not right_fields:
                raise ValueError(f"{self.query_id}: boundary interface thiếu role/key: {interface!r}")
            if len(left_fields) != len(right_fields):
                raise ValueError(
                    f"{self.query_id}: số trường binding hai phía không bằng nhau: {interface!r}"
                )
            if left_exit_state not in states or right_entry_state not in states:
                raise ValueError(
                    f"{self.query_id}: interface dùng automaton state chưa khai báo: {interface!r}"
                )
            if not self._state_path_exists(left_exit_state, right_entry_state):
                raise ValueError(
                    f"{self.query_id}: không có automaton path từ "
                    f"{left_exit_state} tới {right_entry_state}: {interface!r}"
                )
            identity = tuple(interface)
            if identity in seen_interface_ids:
                raise ValueError(f"{self.query_id}: boundary interface trùng: {interface!r}")
            seen_interface_ids.add(identity)

    def select_complete_local_candidates(
        self,
        candidates: DataFrame,
        config,
    ) -> DataFrame:
        """Chọn candidate đã hoàn thành pattern cục bộ, độc lập validity."""
        if self.local_candidate_completion_filter is None:
            return candidates
        return self.local_candidate_completion_filter(candidates, config)

    def resolve_dedup_columns(
        self,
        kind: str,
        available_columns: Sequence[str],
    ) -> tuple[str, ...]:
        if kind not in {"candidate", "witness"}:
            raise ValueError(f"Loại dedup không hợp lệ: {kind}")
        configured = (
            self.candidate_dedup_columns
            if kind == "candidate"
            else self.witness_dedup_columns
        )
        if not configured:
            raise ValueError(
                f"QuerySpec {self.query_id} chưa khai báo {kind}_dedup_columns"
            )
        missing = [c for c in configured if c not in available_columns]
        if missing:
            raise ValueError(
                f"QuerySpec {self.query_id}: thiếu cột dedup {kind}: {missing}; "
                f"available={list(available_columns)}"
            )
        return tuple(configured)

    def deduplicate_candidates(self, candidates: DataFrame) -> DataFrame:
        keys = self.resolve_dedup_columns("candidate", candidates.columns)
        return candidates.dropDuplicates(list(keys))

    def hard_validate_candidates(self, candidates: DataFrame) -> DataFrame:
        """W_V = HardValidate(C_M,Q), thực hiện sau candidate merge/dedup.

        Evaluator query-specific tạo predicate ``is_valid`` bằng cùng cấu hình và
        cùng hard constraints cho Baseline/Proposed. Do Spark lazy, filter tại đây
        vẫn nằm trong lineage Hard Validation của C_M. Candidate key dùng
        ``candidate_id`` query-specific để không loại sớm hai evidence path khác
        nhau nhưng normalize về cùng witness.
        """
        deduplicated = self.deduplicate_candidates(candidates)
        if "is_valid" in deduplicated.columns:
            return deduplicated.filter(F.col("is_valid") == F.lit(True))
        if "validation_status" in deduplicated.columns:
            return deduplicated.filter(
                F.upper(F.col("validation_status").cast("string")).isin(
                    "PASS", "VALID"
                )
            )
        raise ValueError(
            f"QuerySpec {self.query_id}: candidate thiếu is_valid/validation_status"
        )

    def normalize_valid_candidates(self, valid_candidates: DataFrame) -> DataFrame:
        """W_F = Deduplicate(Normalize(W_V)) theo khóa ngữ nghĩa."""
        compare_cols = list(self.correctness_compare_columns)
        missing = [c for c in compare_cols if c not in valid_candidates.columns]
        if missing:
            raise ValueError(
                f"QuerySpec {self.query_id}: thiếu cột correctness: {missing}"
            )

        id_parts = [
            F.coalesce(F.col(c).cast("string"), F.lit(""))
            for c in compare_cols
        ]
        candidate_columns = [
            c
            for c in valid_candidates.columns
            if c not in {"witness_id", "witness_type"}
        ]
        witnesses = (
            valid_candidates
            .withColumn("witness_id", F.sha2(F.concat_ws("|", *id_parts), 256))
            .withColumn("witness_type", F.lit(self.witness_type))
            .select("witness_id", "witness_type", *candidate_columns)
        )
        witness_keys = self.resolve_dedup_columns("witness", witnesses.columns)
        return witnesses.dropDuplicates(list(witness_keys))

    def finalize_merged_candidates(
        self,
        *,
        candidates: DataFrame,
    ) -> tuple[DataFrame, DataFrame]:
        valid = self.hard_validate_candidates(candidates)
        witnesses = self.normalize_valid_candidates(valid)
        return valid, witnesses


def _config_or_env(config, attribute: str, env_name: str, default=None):
    value = getattr(config, attribute, None)
    if value is None or value == "":
        value = os.getenv(env_name, default)
    return value


def resolve_query_time_scope(config) -> tuple[str, str | None, str | None]:
    mode = str(
        _config_or_env(
            config, "query_time_scope", "QUERY_TIME_SCOPE", "FULL_DATASET"
        )
        or "FULL_DATASET"
    ).upper()
    start = _config_or_env(config, "query_start_time", "QUERY_START_TIME")
    end = _config_or_env(config, "query_end_time", "QUERY_END_TIME")
    return mode, start, end


def validate_query_time_scope_config(config) -> None:
    mode, start, end = resolve_query_time_scope(config)
    if mode not in {"FULL_DATASET", "EXPLICIT_INTERVAL"}:
        raise ValueError(f"query_time_scope không hợp lệ: {mode}")
    if mode == "EXPLICIT_INTERVAL" and (not start or not end):
        raise ValueError(
            "EXPLICIT_INTERVAL yêu cầu đồng thời query_start_time và query_end_time"
        )


def _timestamp_literal(value: str) -> Column:
    text = str(value)
    return F.coalesce(
        F.to_timestamp(F.lit(text)),
        F.to_timestamp(F.from_unixtime(F.lit(text).cast("double"))),
    )


def apply_query_time_scope(edges: DataFrame, config) -> DataFrame:
    """Áp dụng cùng query time scope cho Baseline và từng LocalEval partition.

    Evidence phải nằm hoàn toàn trong [query_start_time, query_end_time].
    FULL_DATASET giữ nguyên DataFrame.
    """
    mode, start, end = resolve_query_time_scope(config)
    if mode == "FULL_DATASET":
        return edges
    validate_query_time_scope_config(config)

    out = prepared_edges(edges)
    query_start = _timestamp_literal(str(start))
    query_end = _timestamp_literal(str(end))
    return out.filter(
        F.col("start_ts").isNotNull()
        & F.col("end_ts").isNotNull()
        & (F.col("start_ts") >= query_start)
        & (F.col("end_ts") <= query_end)
    )


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


def typed_vertices(
    vertices: DataFrame,
    label: str,
    id_alias: str,
    gid_alias: str,
) -> DataFrame:
    return (
        vertices
        .filter(F.col("label") == F.lit(label))
        .select(
            F.col("id").cast("string").alias(id_alias),
            F.col("global_id").cast("string").alias(gid_alias),
            F.col("tw_id").cast("string").alias(f"{id_alias}_tw_id"),
            F.col("partition_id").cast("string").alias(
                f"{id_alias}_partition_id"
            ),
            extract_time_window_number(F.col("tw_id")).alias(
                f"{id_alias}_tw_num"
            ),
        )
        .dropDuplicates([id_alias])
    )


def add_missing_columns(df: DataFrame, schema: Mapping[str, str]) -> DataFrame:
    out = df
    for name, dtype in schema.items():
        if name not in out.columns:
            out = out.withColumn(name, F.lit(None).cast(dtype))
    return out

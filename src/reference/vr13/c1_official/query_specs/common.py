#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared query contracts and stable helper functions.

Only definitions that are genuinely shared by multiple query modules should be
placed here. Query-specific semantics must remain in q11.py, q43.py, and future
query modules.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from graph_io import get_spark_session


@dataclass(frozen=True)
class QuerySpec:
    query_id: str
    aliases: tuple[str, ...]
    query_group: str
    query_name: str
    logical_query_type: str
    rpq_expression: str
    physical_plan: str
    witness_type: str
    compare_columns: Sequence[str]
    local_event_extractor: Callable[..., DataFrame]
    baseline_evaluator: Callable[..., tuple[DataFrame, DataFrame]]
    distributed_evaluator: Callable[..., tuple[DataFrame, DataFrame]]
    candidate_upper_bound: Callable[..., DataFrame] | None = None


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
        T.StructField("max_nexttw_hops", T.IntegerType()),
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
    """Prepare robust timestamp, epoch, and numeric time-window columns."""
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
        .withColumn(
            "tw_num",
            F.regexp_extract(F.col("tw_id").cast("string"), r"(\d+)", 1).cast("int"),
        )
    )


def typed_vertices(
    vertices: DataFrame,
    label: str,
    id_alias: str,
    gid_alias: str,
) -> DataFrame:
    """Project one typed vertex class into a stable query-facing schema."""
    return (
        vertices
        .filter(F.col("label") == F.lit(label))
        .select(
            F.col("id").cast("string").alias(id_alias),
            F.col("global_id").cast("string").alias(gid_alias),
            F.col("tw_id").cast("string").alias(f"{id_alias}_tw_id"),
            F.col("partition_id").cast("string").alias(f"{id_alias}_partition_id"),
            F.regexp_extract(
                F.col("tw_id").cast("string"),
                r"(\d+)",
                1,
            ).cast("int").alias(f"{id_alias}_tw_num"),
        )
        .dropDuplicates([id_alias])
    )

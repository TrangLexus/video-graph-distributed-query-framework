#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Graph/HDFS I/O, normalization, logical NEXT_TW construction, and Spark helpers."""
from __future__ import annotations

import time
from functools import reduce
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config import Stage2Config


REQUIRED_FILES = [
    "nodes_location.csv",
    "nodes_camera.csv",
    "partitions.csv",
    "nodes_person.csv",
    "nodes_person_TW.csv",
    "nodes_thing.csv",
    "nodes_thing_TW.csv",
    "nodes_vehicle.csv",
    "nodes_vehicle_TW.csv",
    "nodes_timewindow.csv",
    "nodes_video.csv",
    "rels.csv",
]


# ---------------------------------------------------------------------------
# Spark/runtime helpers
# ---------------------------------------------------------------------------

def create_spark(config: Stage2Config) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(config.app_name)
        .master(config.spark_master)
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.executor.cores", config.executor_cores)
        .config("spark.cores.max", config.cores_max)
        .config("spark.sql.shuffle.partitions", str(config.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.adaptive.skewJoin.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", config.auto_broadcast_join_threshold)
        .config("spark.sql.files.maxPartitionBytes", config.spark_files_max_partition_bytes)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir(f"/tmp/videographdb_{config.method}")
    spark.conf.set("spark.sql.shuffle.partitions", str(config.spark_shuffle_partitions))
    spark.sparkContext.setLogLevel(config.spark_log_level)
    return spark


def get_spark_session(df: DataFrame):
    try:
        return df.sparkSession
    except AttributeError:
        return df.sql_ctx.sparkSession


def existing_columns(df: DataFrame, cols: Sequence[str]) -> list[str]:
    return [c for c in cols if c in df.columns]


def index_df(df: DataFrame, config: Stage2Config, key_cols: Sequence[str] | None = None, name: str = "df") -> DataFrame:
    key_cols = existing_columns(df, list(key_cols or []))
    if not config.spark_indexing_enabled or not key_cols:
        return df
    try:
        print(f"[Spark-index] {name}: repartition by {key_cols[:3]}")
        return df.repartition(config.spark_shuffle_partitions, *[F.col(c) for c in key_cols[:3]])
    except Exception as exc:
        print(f"[WARN] Could not repartition {name}: {exc!r}")
        return df


def persist_df(df: DataFrame, config: Stage2Config, name: str, key_cols: Sequence[str] | None = None) -> DataFrame:
    out = index_df(df, config, key_cols=key_cols, name=name)
    if config.persist_intermediates:
        try:
            out = out.persist(StorageLevel.MEMORY_AND_DISK)
            print(f"[Persist] {name}: MEMORY_AND_DISK")
        except Exception as exc:
            print(f"[WARN] Could not persist {name}: {exc!r}")
    return out


def safe_count(df: DataFrame, name: str, required: bool = False) -> int:
    t0 = time.perf_counter()
    try:
        n = int(df.count())
        print(f"[Count] {name}: {n} rows, {time.perf_counter() - t0:.6f}s")
        return n
    except Exception as exc:
        print(f"[WARN] Count failed for {name}: {exc!r}")
        if required:
            raise
        return -1


def materialize_df(df: DataFrame, name: str, enabled: bool, required: bool = True) -> tuple[int, float]:
    if not enabled:
        print(f"[Skip materialize] {name}: disabled")
        return -1, 0.0
    t0 = time.perf_counter()
    try:
        n = int(df.count())
        dt = time.perf_counter() - t0
        print(f"[Materialize] {name}: {n} rows, {dt:.6f}s")
        return n, dt
    except Exception as exc:
        dt = time.perf_counter() - t0
        print(f"[WARN] Materialization failed for {name}: {exc!r}, elapsed={dt:.6f}s")
        if required:
            raise
        return -1, dt


def unpersist_all(*dfs: DataFrame) -> None:
    for df in dfs:
        try:
            if df is not None:
                df.unpersist()
        except Exception:
            pass




def _csv_safe_dataframe(df: DataFrame) -> DataFrame:
    """Convert Spark complex columns to CSV-compatible strings.

    Spark CSV cannot write ArrayType, MapType, or StructType columns directly.
    Q1.1 trajectory witnesses contain trace arrays such as entity_tw_trace,
    tw_trace, camera_trace, location_trace, and partition_trace. Serialize
    complex values as JSON strings before writing so the output remains
    deterministic and can still be compared by run_experiment.py.
    """
    out = df
    for field in df.schema.fields:
        dtype = field.dataType
        if isinstance(dtype, (T.ArrayType, T.MapType, T.StructType)):
            # Encode JSON as Base64 before CSV export. Spark CSV can quote JSON,
            # but pandas' C parser may misinterpret Spark-style escaped quotes in
            # very wide trajectory rows. Base64 removes commas, quotes, and
            # newlines while remaining deterministic for witness comparison.
            out = out.withColumn(
                field.name,
                F.base64(F.encode(F.to_json(F.col(field.name)), "UTF-8")),
            )
        elif isinstance(dtype, T.BinaryType):
            out = out.withColumn(field.name, F.base64(F.col(field.name)))
    return out

def write_spark_csv(df: DataFrame, out_dir: Path, name: str, enabled: bool) -> None:
    """Write a Spark DataFrame as CSV safely in Spark Standalone.

    Complex Spark columns are serialized to JSON strings first because Spark CSV
    does not support ArrayType, MapType, or StructType directly. Local outputs are
    written to a temporary HDFS directory and then copied back to the driver/master
    filesystem, avoiding worker-local file:// outputs.
    """
    if not enabled:
        print(f"[Skip write] {name}")
        return

    safe_df = _csv_safe_dataframe(df)
    local_path = (out_dir / name).resolve()
    local_path.parent.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(df)
    tmp_hdfs_dir = f"hdfs:///tmp/videographdb_csv_export/{int(time.time() * 1000)}_{name}"
    print(f"[Write tmp HDFS] {tmp_hdfs_dir}")

    safe_df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_hdfs_dir)

    import shutil
    if local_path.exists():
        shutil.rmtree(local_path)
    local_path.mkdir(parents=True, exist_ok=True)

    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    src = jvm.org.apache.hadoop.fs.Path(tmp_hdfs_dir)
    fs = src.getFileSystem(hconf)

    copied = 0
    for status in fs.listStatus(src):
        src_file = status.getPath()
        filename = src_file.getName()
        if filename.startswith("part-") and filename.endswith(".csv"):
            dst_file = jvm.org.apache.hadoop.fs.Path((local_path / filename).as_uri())
            fs.copyToLocalFile(False, src_file, dst_file)
            copied += 1

    (local_path / "_SUCCESS").write_text("", encoding="utf-8")
    fs.delete(src, True)

    if copied == 0:
        raise RuntimeError(f"No part-*.csv copied from temporary HDFS output: {tmp_hdfs_dir}")

    print(f"[Write] {local_path} ({copied} part file(s) copied from HDFS)")


def write_runtime_summary(rows: list[dict], out_dir: Path, enabled: bool = True) -> None:
    if not enabled:
        print("[Skip write] runtime_summary_all_queries.csv")
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "runtime_summary_all_queries.csv"
    pd.DataFrame(rows).to_csv(out_file, index=False)
    print("[Write]", out_file)


def print_config(config: Stage2Config, spark: SparkSession) -> None:
    print("=" * 96)
    print(f"VideoGraphDB Vr09 runner: {config.method}")
    print("DATASET_ROOT:", config.dataset_root)
    print("BY_PARTITION_DIR:", config.by_partition_dir)
    print("QUERY_IDS:", config.query_ids)
    print("OUTPUT_ROOT:", config.output_root)
    print("Spark master:", spark.sparkContext.master)
    print("Spark app id:", spark.sparkContext.applicationId)
    print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
    print("epsilon_time_seconds:", config.epsilon_time_seconds)
    print("quick_exit_seconds:", config.quick_exit_seconds)
    print("max_nexttw_hops:", config.max_nexttw_hops)
    print("Q14_EXIT_PERSON_ROLE:", config.q14_exit_person_role)
    print("Q14_MIN_PARTITION_COUNT:", config.q14_min_partition_count)
    print("Q14_REQUIRE_DIFFERENT_LOCATION:", config.q14_require_different_location)
    print("Q14_QUICK_EXIT_REFERENCE:", config.q14_quick_exit_reference)
    print("Q14_DIAGNOSTICS_ENABLED:", config.q14_diagnostics_enabled)
    print("BENCHMARK_TIMING_ONLY:", config.benchmark_timing_only)
    print("BENCHMARK_MATERIALIZE_STEP_BOUNDARIES:", config.benchmark_materialize_step_boundaries)
    print("=" * 96)


# ---------------------------------------------------------------------------
# HDFS/local path helpers
# ---------------------------------------------------------------------------

def is_hdfs_path(path: str) -> bool:
    return str(path).startswith("hdfs://")


def join_uri(base: str, *parts: str) -> str:
    base = str(base).rstrip("/")
    tail = "/".join(str(p).strip("/") for p in parts if str(p).strip("/"))
    return base if not tail else f"{base}/{tail}"


def basename_uri(path: str) -> str:
    return str(path).rstrip("/").split("/")[-1]


def _hadoop_path(spark: SparkSession, path: str):
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)


def _hadoop_fs(spark: SparkSession, path: str):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    return _hadoop_path(spark, path).getFileSystem(conf)


def path_exists(spark: SparkSession, path: str) -> bool:
    if is_hdfs_path(path):
        return bool(_hadoop_fs(spark, path).exists(_hadoop_path(spark, path)))
    return Path(path).exists()


def path_is_dir(spark: SparkSession, path: str) -> bool:
    if is_hdfs_path(path):
        fs = _hadoop_fs(spark, path)
        p = _hadoop_path(spark, path)
        return bool(fs.exists(p) and fs.getFileStatus(p).isDirectory())
    return Path(path).is_dir()


def list_subdirs(spark: SparkSession, path: str) -> list[str]:
    if is_hdfs_path(path):
        fs = _hadoop_fs(spark, path)
        p = _hadoop_path(spark, path)
        if not fs.exists(p):
            raise FileNotFoundError(path)
        return sorted([st.getPath().toString() for st in fs.listStatus(p) if st.isDirectory()])
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    return sorted(str(x) for x in p.iterdir() if x.is_dir())


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    print(f"Reading CSV: {path}")
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(path)
    )


def load_tables_from_dir(spark: SparkSession, base_dir: str, dataset_root: str) -> dict[str, DataFrame]:
    tables = {}
    for filename in REQUIRED_FILES:
        part_path = join_uri(base_dir, filename)
        root_path = join_uri(dataset_root, filename)
        if path_exists(spark, part_path):
            path = part_path
        elif path_exists(spark, root_path):
            path = root_path
        else:
            continue
        tables[filename.replace(".csv", "")] = read_csv(spark, path)
    return tables


def discover_partition_dirs(spark: SparkSession, by_partition_dir: str) -> list[str]:
    if not path_is_dir(spark, by_partition_dir):
        raise FileNotFoundError(f"Missing by_partition directory: {by_partition_dir}")
    dirs = list_subdirs(spark, by_partition_dir)
    if not dirs:
        raise FileNotFoundError(f"No partition directory found under: {by_partition_dir}")
    print("Detected partitions:", [basename_uri(p) for p in dirs])
    return dirs


def load_partition_tables(spark: SparkSession, dataset_root: str, by_partition_dir: str) -> list[dict]:
    out = []
    for pdir in discover_partition_dirs(spark, by_partition_dir):
        tables = load_tables_from_dir(spark, pdir, dataset_root)
        tables["partition_name"] = basename_uri(pdir)
        tables["partition_path"] = pdir
        out.append(tables)
    return out


# ---------------------------------------------------------------------------
# Graph normalization
# ---------------------------------------------------------------------------

def first_existing_col(df: DataFrame, candidates: Iterable[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _optional_col(df: DataFrame, candidates: Iterable[str], cast_type: str = "string"):
    cols = [F.col(c).cast(cast_type) for c in candidates if c in df.columns]
    if cols:
        return F.coalesce(*cols)
    return F.lit(None).cast(cast_type)


def normalize_vertices(tables: dict) -> DataFrame:
    dfs: list[DataFrame] = []

    if "nodes_person_TW" in tables:
        df = tables["nodes_person_TW"]
        id_col = first_existing_col(df, ["person_tw_id", "pid_tw", "id", "node_id"])
        gid_col = first_existing_col(df, ["person_id", "id_global", "global_id"])
        if id_col and gid_col:
            dfs.append(df.select(
                F.col(id_col).cast("string").alias("id"),
                F.lit("Person_TW").alias("label"),
                F.col(gid_col).cast("string").alias("global_id"),
                F.col(gid_col).cast("string").alias("person_id"),
                _optional_col(df, ["tw_id", "window_id"]).alias("tw_id"),
                _optional_col(df, ["partition_id", "partition"]).alias("partition_id"),
                _optional_col(df, ["shirt_color"]).alias("shirt_color"),
                _optional_col(df, ["pant_color"]).alias("pant_color"),
                _optional_col(df, ["gender", "sex"]).alias("gender"),
            ))

    if "nodes_thing_TW" in tables:
        df = tables["nodes_thing_TW"]
        id_col = first_existing_col(df, ["thing_tw_id", "tid_tw", "id", "node_id"])
        gid_col = first_existing_col(df, ["thing_id", "id_global", "global_id"])
        if id_col and gid_col:
            dfs.append(df.select(
                F.col(id_col).cast("string").alias("id"),
                F.lit("Thing_TW").alias("label"),
                F.col(gid_col).cast("string").alias("global_id"),
                F.lit(None).cast("string").alias("person_id"),
                _optional_col(df, ["tw_id", "window_id"]).alias("tw_id"),
                _optional_col(df, ["partition_id", "partition"]).alias("partition_id"),
                F.lit(None).cast("string").alias("shirt_color"),
                F.lit(None).cast("string").alias("pant_color"),
                F.lit(None).cast("string").alias("gender"),
            ))

    if "nodes_vehicle_TW" in tables:
        df = tables["nodes_vehicle_TW"]
        id_col = first_existing_col(df, ["vehicle_tw_id", "vid_tw", "id", "node_id"])
        gid_col = first_existing_col(df, ["vehicle_id", "id_global", "global_id"])
        if id_col and gid_col:
            dfs.append(df.select(
                F.col(id_col).cast("string").alias("id"),
                F.lit("Vehicle_TW").alias("label"),
                F.col(gid_col).cast("string").alias("global_id"),
                F.lit(None).cast("string").alias("person_id"),
                _optional_col(df, ["tw_id", "window_id"]).alias("tw_id"),
                _optional_col(df, ["partition_id", "partition"]).alias("partition_id"),
                F.lit(None).cast("string").alias("shirt_color"),
                F.lit(None).cast("string").alias("pant_color"),
                F.lit(None).cast("string").alias("gender"),
            ))

    if not dfs:
        spark = get_spark_session(next(iter(tables.values())))
        schema = T.StructType([
            T.StructField("id", T.StringType()),
            T.StructField("label", T.StringType()),
            T.StructField("global_id", T.StringType()),
            T.StructField("person_id", T.StringType()),
            T.StructField("tw_id", T.StringType()),
            T.StructField("partition_id", T.StringType()),
            T.StructField("shirt_color", T.StringType()),
            T.StructField("pant_color", T.StringType()),
            T.StructField("gender", T.StringType()),
        ])
        return spark.createDataFrame([], schema)

    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs).dropDuplicates(["id"])


def normalize_base_edges(rels: DataFrame) -> DataFrame:
    r = rels
    # edge_id is optional but useful for unified schema / explanation.
    edge_id_col = first_existing_col(r, ["edge_id", "id", "rel_id", "relationship_id"])
    base = r.select(
        _optional_col(r, ["source_id", "source", "src", "from_id", "from"]).alias("src"),
        _optional_col(r, ["destination_id", "destination", "target", "dst", "to_id", "to"]).alias("dst"),
        _optional_col(r, ["type", "label", "relation", "edge_type"]).alias("label"),
        _optional_col(r, ["start_time", "start_ts", "ts_start", "time_start"]).alias("start_time"),
        _optional_col(r, ["end_time", "end_ts", "ts_end", "time_end"]).alias("end_time"),
        _optional_col(r, ["date"]).alias("date"),
        _optional_col(r, ["tw_id", "window_id", "timewindow_id", "time_window_id"]).alias("tw_id"),
        _optional_col(r, ["partition_id", "partition"]).alias("partition_id"),
        _optional_col(r, ["camera_id", "cam_id"]).alias("camera_id"),
        _optional_col(r, ["location_id", "loc_id"]).alias("location_id"),
        _optional_col(r, ["video_id", "vid"]).alias("video_id"),
        _optional_col(r, ["confidence", "score", "conf"], cast_type="double").alias("confidence"),
        _optional_col(r, ["bbox", "bounding_box"]).alias("bbox"),
        _optional_col(r, ["description", "desc"]).alias("description"),
    )
    if edge_id_col:
        base = base.withColumn("edge_id", F.col(edge_id_col).cast("string"))
    else:
        base = base.withColumn(
            "edge_id",
            F.sha2(F.concat_ws("|", "src", "dst", "label", "start_time", "end_time", "tw_id", "partition_id"), 256),
        )
    return base.dropDuplicates()


def union_dataframes(dfs: list[DataFrame]) -> DataFrame:
    dfs = [df for df in dfs if df is not None]
    if not dfs:
        raise ValueError("No DataFrames to union")
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def enrich_vertices_with_edge_context(vertices: DataFrame, edges: DataFrame) -> DataFrame:
    """Fill missing vertex tw_id/partition_id from incident edge context.

    Some generated datasets store nodes_*_TW.csv with only ids such as
    pid_tw/person_id and put tw_id/partition_id in rels.csv. If vertices keep
    tw_id as NULL, global NEXT_TW_* construction becomes empty and Q14/Q4.3 may
    return zero even when role edges exist.
    """
    edge_ctx = (
        edges.select(
            F.col("src").alias("id"),
            F.col("tw_id").alias("_edge_tw_id"),
            F.col("partition_id").alias("_edge_partition_id"),
        )
        .unionByName(edges.select(
            F.col("dst").alias("id"),
            F.col("tw_id").alias("_edge_tw_id"),
            F.col("partition_id").alias("_edge_partition_id"),
        ))
        .filter(F.col("id").isNotNull())
        .groupBy("id")
        .agg(
            F.first("_edge_tw_id", ignorenulls=True).alias("_edge_tw_id"),
            F.first("_edge_partition_id", ignorenulls=True).alias("_edge_partition_id"),
        )
    )
    return (
        vertices.alias("v")
        .join(edge_ctx.alias("e"), "id", "left")
        .withColumn("tw_id", F.coalesce(F.col("v.tw_id"), F.col("e._edge_tw_id")))
        .withColumn("partition_id", F.coalesce(F.col("v.partition_id"), F.col("e._edge_partition_id")))
        .drop("_edge_tw_id", "_edge_partition_id")
    )


def union_partition_graph(partition_tables: list[dict]) -> tuple[DataFrame, DataFrame]:
    vertices_raw = union_dataframes([normalize_vertices(t) for t in partition_tables]).dropDuplicates(["id"])
    edge_dfs = [normalize_base_edges(t["rels"]) for t in partition_tables if "rels" in t]
    edges = union_dataframes(edge_dfs).dropDuplicates()
    vertices = enrich_vertices_with_edge_context(vertices_raw, edges).dropDuplicates(["id"])
    return vertices, edges


# ---------------------------------------------------------------------------
# Logical NEXT_TW_* construction
# ---------------------------------------------------------------------------

def empty_edge_df(spark) -> DataFrame:
    schema = T.StructType([
        T.StructField("src", T.StringType()),
        T.StructField("dst", T.StringType()),
        T.StructField("label", T.StringType()),
        T.StructField("start_time", T.StringType()),
        T.StructField("end_time", T.StringType()),
        T.StructField("date", T.StringType()),
        T.StructField("tw_id", T.StringType()),
        T.StructField("partition_id", T.StringType()),
        T.StructField("camera_id", T.StringType()),
        T.StructField("location_id", T.StringType()),
        T.StructField("video_id", T.StringType()),
        T.StructField("confidence", T.DoubleType()),
        T.StructField("bbox", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("edge_id", T.StringType()),
    ])
    return spark.createDataFrame([], schema)


def _next_tw_for_label(vertices: DataFrame, vertex_label: str, edge_label: str, require_consecutive: bool) -> DataFrame:
    spark = get_spark_session(vertices)
    required = {"id", "label", "global_id", "tw_id", "partition_id"}
    if not required.issubset(set(vertices.columns)):
        return empty_edge_df(spark)

    base = (
        vertices
        .filter((F.col("label") == F.lit(vertex_label)) & F.col("id").isNotNull())
        .withColumn("_tw_num", F.regexp_extract(F.col("tw_id").cast("string"), r"(\d+)", 1).cast("int"))
        .filter(F.col("global_id").isNotNull() & F.col("_tw_num").isNotNull())
        .select(
            F.col("id").cast("string").alias("_src"),
            F.col("global_id").cast("string").alias("_global_id"),
            F.col("tw_id").cast("string").alias("_tw_id"),
            F.col("partition_id").cast("string").alias("_partition_id"),
            F.col("_tw_num"),
        )
        .dropDuplicates(["_src", "_global_id", "_tw_num"])
    )

    w = Window.partitionBy("_global_id").orderBy(F.col("_tw_num"), F.col("_partition_id"), F.col("_src"))
    paired = (
        base
        .withColumn("_dst", F.lead("_src").over(w))
        .withColumn("_next_tw_num", F.lead("_tw_num").over(w))
        .filter(F.col("_dst").isNotNull() & (F.col("_src") != F.col("_dst")))
    )
    if require_consecutive:
        paired = paired.filter((F.col("_next_tw_num") - F.col("_tw_num")) == F.lit(1))

    out = paired.select(
        F.col("_src").alias("src"),
        F.col("_dst").alias("dst"),
        F.lit(edge_label).alias("label"),
        F.lit(None).cast("string").alias("start_time"),
        F.lit(None).cast("string").alias("end_time"),
        F.lit(None).cast("string").alias("date"),
        F.col("_tw_id").alias("tw_id"),
        F.col("_partition_id").alias("partition_id"),
        F.lit(None).cast("string").alias("camera_id"),
        F.lit(None).cast("string").alias("location_id"),
        F.lit(None).cast("string").alias("video_id"),
        F.lit(1.0).cast("double").alias("confidence"),
        F.lit(None).cast("string").alias("bbox"),
        F.lit("query-time logical continuity edge").alias("description"),
    )
    return (
        out.withColumn("edge_id", F.sha2(F.concat_ws("|", "src", "dst", "label", "tw_id", "partition_id"), 256))
        .dropDuplicates(["src", "dst", "label"])
    )


def build_next_tw_edges(vertices: DataFrame, require_consecutive: bool = False) -> DataFrame:
    dfs = [
        _next_tw_for_label(vertices, "Person_TW", "NEXT_TW_PERSON", require_consecutive),
        _next_tw_for_label(vertices, "Thing_TW", "NEXT_TW_THING", require_consecutive),
        _next_tw_for_label(vertices, "Vehicle_TW", "NEXT_TW_VEHICLE", require_consecutive),
    ]
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs).dropDuplicates(["src", "dst", "label"])

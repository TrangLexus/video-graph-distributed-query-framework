#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kiểm tra API của project Vr15 trước khi chép đè bundle.

Chạy tại thư mục project hiện hành hoặc truyền --project-root. Script chỉ đọc
config.py, graph_io.py, queries.py; không sửa file và không cần khởi động Spark.
"""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path

REQUIRED_GRAPH_IO = {
    "basename_uri",
    "create_spark",
    "load_partition_tables",
    "materialize_df",
    "normalize_base_edges",
    "normalize_vertices",
    "persist_df",
    "print_config",
    "union_dataframes",
    "union_partition_graph",
    "unpersist_all",
    "validate_temporal_instance_context",
    "write_runtime_summary",
    "write_spark_csv",
}
REQUIRED_CONFIG = {"Stage2Config", "ensure_supported_query_ids", "load_config"}
EXPECTED_QUERY_IDS = {
    "Q1.1", "Q1.2", "Q1.3",
    "Q2.1", "Q2.2", "Q2.3",
    "Q3.1", "Q3.2", "Q3.3",
    "Q4.1", "Q4.2", "Q4.3",
}
CONFIG_ATTRIBUTES = {
    "dataset_root", "by_partition_dir", "query_ids", "output_root",
    "benchmark_validate_result", "max_time_gap_seconds",
    "candidate_pruning_max_time_window_gap", "time_window_duration_seconds",
    "quick_exit_max_seconds", "exit_person_role", "minimum_partition_count",
    "require_different_location", "quick_exit_reference_role",
    "executor_memory", "executor_cores", "cores_max",
    "spark_available_task_slots", "spark_shuffle_partitions",
    "spark_scheduler_mode", "save_candidate_outputs", "save_final_witnesses",
    "save_intermediate_outputs", "save_runtime_summary",
}


def top_level_symbols(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    symbols: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            symbols.add(node.name)
    return symbols


def class_declared_fields(path: Path, class_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    fields: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for child in node.body:
            if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
                fields.add(child.target.id)
            elif isinstance(child, ast.Assign):
                for target in child.targets:
                    if isinstance(target, ast.Name):
                        fields.add(target.id)
        break
    return fields


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    errors: list[str] = []
    warnings: list[str] = []

    paths = {name: root / name for name in ("config.py", "graph_io.py", "queries.py")}
    for name, path in paths.items():
        if not path.exists():
            errors.append(f"Thiếu {name}: {path}")

    if not errors:
        graph_symbols = top_level_symbols(paths["graph_io.py"])
        missing_graph = sorted(REQUIRED_GRAPH_IO - graph_symbols)
        if missing_graph:
            errors.append(f"graph_io.py thiếu API: {missing_graph}")

        config_symbols = top_level_symbols(paths["config.py"])
        missing_config = sorted(REQUIRED_CONFIG - config_symbols)
        if missing_config:
            errors.append(f"config.py thiếu API: {missing_config}")

        declared = class_declared_fields(paths["config.py"], "Stage2Config")
        if declared:
            missing_fields = sorted(CONFIG_ATTRIBUTES - declared)
            if missing_fields:
                warnings.append(
                    "Stage2Config không khai báo tường minh một số field; load_config có thể "
                    f"gán động, cần kiểm tra runtime: {missing_fields}"
                )
        else:
            warnings.append(
                "Không đọc được field khai báo của Stage2Config; cần xác nhận bằng py_compile/runtime."
            )

        query_text = paths["queries.py"].read_text(encoding="utf-8")
        missing_ids = sorted(q for q in EXPECTED_QUERY_IDS if q not in query_text)
        if missing_ids:
            warnings.append(
                "queries.py không chứa literal của một số query ID; registry có thể được dựng "
                f"gián tiếp. Cần import-check trên cluster: {missing_ids}"
            )
        if "QUERY_REGISTRY" not in query_text:
            errors.append("queries.py thiếu QUERY_REGISTRY")

    payload = {
        "status": "PASS" if not errors else "FAIL",
        "project_root": str(root),
        "errors": errors,
        "warnings": warnings,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())

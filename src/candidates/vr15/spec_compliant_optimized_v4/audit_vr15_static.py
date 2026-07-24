#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kiểm toán tĩnh bộ mã Vr15 C2 mà không cần cài PySpark.

Mục tiêu: phát hiện sai hợp đồng QuerySpec, sai pipeline, dùng global base graph
trong Proposed, physical NEXT_TW, nới hard constraint và các lỗi timing/gate rõ ràng.
Đây không thay thế correctness test trên Spark/HDFS.
"""
from __future__ import annotations

import ast
import importlib
import json
import py_compile
import re
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EXPECTED = {
    "Q1.1": "q11",
    "Q1.2": "q12",
    "Q1.3": "q13",
    "Q2.1": "q21",
    "Q2.2": "q22",
    "Q2.3": "q23",
    "Q3.1": "q31",
    "Q3.2": "q32",
    "Q3.3": "q33",
    "Q4.1": "q41",
    "Q4.2": "q42",
    "Q4.3": "q43",
}


def install_import_stubs() -> None:
    """Cho phép import QuerySpec để kiểm tra metadata mà không chạy Spark."""
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")
    types_mod = types.ModuleType("pyspark.sql.types")
    window = types.ModuleType("pyspark.sql.window")

    class DataFrame:  # pragma: no cover - chỉ là annotation/import stub
        pass

    class Column:
        pass

    class Window:
        pass

    sql.DataFrame = DataFrame
    sql.Column = Column
    sql.functions = functions
    sql.types = types_mod
    sql.Window = Window
    window.Window = Window
    pyspark.sql = sql

    config = types.ModuleType("config")
    class Stage2Config:
        pass
    config.Stage2Config = Stage2Config

    graph_io = types.ModuleType("graph_io")
    graph_io.extract_time_window_number = lambda value: value

    sys.modules.update(
        {
            "pyspark": pyspark,
            "pyspark.sql": sql,
            "pyspark.sql.functions": functions,
            "pyspark.sql.types": types_mod,
            "pyspark.sql.window": window,
            "config": config,
            "graph_io": graph_io,
        }
    )


def assert_true(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def all_calls(tree: ast.AST, name: str) -> list[ast.Call]:
    out: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id == name:
            out.append(node)
        elif isinstance(func, ast.Attribute) and func.attr == name:
            out.append(node)
    return out


def main() -> int:
    errors: list[str] = []
    notes: list[str] = []

    files = [
        ROOT / "baseline.py",
        ROOT / "proposed_distributed.py",
        ROOT / "run_experiment.py",
        *sorted((ROOT / "query_specs").glob("*.py")),
    ]
    for path in files:
        try:
            py_compile.compile(str(path), doraise=True)
        except Exception as exc:
            errors.append(f"py_compile FAIL {path.name}: {exc!r}")

    # Import metadata contracts with lightweight stubs.
    install_import_stubs()
    sys.path.insert(0, str(ROOT))
    loaded = {}
    for query_id, module_name in EXPECTED.items():
        try:
            module = importlib.import_module(f"query_specs.{module_name}")
            specs = [
                value
                for name, value in vars(module).items()
                if name.endswith("_SPEC") and hasattr(value, "query_id")
            ]
            assert_true(len(specs) == 1, f"{module_name}: phải có đúng một *_SPEC", errors)
            if not specs:
                continue
            spec = specs[0]
            loaded[query_id] = spec
            assert_true(spec.query_id == query_id, f"{module_name}: query_id={spec.query_id}, kỳ vọng {query_id}", errors)
            try:
                spec.validate_contract()
            except Exception as exc:
                errors.append(f"{query_id}: validate_contract FAIL: {exc!r}")
            assert_true(tuple(spec.candidate_dedup_columns) == ("candidate_id",), f"{query_id}: candidate key phải giữ evidence path bằng candidate_id", errors)
            assert_true(tuple(spec.witness_dedup_columns) == tuple(spec.correctness_compare_columns), f"{query_id}: witness key phải bằng correctness projection", errors)
            assert_true(bool(spec.hard_constraint_names), f"{query_id}: hard_constraint_names rỗng", errors)
        except Exception as exc:
            errors.append(f"Không import được {module_name}: {exc!r}")

    assert_true(set(loaded) == set(EXPECTED), f"Thiếu QuerySpec: {sorted(set(EXPECTED)-set(loaded))}", errors)

    baseline_path = ROOT / "baseline.py"
    proposed_path = ROOT / "proposed_distributed.py"
    runner_path = ROOT / "run_experiment.py"
    common_path = ROOT / "query_specs" / "common.py"
    baseline = baseline_path.read_text(encoding="utf-8")
    proposed = proposed_path.read_text(encoding="utf-8")
    runner = runner_path.read_text(encoding="utf-8")
    common = common_path.read_text(encoding="utf-8")

    # Baseline physical-plan contract.
    assert_true("union_partition_graph" in baseline, "Baseline thiếu global merge", errors)
    assert_true("time_step1_global_merge_seconds" in baseline, "Baseline thiếu T_B1", errors)
    assert_true("time_step2_global_query_validation_seconds" in baseline, "Baseline thiếu T_B2", errors)
    assert_true("time_algorithm_phase_gap_seconds" in baseline, "Baseline thiếu delta_B", errors)
    assert_true("num_physical_next_tw_edges\": 0" in baseline, "Baseline không chứng minh physical NEXT_TW=0", errors)

    # Proposed physical-plan/complexity contract.
    assert_true("union_partition_graph" not in proposed, "Proposed không được global-union base graph", errors)
    assert_true("build_entity_partition_index" not in proposed, "Proposed đang dựng index O(|V|) không dùng", errors)
    assert_true("ThreadPoolExecutor" in proposed and "threading.Barrier" in proposed, "Proposed thiếu concurrent LocalEval submission", errors)
    assert_true("max(\n        r.completion_offset_seconds" in proposed, "T_LocalEval phải là max completion offset", errors)
    assert_true("partition_work_sum" not in re.search(r"time_algorithm_total\s*=.*?time_end_to_end_total", proposed, re.S).group(0) if re.search(r"time_algorithm_total\s*=.*?time_end_to_end_total", proposed, re.S) else True, "partition_work_sum bị cộng vào runtime", errors)
    assert_true("source.join(\n        target,\n        [\"interface_id\", \"interface_key\", \"target_tw_num\"]" in proposed, "True-boundary phải equi-join theo interface/key/TW", errors)
    assert_true("F.explode(tw_sequence)" in proposed, "Thiếu bounded TW expansion", errors)
    assert_true("left_partition\") != F.col(\"right_partition" in proposed, "Thiếu cross-partition condition", errors)
    assert_true("left_logical_id\") != F.col(\"right_logical_id" in proposed, "Thiếu replicated-logical-fragment exclusion", errors)
    assert_true("same_temporal_instance_same_role" in proposed, "Thiếu temporal-instance replication guard", errors)
    assert_true("right_start_epoch\") >= F.col(\"left_end_epoch" in proposed, "Thiếu actual timestamp ordering", errors)
    assert_true("MERGED_CANDIDATES_HARD_VALIDATE_THEN_NORMALIZE" in proposed, "Sai final witness pipeline", errors)
    assert_true("spec.finalize_merged_candidates(candidates=merged_candidates)" in proposed, "HardValidate chưa chạy từ C_M", errors)
    assert_true("num_valid_witnesses_before_normalization" in proposed, "Thiếu |W_V|", errors)
    assert_true("FRAGMENT_INVARIANT_FAIL" in proposed, "Thiếu invariant bốn lớp fragment", errors)
    assert_true("minimum_partition_count=1" not in proposed and "replace(\n            config" not in proposed, "Proposed nới hard constraint trong LocalEval", errors)

    # Common candidate/witness pipeline.
    assert_true("deduplicated = self.deduplicate_candidates(candidates)" in common, "C_M chưa dedup trước HardValidate", errors)
    assert_true("filter(F.col(\"is_valid\") == F.lit(True))" in common, "HardValidate không áp query-specific validity predicate", errors)
    assert_true("dropDuplicates(list(witness_keys))" in common, "W_F chưa dedup sau normalization", errors)
    assert_true("validate_query_time_scope_config" in common and "EXPLICIT_INTERVAL" in common, "Thiếu query time scope contract", errors)

    # Runner gates and C2-only semantics.
    assert_true(all(mode in runner for mode in ("correctness", "profiling", "profiling_runtime_only")) and "official_runtime_only" not in runner, "Runner chưa tách C2 khỏi C1 official", errors)
    assert_true("source_bundle_hash" in runner and "parameter_signature" in runner, "Correctness gate thiếu source/parameter signature", errors)
    assert_true("witness_set_match" in runner and "only_baseline" in runner and "only_proposed" in runner, "Correctness chỉ đang so count", errors)
    assert_true("median" in runner and "Algorithm_speedup_ratio_of_medians" in runner, "Aggregate speedup chưa dùng ratio of medians", errors)
    assert_true("correctness_gate_root" in runner, "Profiling chưa bắt buộc gate correctness", errors)

    # Heuristic: source files không được dựng physical NEXT_TW DataFrame.
    for path in [baseline_path, proposed_path]:
        tree = parse(path)
        forbidden_calls = all_calls(tree, "build_next_tw_edges") + all_calls(tree, "materialize_next_tw")
        assert_true(not forbidden_calls, f"{path.name}: phát hiện physical NEXT_TW builder", errors)

    # Query-level evidence-path, validity và compact-index elimination contract.
    for query_id, module_name in EXPECTED.items():
        query_path = ROOT / "query_specs" / f"{module_name}.py"
        text = query_path.read_text(encoding="utf-8")
        assert_true("candidate_id" in text, f"{query_id}: thiếu candidate_id", errors)
        assert_true("is_valid" in text, f"{query_id}: thiếu hard-validation predicate", errors)
        assert_true("boundary_interfaces=" in text, f"{query_id}: thiếu boundary interfaces", errors)

        tree = parse(query_path)
        distributed_functions = [
            node for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and "distributed" in node.name
            and "candidate_upper_bound" not in node.name
        ]
        assert_true(bool(distributed_functions), f"{query_id}: thiếu distributed evaluator", errors)
        for fn in distributed_functions:
            vertex_reads = [
                node for node in ast.walk(fn)
                if isinstance(node, ast.Name)
                and node.id == "vertices"
                and isinstance(node.ctx, ast.Load)
            ]
            assert_true(
                not vertex_reads,
                f"{query_id}: distributed evaluator {fn.name} còn dùng vertices; "
                "không được bỏ compact index",
                errors,
            )

    notes.extend(
        [
            "Boundary join complexity: O(I*K*|F| + |J|), với I nhỏ và K=12.",
            "Support selection: typed-identity semi-join O(|F|+|B|), output-sensitive.",
            "LocalEval wall time: max completion offset; work-sum chỉ diagnostic.",
            "Không dựng compact entity index O(|V|) vì 12 evaluator hiện tại không sử dụng.",
            "Static audit không thay thế Spark/HDFS correctness và scalability tests.",
        ]
    )

    payload = {
        "status": "PASS" if not errors else "FAIL",
        "root": str(ROOT),
        "num_query_specs": len(loaded),
        "queries": sorted(loaded),
        "errors": errors,
        "notes": notes,
    }
    (ROOT / "STATIC_AUDIT_REPORT.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Algorithmic orchestration layer for distributed query evaluation.

This module models a *fragment assembly* pipeline, not a graph traversal engine.
It keeps execution semantics explicit and runtime-neutral through four logical
stages:

1. Map (LocalEval): partition-local fragment generation.
2. Shuffle: boundary fragment exchange/collection for cross-partition assembly.
3. Reduce (Stitching): constrained witness assembly from boundary fragments.
4. Optional Reduce (Best-chain): ranking/pruning over selected witness scope.

The module intentionally avoids distributed runtime behavior (scheduling,
transport, retries, fault tolerance). It only coordinates algorithmic stages.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from src.best_chain import best_chain
from src.local_eval import local_eval
from src.stitching import stitch_fragments


Fragment = Dict[str, Any]
QuerySpec = Mapping[str, Any]
AutomatonSpec = Mapping[str, Any]
Statistics = MutableMapping[str, Any]
BestChainScope = str

VALID_BEST_CHAIN_SCOPES = {"stitched_only", "local_only", "all"}


@dataclass
class QueryRunResult:
    """Structured output of ``run_query`` for stage-traceable research workflows."""

    local_fragments: List[Fragment]
    local_complete_witnesses: List[Fragment]
    boundary_fragments: List[Fragment]
    stitched_witnesses: List[Fragment]
    best_result: Optional[Dict[str, Any]]
    metadata: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        """Return a dictionary representation for compatibility layers."""
        return {
            "local_fragments": self.local_fragments,
            "local_complete_witnesses": self.local_complete_witnesses,
            "boundary_fragments": self.boundary_fragments,
            "stitched_witnesses": self.stitched_witnesses,
            "best_result": self.best_result,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class BoundaryClassification:
    """Explicit boundary classification result for a LocalEval fragment.

    Fields are intentionally extensible so future work can model multiple
    boundary notions (e.g., temporal/spatial/structural) without changing the
    orchestration skeleton.
    """

    is_boundary: bool
    boundary_kind: str
    missing_annotation: bool


def classify_boundary_fragment(fragment: Mapping[str, Any]) -> BoundaryClassification:
    """Classify a fragment for local-complete vs boundary routing.

    Current contract:
    - ``is_boundary`` (bool-like) controls routing.
    - if ``is_boundary`` is absent, routing defaults to local-complete *and* the
      missing annotation is surfaced via metadata for defensive auditing.

    Future extension points:
    - ``boundary_type`` could encode temporal/spatial/structural categories.
    """
    missing_annotation = "is_boundary" not in fragment
    is_boundary = bool(fragment.get("is_boundary", False))
    boundary_kind = str(fragment.get("boundary_type", "unspecified"))
    return BoundaryClassification(
        is_boundary=is_boundary,
        boundary_kind=boundary_kind,
        missing_annotation=missing_annotation,
    )


def split_fragments_for_stitching(
    fragments: Sequence[Fragment],
) -> Tuple[List[Fragment], List[Fragment], Dict[str, Any]]:
    """Separate complete local witnesses from boundary fragments.

    Returns:
        ``(local_complete_witnesses, boundary_fragments, boundary_diagnostics)``.
    """
    local_complete: List[Fragment] = []
    boundary: List[Fragment] = []

    missing_boundary_annotation_count = 0
    boundary_kind_counts: Dict[str, int] = {}

    for fragment in fragments:
        classification = classify_boundary_fragment(fragment)
        if classification.missing_annotation:
            missing_boundary_annotation_count += 1

        boundary_kind_counts[classification.boundary_kind] = (
            boundary_kind_counts.get(classification.boundary_kind, 0) + 1
        )

        if classification.is_boundary:
            boundary.append(fragment)
        else:
            local_complete.append(fragment)

    diagnostics = {
        "missing_boundary_annotation_count": missing_boundary_annotation_count,
        "boundary_kind_counts": boundary_kind_counts,
        "missing_boundary_annotation_detected": missing_boundary_annotation_count > 0,
    }
    return local_complete, boundary, diagnostics


def map_local_eval(
    partition_inputs: Iterable[Any],
    query: QuerySpec,
    automaton: Optional[AutomatonSpec],
) -> Tuple[List[Fragment], Statistics]:
    """Map stage: run LocalEval independently for each partition.

    LocalEval = partition-local fragment generation.
    """
    local_fragments: List[Fragment] = []
    stats: Statistics = {"num_partitions": 0}

    for partition in partition_inputs:
        stats["num_partitions"] = int(stats["num_partitions"]) + 1
        partition_fragments = local_eval(partition_graph=partition, query=query, automaton=automaton)
        local_fragments.extend(partition_fragments)

    return local_fragments, stats


def shuffle_boundary_fragments(boundary_fragments: Sequence[Fragment]) -> List[Fragment]:
    """Shuffle stage: collect fragments that cross partition boundaries.

    This function keeps the Map -> Shuffle -> Reduce abstraction explicit without
    introducing distributed transport mechanics.
    """
    # In a real distributed runtime this stage would exchange boundary fragments
    # across workers/partitions. Here we preserve semantics by passing them
    # through as an explicit orchestration stage.
    return list(boundary_fragments)


def reduce_stitching(
    boundary_fragments: Sequence[Fragment],
    stitching_constraints: Mapping[str, Any],
) -> Tuple[List[Fragment], Optional[str]]:
    """Reduce stage: invoke Stitching for cross-partition constrained assembly.

    Stitching = cross-partition constrained assembly over boundary fragments.
    """
    stitched_witnesses: List[Fragment] = []
    stitching_error: Optional[str] = None

    if boundary_fragments:
        try:
            stitched_witnesses = stitch_fragments(list(boundary_fragments), dict(stitching_constraints))
        except NotImplementedError as exc:
            stitching_error = str(exc)

    return stitched_witnesses, stitching_error


def reduce_best_chain(
    stitched_witnesses: Sequence[Fragment],
    local_complete_witnesses: Sequence[Fragment],
    *,
    apply_best_chain: bool,
    best_chain_scope: BestChainScope,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], List[Fragment]]:
    """Optional Reduce stage: invoke Best-chain over selected witness scope.

    Supported scopes:
    - ``stitched_only``: rank/prune stitched witnesses (legacy default).
    - ``local_only``: rank/prune local complete witnesses only.
    - ``all``: rank/prune over union(local complete, stitched).
    """
    if best_chain_scope not in VALID_BEST_CHAIN_SCOPES:
        raise ValueError(
            f"Unsupported best_chain_scope={best_chain_scope!r}. "
            f"Expected one of {sorted(VALID_BEST_CHAIN_SCOPES)}."
        )

    if best_chain_scope == "stitched_only":
        candidates = list(stitched_witnesses)
    elif best_chain_scope == "local_only":
        candidates = list(local_complete_witnesses)
    else:
        candidates = list(local_complete_witnesses) + list(stitched_witnesses)

    if not apply_best_chain or not candidates:
        return None, None, candidates

    best_chain_error: Optional[str] = None
    best_result: Optional[Dict[str, Any]] = None
    try:
        best_result = best_chain(candidates)
    except NotImplementedError as exc:
        best_chain_error = str(exc)

    return best_result, best_chain_error, candidates


def run_query(
    partition_inputs: Iterable[Any],
    query: QuerySpec,
    automaton: Optional[AutomatonSpec] = None,
    *,
    stitching_constraints: Optional[Mapping[str, Any]] = None,
    apply_best_chain: bool = False,
    best_chain_scope: BestChainScope = "stitched_only",
) -> Dict[str, Any]:
    """Run the orchestration pipeline: LocalEval -> Stitching -> Best-chain.

    Research semantics:
    - LocalEval: partition-local fragment generation.
    - Stitching: cross-partition constrained witness assembly.
    - Best-chain: optional post-processing on configurable witness scope.

    Notes:
        This function orchestrates fragment assembly only. It does not model a
        traversal runtime, distributed scheduler, or transport subsystem.
    """
    constraints: Mapping[str, Any] = stitching_constraints or {}

    # Map stage (LocalEval per partition)
    local_fragments, stats = map_local_eval(partition_inputs, query, automaton)

    local_complete_witnesses, boundary_fragments, boundary_diagnostics = split_fragments_for_stitching(local_fragments)

    # Shuffle stage (boundary fragment exchange)
    shuffled_boundary_fragments = shuffle_boundary_fragments(boundary_fragments)

    # Reduce stage (Stitching)
    stitched_witnesses, stitching_error = reduce_stitching(shuffled_boundary_fragments, constraints)

    # Optional Reduce stage (Best-chain)
    best_result, best_chain_error, best_chain_candidates = reduce_best_chain(
        stitched_witnesses,
        local_complete_witnesses,
        apply_best_chain=apply_best_chain,
        best_chain_scope=best_chain_scope,
    )

    metadata: Dict[str, Any] = {
        "statistics": {
            **stats,
            "num_local_fragments": len(local_fragments),
            "num_local_complete_witnesses": len(local_complete_witnesses),
            "num_boundary_fragments": len(boundary_fragments),
            "num_stitched_witnesses": len(stitched_witnesses),
            "num_best_chain_candidates": len(best_chain_candidates),
        },
        "stage_status": {
            "stitching_attempted": bool(shuffled_boundary_fragments),
            "best_chain_attempted": bool(apply_best_chain and best_chain_candidates),
            "stitching_error": stitching_error,
            "best_chain_error": best_chain_error,
        },
        "boundary_diagnostics": boundary_diagnostics,
        "best_chain": {
            "applied": apply_best_chain,
            "scope": best_chain_scope,
            "scope_description": {
                "stitched_only": "Best-chain considers only stitched_witnesses.",
                "local_only": "Best-chain considers only local_complete_witnesses.",
                "all": "Best-chain considers union(local_complete_witnesses, stitched_witnesses).",
            }[best_chain_scope],
        },
    }

    return QueryRunResult(
        local_fragments=local_fragments,
        local_complete_witnesses=local_complete_witnesses,
        boundary_fragments=shuffled_boundary_fragments,
        stitched_witnesses=stitched_witnesses,
        best_result=best_result,
        metadata=metadata,
    ).as_dict()

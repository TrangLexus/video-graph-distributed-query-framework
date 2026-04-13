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

from dataclasses import asdict, dataclass
from typing import Any, Dict, Hashable, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from src.best_chain import best_chain
from src.local_eval import local_eval
from src.stitching import BoundaryFragment, stitch_fragments


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


def _derive_stitch_key(fragment: Mapping[str, Any]) -> Hashable:
    """Derive a stitch-key compatible with cross-partition assembly.

    Preferred order:
    1) explicit stitch key emitted upstream,
    2) entity-level key (stable cross-partition proxy),
    3) automaton frontier state (coarser fallback).
    """
    if fragment.get("stitch_key") is not None:
        return fragment["stitch_key"]
    bindings = fragment.get("bindings", {})
    if isinstance(bindings, Mapping) and bindings.get("entity_id") is not None:
        return ("entity", bindings.get("entity_id"))
    if fragment.get("automaton_state") is not None:
        return ("state", fragment.get("automaton_state"))
    return ("fallback", "global")


def _derive_fragment_id(fragment: Mapping[str, Any], ordinal: int) -> str:
    """Derive a deterministic fragment identifier for stitching-layer inputs."""
    if fragment.get("fragment_id") is not None:
        return str(fragment["fragment_id"])
    metadata = fragment.get("metadata", {})
    path = metadata.get("path") if isinstance(metadata, Mapping) else None
    vertex = fragment.get("vertex")
    frontier_state = fragment.get("automaton_state")
    return f"bf::{ordinal}::{vertex}::{frontier_state}::{path}"


def _derive_endpoints(fragment: Mapping[str, Any]) -> Tuple[Optional[Hashable], Optional[Hashable]]:
    """Infer endpoint interface for structural compatibility checks.

    Fallback policy (used only when LocalEval did not emit explicit endpoints):
    - prefer path-tail adjacency: endpoint_in := path[-2], endpoint_out := path[-1]
    - if no path information exists, fall back to fragment ``vertex`` for
      endpoint_out only.
    """
    metadata = fragment.get("metadata", {})
    path = metadata.get("path", []) if isinstance(metadata, Mapping) else []

    endpoint_in = fragment.get("endpoint_in")
    endpoint_out = fragment.get("endpoint_out")
    if endpoint_in is None and len(path) >= 2:
        endpoint_in = path[-2]
    if endpoint_out is None:
        endpoint_out = fragment.get("vertex", path[-1] if path else None)

    return endpoint_in, endpoint_out


def _derive_temporal_interface(fragment: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """Infer conservative temporal interface for stitching compatibility.

    LocalEval-provided ``t_start``/``t_end`` are preserved whenever present.
    Only when absent do we consult metadata as a compatibility fallback.
    """
    t_start = fragment.get("t_start")
    t_end = fragment.get("t_end")
    if t_start is None:
        metadata = fragment.get("metadata", {})
        if isinstance(metadata, Mapping):
            t_start = metadata.get("t_start", metadata.get("time_start"))
    if t_end is None:
        metadata = fragment.get("metadata", {})
        if isinstance(metadata, Mapping):
            t_end = metadata.get("t_end", metadata.get("time_end"))
    return t_start, t_end


def adapt_boundary_fragments_for_stitching(boundary_fragments: Sequence[Fragment]) -> List[BoundaryFragment]:
    """Adapter layer: LocalEval fragment dictionaries -> Stitching fragments.

    This function is the explicit LocalEval→Stitching contract normalizer.
    It keeps LocalEval schema unchanged while materializing the typed
    `BoundaryFragment` inputs expected by `src/stitching.py`.
    """
    adapted: List[BoundaryFragment] = []

    for ordinal, fragment in enumerate(boundary_fragments):
        # Boundary fragment adaptation: LocalEval dict -> typed Stitching input.
        # Preservation of upstream interface fields is preferred over derivation.
        stitch_key = fragment.get("stitch_key")
        if stitch_key is None:
            stitch_key = _derive_stitch_key(fragment)

        fragment_id = fragment.get("fragment_id")
        if fragment_id is None:
            fragment_id = _derive_fragment_id(fragment, ordinal)

        # Fallback derivation policy: use derivation only when LocalEval left
        # endpoint interface values unspecified.
        endpoint_in, endpoint_out = _derive_endpoints(fragment)

        # Preserve temporal handoff from LocalEval; derive conservative fallback
        # only when t_start/t_end are missing.
        t_start, t_end = _derive_temporal_interface(fragment)

        bindings = fragment.get("bindings", {})
        metadata = fragment.get("metadata", {})

        adapted.append(
            BoundaryFragment(
                fragment_id=str(fragment_id),
                stitch_key=stitch_key,
                fragment_type=fragment.get("fragment_type"),
                bindings=dict(bindings) if isinstance(bindings, Mapping) else {},
                metadata=dict(metadata) if isinstance(metadata, Mapping) else {},
                is_boundary=bool(fragment.get("is_boundary", True)),
                entry_state=fragment.get("entry_state"),
                exit_state=fragment.get("exit_state"),
                payload=dict(fragment),
                endpoint_in=endpoint_in,
                endpoint_out=endpoint_out,
                t_start=t_start,
                t_end=t_end,
                automaton_state=fragment.get("automaton_state"),
            )
        )
    return adapted


def prepare_shuffle_groups(boundary_fragments: Sequence[Fragment]) -> Dict[Hashable, List[BoundaryFragment]]:
    """Explicit shuffle preparation stage for stitching reduce input."""
    # Shuffle preparation for stitching: group typed boundary fragments by
    # stitch_key before reduce-stage invocation.
    grouped: Dict[Hashable, List[BoundaryFragment]] = {}
    for boundary_fragment in adapt_boundary_fragments_for_stitching(boundary_fragments):
        grouped.setdefault(boundary_fragment.stitch_key, []).append(boundary_fragment)
    return grouped


def reduce_stitching(
    grouped_boundary_fragments: Mapping[Hashable, Sequence[BoundaryFragment]],
    automaton: Optional[AutomatonSpec],
    query: QuerySpec,
    stitching_constraints: Mapping[str, Any],
) -> Tuple[List[Fragment], Optional[str]]:
    """Reduce stage: invoke Stitching for cross-partition constrained assembly.

    Stitching = cross-partition constrained assembly over boundary fragments.
    """
    stitched_witnesses: List[Fragment] = []
    stitching_error: Optional[str] = None

    if grouped_boundary_fragments:
        try:
            # Reduce-stage stitching invocation: the orchestration layer delegates
            # constrained cross-partition assembly to ``stitch_fragments``.
            # Keep constraints explicit for orchestration diagnostics/research
            # traceability while preserving the Algorithm 2B call contract.
            _ = dict(stitching_constraints)
            automaton_spec: AutomatonSpec = automaton or {}
            stitched = []
            for group in grouped_boundary_fragments.values():
                stitched.extend(stitch_fragments(group, automaton_spec, query))
            stitched_witnesses = [asdict(witness) for witness in stitched]
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
    shuffled_groups = prepare_shuffle_groups(shuffled_boundary_fragments)

    # Reduce stage (Stitching)
    stitched_witnesses, stitching_error = reduce_stitching(shuffled_groups, automaton, query, constraints)

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
            "num_shuffle_groups": len(shuffled_groups),
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

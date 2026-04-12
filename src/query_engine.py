"""Algorithmic orchestration layer for distributed query evaluation.

This module provides a runtime-neutral coordinator for a three-stage execution
pipeline:

1. LocalEval: partition-local fragment generation.
2. Stitching: cross-partition witness assembly over boundary fragments.
3. Best-chain: optional post-processing (e.g., ranking/pruning) over stitched
   candidates.

Design intent:
- Keep control flow explicit and research-readable.
- Preserve interchangeability of execution backends (single-process today,
  Spark/GraphFrames/distributed runtime adapters later).
- Avoid embedding infrastructure-specific assumptions in this module.
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


@dataclass
class QueryRunResult:
    """Structured output of ``run_query``.

    The dataclass keeps the module executable and explicit while remaining easy
    to serialize via :meth:`as_dict` for downstream consumers.
    """

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


def _is_boundary_fragment(fragment: Mapping[str, Any]) -> bool:
    """Detect whether a local fragment requires cross-partition stitching.

    Assumption (kept explicit): LocalEval emits a boolean ``is_boundary`` field
    in fragment payloads for partial fragments touching partition boundaries.
    If unavailable, this function conservatively treats fragments as complete.
    """
    return bool(fragment.get("is_boundary", False))


def split_fragments_for_stitching(
    fragments: Sequence[Fragment],
) -> Tuple[List[Fragment], List[Fragment]]:
    """Separate complete local witnesses from boundary fragments.

    Returns:
        ``(local_complete_witnesses, boundary_fragments)``.
    """
    local_complete: List[Fragment] = []
    boundary: List[Fragment] = []

    for fragment in fragments:
        if _is_boundary_fragment(fragment):
            boundary.append(fragment)
        else:
            local_complete.append(fragment)

    return local_complete, boundary


def _iter_partitions(partition_inputs: Iterable[Any]) -> Iterable[Any]:
    """Yield partition-local inputs without constraining concrete container type."""
    for partition in partition_inputs:
        yield partition


def run_query(
    partition_inputs: Iterable[Any],
    query: QuerySpec,
    automaton: Optional[AutomatonSpec] = None,
    *,
    stitching_constraints: Optional[Mapping[str, Any]] = None,
    apply_best_chain: bool = False,
) -> Dict[str, Any]:
    """Run the end-to-end algorithmic pipeline for distributed query semantics.

    Parameters:
        partition_inputs: Iterable of partition-local graph inputs.
        query: Query specification ``Q``.
        automaton: Optional automaton ``𝒜``; if omitted, LocalEval may derive it
            from ``query``.
        stitching_constraints: Optional constraints used by Stitching.
        apply_best_chain: Whether to invoke Best-chain on stitched candidates.

    Returns:
        A structured dictionary with stage-wise outputs and metadata.

    Notes:
        This function intentionally orchestrates algorithmic stages only. It does
        not claim to implement distributed scheduling, fault tolerance, or data
        transport layers.
    """
    constraints: Mapping[str, Any] = stitching_constraints or {}
    local_fragments: List[Fragment] = []
    stats: Statistics = {"num_partitions": 0}

    # Step 1: Partition-local fragment generation
    # - for each partition P_i:
    #   - call LocalEval(P_i, 𝒜, Q)
    #   - collect fragment output 𝓕_i
    for partition in _iter_partitions(partition_inputs):
        stats["num_partitions"] = int(stats["num_partitions"]) + 1
        partition_fragments = local_eval(partition_graph=partition, query=query, automaton=automaton)
        local_fragments.extend(partition_fragments)

    # Step 2: Local/global separation
    # - identify:
    #   - complete local witnesses that need no cross-partition assembly
    #   - boundary fragments that must go to Stitching
    local_complete_witnesses, boundary_fragments = split_fragments_for_stitching(local_fragments)

    # Step 3: Global stitching
    # - call Stitching(𝓕^∂, 𝒜, Q)
    # - obtain stitched cross-partition witnesses
    stitched_witnesses: List[Fragment] = []
    stitching_error: Optional[str] = None
    if boundary_fragments:
        try:
            stitched_witnesses = stitch_fragments(boundary_fragments, dict(constraints))
        except NotImplementedError as exc:
            # Honest fallback for skeleton repositories where stitching logic
            # is intentionally deferred to future runtime/integration layers.
            stitching_error = str(exc)

    # Step 4: Optional post-processing
    # - optionally apply Best-chain or another selection layer
    best_result: Optional[Dict[str, Any]] = None
    best_chain_error: Optional[str] = None
    if apply_best_chain and stitched_witnesses:
        try:
            best_result = best_chain(stitched_witnesses)
        except NotImplementedError as exc:
            best_chain_error = str(exc)

    # Step 5: Final packaging
    # - return a structured result with fields such as:
    #   - local_fragments
    #   - local_complete_witnesses
    #   - boundary_fragments
    #   - stitched_witnesses
    #   - best_result
    #   - metadata / statistics if available
    metadata: Dict[str, Any] = {
        "statistics": {
            **stats,
            "num_local_fragments": len(local_fragments),
            "num_local_complete_witnesses": len(local_complete_witnesses),
            "num_boundary_fragments": len(boundary_fragments),
            "num_stitched_witnesses": len(stitched_witnesses),
        },
        "stage_status": {
            "stitching_attempted": bool(boundary_fragments),
            "best_chain_attempted": bool(apply_best_chain and stitched_witnesses),
            "stitching_error": stitching_error,
            "best_chain_error": best_chain_error,
        },
    }

    return QueryRunResult(
        local_fragments=local_fragments,
        local_complete_witnesses=local_complete_witnesses,
        boundary_fragments=boundary_fragments,
        stitched_witnesses=stitched_witnesses,
        best_result=best_result,
        metadata=metadata,
    ).as_dict()


# Module note:
# This file is the algorithmic orchestration layer for the query framework.
# True distributed execution details (e.g., Spark/GraphFrames scheduling,
# partition transport, and fault-tolerant runtime mechanics) belong to later
# integration layers. The current objective is semantic clarity and modularity.

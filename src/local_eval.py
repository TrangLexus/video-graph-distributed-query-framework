"""Algorithm 2A skeleton: LocalEval (Automaton-Guided Fragment Generation).

This module provides a research-oriented, executable implementation skeleton for
partition-local motif/query exploration. The design intentionally keeps graph and
automaton adapters lightweight and replaceable so future Spark/GraphFrames
integration can plug in concrete runtimes without rewriting the control flow.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple


PartitionGraph = Mapping[str, Any]
QuerySpec = Mapping[str, Any]
AutomatonSpec = Mapping[str, Any]
Bindings = MutableMapping[str, Any]
Metadata = MutableMapping[str, Any]


class FragmentType(str, Enum):
    """Typed fragment categories used by local evaluation."""

    PATH = "path fragment"
    EVENT = "event fragment"
    INTERACTION = "interaction fragment"
    PATTERN = "pattern fragment"


@dataclass(frozen=True)
class SearchState:
    """State in the partition-local frontier: (v, q, λ, θ)."""

    vertex: Any
    automaton_state: Any
    bindings: Dict[str, Any]
    metadata: Dict[str, Any]


@dataclass
class TypedFragment:
    """Materialized local fragment plus semantic type."""

    fragment_type: FragmentType
    payload: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        """Compatibility representation expected by existing query engine wiring."""
        return {"fragment_type": self.fragment_type.value, **self.payload}


@dataclass
class LocalEvalResult:
    """Container for produced typed fragments within one partition."""

    fragments: List[TypedFragment] = field(default_factory=list)

    def as_fragment_list(self) -> List[Dict[str, Any]]:
        """Flatten typed fragments to legacy list[dict] representation."""
        return [fragment.as_dict() for fragment in self.fragments]


# ---------------------------------------------------------------------------
# Helper utilities (lightweight placeholders; replace with domain semantics).
# ---------------------------------------------------------------------------

def iter_partition_vertices(partition: PartitionGraph) -> Iterable[Any]:
    """Yield partition vertices from common placeholder layouts."""
    vertices = partition.get("vertices", [])
    return vertices if isinstance(vertices, Iterable) else []


def iter_admissible_edges(partition: PartitionGraph, vertex: Any) -> Iterable[Tuple[Any, Any, Any, Dict[str, Any]]]:
    """Yield outgoing/admissible edges as (v, a, u, edge_meta).

    Placeholder contract: partition["adjacency"][v] contains a sequence of dicts
    with keys like "label", "dst", and optional "meta".
    """
    adjacency = partition.get("adjacency", {})
    for raw in adjacency.get(vertex, []):
        label = raw.get("label")
        dst = raw.get("dst")
        edge_meta = raw.get("meta", {})
        yield vertex, label, dst, edge_meta


def match_start_condition(vertex: Any, start_state: Any, query: QuerySpec) -> bool:
    """Return whether a vertex can seed the start automaton state q0.

    Placeholder rule: if query["start_vertices"] is provided, only those vertices
    can seed; otherwise all vertices are admissible.
    """
    allowed = query.get("start_vertices")
    if not allowed:
        return True
    return vertex in allowed and start_state is not None


def init_bindings(vertex: Any) -> Dict[str, Any]:
    """Initialize λ for a start seed state."""
    return {"seed_vertex": vertex}


def init_metadata(vertex: Any) -> Dict[str, Any]:
    """Initialize θ for a start seed state."""
    return {"path": [vertex], "depth": 0, "events": []}


def exists_transition(
    automaton_state: Any,
    edge_label: Any,
    destination: Any,
    edge_meta: Mapping[str, Any],
    automaton: AutomatonSpec,
    query: QuerySpec,
) -> bool:
    """Check whether automaton permits transition for current edge.

    Placeholder rule: transition exists iff (q, label) is present in
    automaton["delta"]; optional query edge label allowlist can further prune.
    """
    label_filter = query.get("edge_labels")
    if label_filter and edge_label not in label_filter:
        return False
    delta = automaton.get("delta", {})
    _ = destination, edge_meta
    return (automaton_state, edge_label) in delta


def delta(automaton_state: Any, edge_label: Any, automaton: AutomatonSpec) -> Any:
    """Automaton transition function q' = δ(q, a)."""
    return automaton.get("delta", {}).get((automaton_state, edge_label))


def update_bindings(bindings: Bindings, vertex: Any, edge_meta: Mapping[str, Any], query: QuerySpec) -> Dict[str, Any]:
    """Update λ with edge/vertex information for downstream constraints.

    Placeholder behavior appends vertices visited and captures edge attributes.
    """
    _ = query
    updated = dict(bindings)
    visited = list(updated.get("visited_vertices", []))
    visited.append(vertex)
    updated["visited_vertices"] = visited
    if edge_meta:
        updated["last_edge_meta"] = dict(edge_meta)
    return updated


def update_metadata(
    metadata: Metadata,
    vertex: Any,
    edge_meta: Mapping[str, Any],
    next_automaton_state: Any,
    query: QuerySpec,
) -> Dict[str, Any]:
    """Update θ with structural and event-trace context."""
    _ = query
    updated = dict(metadata)
    path = list(updated.get("path", []))
    path.append(vertex)
    updated["path"] = path
    updated["depth"] = int(updated.get("depth", 0)) + 1
    updated["automaton_state"] = next_automaton_state
    if edge_meta.get("event") is not None:
        events = list(updated.get("events", []))
        events.append(edge_meta["event"])
        updated["events"] = events
    return updated


def local_constraints_satisfied(
    bindings: Bindings,
    metadata: Metadata,
    query: QuerySpec,
) -> bool:
    """Evaluate local pruning constraints.

    Placeholder supports max_depth and simple-vertex-path pruning.
    """
    max_depth = query.get("max_depth")
    depth = int(metadata.get("depth", 0))
    if max_depth is not None and depth > int(max_depth):
        return False

    if query.get("simple_path", False):
        visited = bindings.get("visited_vertices", [])
        if len(visited) != len(set(visited)):
            return False

    return True


def is_accepting_state(automaton_state: Any, automaton: AutomatonSpec) -> bool:
    """Return whether automaton state is accepting."""
    accepting = automaton.get("accepting_states", set())
    return automaton_state in accepting


def is_boundary_state(vertex: Any, metadata: Metadata, partition: PartitionGraph, query: QuerySpec) -> bool:
    """Detect partition/temporal/structural boundaries for partial fragment emission."""
    boundary_vertices: Set[Any] = set(partition.get("boundary_vertices", []))
    if vertex in boundary_vertices:
        return True

    temporal_limit = query.get("temporal_depth_boundary")
    if temporal_limit is not None and int(metadata.get("depth", 0)) >= int(temporal_limit):
        return True

    structural_limit = query.get("structural_depth_boundary")
    if structural_limit is not None and int(metadata.get("depth", 0)) >= int(structural_limit):
        return True

    return False


def materialize_fragment(state: SearchState, partition: PartitionGraph, query: QuerySpec, *, boundary: bool) -> Dict[str, Any]:
    """Materialize a fragment payload from the current search state."""
    _ = partition, query
    return {
        "vertex": state.vertex,
        "automaton_state": state.automaton_state,
        "bindings": dict(state.bindings),
        "metadata": dict(state.metadata),
        "is_boundary": boundary,
    }


def assign_fragment_type(payload: Mapping[str, Any], query: QuerySpec) -> FragmentType:
    """Assign semantic fragment type using lightweight rules.

    Placeholder policy (override later with learned/domain logic):
      - event fragment: event trace exists
      - interaction fragment: metadata marks interaction edges
      - pattern fragment: explicit pattern query mode
      - otherwise path fragment
    """
    metadata = payload.get("metadata", {}) if isinstance(payload, Mapping) else {}
    if metadata.get("events"):
        return FragmentType.EVENT
    if metadata.get("interaction", False):
        return FragmentType.INTERACTION
    if query.get("mode") == "pattern":
        return FragmentType.PATTERN
    return FragmentType.PATH


def normalize_fragments(fragments: Sequence[TypedFragment]) -> List[TypedFragment]:
    """Normalize typed fragments (deduplicate by stable signature)."""
    dedup: Dict[Tuple[Any, ...], TypedFragment] = {}
    for fragment in fragments:
        payload = fragment.payload
        signature = (
            fragment.fragment_type.value,
            payload.get("vertex"),
            payload.get("automaton_state"),
            tuple(payload.get("metadata", {}).get("path", [])),
            bool(payload.get("is_boundary", False)),
        )
        dedup[signature] = fragment
    return list(dedup.values())


# ---------------------------------------------------------------------------
# Main LocalEval routine.
# ---------------------------------------------------------------------------

def local_eval(
    partition_graph: PartitionGraph,
    query: QuerySpec,
    automaton: Optional[AutomatonSpec] = None,
) -> List[Dict[str, Any]]:
    """Run Algorithm 2A LocalEval on one partition.

    Args:
        partition_graph: Partition-local graph data (runtime-neutral placeholder).
        query: Query constraints/specification.
        automaton: Condition-aware automaton. If omitted, uses
            ``query.get("automaton", {})``.

    Returns:
        Normalized set of typed fragments represented as ``list[dict]``.
    """
    automaton_spec: AutomatonSpec = automaton or query.get("automaton", {})
    start_state = automaton_spec.get("start_state", "q0")

    # 1) Initialize fragment set 𝓕_i ← ∅ and 2) frontier 𝒮 ← ∅.
    typed_fragments: List[TypedFragment] = []
    frontier: List[SearchState] = []

    # Seed initialization.
    for vertex in iter_partition_vertices(partition_graph):
        if match_start_condition(vertex, start_state, query):
            seed = SearchState(
                vertex=vertex,
                automaton_state=start_state,
                bindings=init_bindings(vertex),
                metadata=init_metadata(vertex),
            )
            frontier.append(seed)

    # Automaton-guided exploration.
    while frontier:
        state = frontier.pop()
        for _, edge_label, dst, edge_meta in iter_admissible_edges(partition_graph, state.vertex):
            if not exists_transition(
                state.automaton_state,
                edge_label,
                dst,
                edge_meta,
                automaton_spec,
                query,
            ):
                continue

            next_automaton_state = delta(state.automaton_state, edge_label, automaton_spec)
            if next_automaton_state is None:
                continue

            next_bindings = update_bindings(state.bindings, dst, edge_meta, query)
            next_metadata = update_metadata(state.metadata, dst, edge_meta, next_automaton_state, query)

            if not local_constraints_satisfied(next_bindings, next_metadata, query):
                continue

            next_state = SearchState(
                vertex=dst,
                automaton_state=next_automaton_state,
                bindings=next_bindings,
                metadata=next_metadata,
            )

            if is_accepting_state(next_state.automaton_state, automaton_spec):
                accepted_payload = materialize_fragment(next_state, partition_graph, query, boundary=False)
                typed_fragments.append(
                    TypedFragment(assign_fragment_type(accepted_payload, query), accepted_payload)
                )
            else:
                frontier.append(next_state)

            # Boundary fragment emission.
            if is_boundary_state(next_state.vertex, next_state.metadata, partition_graph, query):
                boundary_payload = materialize_fragment(next_state, partition_graph, query, boundary=True)
                typed_fragments.append(
                    TypedFragment(assign_fragment_type(boundary_payload, query), boundary_payload)
                )

    # Fragment normalization.
    normalized = normalize_fragments(typed_fragments)
    return LocalEvalResult(fragments=normalized).as_fragment_list()


def evaluate_partition(
    partition_graph: PartitionGraph,
    automaton: AutomatonSpec,
    query: QuerySpec,
) -> LocalEvalResult:
    """Compatibility wrapper exposing a typed ``LocalEvalResult`` object."""
    fragments = local_eval(partition_graph=partition_graph, automaton=automaton, query=query)
    typed = [TypedFragment(FragmentType(f["fragment_type"]), {k: v for k, v in f.items() if k != "fragment_type"}) for f in fragments]
    return LocalEvalResult(fragments=typed)


# Complexity note:
# - Runtime depends on frontier growth, local branching factor, automaton transition
#   density, and pruning strength from local constraints.
# - Without strong pruning, worst-case exploration can be exponential in
#   path/pattern depth.
# - In practice, performance is expected to track the effectiveness of local
#   constraint filtering and automaton-guided pruning.

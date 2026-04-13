"""Algorithm 2A skeleton: LocalEval (Automaton-Guided Fragment Generation).

This module provides a research-oriented, executable implementation skeleton for
partition-local motif/query exploration. The design intentionally keeps graph and
automaton adapters lightweight and replaceable so future Spark/GraphFrames
integration can plug in concrete runtimes without rewriting the control flow.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
from typing import Any, Dict, Hashable, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple


PartitionGraph = Mapping[str, Any]
QuerySpec = Mapping[str, Any]
AutomatonSpec = Mapping[str, Any]
Bindings = MutableMapping[str, Any]
Metadata = MutableMapping[str, Any]


class FragmentType(str, Enum):
    """Typed fragment categories used by local evaluation."""

    PATH = "path"
    EVENT = "event"
    INTERACTION = "interaction"
    PATTERN = "pattern"


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


def init_bindings(vertex: Any, query: QuerySpec) -> Dict[str, Any]:
    """Initialize λ for a start seed state.

    For ``simple_path=True`` we explicitly seed ``visited_vertices`` with the
    seed vertex itself so path-simplicity constraints are correct from depth 0.
    """
    bindings: Dict[str, Any] = {"seed_vertex": vertex}
    if query.get("simple_path", False):
        bindings["visited_vertices"] = [vertex]
    return bindings


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


def _extract_time_value(value: Any) -> Optional[Any]:
    """Extract a comparable temporal marker from loosely-typed values."""
    if isinstance(value, Mapping):
        for candidate_key in ("timestamp", "time", "ts", "t"):
            if candidate_key in value:
                return value[candidate_key]
        return None
    return value


def extract_endpoints(state: SearchState) -> Tuple[Any, Any]:
    """Extract conservative structural endpoints from current local path context."""
    path = state.metadata.get("path", [])
    endpoint_in = path[0] if isinstance(path, list) and path else state.vertex
    endpoint_out = state.vertex
    return endpoint_in, endpoint_out


def extract_temporal_interface(state: SearchState) -> Tuple[Optional[Any], Optional[Any]]:
    """Extract ``(t_start, t_end)`` and default to explicit ``None`` when unknown."""
    metadata = state.metadata

    explicit_start = metadata.get("t_start", metadata.get("time_start"))
    explicit_end = metadata.get("t_end", metadata.get("time_end"))
    if explicit_start is not None or explicit_end is not None:
        return explicit_start, explicit_end

    events = metadata.get("events", [])
    temporal_points: List[Any] = []
    if isinstance(events, list):
        for event in events:
            extracted = _extract_time_value(event)
            if extracted is not None:
                temporal_points.append(extracted)

    if temporal_points:
        return temporal_points[0], temporal_points[-1]
    return None, None


def _stable_json(data: Mapping[str, Any]) -> str:
    """Stable JSON serialization used by deterministic ID/key helpers."""
    return json.dumps(data, sort_keys=True, default=repr, separators=(",", ":"))


def generate_fragment_id(payload: Mapping[str, Any]) -> str:
    """Generate deterministic fragment identity from semantic interface fields."""
    id_basis = {
        "fragment_type": payload.get("fragment_type"),
        "entry_state": payload.get("entry_state"),
        "exit_state": payload.get("exit_state"),
        "endpoint_in": payload.get("endpoint_in"),
        "endpoint_out": payload.get("endpoint_out"),
        "t_start": payload.get("t_start"),
        "t_end": payload.get("t_end"),
        "is_boundary": bool(payload.get("is_boundary", False)),
        "vertex": payload.get("vertex"),
        "bindings": payload.get("bindings", {}),
    }
    digest = hashlib.sha1(_stable_json(id_basis).encode("utf-8")).hexdigest()
    return f"frag_{digest[:16]}"


def generate_stitch_key(payload: Mapping[str, Any]) -> str:
    """Generate deterministic stitch key for boundary/interface grouping."""
    key_basis = {
        "entry_state": payload.get("entry_state"),
        "exit_state": payload.get("exit_state"),
        "endpoint_in": payload.get("endpoint_in"),
        "endpoint_out": payload.get("endpoint_out"),
        "t_start": payload.get("t_start"),
        "t_end": payload.get("t_end"),
        "is_boundary": bool(payload.get("is_boundary", False)),
    }
    digest = hashlib.sha1(_stable_json(key_basis).encode("utf-8")).hexdigest()
    return f"stitch_{digest[:16]}"


def _provisional_fragment_type_policy(payload: Mapping[str, Any], query: QuerySpec) -> FragmentType:
    """Provisional typed-fragment policy (research placeholder).

    Placeholder policy (override later with learned/domain logic):
      - event fragment: event trace exists
      - interaction fragment: metadata marks interaction edges
      - pattern fragment: explicit pattern query mode
      - otherwise path fragment
    """
    fragment_type_hint = payload.get("fragment_type")
    if fragment_type_hint in {t.value for t in FragmentType}:
        return FragmentType(fragment_type_hint)

    metadata = payload.get("metadata", {}) if isinstance(payload, Mapping) else {}
    if metadata.get("events"):
        return FragmentType.EVENT
    if metadata.get("interaction", False):
        return FragmentType.INTERACTION
    if query.get("mode") == "pattern":
        return FragmentType.PATTERN
    return FragmentType.PATH


def assign_fragment_type(payload: Mapping[str, Any], query: QuerySpec) -> FragmentType:
    """Assign semantic fragment type using an isolated provisional policy."""
    return _provisional_fragment_type_policy(payload, query)


def materialize_fragment_payload(
    state: SearchState,
    partition: PartitionGraph,
    query: QuerySpec,
    *,
    boundary: bool,
    entry_state: Any,
    exit_state: Any,
) -> Dict[str, Any]:
    """Materialize fragment payload with explicit stitching-facing interface."""
    _ = partition
    endpoint_in, endpoint_out = extract_endpoints(state)
    t_start, t_end = extract_temporal_interface(state)

    metadata = dict(state.metadata)
    metadata.setdefault("query_mode", query.get("mode"))
    payload: Dict[str, Any] = {
        "vertex": state.vertex,
        "automaton_state": exit_state,
        "entry_state": entry_state,
        "exit_state": exit_state,
        "endpoint_in": endpoint_in,
        "endpoint_out": endpoint_out,
        "t_start": t_start,
        "t_end": t_end,
        "bindings": dict(state.bindings),
        "metadata": metadata,
        "is_boundary": bool(boundary),
    }
    payload["fragment_type"] = assign_fragment_type(payload, query).value
    payload["fragment_id"] = generate_fragment_id(payload)
    payload["stitch_key"] = generate_stitch_key(payload)
    return payload


def materialize_fragment(state: SearchState, partition: PartitionGraph, query: QuerySpec, *, boundary: bool) -> Dict[str, Any]:
    """Compatibility shim for historical callers.

    New call sites should use ``materialize_fragment_payload`` for explicit
    entry/exit semantics and boundary interface construction.
    """
    return materialize_fragment_payload(
        state=state,
        partition=partition,
        query=query,
        boundary=boundary,
        entry_state=state.automaton_state,
        exit_state=state.automaton_state,
    )


def _normalize_for_hash(value: Any) -> Hashable:
    """Normalize nested structures to stable hashable values."""
    if isinstance(value, Mapping):
        return tuple(sorted((str(k), _normalize_for_hash(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_normalize_for_hash(v) for v in value)
    if isinstance(value, set):
        return frozenset(_normalize_for_hash(v) for v in value)
    return value if isinstance(value, Hashable) else repr(value)


def fragment_signature(fragment: TypedFragment) -> Tuple[Hashable, ...]:
    """Build a stronger deterministic signature for fragment deduplication."""
    payload = fragment.payload
    return (
        fragment.fragment_type.value,
        _normalize_for_hash(payload.get("fragment_id")),
        _normalize_for_hash(payload.get("stitch_key")),
        _normalize_for_hash(payload.get("vertex")),
        _normalize_for_hash(payload.get("automaton_state")),
        _normalize_for_hash(payload.get("entry_state")),
        _normalize_for_hash(payload.get("exit_state")),
        _normalize_for_hash(payload.get("endpoint_in")),
        _normalize_for_hash(payload.get("endpoint_out")),
        _normalize_for_hash(payload.get("t_start")),
        _normalize_for_hash(payload.get("t_end")),
        bool(payload.get("is_boundary", False)),
        _normalize_for_hash(payload.get("bindings", {})),
        _normalize_for_hash(payload.get("metadata", {})),
    )


def normalize_fragments(fragments: Sequence[TypedFragment]) -> List[TypedFragment]:
    """Normalize typed fragments (deduplicate by stable signature)."""
    dedup: Dict[Tuple[Any, ...], TypedFragment] = {}
    for fragment in fragments:
        # Fragment normalization and deduplication.
        signature = fragment_signature(fragment)
        dedup[signature] = fragment
    return list(dedup.values())


def state_memoization_key(state: SearchState, query: QuerySpec) -> Tuple[Hashable, ...]:
    """Project search state to a finite memoization key for cycle safety.

    Default projection uses ``(vertex, automaton_state)`` plus optional
    user-selected fields from ``bindings``/``metadata``:
      - ``query["memoization_binding_keys"]``
      - ``query["memoization_metadata_keys"]``

    The default intentionally excludes path-history fields (e.g. ``path``,
    ``events``, ``visited_vertices``, ``depth``) so cyclic traversals cannot
    keep generating infinitely many distinct keys when no depth bound is set.
    """
    binding_keys = query.get("memoization_binding_keys", [])
    metadata_keys = query.get("memoization_metadata_keys", [])
    selected_bindings = {key: state.bindings.get(key) for key in binding_keys}
    selected_metadata = {key: state.metadata.get(key) for key in metadata_keys}
    return (
        _normalize_for_hash(state.vertex),
        _normalize_for_hash(state.automaton_state),
        _normalize_for_hash(selected_bindings),
        _normalize_for_hash(selected_metadata),
    )


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
    visited_state_keys: Set[Tuple[Hashable, ...]] = set()

    def emit_fragment(state: SearchState, *, boundary: bool, entry_state: Any, exit_state: Any) -> None:
        payload = materialize_fragment_payload(
            state=state,
            partition=partition_graph,
            query=query,
            boundary=boundary,
            entry_state=entry_state,
            exit_state=exit_state,
        )
        typed_fragments.append(TypedFragment(assign_fragment_type(payload, query), payload))

    # Seed initialization.
    for vertex in iter_partition_vertices(partition_graph):
        if match_start_condition(vertex, start_state, query):
            seed = SearchState(
                vertex=vertex,
                automaton_state=start_state,
                bindings=init_bindings(vertex, query),
                metadata=init_metadata(vertex),
            )
            # Accepting fragment emission.
            if is_accepting_state(seed.automaton_state, automaton_spec):
                emit_fragment(
                    seed,
                    boundary=False,
                    entry_state=seed.automaton_state,
                    exit_state=seed.automaton_state,
                )

            # Boundary fragment emission.
            seed_is_boundary = is_boundary_state(seed.vertex, seed.metadata, partition_graph, query)
            if seed_is_boundary:
                # Boundary fragment interface construction.
                emit_fragment(
                    seed,
                    boundary=True,
                    entry_state=seed.automaton_state,
                    exit_state=seed.automaton_state,
                )
                if not is_accepting_state(seed.automaton_state, automaton_spec):
                    continue

            frontier.append(seed)

    # Automaton-guided exploration.
    while frontier:
        state = frontier.pop()

        # Cycle / visited-state protection.
        memo_key = state_memoization_key(state, query)
        if memo_key in visited_state_keys:
            continue
        visited_state_keys.add(memo_key)

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

            accepting = is_accepting_state(next_state.automaton_state, automaton_spec)
            if accepting:
                # Accepting fragment emission.
                emit_fragment(
                    next_state,
                    boundary=False,
                    entry_state=state.automaton_state,
                    exit_state=next_state.automaton_state,
                )

            boundary = is_boundary_state(next_state.vertex, next_state.metadata, partition_graph, query)
            if boundary:
                # Boundary fragment emission.
                # Boundary fragment interface construction.
                emit_fragment(
                    next_state,
                    boundary=True,
                    entry_state=state.automaton_state,
                    exit_state=next_state.automaton_state,
                )
                # Boundary acts as a handoff/cut point: do not continue expansion
                # for non-accepting states beyond this local partition boundary.
                if not accepting:
                    continue

            if not accepting:
                frontier.append(next_state)

    # Fragment normalization and deduplication.
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

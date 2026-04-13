"""Research-oriented executable skeleton for cross-partition stitching.

This module provides an explicit, constraint-driven scaffold for Algorithm 2
(`stitching`) and Algorithm 2B (`stitch_fragments`) in distributed
spatio-temporal query processing.

The implementation is intentionally lightweight and runtime-agnostic:

* It preserves the formal control flow and checkpoints from the target
  semantics (grouping, candidate expansion, constraints, automaton filtering,
  closure filtering, deduplication).
* It does not assume unavailable details about fragment schema, distributed
  state backends, or full automaton internals.
* It is fully executable Python and designed for future replacement of
  placeholders with production logic (e.g., Spark-backed group processing).
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Hashable, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

# -----------------------------
# Lightweight typed structures
# -----------------------------

StitchKey = Hashable
StateId = Hashable
QuerySpec = Mapping[str, Any]
AutomatonSpec = Mapping[str, Any]


@dataclass(frozen=True)
class BoundaryFragment:
    """Boundary-fragment interface consumed by cross-partition stitching.

    The schema mirrors richer LocalEval outputs while keeping stitching
    independent from LocalEval internals. Explicit interface fields are used
    first; `payload` remains as a compatibility fallback for older fragments.
    """

    fragment_id: str
    stitch_key: StitchKey
    fragment_type: Optional[str] = None
    bindings: Mapping[str, Hashable] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    is_boundary: bool = True
    entry_state: Optional[StateId] = None
    exit_state: Optional[StateId] = None
    endpoint_in: Optional[Hashable] = None
    endpoint_out: Optional[Hashable] = None
    t_start: Optional[float] = None
    t_end: Optional[float] = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    automaton_state: Optional[StateId] = None  # Backward-compatibility fallback.


@dataclass
class PartialAssembly:
    """A partial stitched structure under construction.

    This representation already supports n-ary assembly: `fragment_ids` may
    contain any number of fragments; stitching is not limited to pairwise final
    outputs.
    """

    stitch_key: StitchKey
    fragment_ids: Tuple[str, ...]
    fragments: Tuple[BoundaryFragment, ...]
    trace: Mapping[str, Any] = field(default_factory=dict)
    expected_next_endpoint_in: Optional[Hashable] = None
    frontier_state: Optional[StateId] = None


@dataclass(frozen=True)
class StitchedWitness:
    """A complete cross-partition witness produced by stitching."""

    stitch_key: StitchKey
    fragment_ids: Tuple[str, ...]
    evidence: Mapping[str, Any] = field(default_factory=dict)


ConstraintSystem = Mapping[str, Any]
FragmentGroup = Sequence[BoundaryFragment]
GroupedFragments = Mapping[StitchKey, List[BoundaryFragment]]


# -----------------------------
# Constraint evaluation records
# -----------------------------


@dataclass(frozen=True)
class ConstraintCheckResult:
    """Result for one named compatibility dimension."""

    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class ConstraintEvaluation:
    """Structured evaluation of Φ(C, F, Q).

    This keeps the skeleton executable while exposing explicit reasons for
    pruning. The evaluation is intentionally stricter than permissive
    boolean plumbing and can later be replaced by richer solvers.
    """

    passed: bool
    checks: Tuple[ConstraintCheckResult, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)


# -----------------------------
# Helper functions
# -----------------------------


def group_by_stitch_key(fragments: Iterable[BoundaryFragment]) -> Dict[StitchKey, List[BoundaryFragment]]:
    """Group boundary fragments by stitch key.

    This is the executable counterpart of GroupByKey(𝓕^∂).
    """

    # Group-by-stitch-key stage
    groups: MutableMapping[StitchKey, List[BoundaryFragment]] = defaultdict(list)
    for fragment in fragments:
        groups[fragment.stitch_key].append(fragment)
    return dict(groups)



def initialize_assemblies(group: FragmentGroup) -> Deque[PartialAssembly]:
    """Initialize partial assemblies from one fragment group.

    The current skeleton seeds one single-fragment assembly per fragment.
    Future implementations can inject richer seeds (e.g., constrained multi-seed
    initialization, learned priors, or automaton-driven bootstrapping).
    """

    # Partial assembly initialization
    queue: Deque[PartialAssembly] = deque()
    for fragment in group:
        queue.append(
            PartialAssembly(
                stitch_key=fragment.stitch_key,
                fragment_ids=(fragment.fragment_id,),
                fragments=(fragment,),
                trace={"seed": True},
                expected_next_endpoint_in=fragment.endpoint_out,
                frontier_state=fragment.exit_state or fragment.automaton_state,
            )
        )
    return queue



def expand_candidates(assembly: PartialAssembly, group: FragmentGroup) -> List[BoundaryFragment]:
    """Expand candidate fragments for a given partial assembly.

    Placeholder policy: any fragment in the same group not already in the
    assembly is a candidate. This supports n-ary growth while remaining simple.
    """

    # Candidate expansion
    used_ids: Set[str] = set(assembly.fragment_ids)
    return [fragment for fragment in group if fragment.fragment_id not in used_ids]


def check_temporal_consistency(assembly: PartialAssembly, candidate: BoundaryFragment) -> ConstraintCheckResult:
    """Check temporal compatibility as a first-class admissibility dimension."""
    # Temporal compatibility
    last = assembly.fragments[-1]
    last_start = last.t_start if last.t_start is not None else last.payload.get("t_start")
    last_end = last.t_end if last.t_end is not None else last.payload.get("t_end")
    cand_start = candidate.t_start if candidate.t_start is not None else candidate.payload.get("t_start")
    cand_end = candidate.t_end if candidate.t_end is not None else candidate.payload.get("t_end")

    if not all(isinstance(v, (int, float)) for v in (last_start, last_end, cand_start, cand_end)):
        return ConstraintCheckResult(
            name="temporal_consistency",
            passed=False,
            detail="missing or non-numeric t_start/t_end; strict fallback rejects unknown interval order",
        )
    if float(last_start) > float(last_end) or float(cand_start) > float(cand_end):
        return ConstraintCheckResult(
            name="temporal_consistency",
            passed=False,
            detail="invalid interval shape detected (t_start > t_end)",
        )
    ok = float(cand_start) >= float(last_end)
    return ConstraintCheckResult(
        name="temporal_consistency",
        passed=ok,
        detail=f"candidate.t_start={cand_start} >= last.t_end={last_end}",
    )


def check_identity_consistency(assembly: PartialAssembly, candidate: BoundaryFragment) -> ConstraintCheckResult:
    """Check identity compatibility using explicit `bindings` first."""
    # Identity compatibility
    merged: Dict[str, Hashable] = {}
    for fragment in (*assembly.fragments, candidate):
        source_bindings: Mapping[str, Hashable]
        if fragment.bindings:
            source_bindings = fragment.bindings
        else:
            fallback_entity = fragment.metadata.get("entity_id", fragment.payload.get("entity_id"))
            source_bindings = {"entity_id": fallback_entity} if fallback_entity is not None else {}
        for var, value in source_bindings.items():
            if var in merged and merged[var] != value:
                return ConstraintCheckResult(
                    name="identity_consistency",
                    passed=False,
                    detail=f"binding conflict for {var}: {merged[var]} != {value}",
                )
            merged[var] = value
    ok = bool(merged)
    return ConstraintCheckResult(
        name="identity_consistency",
        passed=ok,
        detail=f"merged_bindings={dict(sorted(merged.items(), key=lambda item: item[0]))}",
    )


def check_structural_compatibility(assembly: PartialAssembly, candidate: BoundaryFragment) -> ConstraintCheckResult:
    """Check structural endpoint compatibility for assembly extension.

    Placeholder assumption: this skeleton models path-like interface matching
    (`endpoint_out` of current tail to `endpoint_in` of candidate). N-ary
    assembly is still supported through iterative extension of partials.
    """
    # Endpoint compatibility
    expected = assembly.expected_next_endpoint_in
    cand_in = candidate.endpoint_in
    if expected is None or cand_in is None:
        return ConstraintCheckResult(
            name="structural_compatibility",
            passed=False,
            detail="missing endpoint interface fields; strict fallback rejects unknown adjacency",
        )
    if expected != cand_in:
        return ConstraintCheckResult(
            name="structural_compatibility",
            passed=False,
            detail=f"expected endpoint_in={expected}, candidate.endpoint_in={cand_in}",
        )
    return ConstraintCheckResult(
        name="structural_compatibility",
        passed=True,
        detail="endpoint_out -> endpoint_in interface matches",
    )


def check_relation_fragment_compatibility(assembly: PartialAssembly, candidate: BoundaryFragment) -> ConstraintCheckResult:
    """Check fragment-type / relation compatibility under conservative rules."""
    last = assembly.fragments[-1]
    last_type = last.fragment_type or str(last.payload.get("fragment_type")) if last.payload.get("fragment_type") is not None else None
    cand_type = candidate.fragment_type or (
        str(candidate.payload.get("fragment_type")) if candidate.payload.get("fragment_type") is not None else None
    )
    if last_type is None or cand_type is None:
        return ConstraintCheckResult(
            name="relation_fragment_compatibility",
            passed=False,
            detail="fragment_type unavailable; strict fallback rejects unknown relation semantics",
        )
    repeated = cand_type == last_type
    return ConstraintCheckResult(
        name="relation_fragment_compatibility",
        passed=not repeated,
        detail=f"last_type={last_type}, candidate_type={cand_type}",
    )


def check_automaton_progression_compatibility(
    assembly: PartialAssembly, candidate: BoundaryFragment, automaton: AutomatonSpec
) -> ConstraintCheckResult:
    """Validate sequential automaton progression for one extension edge.

    Explicitly uses `entry_state`/`exit_state` when available, with documented
    fallback to `automaton_state` for legacy fragments.
    """
    # Automaton progression validation
    left_state = assembly.frontier_state
    if left_state is None:
        tail = assembly.fragments[-1]
        left_state = tail.exit_state or tail.automaton_state
    right_entry = candidate.entry_state or candidate.automaton_state
    right_exit = candidate.exit_state or candidate.automaton_state
    if right_entry is None or right_exit is None:
        return ConstraintCheckResult(
            name="automaton_progression",
            passed=False,
            detail="candidate entry_state/exit_state unavailable; strict fallback rejects unknown progression",
        )

    allowed_states = automaton.get("allowed_states")
    if allowed_states is not None:
        allowed_state_set = set(allowed_states)
        if right_entry not in allowed_state_set or right_exit not in allowed_state_set:
            return ConstraintCheckResult(
                name="automaton_progression",
                passed=False,
                detail=f"candidate states not in allowed_states: entry={right_entry}, exit={right_exit}",
            )

    if left_state is None:
        return ConstraintCheckResult(
            name="automaton_progression",
            passed=False,
            detail="assembly frontier_state unavailable; strict fallback rejects unknown progression",
        )

    transitions = automaton.get("allowed_transitions", [])
    transition_set: Set[Tuple[StateId, StateId]] = set()
    for transition in transitions:
        if isinstance(transition, Sequence) and len(transition) == 2:
            transition_set.add((transition[0], transition[1]))

    if transition_set:
        ok = (left_state, right_entry) in transition_set
        return ConstraintCheckResult(
            name="automaton_progression",
            passed=ok,
            detail=f"requires transition ({left_state} -> {right_entry})",
        )
    ok = left_state == right_entry
    return ConstraintCheckResult(
        name="automaton_progression",
        passed=ok,
        detail=f"fallback boundary state continuity ({left_state} == {right_entry})",
    )


def check_query_constraints(assembly: PartialAssembly, candidate: BoundaryFragment, query: QuerySpec) -> ConstraintCheckResult:
    """Evaluate explicit query-aware compatibility constraints.

    Query semantics are a first-class admissibility stage. This remains a
    placeholder, but it actively prunes candidates according to query-specified
    constraint hints instead of acting as metadata-only annotation.
    Supported lightweight keys:
      - required_fragment_types: iterable[str]
      - forbidden_fragment_types: iterable[str]
      - allow_same_type_adjacency: bool
      - required_entity_id: hashable
    """

    # Query-aware compatibility checks
    required_types = {str(t) for t in query.get("required_fragment_types", [])}
    forbidden_types = {str(t) for t in query.get("forbidden_fragment_types", [])}
    allow_same_type_adjacency = bool(query.get("allow_same_type_adjacency", False))
    required_entity_id = query.get("required_entity_id")

    candidate_type = candidate.fragment_type
    last_type = assembly.fragments[-1].fragment_type
    candidate_entity = candidate.bindings.get("entity_id", candidate.metadata.get("entity_id"))
    type_allowed = candidate_type not in forbidden_types and (not required_types or candidate_type in required_types)
    adjacency_allowed = allow_same_type_adjacency or (candidate_type != last_type)
    entity_allowed = required_entity_id is None or candidate_entity == required_entity_id
    boundary_allowed = candidate.is_boundary

    ok = type_allowed and adjacency_allowed and entity_allowed and boundary_allowed
    return ConstraintCheckResult(
        name="query_constraints",
        passed=ok,
        detail=(
            f"type_allowed={type_allowed}, adjacency_allowed={adjacency_allowed}, "
            f"entity_allowed={entity_allowed}, boundary_allowed={boundary_allowed}"
        ),
    )


def evaluate_constraint_system(
    assembly: PartialAssembly, candidate: BoundaryFragment, query: QuerySpec, automaton: AutomatonSpec
) -> ConstraintEvaluation:
    """Structured constraint evaluation Φ(C, F, Q).

    Dimensions are evaluated explicitly:
    temporal, identity, structural, relation/fragment-type, and query-aware
    constraints. The result carries explainable failures for downstream tracing.
    """

    # Structured constraint evaluation Φ
    same_key_check = ConstraintCheckResult(
        name="stitch_key_consistency",
        passed=assembly.stitch_key == candidate.stitch_key,
        detail=f"assembly_key={assembly.stitch_key}, candidate_key={candidate.stitch_key}",
    )
    checks: Tuple[ConstraintCheckResult, ...] = (
        same_key_check,
        check_temporal_consistency(assembly, candidate),
        check_identity_consistency(assembly, candidate),
        check_structural_compatibility(assembly, candidate),
        check_automaton_progression_compatibility(assembly, candidate, automaton),
        check_relation_fragment_compatibility(assembly, candidate),
        check_query_constraints(assembly, candidate, query),
    )
    passed = all(check.passed for check in checks)
    return ConstraintEvaluation(
        passed=passed,
        checks=checks,
        metadata={"candidate_id": candidate.fragment_id, "query_name": query.get("name", "unspecified")},
    )



def preserve_automaton_consistency(assembly: PartialAssembly, automaton: AutomatonSpec) -> bool:
    """Check whether stitched assembly remains consistent with automaton 𝒜.

    Placeholder policy (stricter than set-membership-only):
    * validate known states against `allowed_states` when provided;
    * validate local transitions using either:
      - explicit `entry_state` / `exit_state` in fragment payload, or
      - fallback to fragment-level `automaton_state`;
    * if automaton exposes `allowed_transitions`, enforce them for adjacent
      stitched fragments when both states are known.
    """

    # Automaton progression validation
    allowed_states = automaton.get("allowed_states")
    allowed_transitions = automaton.get("allowed_transitions", [])
    transition_set: Set[Tuple[StateId, StateId]] = set()
    for transition in allowed_transitions:
        if isinstance(transition, Sequence) and len(transition) == 2:
            transition_set.add((transition[0], transition[1]))

    allowed: Optional[Set[StateId]] = set(allowed_states) if allowed_states else None
    for fragment in assembly.fragments:
        state = fragment.automaton_state
        if state is not None and allowed is not None and state not in allowed:
            return False

        entry_state = fragment.entry_state or fragment.payload.get("entry_state")
        exit_state = fragment.exit_state or fragment.payload.get("exit_state")
        if allowed is not None:
            if entry_state is not None and entry_state not in allowed:
                return False
            if exit_state is not None and exit_state not in allowed:
                return False

    for left, right in zip(assembly.fragments, assembly.fragments[1:]):
        left_exit = left.exit_state or left.payload.get("exit_state", left.automaton_state)
        right_entry = right.entry_state or right.payload.get("entry_state", right.automaton_state)
        if left_exit is not None and right_entry is not None:
            if transition_set:
                if (left_exit, right_entry) not in transition_set:
                    return False
            elif left_exit != right_entry:
                # Conservative fallback when explicit transitions are absent.
                return False
    return True



def preserve_closure(assembly: PartialAssembly) -> bool:
    """Check closure preservation of partial stitched assembly.

    Placeholder policy for closure over partial witnesses:
    * fragment identifiers are unique;
    * `fragment_ids` and `fragments` stay cardinality-aligned;
    * all fragment IDs in `fragments` are represented in `fragment_ids`;
    * all fragments in an assembly share one stitch key.
    """

    # Closure preservation
    if len(set(assembly.fragment_ids)) != len(assembly.fragment_ids):
        return False
    if len(assembly.fragment_ids) != len(assembly.fragments):
        return False
    fragment_ids_from_payload = {fragment.fragment_id for fragment in assembly.fragments}
    if fragment_ids_from_payload != set(assembly.fragment_ids):
        return False
    if any(fragment.stitch_key != assembly.stitch_key for fragment in assembly.fragments):
        return False
    return True



def stitch(assembly: PartialAssembly, candidate: BoundaryFragment) -> PartialAssembly:
    """Stitch one candidate fragment into a partial assembly.

    Produces a new assembly object to keep the search process purely functional
    with respect to previously enqueued states.
    """

    new_ids = (*assembly.fragment_ids, candidate.fragment_id)
    new_fragments = (*assembly.fragments, candidate)
    new_trace = dict(assembly.trace)
    new_trace["last_added"] = candidate.fragment_id

    return PartialAssembly(
        stitch_key=assembly.stitch_key,
        fragment_ids=new_ids,
        fragments=new_fragments,
        trace=new_trace,
        expected_next_endpoint_in=candidate.endpoint_out,
        frontier_state=candidate.exit_state or candidate.payload.get("exit_state", candidate.automaton_state),
    )



def is_complete_witness(assembly: PartialAssembly, automaton: AutomatonSpec, query: QuerySpec) -> bool:
    """Determine if a partial assembly is complete for query Q under automaton 𝒜.

    Placeholder completion semantics:
    * optional query integer `min_fragments` defines minimum assembled size,
    * automaton consistency must also hold.
    """

    # Witness completion
    min_fragments = int(query.get("min_fragments", 2))
    required_terminal_states = set(query.get("required_terminal_states", []))
    size_ok = len(assembly.fragment_ids) >= min_fragments
    automaton_ok = preserve_automaton_consistency(assembly, automaton)
    if not required_terminal_states:
        return size_ok and automaton_ok

    tail = assembly.fragments[-1]
    tail_terminal = tail.exit_state or tail.payload.get("exit_state", tail.automaton_state)
    terminal_ok = tail_terminal in required_terminal_states if tail_terminal is not None else False
    return size_ok and automaton_ok and terminal_ok



def deduplicate_witnesses(witnesses: Iterable[StitchedWitness]) -> List[StitchedWitness]:
    """Deduplicate stitched witnesses by canonical (stitch_key, fragment_ids)."""

    # Final deduplication
    unique: Dict[Tuple[StitchKey, Tuple[str, ...]], StitchedWitness] = {}
    for witness in witnesses:
        key = (witness.stitch_key, tuple(sorted(witness.fragment_ids)))
        unique[key] = StitchedWitness(
            stitch_key=witness.stitch_key,
            fragment_ids=tuple(sorted(witness.fragment_ids)),
            evidence=witness.evidence,
        )
    return list(unique.values())


# -----------------------------
# Algorithm 2B: per-group core
# -----------------------------


def stitch_fragments(group: FragmentGroup, automaton: AutomatonSpec, query: QuerySpec) -> List[StitchedWitness]:
    """Algorithm 2B: stitch fragments within one stitch-key group.

    Args:
        group: Fragment group G ⊆ 𝓕_border.
        automaton: Condition-aware automaton specification 𝒜.
        query: Query specification Q.

    Returns:
        Deduplicated complete witnesses 𝓦_G.
    """

    # Partial assembly initialization
    # Step 1: Initialize partial assemblies 𝓒 ← InitializeAssemblies(G)
    assemblies: Deque[PartialAssembly] = initialize_assemblies(group)

    # Step 2: Initialize complete witness set 𝓦_G ← ∅
    group_witnesses: List[StitchedWitness] = []

    # Step 3: While 𝓒 is not empty
    while assemblies:
        current = assemblies.popleft()

        # if IsCompleteWitness(C, 𝒜, Q), emit it and continue
        if is_complete_witness(current, automaton, query):
            group_witnesses.append(
                StitchedWitness(
                    stitch_key=current.stitch_key,
                    fragment_ids=current.fragment_ids,
                    evidence={"trace": current.trace},
                )
            )
            continue

        # Candidate expansion
        # CandidateSet ← ExpandCandidates(C, G)
        candidates = expand_candidates(current, group)

        # for each candidate fragment F
        for candidate in candidates:
            # Structured constraint evaluation Φ
            # Query-aware compatibility checks
            constraint_evaluation = evaluate_constraint_system(current, candidate, query, automaton)
            if not constraint_evaluation.passed:
                continue

            # C' ← Stitch(C, F)
            stitched = stitch(current, candidate)

            # Automaton progression validation
            # if PreserveAutomatonConsistency(C', 𝒜)
            if not preserve_automaton_consistency(stitched, automaton):
                continue

            # Closure preservation
            # if PreserveClosure(C')
            if not preserve_closure(stitched):
                continue

            # add C' back to 𝓒
            stitched_trace = dict(stitched.trace)
            stitched_trace["last_constraint_eval"] = [
                {"name": check.name, "passed": check.passed, "detail": check.detail}
                for check in constraint_evaluation.checks
            ]
            stitched = PartialAssembly(
                stitch_key=stitched.stitch_key,
                fragment_ids=stitched.fragment_ids,
                fragments=stitched.fragments,
                trace=stitched_trace,
                expected_next_endpoint_in=stitched.expected_next_endpoint_in,
                frontier_state=stitched.frontier_state,
            )
            assemblies.append(stitched)

    # Step 4: Return DeduplicateWitnesses(𝓦_G)
    return deduplicate_witnesses(group_witnesses)


# -----------------------------
# Algorithm 2: global orchestration
# -----------------------------


def stitching(boundary_fragments: Iterable[BoundaryFragment], automaton: AutomatonSpec, query: QuerySpec) -> List[StitchedWitness]:
    """Algorithm 2: global stitching over all boundary fragments 𝓕^∂.

    Args:
        boundary_fragments: Global boundary fragments 𝓕^∂ = ⋃ᵢ 𝓕ᵢ^∂.
        automaton: Condition-aware automaton 𝒜.
        query: Query Q.

    Returns:
        Deduplicated stitched cross-partition witnesses 𝓦_stitch.
    """

    # Group-by-stitch-key stage
    # Step 1: Group fragments by stitch key: 𝓖 ← GroupByKey(𝓕^∂)
    grouped = group_by_stitch_key(boundary_fragments)

    # Step 2: Initialize global stitched witness set 𝓦_stitch ← ∅
    global_witnesses: List[StitchedWitness] = []

    # Step 3: For each group G ∈ 𝓖
    for group in grouped.values():
        global_witnesses.extend(stitch_fragments(group, automaton, query))

    # Step 4: Return DeduplicateWitnesses(𝓦_stitch)
    return deduplicate_witnesses(global_witnesses)


# Complexity summary (for research documentation and planning)
#
# Time Complexity:
# - Let B = |𝓕^∂|.
# - Let group sizes be g₁, …, gᵣ with Σ gⱼ = B.
# - If each partial assembly examines at most β candidates and compatibility
#   checking costs γ, then worst-case per-group search is exponential:
#   T_Stitch(Gⱼ) = O(2^(gⱼ) · β · γ).
# - Hence total global stitching cost is:
#   T_Stitching = Σⱼ O(2^(gⱼ) · β · γ).
#
# Space Complexity:
# - Worst-case O(maxⱼ 2^(gⱼ)).
# - Practical performance depends strongly on pruning strength, automaton
#   filtering, and stitch-key selectivity.

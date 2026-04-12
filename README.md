# video-graph-distributed-query-framework

Distributed spatio-temporal query framework for partitioned video knowledge graphs (LocalEval + Stitching + Best-chain, Spark-based)

## Refactored LocalEval for Spark

The LocalEval phase below is refactored to:
- minimize shuffle,
- push down temporal filtering,
- reduce join depth, and
- align with Spark's partition-local execution model.

### Algorithm: `LocalEvalSpark(P_i, A, Q)`

**Input**
- `P_i`: graph partition (edges/vertices co-located by partition key)
- `A`: condition-aware automaton `(Q_states, Σ, δ, q0, F)`
- `Q`: query `(Π, Θ, Ψ)` where `Θ` includes temporal predicates

**Output**
- `F_i`: partition-local fragments
- `F_i_border`: only fragments that may require cross-partition stitching

```text
1:  // Partition-local setup (no shuffle)
2:  E_i ← LoadEdges(P_i)
3:  V_i ← LoadVertices(P_i)
4:
5:  // Temporal pushdown and transition pruning
6:  T_q ← ExtractTimeWindow(Θ)                          // [t_min, t_max]
7:  E_i' ← Filter(E_i, edge.time ∈ T_q)                // pushed to scan
8:  Δ_A ← CompileTransitionPredicates(A, Π, Ψ)         // per-state edge constraints
9:  E_i'' ← AnnotateAndPrune(E_i', Δ_A)                // keep only transition-relevant edges
10:
11: // Build compact frontier index to avoid repeated graph scans
12: Seed_i ← SelectSeedVertices(V_i, E_i'', q0)
13: Frontier_0 ← Project(Seed_i, cols = {vid, state=q0, t_low=-∞, t_high=+∞, bindings=∅, depth=0})
14:
15: F_i ← ∅
16: F_i_border ← ∅
17:
18: // Level-synchronous expansion (Spark-friendly iterative DataFrame steps)
19: for depth = 0 to MaxDepth(A, Q) do
20:     if IsEmpty(Frontier_depth) then break
21:
22:     // Narrow join: frontier × transition-relevant local edges only
23:     Cand ← Join(
24:              Frontier_depth f,
25:              E_i'' e,
26:              on = (f.vid = e.src AND TransitionAllowed(f.state, e, Δ_A))
27:            )
28:
29:     // Push temporal consistency before materialization
30:     Cand_t ← Filter(Cand, TemporalConsistent(f.t_low, f.t_high, e.time, Θ))
31:
32:     // Update bindings/state interval in one projection (reduce join depth)
33:     Next ← Project(Cand_t,
34:              vid = e.dst,
35:              state = δ(f.state, e.label, e.attrs),
36:              t_low = max(f.t_low, e.t_start),
37:              t_high = min(f.t_high, e.t_end),
38:              bindings = MergeBindings(f.bindings, e, Π),
39:              trace = AppendEdgeId(f.trace, e.eid),
40:              depth = f.depth + 1,
41:              border_flag = IsBorderEdge(e)
42:            )
43:
44:     // Early semantic pruning and local dedup
45:     Valid ← Filter(Next, SatisfyPartialSemantics(bindings, state, Π, Ψ))
46:     Valid ← DedupBySignature(Valid, key = {vid, state, bindings, t_low, t_high})
47:
48:     // Emit partial/complete fragments without extra joins
49:     Emit ← Filter(Valid, IsFragmentBoundary(state, Π, Ψ) OR state ∈ F)
50:     F_i ← F_i ∪ Project(Emit, {bindings, [t_low,t_high], state, trace, partition=P_i})
51:
52:     // Keep only stitch-relevant border fragments
53:     BorderEmit ← Filter(Emit, border_flag = true)
54:     F_i_border ← F_i_border ∪ Project(BorderEmit,
55:                      stitch_key = Hash(bindings_keys, TemporalAnchor([t_low,t_high]), state),
56:                      payload = {bindings, [t_low,t_high], state, trace, partition=P_i}
57:                    )
58:
59:     Frontier_{depth+1} ← RepartitionByDst(Valid)      // local partitioning hint only
60: end for
61:
62: return (F_i, F_i_border)
```

### Why this is Spark-aligned

- **Shuffle minimization**: all heavy expansion is partition-local; only `F_i_border` is exported for global stitching.
- **Temporal pushdown**: time-window filtering happens before expansion joins.
- **Reduced join depth**: each iteration uses a single frontier-to-edge join plus projection-based state/binding updates.
- **Execution model fit**: level-synchronous frontier expansion maps to iterative DataFrame/Dataset stages with early pruning and dedup.

## Refactored Distributed Evaluation

```text
Algorithm DistributedQueryEvalSpark(𝒫, 𝒜, Q)

1:  // Phase 1: Partition-local evaluation (parallel)
2:  for each partition P_i ∈ 𝒫 in parallel do
3:      (𝓕_i, 𝓕_i_border) ← LocalEvalSpark(P_i, 𝒜, Q)
4:  end for
5:
6:  // Phase 2: Union local complete/partial results (no global shuffle for non-border)
7:  𝓕_local ← UnionAll_i(𝓕_i)
8:
9:  // Phase 3: Shuffle only border payloads using compact stitch key
10:  𝓑 ← UnionAll_i(𝓕_i_border)
11:  𝒢 ← RepartitionAndGroup(𝓑, key = stitch_key)
12:
13: // Phase 4: Constraint-driven stitching per key-group
14: 𝓦_stitch ← flatMapGroups(𝒢, g -> StitchFragments(g, 𝒜, Q))
15:
16: // Phase 5: Add local complete witnesses directly
17: 𝓦_local ← Filter(𝓕_local, IsCompleteWitness(fragment, 𝒜, Q))
18:
19: return Distinct(𝓦_local ∪ 𝓦_stitch)
```

## Improved Stitching (Constraint-driven Assembly)

To make Stitching more rigorous and implementation-ready, the framework separates:
1. **partition-local fragment generation** (`LocalEval`), and
2. **cross-partition constrained composition** (`StitchFragments`).

This keeps local traversal automaton-guided, while making global composition explicitly constraint-driven.

### Algorithm 2: Fragment Stitching (high-level)

```text
Input:
    F           // set of fragments
    Φ           // compatibility constraint system

Output:
    W           // set of query witnesses

1: Initialize W ← ∅
2: for each fragment F_1 in F do
3:     Initialize candidate chain C ← {F_1}
4:     Expand C by selecting fragment F_j such that Φ(C, F_j) holds
5:     Recursively extend C while compatibility is satisfied
6:     if C forms a complete witness then
7:         Add C to W
8:     end if
9: end for
10: return W
```

### Algorithm 2A: `LocalEval(P_i, 𝒜, Q)` — Automaton-guided Fragment Generation

```text
Input:
• Partition P_i
• Condition-Aware Automaton 𝒜
• Query Q
Output:
• Set of typed fragments 𝓕_i

1:  Initialize fragment set 𝓕_i ← ∅
2:  Initialize search frontier 𝒮 ← ∅
3:  for each vertex v ∈ V(P_i) do
4:      if MatchStartCondition(v, q_0, Q) = true then
5:          Create initial state s ← (v, q_0, InitBindings(v), InitMetadata(v))
6:          𝒮 ← 𝒮 ∪ {s}
7:      end if
8:  end for
9:  while 𝒮 is not empty do
10:     Extract a state s = (v, q, λ, θ) from 𝒮
11:     for each admissible edge e = (v, a, u) in P_i do
12:         if ExistsTransition(q, a, u, e, 𝒜, Q) = true then
13:             q' ← δ(q, a)
14:             λ' ← UpdateBindings(λ, u, e, Q)
15:             θ' ← UpdateMetadata(θ, u, e, q', Q)
16:             if ViolatesLocalConstraints(λ', θ', Q) = false then
17:                 s' ← (u, q', λ', θ')
18:                 if IsAcceptingState(q', 𝒜) = true then
19:                     F ← MaterializeFragment(s')
20:                     AssignType(F, Q)
21:                     𝓕_i ← 𝓕_i ∪ {F}
22:                 else
23:                     𝒮 ← 𝒮 ∪ {s'}
24:                 end if
25:             end if
26:         end if
27:     end for
28:     if IsBoundaryState(s, P_i, Q) = true then
29:         F ← MaterializeFragment(s)
30:         AssignType(F, Q)
31:         𝓕_i ← 𝓕_i ∪ {F}
32:     end if
33: end while
34: return NormalizeFragments(𝓕_i)
```

**Academic interpretation of `LocalEval`:**
- Operates strictly inside one partition.
- Is driven by the CAA state transition system.
- Emits both complete local fragments and boundary partial fragments for later stitching.

Important detail:
- `AssignType(F, Q)` is required so downstream stitching can enforce type-aware constraints across:
  - path fragments,
  - event fragments,
  - interaction fragments,
  - pattern fragments.

### Algorithm 2B: `StitchFragments(G, 𝒜, Q)` — Constraint-Driven Assembly under CAA

```text
Input:
• Fragment group G ⊆ 𝓕_border
• Condition-Aware Automaton 𝒜
• Query Q
Output:
• Set of complete query witnesses 𝓦_G

1:  Initialize partial assemblies 𝒞 ← InitializeAssemblies(G)
2:  Initialize complete witness set 𝓦_G ← ∅
3:  while 𝒞 is not empty do
4:      Extract a partial assembly C from 𝒞
5:      if IsCompleteWitness(C, 𝒜, Q) = true then
6:          𝓦_G ← 𝓦_G ∪ {C}
7:          continue
8:      end if
9:      CandidateSet ← ExpandCandidates(C, G)
10:     for each fragment F ∈ CandidateSet do
11:         if SatisfyConstraintSystem(Φ(C, F, Q)) = true then
12:             C' ← Stitch(C, F)
13:             if PreserveAutomatonConsistency(C', 𝒜) = true then
14:                 if PreserveClosure(C') = true then
15:                     𝒞 ← 𝒞 ∪ {C'}
16:                 end if
17:             end if
18:         end if
19:     end for
20: end while
21: return DeduplicateWitnesses(𝓦_G)
```

**Why this stitching model is stronger:**
- Stitching is treated as a **partial function** (only valid for compatible inputs).
- Compatibility is not ad hoc; it is a formal **constraint system** `Φ`.
- The same mechanism naturally extends from binary joins to **n-ary assembly**.

Operationally:
- `InitializeAssemblies(G)` seeds assembly search from single fragments or configured seeds.
- `ExpandCandidates(C, G)` prunes to only potentially stitchable fragments.
- `SatisfyConstraintSystem(Φ(C, F, Q))` checks hard semantic/temporal/topological constraints.
- `Stitch(C, F)` applies the assembly operator (⊕).
- `PreserveAutomatonConsistency` ensures the new assembly maps to a valid run of `𝒜`.
- `PreserveClosure` ensures every intermediate result remains a legal fragment/partial witness.

### Practical implementation notes

- Partition graph data by `(src_vertex_partition, time_bucket)` where possible to improve scan locality.
- Use broadcast side inputs only for **small** automaton metadata (`Δ_A`, state transition tables).
- Cache `E_i''` if multiple frontier levels are expected; otherwise rely on pipelined execution.
- Prefer typed `Dataset`/`DataFrame` transformations with column pruning over UDF-heavy code paths.

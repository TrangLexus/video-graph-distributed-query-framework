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

### Practical implementation notes

- Partition graph data by `(src_vertex_partition, time_bucket)` where possible to improve scan locality.
- Use broadcast side inputs only for **small** automaton metadata (`Δ_A`, state transition tables).
- Cache `E_i''` if multiple frontier levels are expected; otherwise rely on pipelined execution.
- Prefer typed `Dataset`/`DataFrame` transformations with column pruning over UDF-heavy code paths.

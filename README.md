# video-graph-distributed-query-framework

Distributed spatio-temporal query framework for partitioned video knowledge graphs.

## IEEE-Style Algorithmic Formulation

Let the distributed graph be partitioned as \(\mathcal{P}=\{P_1,\dots,P_k\}\). Let \(\mathcal{A}=(Q_\mathcal{A},\Sigma,\delta,q_0,F_\mathcal{A})\) be a condition-aware automaton (CAA), and let query \(Q=(\Pi,\Theta,\Psi)\) encode structural, temporal, and semantic constraints.

---

### Algorithm 1: `LocalEval(P_i, \mathcal{A}, Q)`

**Input:** partition \(P_i=(V_i,E_i)\), CAA \(\mathcal{A}\), query \(Q\).  
**Output:** local complete fragments \(\mathcal{F}_i\), boundary fragments \(\mathcal{F}^{\partial}_i\).

```text
Algorithm 1 LocalEval(P_i, 𝒜, Q)
1:  𝓕_i ← ∅ ; 𝓕_i^∂ ← ∅
2:  E_i' ← TemporalFilter(E_i, Θ)               // pushdown on time predicates
3:  Δ_A ← CompileTransitionPredicates(𝒜, Π, Ψ)
4:  S_0 ← InitializeSeeds(V_i, E_i', q_0, Q)
5:  Frontier ← S_0
6:  while Frontier ≠ ∅ do
7:      Next ← ∅
8:      for each state tuple s=(v,q,λ,τ,trace) ∈ Frontier do
9:          for each edge e=(v,a,u) ∈ Adj_{E_i'}(v) do
10:             if TransitionAllowed(q,e,Δ_A) and TemporalConsistent(τ,e,Θ) then
11:                 q' ← δ(q,a)
12:                 λ' ← UpdateBindings(λ,e,Π)
13:                 τ' ← UpdateInterval(τ,e)
14:                 if SatisfyLocalSemantics(λ',q',Ψ) then
15:                     s' ← (u,q',λ',τ',Append(trace,e))
16:                     if q' ∈ F_𝒜 then
17:                         𝓕_i ← 𝓕_i ∪ {Materialize(s')}
18:                     else
19:                         Next ← Next ∪ {s'}
20:                     end if
21:                     if IsBoundaryState(s',P_i) then
22:                         𝓕_i^∂ ← 𝓕_i^∂ ∪ {Materialize(s')}
23:                     end if
24:                 end if
25:             end if
26:         end for
27:      end for
28:      Frontier ← Deduplicate(Next)
29:  end while
30:  return (Normalize(𝓕_i), Normalize(𝓕_i^∂))
```

**Time Complexity (Algorithm 1).**  
Let \(|V_i|\) and \(|E_i|\) be vertices/edges in partition \(P_i\), \(d_i\) the maximum out-degree, \(m=|Q_\mathcal{A}|\) automaton states, and \(L\) the maximum exploration depth induced by \(Q\). In the worst case, each frontier state expands over up to \(d_i\) edges for up to \(L\) levels, yielding:
\[
T_{\text{LocalEval}}(P_i)=\mathcal{O}\!\left(\min\{m|V_i|,\,|V_i|d_i^L\}\cdot d_i\right),
\]
which is upper-bounded by \(\mathcal{O}(m|E_i|L)\) under level-wise deduplication and bounded repeated-state expansion. Space complexity is \(\mathcal{O}(m|V_i|+|\mathcal{F}_i|+|\mathcal{F}_i^\partial|)\).

---

### Algorithm 2: `Stitching(\mathcal{F}^{\partial}, \mathcal{A}, Q)`

**Input:** global boundary fragments \(\mathcal{F}^{\partial}=\bigcup_i \mathcal{F}^{\partial}_i\), CAA \(\mathcal{A}\), query \(Q\).  
**Output:** stitched cross-partition witnesses \(\mathcal{W}_{\text{stitch}}\).

```text
Algorithm 2 Stitching(𝓕^∂, 𝒜, Q)
1:  Group fragments by stitch key: 𝓖 ← GroupByKey(𝓕^∂)
2:  𝓦_stitch ← ∅
3:  for each group G ∈ 𝓖 do
4:      𝓒 ← InitializeAssemblies(G)
5:      while 𝓒 ≠ ∅ do
6:          C ← Pop(𝓒)
7:          if IsCompleteWitness(C,𝒜,Q) then
8:              𝓦_stitch ← 𝓦_stitch ∪ {C}
9:              continue
10:         end if
11:         Cand ← ExpandCandidates(C,G)
12:         for each F ∈ Cand do
13:             if Compatible(C,F,Q) and PreserveAutomaton(C,F,𝒜) then
14:                 C' ← Stitch(C,F)
15:                 if PreserveClosure(C') then
16:                     𝓒 ← 𝓒 ∪ {C'}
17:                 end if
18:             end if
19:         end for
20:      end while
21:  end for
22:  return DeduplicateWitnesses(𝓦_stitch)
```

**Time Complexity (Algorithm 2).**  
Let \(B=|\mathcal{F}^{\partial}|\), and let group sizes be \(g_1,\dots,g_r\) with \(\sum_j g_j=B\). If each partial assembly examines at most \(\beta\) candidates and compatibility checking costs \(\gamma\), then per-group search is exponential in the worst case (set assembly):
\[
T_{\text{Stitch}}(G_j)=\mathcal{O}(2^{g_j}\beta\gamma).
\]
Hence,
\[
T_{\text{Stitching}}=\sum_{j=1}^{r}\mathcal{O}(2^{g_j}\beta\gamma),
\]
with practical performance dominated by pruning strength in `Compatible`, automaton-consistency filtering, and stitch-key selectivity. Space complexity is \(\mathcal{O}(\max_j 2^{g_j})\) in the worst case, but typically far lower under aggressive constraint pruning.

---

## End-to-End Evaluation Pipeline

1. Run **Algorithm 1** independently on each partition \(P_i\) in parallel to produce \((\mathcal{F}_i,\mathcal{F}_i^\partial)\).  
2. Union complete local witnesses from \(\mathcal{F}_i\).  
3. Apply **Algorithm 2** only to boundary fragments \(\mathcal{F}^{\partial}\).  
4. Return distinct witnesses from local and stitched results.

This decomposition minimizes cross-partition communication while preserving formal automaton and query semantics.

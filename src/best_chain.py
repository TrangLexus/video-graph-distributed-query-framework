"""Best-chain: optional post-processing over stitched candidate witnesses.

This module is intentionally **downstream** of the core distributed query
pipeline:

1. LocalEval creates partition-local fragments.
2. Stitching composes admissible cross-partition candidate witnesses.
3. Best-chain optionally ranks/prunes/selects among those already-constructed
   candidates.

Important semantic boundary:
- Best-chain is a post-processing optimization layer.
- It does not determine witness validity.
- It does not replace Stitching.

Accordingly, this module only evaluates already-admissible candidates and does
not perform witness construction, graph traversal, or automaton-run assembly.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any, Mapping, Optional, Sequence


# ---------------------------------------------------------------------------
# Lightweight types
# ---------------------------------------------------------------------------

Candidate = Mapping[str, Any]


@dataclass(frozen=True)
class BestChainConfig:
    """Configurable, provisional policy for scoring and pruning.

    The schema and weights here are placeholders for research iteration. They
    should be treated as replaceable policy knobs, not finalized semantics.
    """

    # Provisional scoring weights (all replaceable).
    weight_continuity: float = 1.5
    weight_completeness: float = 1.0
    weight_consistency: float = 1.5
    weight_length: float = 0.5
    weight_penalty: float = 1.0

    # Optional pruning controls; both disabled by default to keep behavior safe.
    min_score: Optional[float] = None
    max_candidates_after_prune: Optional[int] = None


@dataclass(frozen=True)
class ChainFeatures:
    """Extracted placeholder features for an already-assembled candidate.

    Expected candidate keys are intentionally lightweight and optional:
    - fragments | segments | steps: sequence payload used for coarse length
    - temporal_breaks: nonnegative proxy for temporal fragmentation
    - identity_mismatches: nonnegative proxy for identity discontinuity
    - automaton_violations: nonnegative proxy for run inconsistency
    - coverage: [0, 1] proxy for structural/semantic completeness

    Missing keys fall back to conservative defaults.
    """

    continuity_score: float
    completeness_score: float
    consistency_score: float
    penalty_score: float
    length_score: float


DEFAULT_CONFIG = BestChainConfig()


# ---------------------------------------------------------------------------
# Candidate feature extraction
# ---------------------------------------------------------------------------

def _to_nonnegative_float(value: Any, default: float = 0.0) -> float:
    """Best-effort conversion to nonnegative float with safe fallback."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, numeric)


def _clamp_01(value: Any, default: float = 1.0) -> float:
    """Clamp value into [0, 1] for placeholder normalized features."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = default
    return max(0.0, min(1.0, numeric))


def extract_chain_features(candidate: Candidate) -> ChainFeatures:
    """Extract provisional features from a stitched candidate.

    This stage is intentionally schema-light and does not validate admissibility.
    Admissibility is assumed to be enforced earlier by Stitching/constraints.
    """
    # Candidate feature extraction
    chain_like: Any = (
        candidate.get("fragments")
        or candidate.get("segments")
        or candidate.get("steps")
        or []
    )
    length = float(len(chain_like) if isinstance(chain_like, Sequence) else 0)

    temporal_breaks = _to_nonnegative_float(candidate.get("temporal_breaks"), default=0.0)
    identity_mismatches = _to_nonnegative_float(candidate.get("identity_mismatches"), default=0.0)
    automaton_violations = _to_nonnegative_float(candidate.get("automaton_violations"), default=0.0)
    coverage = _clamp_01(candidate.get("coverage"), default=1.0)

    continuity = 1.0 / (1.0 + temporal_breaks)
    consistency = 1.0 / (1.0 + identity_mismatches + automaton_violations)
    penalty = temporal_breaks + identity_mismatches + automaton_violations

    return ChainFeatures(
        continuity_score=continuity,
        completeness_score=coverage,
        consistency_score=consistency,
        penalty_score=penalty,
        length_score=length,
    )


# ---------------------------------------------------------------------------
# Candidate scoring
# ---------------------------------------------------------------------------

def score_candidate(candidate: Candidate, config: Optional[BestChainConfig] = None) -> float:
    """Compute a provisional scalar score for candidate prioritization.

    The scoring policy is intentionally modular and replaceable. It ranks
    already-admissible candidates; it does not establish witness correctness.
    """
    # Candidate scoring
    cfg = config or DEFAULT_CONFIG
    features = extract_chain_features(candidate)

    positive = (
        cfg.weight_continuity * features.continuity_score
        + cfg.weight_completeness * features.completeness_score
        + cfg.weight_consistency * features.consistency_score
        + cfg.weight_length * features.length_score
    )
    penalties = cfg.weight_penalty * features.penalty_score
    return positive - penalties


# ---------------------------------------------------------------------------
# Candidate ranking
# ---------------------------------------------------------------------------

def compare_candidates(
    left: Candidate,
    right: Candidate,
    config: Optional[BestChainConfig] = None,
) -> int:
    """Comparator for descending score order with deterministic tie-breaks."""
    cfg = config or DEFAULT_CONFIG
    left_score = score_candidate(left, cfg)
    right_score = score_candidate(right, cfg)

    if left_score > right_score:
        return -1
    if left_score < right_score:
        return 1

    # Deterministic tie-break: prefer longer chain payload.
    left_len = extract_chain_features(left).length_score
    right_len = extract_chain_features(right).length_score
    if left_len > right_len:
        return -1
    if left_len < right_len:
        return 1
    return 0


def rank_candidates(
    candidates: Sequence[Candidate],
    config: Optional[BestChainConfig] = None,
) -> list[Candidate]:
    """Rank already-assembled candidates from best to worst.

    Empty input is valid and returns an empty list.
    """
    # Candidate ranking
    cfg = config or DEFAULT_CONFIG
    return sorted(candidates, key=cmp_to_key(lambda a, b: compare_candidates(a, b, cfg)))


# ---------------------------------------------------------------------------
# Candidate pruning
# ---------------------------------------------------------------------------

def prune_candidates(
    candidates: Sequence[Candidate],
    config: Optional[BestChainConfig] = None,
) -> list[Candidate]:
    """Optionally prune ranked candidates using explicit, configurable policy.

    This pruning stage is a convenience for post-processing scale management.
    It is not a validity predicate and must not be confused with admissibility.
    """
    # Candidate pruning
    cfg = config or DEFAULT_CONFIG
    ranked = rank_candidates(candidates, cfg)

    kept = ranked
    if cfg.min_score is not None:
        kept = [candidate for candidate in kept if score_candidate(candidate, cfg) >= cfg.min_score]

    if cfg.max_candidates_after_prune is not None:
        kept = kept[: cfg.max_candidates_after_prune]

    return kept


# ---------------------------------------------------------------------------
# Best-chain selection
# ---------------------------------------------------------------------------

def best_chain(
    candidates: Sequence[Candidate],
    config: Optional[BestChainConfig] = None,
) -> Optional[Candidate]:
    """Select one representative best candidate after optional pruning.

    Best-chain remains optional: callers may bypass this function and consume
    full Stitching outputs directly. Therefore, using or skipping this function
    does not alter core soundness/completeness of witness construction.
    """
    # Best-chain selection
    surviving = prune_candidates(candidates, config)
    return surviving[0] if surviving else None


def top_k_chains(
    candidates: Sequence[Candidate],
    k: int,
    config: Optional[BestChainConfig] = None,
) -> list[Candidate]:
    """Return up to the top-k candidates after optional pruning.

    This supports representative-set workflows when one witness is too narrow.
    """
    # Optional top-k selection
    if k <= 0:
        return []
    return prune_candidates(candidates, config)[:k]


# ---------------------------------------------------------------------------
# Complexity note (intended behavior, not formal guarantee)
#
# If there are N candidate chains and scoring one candidate costs σ,
# then full scoring costs O(N · σ).
# Full ranking costs O(N log N) after scoring.
# Top-k can later be optimized, but full sort is acceptable for now.
# Practical benefit: Best-chain reduces/prioritizes candidates after Stitching;
# it does not change the valid witness set constructed by core evaluation.
# ---------------------------------------------------------------------------

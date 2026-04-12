"""
Research-oriented scaffolding for the LocalEval phase.

This module intentionally keeps execution behavior unchanged while providing a
clearer internal structure aligned with the algorithmic formulation used in
this repository.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


PartitionGraph = Any
QuerySpec = Dict[str, Any]
Fragment = Dict[str, Any]
LocalEvalExecutor = Callable[[PartitionGraph, QuerySpec], List[Fragment]]


class LocalEvalNotImplementedError(NotImplementedError):
    """Raised when LocalEval execution logic has not yet been provided."""


def _default_executor(_: PartitionGraph, __: QuerySpec) -> List[Fragment]:
    """Default LocalEval behavior: preserve current unimplemented state."""
    raise LocalEvalNotImplementedError("Implement LocalEval here.")


@dataclass
class LocalEvalResult:
    """
    Container for partition-local evaluation outputs.

    complete_fragments:
        Fully matched local fragments (\\mathcal{F}_i in the formulation).
    boundary_fragments:
        Fragments that terminate on partition boundaries
        (\\mathcal{F}^\\partial_i in the formulation).
    """

    complete_fragments: List[Fragment] = field(default_factory=list)
    boundary_fragments: List[Fragment] = field(default_factory=list)

    def as_fragment_list(self) -> List[Fragment]:
        """
        Return a list representation compatible with current query engine wiring.

        Notes:
            The current query engine expects a flat list of local fragments.
            Until boundary-aware stitching is introduced, this method exposes only
            complete fragments to preserve behavior.
        """
        return self.complete_fragments


@dataclass
class LocalEval:
    """
    Research-oriented LocalEval façade.

    The implementation is decomposed into evaluation-only methods while keeping
    optional display/inspection concerns separate.
    """

    executor: LocalEvalExecutor = _default_executor

    def evaluate_partition(self, partition_graph: PartitionGraph, query: QuerySpec) -> LocalEvalResult:
        """
        Run local evaluation for a single partition.

        Args:
            partition_graph: Partition-local graph structure.
            query: Query specification.

        Returns:
            LocalEvalResult containing complete and boundary fragments.
        """
        complete = self.executor(partition_graph, query)
        return LocalEvalResult(complete_fragments=complete, boundary_fragments=[])

    def format_summary(self, result: LocalEvalResult) -> str:
        """Render a human-readable summary for diagnostics or notebooks."""
        return (
            "LocalEvalResult("
            f"complete={len(result.complete_fragments)}, "
            f"boundary={len(result.boundary_fragments)}"
            ")"
        )


# Module-level default evaluator keeps external API stable.
_DEFAULT_LOCAL_EVAL = LocalEval()


def local_eval(partition_graph: Any, query: Dict) -> List[Dict]:
    """
    Evaluate the query on a single partition and return local fragments.

    Args:
        partition_graph: Partition-local graph structure.
        query: Query specification.

    Returns:
        A list of local fragments / partial witnesses.
    """
    result = _DEFAULT_LOCAL_EVAL.evaluate_partition(partition_graph, query)
    return result.as_fragment_list()


def get_local_eval() -> LocalEval:
    """Expose the default LocalEval instance for advanced usage/testing."""
    return _DEFAULT_LOCAL_EVAL


def configure_local_eval_executor(executor: Optional[LocalEvalExecutor]) -> None:
    """
    Configure the LocalEval execution backend.

    Passing ``None`` resets LocalEval to its default not-implemented executor.
    """
    _DEFAULT_LOCAL_EVAL.executor = executor or _default_executor

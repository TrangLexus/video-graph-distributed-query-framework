"""
Top-level query engine orchestrating LocalEval, Stitching, and Best-chain.
"""

from typing import Any, Dict, List

from src.local_eval import local_eval
from src.stitching import stitch_fragments
from src.best_chain import best_chain


def run_query(partition_graphs: List[Any], query: Dict, constraints: Dict) -> Dict:
    """
    Execute a distributed query in three stages:
    1. LocalEval
    2. Stitching
    3. Best-chain

    Args:
        partition_graphs: List of partition-local graphs.
        query: Query specification.
        constraints: Stitching constraints.

    Returns:
        Final query result.
    """
    local_results = []
    for graph in partition_graphs:
        local_results.extend(local_eval(graph, query))

    stitched = stitch_fragments(local_results, constraints)
    best = best_chain(stitched)

    return {
        "local_results": local_results,
        "stitched_results": stitched,
        "best_result": best,
    }
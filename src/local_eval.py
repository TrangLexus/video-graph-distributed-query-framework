"""
LocalEval phase for partition-local query evaluation.
"""

from typing import Any, Dict, List


def local_eval(partition_graph: Any, query: Dict) -> List[Dict]:
    """
    Evaluate the query on a single partition and return local fragments.

    Args:
        partition_graph: Partition-local graph structure.
        query: Query specification.

    Returns:
        A list of local fragments / partial witnesses.
    """
    raise NotImplementedError("Implement LocalEval here.")
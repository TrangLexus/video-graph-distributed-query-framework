from __future__ import annotations

from query_specs.common import (
    QuerySpec,
    empty_candidates_like,
    empty_witnesses_like,
)

from query_specs.q11 import Q11_SPEC
from query_specs.q12 import Q12_SPEC
from query_specs.q13 import Q13_SPEC
from query_specs.q21 import Q21_SPEC
from query_specs.q22 import Q22_SPEC
from query_specs.q23 import Q23_SPEC
from query_specs.q31 import Q31_SPEC
from query_specs.q32 import Q32_SPEC
from query_specs.q33 import Q33_SPEC
from query_specs.q41 import Q41_SPEC
from query_specs.q42 import Q42_SPEC
from query_specs.q43 import Q43_SPEC


QUERY_REGISTRY: dict[str, QuerySpec] = {
    "Q1.1": Q11_SPEC,
    "Q1.2": Q12_SPEC,
    "Q1.3": Q13_SPEC,

    "Q2.1": Q21_SPEC,
    "Q2.2": Q22_SPEC,
    "Q2.3": Q23_SPEC,

    "Q3.1": Q31_SPEC,
    "Q3.2": Q32_SPEC,
    "Q3.3": Q33_SPEC,

    "Q4.1": Q41_SPEC,
    "Q4.2": Q42_SPEC,
    "Q4.3": Q43_SPEC,

}


__all__ = [
    "QuerySpec",
    "QUERY_REGISTRY",
    "empty_candidates_like",
    "empty_witnesses_like",
]

def validate_query_registry() -> None:
    expected = {f"Q{g}.{i}" for g in range(1, 5) for i in range(1, 4)}
    actual = set(QUERY_REGISTRY)
    if actual != expected:
        raise RuntimeError(f"Query registry Vr15 không đúng 12 query: thiếu={sorted(expected-actual)}, thừa={sorted(actual-expected)}")
    for key, spec in QUERY_REGISTRY.items():
        if key != spec.query_id:
            raise RuntimeError(f"Registry key {key} không khớp QuerySpec.query_id={spec.query_id}")

validate_query_registry()

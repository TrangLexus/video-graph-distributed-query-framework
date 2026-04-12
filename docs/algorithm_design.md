# Algorithm Design

## Overview

This repository isolates the algorithmic core of the distributed query framework:

1. LocalEval
2. Stitching
3. Best-chain

The goal is to refine the algorithm independently from the deployment environment.

## Modules

- `src/local_eval.py`: partition-local fragment generation
- `src/stitching.py`: cross-partition fragment assembly
- `src/best_chain.py`: candidate ranking / pruning
- `src/query_engine.py`: orchestration logic

## Notes

This repository is intentionally lightweight and does not include the full distributed runtime.
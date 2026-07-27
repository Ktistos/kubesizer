from dataclasses import dataclass
from typing import Any, Hashable


@dataclass(frozen=True)
class OptimizationResult:
    """Algorithm-independent optimization result.

    The meaning of ``cost`` is defined by the selected problem and algorithm.
    For A* replica sizing it is the path cost from the all-ones state. For
    exhaustive node placement it is the weighted placement objective.

    ``path`` is algorithm-specific. Exact enumeration returns only the chosen
    configuration because it does not construct a transition path.
    """

    configuration: Hashable
    cost: float
    metrics: Any
    explored_states: int
    path: tuple[Hashable, ...]

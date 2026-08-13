from dataclasses import dataclass
from typing import Any, Hashable


@dataclass(frozen=True)
class OptimizationResult:
    """Algorithm-independent optimization result.

    The meaning of ``cost`` is defined by the selected problem and algorithm.
    For replica sizing it is the path cost from the initial stable state. For
    node placement it is the weighted placement objective.

    ``path`` is algorithm-specific. Placement algorithms return only the chosen
    configuration because they do not construct a replica-transition path.
    """

    configuration: Hashable
    cost: float
    metrics: Any
    explored_states: int
    path: tuple[Hashable, ...]

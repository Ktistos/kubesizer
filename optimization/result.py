from dataclasses import dataclass
from typing import Any, Hashable


@dataclass(frozen=True)
class OptimizationResult:
    """Algorithm-independent optimization result.

    The meaning of ``cost`` is defined by the selected problem and algorithm.
    Its meaning is method-specific; use objective_value when a richer exact or
    heuristic result exposes it.

    ``path`` is algorithm-specific. Placement methods normally return only
    the chosen configuration because they do not construct a replica-sizing
    transition path.
    """

    configuration: Hashable
    cost: float
    metrics: Any
    explored_states: int
    path: tuple[Hashable, ...]

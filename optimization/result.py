from dataclasses import dataclass
from typing import Any, Hashable


@dataclass(frozen=True)
class OptimizationResult:
    """Result of an optimization search.

    ``cost`` is the accumulated path cost. For replica sizing it equals the
    number of replicas added to the all-ones initial configuration.
    """

    configuration: Hashable
    cost: float
    metrics: Any
    explored_states: int
    path: tuple[Hashable, ...]

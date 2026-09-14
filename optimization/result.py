from dataclasses import dataclass
from typing import Any, Hashable


@dataclass(frozen=True)
class OptimizationResult:
    """Algorithm-independent optimization result.

    The meaning of ``cost`` is defined by the selected problem and algorithm.
    Its meaning is method-specific; exact replica search also exposes the
    complete configuration objective as ``objective_value``.

    ``path`` is algorithm-specific and records the replica-sizing transitions
    when the selected optimizer exposes them.
    """

    configuration: Hashable
    cost: float
    metrics: Any
    explored_states: int
    path: tuple[Hashable, ...]

import math
from enum import Enum


class FeasibilityStatus(str, Enum):
    """Feasibility status of the configuration returned by exact search."""

    FEASIBLE = "FEASIBLE"
    INFEASIBLE = "INFEASIBLE"
    UNKNOWN = "UNKNOWN"


class TerminationReason(str, Enum):
    LOWER_BOUND_MATCHED = "LOWER_BOUND_MATCHED"
    STATISTICALLY_UNRESOLVED = "STATISTICALLY_UNRESOLVED"


def certified_bound_gaps(objective_value, lower_bound):
    """Return valid absolute and relative minimization gaps."""
    if lower_bound is None:
        return None, None
    objective_value = float(objective_value)
    lower_bound = float(lower_bound)
    if not math.isfinite(objective_value) or not math.isfinite(lower_bound):
        return None, None
    absolute_gap = max(0.0, objective_value - lower_bound)
    relative_gap = absolute_gap / lower_bound if lower_bound > 0 else None
    return absolute_gap, relative_gap

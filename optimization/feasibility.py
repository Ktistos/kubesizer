import math
import time
from dataclasses import dataclass
from enum import Enum


class FeasibilityStatus(str, Enum):
    """Three-state feasibility used by heuristic optimizers."""

    FEASIBLE = "FEASIBLE"
    INFEASIBLE = "INFEASIBLE"
    UNKNOWN = "UNKNOWN"


class TerminationReason(str, Enum):
    LOWER_BOUND_MATCHED = "LOWER_BOUND_MATCHED"
    LOCAL_SEARCH_COMPLETED = "LOCAL_SEARCH_COMPLETED"
    EVALUATION_BUDGET_EXHAUSTED = "EVALUATION_BUDGET_EXHAUSTED"
    TOTAL_CALL_BUDGET_EXHAUSTED = "TOTAL_CALL_BUDGET_EXHAUSTED"
    SIMULATION_REPLICATION_BUDGET_EXHAUSTED = "SIMULATION_REPLICATION_BUDGET_EXHAUSTED"
    TIME_BUDGET_EXHAUSTED = "TIME_BUDGET_EXHAUSTED"
    RESTART_LIMIT_REACHED = "RESTART_LIMIT_REACHED"
    NO_IMPROVEMENT_LIMIT_REACHED = "NO_IMPROVEMENT_LIMIT_REACHED"
    NO_FEASIBLE_SOLUTION_FOUND = "NO_FEASIBLE_SOLUTION_FOUND"
    STATISTICALLY_UNRESOLVED = "STATISTICALLY_UNRESOLVED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class HeuristicTraceEntry:
    phase: str
    restart: int
    configuration: object
    objective_value: float | None
    feasibility_status: FeasibilityStatus | None
    action: str


_FEASIBLE_NAMES = {
    "FEASIBLE",
    "STATISTICALLY_FEASIBLE",
    "RELIABLY_FEASIBLE",
    "VALIDATED_FEASIBLE",
}
_INFEASIBLE_NAMES = {
    "INFEASIBLE",
    "STATISTICALLY_INFEASIBLE",
    "RELIABLY_INFEASIBLE",
    "ANALYTICALLY_UNSTABLE",
    "STRUCTURALLY_INFEASIBLE",
}
_UNKNOWN_NAMES = {
    "UNKNOWN",
    "UNCERTAIN",
    "CONTINUE_SAMPLING",
    "INCONCLUSIVE",
    "SIMULATION_FAILURE",
    "CALIBRATION_FAILURE",
}


def _enum_name(value):
    if value is None:
        return None
    raw = getattr(value, "value", value)
    return str(raw).upper()


def evaluation_feasibility_status(evaluation):
    """Adapt deterministic or confidence-aware evaluations to three states.

    Statistical classifications take precedence over compatibility booleans so
    an inconclusive result can never be silently accepted as feasible.
    """
    if evaluation is None:
        return FeasibilityStatus.UNKNOWN

    for attribute in (
        "feasibility_status",
        "classification",
        "reliability_status",
    ):
        if not hasattr(evaluation, attribute):
            continue
        name = _enum_name(getattr(evaluation, attribute))
        if name in _FEASIBLE_NAMES:
            return FeasibilityStatus.FEASIBLE
        if name in _INFEASIBLE_NAMES:
            return FeasibilityStatus.INFEASIBLE
        if name in _UNKNOWN_NAMES:
            return FeasibilityStatus.UNKNOWN

    for attribute in ("feasible", "valid"):
        if hasattr(evaluation, attribute):
            return (
                FeasibilityStatus.FEASIBLE
                if bool(getattr(evaluation, attribute))
                else FeasibilityStatus.INFEASIBLE
            )
    raise TypeError(
        "Evaluation must expose a feasibility status, classification, "
        "reliability status, feasible flag, or valid flag."
    )


def evaluation_replication_count(evaluation):
    """Return simulation replications represented by an evaluation, if any."""
    candidates = (
        evaluation,
        getattr(evaluation, "configuration_evaluation", None),
    )
    values = []
    for candidate in candidates:
        if candidate is None:
            continue
        value = getattr(candidate, "replications_used", None)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            values.append(value)
    return max(values, default=0)


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


class EvaluationBudgetExhausted(RuntimeError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(str(reason.value))


class HeuristicEvaluationController:
    """Shared cache and hard budgets for expensive candidate evaluations."""

    def __init__(
        self,
        *,
        maximum_unique_evaluations,
        maximum_total_evaluator_calls=None,
        maximum_simulation_replications=None,
        time_budget_seconds,
        clock=None,
        cancellation_check=None,
    ):
        self.maximum_unique_evaluations = maximum_unique_evaluations
        self.maximum_total_evaluator_calls = (
            maximum_unique_evaluations
            if maximum_total_evaluator_calls is None
            else maximum_total_evaluator_calls
        )
        if maximum_simulation_replications is not None and (
            isinstance(maximum_simulation_replications, bool)
            or not isinstance(maximum_simulation_replications, int)
            or maximum_simulation_replications <= 0
        ):
            raise ValueError(
                "maximum_simulation_replications must be a positive integer."
            )
        self.maximum_simulation_replications = maximum_simulation_replications
        self.time_budget_seconds = float(time_budget_seconds)
        self.clock = clock or time.perf_counter
        self.cancellation_check = cancellation_check
        self.started = self.clock()
        self.cache = {}
        self.total_evaluator_calls = 0
        self.simulation_replications = 0
        self.exhaustion_reason = None

    @property
    def runtime_seconds(self):
        return max(0.0, self.clock() - self.started)

    @property
    def unique_evaluation_count(self):
        return len(self.cache)

    @property
    def budget_exhausted(self):
        return self.exhaustion_reason is not None

    def check_available(self, *, new_configuration=False):
        """Raise with a stable reason when no more work may start."""
        self._check_budget(new_configuration=new_configuration)

    def _check_budget(self, *, new_configuration):
        if self.cancellation_check is not None and self.cancellation_check():
            self.exhaustion_reason = TerminationReason.CANCELLED
        elif self.runtime_seconds >= self.time_budget_seconds:
            self.exhaustion_reason = TerminationReason.TIME_BUDGET_EXHAUSTED
        elif (
            self.maximum_simulation_replications is not None
            and self.simulation_replications >= self.maximum_simulation_replications
        ):
            self.exhaustion_reason = (
                TerminationReason.SIMULATION_REPLICATION_BUDGET_EXHAUSTED
            )
        elif self.total_evaluator_calls >= self.maximum_total_evaluator_calls:
            self.exhaustion_reason = TerminationReason.TOTAL_CALL_BUDGET_EXHAUSTED
        elif (
            new_configuration
            and self.unique_evaluation_count >= self.maximum_unique_evaluations
        ):
            self.exhaustion_reason = TerminationReason.EVALUATION_BUDGET_EXHAUSTED
        if self.exhaustion_reason is not None:
            raise EvaluationBudgetExhausted(self.exhaustion_reason)

    def evaluate(
        self,
        key,
        evaluator,
        *,
        refiner=None,
        refine_unknown=False,
    ):
        cached = self.cache.get(key)
        if cached is not None:
            if not (
                refine_unknown
                and refiner is not None
                and evaluation_feasibility_status(cached) == FeasibilityStatus.UNKNOWN
            ):
                return cached, False
            self._check_budget(new_configuration=False)
            previous_replications = evaluation_replication_count(cached)
            refined = refiner(cached)
            self.total_evaluator_calls += 1
            self.simulation_replications += max(
                0,
                evaluation_replication_count(refined) - previous_replications,
            )
            self.cache[key] = refined
            return refined, True

        self._check_budget(new_configuration=True)
        evaluation = evaluator()
        self.total_evaluator_calls += 1
        self.simulation_replications += evaluation_replication_count(evaluation)
        self.cache[key] = evaluation
        return evaluation, True

import math
import time
from dataclasses import dataclass

from .base import Optimizer
from .exceptions import NoFeasibleConfigurationError
from .result import OptimizationResult


@dataclass(frozen=True)
class ScalableReplicaOptimizationResult(OptimizationResult):
    """Budgeted feasible result without a global minimality claim."""

    minimality_proven: bool
    unresolved_lower_cost_states: tuple
    solution_mode: str
    budget_exhausted: bool
    configuration_evaluation_budget: int
    time_budget_seconds: float
    candidate_width: int
    locally_minimal: bool

    @property
    def optimality_proven(self):
        return False


class ScalableReplicaOptimizer(Optimizer):
    """Follow promising additions, then remove unnecessary replicas locally.

    Statistical feasibility is delegated entirely to the calibrated problem.
    The time budget is checked between configuration evaluations; one native
    evaluation already in progress is allowed to finish.
    """

    def __init__(
        self,
        *,
        maximum_configuration_evaluations=64,
        time_budget_seconds=300.0,
        candidate_width=8,
        clock=None,
    ):
        if (
            isinstance(maximum_configuration_evaluations, bool)
            or not isinstance(maximum_configuration_evaluations, int)
            or maximum_configuration_evaluations <= 0
        ):
            raise ValueError("maximum_configuration_evaluations must be positive.")
        if not math.isfinite(time_budget_seconds) or time_budget_seconds <= 0:
            raise ValueError("time_budget_seconds must be finite and positive.")
        if (
            isinstance(candidate_width, bool)
            or not isinstance(candidate_width, int)
            or candidate_width <= 0
        ):
            raise ValueError("candidate_width must be positive.")
        self.maximum_configuration_evaluations = maximum_configuration_evaluations
        self.time_budget_seconds = float(time_budget_seconds)
        self.candidate_width = candidate_width
        self._clock = clock or time.perf_counter

    @staticmethod
    def _interval_score(evaluation):
        configuration_evaluation = getattr(
            evaluation,
            "configuration_evaluation",
            None,
        )
        intervals = getattr(configuration_evaluation, "intervals", {}) or {}
        if intervals:
            return max(
                interval.upper_bound / interval.slo
                for interval in intervals.values()
            )
        tie_breaker = float(getattr(evaluation, "tie_breaker", math.inf))
        return tie_breaker if math.isfinite(tie_breaker) else math.inf

    def optimize(self, problem):
        started = self._clock()
        initial = tuple(problem.initial_state())
        evaluations = {}
        path = [initial]
        budget_exhausted = False

        def time_expired():
            return self._clock() - started >= self.time_budget_seconds

        def evaluate(state):
            nonlocal budget_exhausted
            state = tuple(state)
            if state in evaluations:
                return evaluations[state]
            if (
                len(evaluations) >= self.maximum_configuration_evaluations
                or time_expired()
            ):
                budget_exhausted = True
                return None
            if hasattr(problem, "evaluate_and_check"):
                evaluation, _ = problem.evaluate_and_check(state)
            else:
                evaluation = problem.evaluate(state)
            evaluations[state] = evaluation
            return evaluation

        current = initial
        current_evaluation = evaluate(current)
        if current_evaluation is None:
            raise NoFeasibleConfigurationError(
                "Scalable replica search exhausted its budget before evaluation."
            )

        while not bool(getattr(current_evaluation, "feasible", False)):
            neighbors = tuple(problem.neighbors(current))
            if not neighbors:
                raise NoFeasibleConfigurationError(
                    "No statistically feasible replica configuration was found "
                    "within the calibrated bounds."
                )
            ranked_neighbors = sorted(
                neighbors,
                key=lambda state: (problem.search_tie_breaker(state), state),
            )[: self.candidate_width]
            candidates = []
            for neighbor in ranked_neighbors:
                evaluation = evaluate(neighbor)
                if evaluation is None:
                    break
                candidates.append((neighbor, evaluation))
            if not candidates:
                raise NoFeasibleConfigurationError(
                    "Scalable replica search exhausted its evaluation or time "
                    "budget before finding a statistically feasible configuration."
                )
            current, current_evaluation = min(
                candidates,
                key=lambda item: (
                    0 if bool(getattr(item[1], "feasible", False)) else 1,
                    self._interval_score(item[1]),
                    problem.search_tie_breaker(item[0]),
                    item[0],
                ),
            )
            path.append(current)

        locally_minimal = False
        while True:
            removable = []
            for service_id, (replicas, lower_bound) in enumerate(
                zip(current, initial)
            ):
                if replicas <= lower_bound:
                    continue
                candidate = list(current)
                candidate[service_id] -= 1
                candidate = tuple(candidate)
                evaluation = evaluate(candidate)
                if evaluation is None:
                    break
                removable.append((candidate, evaluation))
            if budget_exhausted:
                break
            feasible_removals = tuple(
                item
                for item in removable
                if bool(getattr(item[1], "feasible", False))
            )
            if not feasible_removals:
                locally_minimal = True
                break
            current, current_evaluation = min(
                feasible_removals,
                key=lambda item: (
                    sum(item[0]),
                    self._interval_score(item[1]),
                    item[0],
                ),
            )
            path.append(current)

        unresolved = tuple(
            state
            for state, evaluation in evaluations.items()
            if sum(state) < sum(current)
            and not bool(getattr(evaluation, "feasible", False))
            and getattr(
                getattr(evaluation, "classification", None),
                "value",
                getattr(evaluation, "classification", None),
            ) in {"CONTINUE_SAMPLING", "INCONCLUSIVE"}
        )
        return ScalableReplicaOptimizationResult(
            configuration=current,
            cost=float(sum(current) - sum(initial)),
            metrics=current_evaluation,
            explored_states=len(evaluations),
            path=tuple(path),
            minimality_proven=False,
            unresolved_lower_cost_states=unresolved,
            solution_mode="SCALABLE",
            budget_exhausted=budget_exhausted,
            configuration_evaluation_budget=(
                self.maximum_configuration_evaluations
            ),
            time_budget_seconds=self.time_budget_seconds,
            candidate_width=self.candidate_width,
            locally_minimal=locally_minimal,
        )

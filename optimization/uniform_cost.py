import heapq
import itertools
import math
import time
from dataclasses import dataclass

from .base import GraphSearchProblem, Optimizer
from .exceptions import NoFeasibleConfigurationError
from .feasibility import (
    FeasibilityStatus,
    TerminationReason,
    certified_bound_gaps,
    evaluation_replication_count,
)
from .result import OptimizationResult


@dataclass(frozen=True)
class ReplicaOptimizationResult(OptimizationResult):
    """Uniform-cost result with statistical minimality evidence."""

    minimality_proven: bool
    unresolved_lower_cost_states: tuple
    method: str
    objective_value: float
    feasibility_status: FeasibilityStatus
    termination_reason: TerminationReason
    evaluation_count: int
    unique_evaluation_count: int
    simulation_replications: int
    runtime_seconds: float
    restart_count: int
    random_seed: int | None
    budget_exhausted: bool
    deletion_local_certified: bool
    relocation_local_certified: bool
    swap_local_certified: bool
    global_optimal: bool
    lower_bound: float | None
    absolute_bound_gap: float | None
    relative_bound_gap: float | None
    trace: tuple
    selection_reason: str | None

    @property
    def optimality_proven(self):
        return self.global_optimal


class UniformCostReplicaOptimizer(Optimizer):
    """Find the lowest-cost reliably feasible replica configuration.

    Actual path cost is the only cross-level priority. An optional
    ``search_tie_breaker(state)`` supplied by the problem changes evaluation
    order only among equal-cost states and therefore cannot invalidate
    minimum-cost optimality.
    """

    _UNRESOLVED_CLASSIFICATIONS = {"CONTINUE_SAMPLING", "INCONCLUSIVE"}

    def __init__(self, *, selection_reason=None, clock=None):
        self.selection_reason = selection_reason
        self._clock = clock or time.perf_counter

    def optimize(self, problem: GraphSearchProblem) -> ReplicaOptimizationResult:
        started = self._clock()
        initial = problem.initial_state()
        counter = itertools.count()
        frontier: list[tuple[float, float, int, object]] = []
        best_cost = {initial: 0.0}
        parents: dict[object, object | None] = {initial: None}
        heapq.heappush(
            frontier,
            (0.0, self._state_ordering(problem, initial), next(counter), initial),
        )
        explored_states = 0
        simulation_replications = 0
        best_goal = None
        unresolved_states = []

        while frontier:
            queued_cost, _, _, state = heapq.heappop(frontier)
            if queued_cost != best_cost.get(state):
                continue
            if best_goal is not None and queued_cost > best_goal[0]:
                break

            explored_states += 1
            if hasattr(problem, "evaluate_and_check"):
                evaluation, state_is_feasible = problem.evaluate_and_check(state)
            else:
                state_is_feasible = problem.is_feasible(state)
                evaluation = problem.evaluate(state) if state_is_feasible else None

            simulation_replications += evaluation_replication_count(evaluation)
            classification = getattr(evaluation, "classification", None)
            classification_value = getattr(classification, "value", classification)
            if classification_value in self._UNRESOLVED_CLASSIFICATIONS:
                unresolved_states.append((queued_cost, state))

            if state_is_feasible:
                evaluation_tie_breaker = float(getattr(evaluation, "tie_breaker", 0.0))
                if not math.isfinite(evaluation_tie_breaker):
                    raise ValueError(
                        "A feasible evaluation tie_breaker must be finite."
                    )
                candidate_rank = (queued_cost, evaluation_tie_breaker, state)
                if best_goal is None or candidate_rank < best_goal[:3]:
                    best_goal = (
                        queued_cost,
                        evaluation_tie_breaker,
                        state,
                        evaluation,
                        self._reconstruct_path(state, parents),
                    )
                continue

            for neighbor in problem.neighbors(state):
                transition_cost = float(problem.step_cost(state, neighbor))
                if not math.isfinite(transition_cost) or transition_cost < 0:
                    raise ValueError(
                        "Uniform-cost search requires finite non-negative step costs."
                    )
                tentative_cost = queued_cost + transition_cost
                if tentative_cost >= best_cost.get(neighbor, math.inf):
                    continue
                best_cost[neighbor] = tentative_cost
                parents[neighbor] = state
                heapq.heappush(
                    frontier,
                    (
                        tentative_cost,
                        self._state_ordering(problem, neighbor),
                        next(counter),
                        neighbor,
                    ),
                )

        if best_goal is None:
            raise NoFeasibleConfigurationError(
                "No reliably feasible configuration exists in the reachable "
                "replica space."
            )

        goal_cost, _, state, evaluation, path = best_goal
        unresolved_lower = tuple(
            unresolved_state
            for unresolved_cost, unresolved_state in unresolved_states
            if unresolved_cost < goal_cost
        )
        objective = (
            float(problem.configuration_cost(state))
            if hasattr(problem, "configuration_cost")
            else float(getattr(evaluation, "cost", goal_cost))
        )
        stable = (
            problem.stable_configuration()
            if hasattr(problem, "stable_configuration")
            else initial
        )
        lower_bound = (
            float(problem.configuration_cost(stable))
            if hasattr(problem, "configuration_cost")
            else objective - goal_cost
        )
        absolute_gap, relative_gap = certified_bound_gaps(
            objective, objective if not unresolved_lower else lower_bound
        )
        proven = not unresolved_lower
        return ReplicaOptimizationResult(
            configuration=state,
            cost=goal_cost,
            metrics=evaluation,
            explored_states=explored_states,
            path=path,
            minimality_proven=proven,
            unresolved_lower_cost_states=unresolved_lower,
            method="UNIFORM_COST_EXACT",
            objective_value=objective,
            feasibility_status=FeasibilityStatus.FEASIBLE,
            termination_reason=(
                TerminationReason.LOWER_BOUND_MATCHED
                if proven
                else TerminationReason.STATISTICALLY_UNRESOLVED
            ),
            evaluation_count=explored_states,
            unique_evaluation_count=explored_states,
            simulation_replications=simulation_replications,
            runtime_seconds=max(0.0, self._clock() - started),
            restart_count=1,
            random_seed=None,
            budget_exhausted=False,
            deletion_local_certified=proven,
            relocation_local_certified=False,
            swap_local_certified=False,
            global_optimal=proven,
            lower_bound=(objective if proven else lower_bound),
            absolute_bound_gap=absolute_gap,
            relative_bound_gap=relative_gap,
            trace=(),
            selection_reason=self.selection_reason,
        )

    @staticmethod
    def _state_ordering(problem, state):
        value = (
            problem.search_tie_breaker(state)
            if hasattr(problem, "search_tie_breaker")
            else 0.0
        )
        value = float(value)
        if not math.isfinite(value):
            raise ValueError("The same-cost search tie-breaker must be finite.")
        return value

    @staticmethod
    def _reconstruct_path(state, parents):
        path = []
        while state is not None:
            path.append(state)
            state = parents[state]
        return tuple(reversed(path))

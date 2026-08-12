import heapq
import itertools
import math
from dataclasses import dataclass

from .base import GraphSearchProblem, Optimizer
from .exceptions import NoFeasibleConfigurationError
from .result import OptimizationResult


@dataclass(frozen=True)
class ReplicaOptimizationResult(OptimizationResult):
    """Uniform-cost result with statistical minimality evidence."""

    minimality_proven: bool
    unresolved_lower_cost_states: tuple


class UniformCostReplicaOptimizer(Optimizer):
    """Find the lowest-cost reliably feasible replica configuration.

    Actual path cost is the only cross-level priority. An optional
    ``search_tie_breaker(state)`` supplied by the problem changes evaluation
    order only among equal-cost states and therefore cannot invalidate
    minimum-cost optimality.
    """

    _UNRESOLVED_CLASSIFICATIONS = {"CONTINUE_SAMPLING", "INCONCLUSIVE"}

    def optimize(self, problem: GraphSearchProblem) -> ReplicaOptimizationResult:
        initial = problem.initial_state()
        counter = itertools.count()
        frontier = []
        best_cost = {initial: 0.0}
        parents = {initial: None}
        heapq.heappush(
            frontier,
            (0.0, self._state_ordering(problem, initial), next(counter), initial),
        )
        explored_states = 0
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

            classification = getattr(evaluation, "classification", None)
            classification_value = getattr(classification, "value", classification)
            if classification_value in self._UNRESOLVED_CLASSIFICATIONS:
                unresolved_states.append((queued_cost, state))

            if state_is_feasible:
                evaluation_tie_breaker = float(
                    getattr(evaluation, "tie_breaker", 0.0)
                )
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
        return ReplicaOptimizationResult(
            configuration=state,
            cost=goal_cost,
            metrics=evaluation,
            explored_states=explored_states,
            path=path,
            minimality_proven=not unresolved_lower,
            unresolved_lower_cost_states=unresolved_lower,
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

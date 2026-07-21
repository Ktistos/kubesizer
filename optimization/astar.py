import heapq
import itertools

from .base import GraphSearchProblem, Optimizer
from .exceptions import NoFeasibleConfigurationError
from .result import OptimizationResult


class AStarOptimizer(Optimizer):
    """Heap-based A* search for non-negative step costs.

    Equal-cost goals may expose a numeric ``tie_breaker`` on their evaluation.
    Optimality requires a finite reachable search space, non-negative step
    costs, and a non-negative admissible heuristic.
    """

    def optimize(self, problem: GraphSearchProblem) -> OptimizationResult:
        initial = problem.initial_state()
        counter = itertools.count()
        frontier = []
        best_g = {initial: 0.0}
        parents = {initial: None}

        initial_heuristic = problem.heuristic(initial)
        if initial_heuristic < 0:
            raise ValueError("A* requires a non-negative heuristic.")
        heapq.heappush(
            frontier,
            (initial_heuristic, next(counter), 0.0, initial),
        )
        explored_states = 0
        best_goal = None

        while frontier:
            priority, _, queued_g, state = heapq.heappop(frontier)
            if queued_g != best_g.get(state):
                continue
            if best_goal is not None and priority > best_goal[0]:
                break

            explored_states += 1
            if problem.is_feasible(state):
                evaluation = problem.evaluate(state)
                tie_breaker = getattr(evaluation, "tie_breaker", 0.0)
                candidate_rank = (queued_g, tie_breaker)
                if best_goal is None or candidate_rank < best_goal[:2]:
                    best_goal = (
                        queued_g,
                        tie_breaker,
                        state,
                        evaluation,
                        self.__reconstruct_path(state, parents),
                    )
                continue

            for neighbor in problem.neighbors(state):
                transition_cost = problem.step_cost(state, neighbor)
                if transition_cost < 0:
                    raise ValueError("A* requires non-negative step costs.")

                tentative_g = queued_g + transition_cost
                if tentative_g >= best_g.get(neighbor, float("inf")):
                    continue

                neighbor_heuristic = problem.heuristic(neighbor)
                if neighbor_heuristic < 0:
                    raise ValueError("A* requires a non-negative heuristic.")
                best_g[neighbor] = tentative_g
                parents[neighbor] = state
                heapq.heappush(
                    frontier,
                    (
                        tentative_g + neighbor_heuristic,
                        next(counter),
                        tentative_g,
                        neighbor,
                    ),
                )

        if best_goal is not None:
            queued_g, _, state, evaluation, path = best_goal
            return OptimizationResult(
                configuration=state,
                cost=queued_g,
                metrics=evaluation,
                explored_states=explored_states,
                path=path,
            )

        raise NoFeasibleConfigurationError(
            "No feasible configuration exists in the reachable search space."
        )

    @staticmethod
    def __reconstruct_path(state, parents):
        path = []
        while state is not None:
            path.append(state)
            state = parents[state]
        return tuple(reversed(path))

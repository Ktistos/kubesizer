from collections import Counter

import pytest

from optimization import (
    AStarOptimizer,
    GraphSearchProblem,
    NoFeasibleConfigurationError,
)


class WeightedGraphProblem(GraphSearchProblem):
    def __init__(self):
        self.graph = {
            "start": (("a", 5), ("b", 1)),
            "b": (("a", 1),),
            "a": (("goal", 10),),
            "goal": (),
        }
        self.feasibility_checks = Counter()

    def initial_state(self):
        return "start"

    def neighbors(self, state):
        return tuple(neighbor for neighbor, _ in self.graph[state])

    def step_cost(self, current, neighbor):
        return dict(self.graph[current])[neighbor]

    def heuristic(self, state):
        return 0

    def evaluate(self, configuration):
        return {"state": configuration}

    def is_feasible(self, configuration):
        self.feasibility_checks[configuration] += 1
        return configuration == "goal"


class FiniteInfeasibleProblem(GraphSearchProblem):
    def initial_state(self):
        return 0

    def neighbors(self, state):
        return (state + 1,) if state < 2 else ()

    def step_cost(self, current, neighbor):
        return 1

    def heuristic(self, state):
        return 0

    def evaluate(self, configuration):
        return configuration

    def is_feasible(self, configuration):
        return False


def test_astar_ignores_obsolete_entries_and_reconstructs_best_path():
    problem = WeightedGraphProblem()

    result = AStarOptimizer().optimize(problem)

    assert result.configuration == "goal"
    assert result.cost == 12
    assert result.path == ("start", "b", "a", "goal")
    assert result.explored_states == 4
    assert problem.feasibility_checks["a"] == 1


def test_astar_raises_when_finite_search_space_has_no_feasible_state():
    with pytest.raises(NoFeasibleConfigurationError):
        AStarOptimizer().optimize(FiniteInfeasibleProblem())

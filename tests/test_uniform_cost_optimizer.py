from types import SimpleNamespace

import pytest

from metrics import Metrics
from optimization import (
    GraphSearchProblem,
    ReplicaSizingProblem,
    UniformCostReplicaOptimizer,
)


class SearchProblem(GraphSearchProblem):
    def __init__(self, graph, evaluations, *, ordering=None):
        self.graph = graph
        self.evaluations = evaluations
        self.ordering = ordering or {}
        self.evaluation_order = []

    def initial_state(self):
        return "start"

    def neighbors(self, state):
        return tuple(neighbor for neighbor, _ in self.graph[state])

    def step_cost(self, current, neighbor):
        return dict(self.graph[current])[neighbor]

    def heuristic(self, state):
        raise AssertionError("Uniform-cost search must not request a heuristic.")

    def search_tie_breaker(self, state):
        return self.ordering.get(state, 0.0)

    def evaluate_and_check(self, state):
        self.evaluation_order.append(state)
        evaluation = self.evaluations[state]
        return evaluation, evaluation.feasible

    def evaluate(self, state):
        return self.evaluations[state]

    def is_feasible(self, state):
        return self.evaluations[state].feasible


def evaluation(*, feasible=False, tie_breaker=0.0, classification=None):
    return SimpleNamespace(
        feasible=feasible,
        tie_breaker=tie_breaker,
        classification=classification,
    )


def test_uniform_cost_finds_lowest_actual_cost_without_using_a_heuristic():
    problem = SearchProblem(
        {
            "start": (("a", 5), ("b", 1)),
            "b": (("a", 1),),
            "a": (("goal", 10),),
            "goal": (),
        },
        {
            "start": evaluation(),
            "a": evaluation(),
            "b": evaluation(),
            "goal": evaluation(feasible=True),
        },
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.configuration == "goal"
    assert result.cost == 12
    assert result.path == ("start", "b", "a", "goal")
    assert result.minimality_proven


def test_uniform_cost_finishes_equal_cost_level_for_goal_tie_breaking():
    problem = SearchProblem(
        {
            "start": (("high-headroom", 1), ("low-headroom", 1)),
            "high-headroom": (),
            "low-headroom": (),
        },
        {
            "start": evaluation(),
            "high-headroom": evaluation(feasible=True, tie_breaker=0.2),
            "low-headroom": evaluation(feasible=True, tie_breaker=0.5),
        },
        ordering={"high-headroom": 0.9, "low-headroom": 0.1},
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert problem.evaluation_order == [
        "start",
        "low-headroom",
        "high-headroom",
    ]
    assert result.configuration == "high-headroom"
    assert result.cost == 1


def test_lower_cost_inconclusive_state_prevents_minimality_claim():
    problem = SearchProblem(
        {
            "start": (("uncertain", 1), ("bridge", 1)),
            "uncertain": (),
            "bridge": (("goal", 1),),
            "goal": (),
        },
        {
            "start": evaluation(classification="INFEASIBLE"),
            "uncertain": evaluation(classification="INCONCLUSIVE"),
            "bridge": evaluation(classification="INFEASIBLE"),
            "goal": evaluation(feasible=True, classification="FEASIBLE"),
        },
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.configuration == "goal"
    assert not result.minimality_proven
    assert result.unresolved_lower_cost_states == ("uncertain",)


class AnalyticalSystem:
    def __init__(self):
        self.configuration = (2, 1)

    def get_services(self):
        return {"busy": object(), "light": object()}

    def get_minimum_stable_replicas(self):
        return (2, 1)

    def configure_system_replicas(self, configuration):
        self.configuration = tuple(configuration)

    def solve(self):
        return Metrics(
            (
                1.6 / self.configuration[0],
                0.2 / self.configuration[1],
            ),
            (0.1, 0.1),
            True,
        )


def test_replica_problem_starts_at_stable_anchor_and_counts_reachable_space():
    problem = ReplicaSizingProblem(
        AnalyticalSystem(),
        simulation_duration=1.0,
        request_class_slos={},
        max_replicas=(4, 3),
    )

    assert problem.initial_state() == (2, 1)
    assert problem.configuration_count() == 9
    assert problem.search_tie_breaker((3, 1)) == pytest.approx(1.6 / 3)
    assert problem.search_tie_breaker((2, 2)) == pytest.approx(0.8)

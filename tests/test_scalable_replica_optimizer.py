from types import SimpleNamespace

import pytest

from optimization import (
    NoFeasibleConfigurationError,
    ScalableReplicaOptimizer,
)


def _evaluation(feasible, upper_bound, classification):
    interval = SimpleNamespace(upper_bound=upper_bound, slo=1.0)
    return SimpleNamespace(
        feasible=feasible,
        tie_breaker=upper_bound,
        classification=SimpleNamespace(value=classification),
        configuration_evaluation=SimpleNamespace(
            intervals={"checkout": interval},
        ),
    )


class ReplicaProblem:
    def __init__(self, evaluations, ordering=None, maximum=(3, 3)):
        self.evaluations = evaluations
        self.ordering = ordering or {}
        self.maximum = maximum
        self.evaluation_order = []

    def initial_state(self):
        return (1, 1)

    def neighbors(self, state):
        result = []
        for index, value in enumerate(state):
            if value >= self.maximum[index]:
                continue
            candidate = list(state)
            candidate[index] += 1
            result.append(tuple(candidate))
        return tuple(result)

    def search_tie_breaker(self, state):
        return self.ordering.get(state, 1.0)

    def evaluate_and_check(self, state):
        self.evaluation_order.append(state)
        evaluation = self.evaluations[state]
        return evaluation, evaluation.feasible


def test_scalable_replica_search_finds_feasible_then_removes_redundant_replica():
    problem = ReplicaProblem(
        {
            (1, 1): _evaluation(False, 1.8, "STATISTICALLY_INFEASIBLE"),
            (2, 1): _evaluation(False, 1.2, "STATISTICALLY_INFEASIBLE"),
            (2, 2): _evaluation(True, 0.8, "STATISTICALLY_FEASIBLE"),
            (1, 2): _evaluation(True, 0.9, "STATISTICALLY_FEASIBLE"),
        },
        ordering={(2, 1): 0.2, (1, 2): 0.8, (2, 2): 0.1},
    )

    result = ScalableReplicaOptimizer(
        maximum_configuration_evaluations=10,
        candidate_width=1,
        time_budget_seconds=10,
    ).optimize(problem)

    assert result.configuration == (1, 2)
    assert result.metrics.feasible
    assert result.solution_mode == "SCALABLE"
    assert result.locally_minimal
    assert not result.minimality_proven
    assert not result.optimality_proven
    assert not result.budget_exhausted
    assert result.path == ((1, 1), (2, 1), (2, 2), (1, 2))


def test_scalable_replica_search_never_prefers_inconclusive_over_feasible():
    problem = ReplicaProblem(
        {
            (1, 1): _evaluation(False, 1.5, "STATISTICALLY_INFEASIBLE"),
            (2, 1): _evaluation(False, 0.4, "INCONCLUSIVE"),
            (1, 2): _evaluation(True, 0.9, "STATISTICALLY_FEASIBLE"),
        },
        ordering={(2, 1): 0.1, (1, 2): 0.2},
        maximum=(2, 2),
    )

    result = ScalableReplicaOptimizer(
        maximum_configuration_evaluations=4,
        candidate_width=2,
        time_budget_seconds=10,
    ).optimize(problem)

    assert result.configuration == (1, 2)
    assert result.metrics.classification.value == "STATISTICALLY_FEASIBLE"


def test_scalable_replica_search_fails_clearly_when_budget_has_no_feasible_result():
    problem = ReplicaProblem(
        {(1, 1): _evaluation(False, 2.0, "STATISTICALLY_INFEASIBLE")},
    )

    with pytest.raises(NoFeasibleConfigurationError, match="budget"):
        ScalableReplicaOptimizer(
            maximum_configuration_evaluations=1,
            candidate_width=1,
            time_budget_seconds=10,
        ).optimize(problem)

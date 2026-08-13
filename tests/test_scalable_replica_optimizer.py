from types import SimpleNamespace

import pytest

from optimization import (
    NoFeasibleConfigurationError,
    OptimizationMode,
    ScalableReplicaOptimizer,
    optimize_replica_configuration,
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


class FunctionalReplicaProblem:
    def __init__(
        self,
        *,
        stable,
        maximum,
        status,
        rank=None,
        costs=None,
    ):
        self._stable = tuple(stable)
        self._maximum = tuple(maximum)
        self._status = status
        self._rank = rank or (lambda _current, _candidate: 0.0)
        self._costs = tuple(costs or (1.0,) * len(stable))
        self.evaluation_calls = {}

    def initial_state(self):
        return self._stable

    def stable_configuration(self):
        return self._stable

    def maximum_configuration(self):
        return self._maximum

    def configuration_count(self):
        result = 1
        for low, high in zip(self._stable, self._maximum):
            result *= high - low + 1
        return result

    def replica_costs(self):
        return self._costs

    def configuration_cost(self, configuration):
        return sum(count * cost for count, cost in zip(configuration, self._costs))

    def candidate_is_structurally_feasible(self, configuration):
        return all(
            low <= value <= high
            for low, value, high in zip(self._stable, configuration, self._maximum)
        )

    def neighbors(self, state):
        values = []
        for index, count in enumerate(state):
            if count >= self._maximum[index]:
                continue
            candidate = list(state)
            candidate[index] += 1
            values.append(tuple(candidate))
        return tuple(values)

    def step_cost(self, current, neighbor):
        index = next(
            index
            for index, (left, right) in enumerate(zip(current, neighbor))
            if right != left
        )
        return self._costs[index]

    def rank_replica_addition(self, current, candidate):
        return self._rank(current, candidate)

    def search_tie_breaker(self, state):
        return 0.0

    def evaluate_and_check(self, state):
        state = tuple(state)
        self.evaluation_calls[state] = self.evaluation_calls.get(state, 0) + 1
        status = self._status(state)
        feasible = status == "STATISTICALLY_FEASIBLE"
        evaluation = _evaluation(
            feasible,
            0.5 if feasible else 1.5,
            status,
        )
        evaluation.cost = self.configuration_cost(state) if feasible else float("inf")
        return evaluation, feasible

    def evaluate(self, state):
        return self.evaluate_and_check(state)[0]

    def is_feasible(self, state):
        return self.evaluate_and_check(state)[1]


def _counterexample_problem():
    def status(configuration):
        return (
            "STATISTICALLY_FEASIBLE"
            if configuration[0] >= 3 or configuration[1] >= 2
            else "STATISTICALLY_INFEASIBLE"
        )

    def rank(current, candidate):
        changed = next(
            index
            for index, (left, right) in enumerate(zip(current, candidate))
            if left != right
        )
        if current[0] < 3:
            return 10.0 if changed == 0 else 1.0
        return 10.0 if changed == 1 else 1.0

    return FunctionalReplicaProblem(
        stable=(1, 1, 1),
        maximum=(3, 2, 1),
        status=status,
        rank=rank,
    )


def test_replica_vns_escapes_five_replica_deletion_basin_to_four():
    budget_stopped = ScalableReplicaOptimizer(
        maximum_configuration_evaluations=3,
        candidate_width=1,
        maximum_restarts=1,
        maximum_vns_neighborhood=1,
        random_seed=7,
    ).optimize(_counterexample_problem())

    improved_problem = _counterexample_problem()
    improved = ScalableReplicaOptimizer(
        maximum_configuration_evaluations=20,
        candidate_width=1,
        maximum_restarts=1,
        maximum_vns_neighborhood=1,
        random_seed=7,
        collect_trace=True,
    ).optimize(improved_problem)
    exact = (
        __import__("optimization")
        .UniformCostReplicaOptimizer()
        .optimize(_counterexample_problem())
    )

    assert budget_stopped.configuration == (3, 1, 1)
    assert budget_stopped.objective_value == 5
    assert budget_stopped.budget_exhausted
    assert not budget_stopped.deletion_local_certified
    assert improved.configuration == (1, 2, 1)
    assert improved.objective_value == 4
    assert improved.deletion_local_certified
    assert exact.configuration == improved.configuration
    assert exact.objective_value == improved.objective_value
    assert not improved.global_optimal
    assert improved.absolute_bound_gap == 1
    assert any(entry.action == "accepted-improvement" for entry in improved.trace)


def test_unknown_removal_prevents_deletion_local_certificate():
    problem = FunctionalReplicaProblem(
        stable=(1, 1),
        maximum=(2, 1),
        status=lambda state: (
            "STATISTICALLY_FEASIBLE" if state == (2, 1) else "INCONCLUSIVE"
        ),
    )

    result = ScalableReplicaOptimizer(
        maximum_configuration_evaluations=4,
        maximum_restarts=1,
        maximum_vns_neighborhood=1,
        random_seed=3,
    ).optimize(problem)

    assert result.feasibility_status.value == "FEASIBLE"
    assert not result.deletion_local_certified
    assert result.unknown_evaluation_count == 1
    assert result.unresolved_lower_cost_states == ((1, 1),)


def test_stable_vector_weighted_cost_cache_and_seed_are_reproducible():
    def make():
        return FunctionalReplicaProblem(
            stable=(2, 1),
            maximum=(3, 2),
            costs=(5.0, 1.0),
            status=lambda state: (
                "STATISTICALLY_FEASIBLE"
                if state[1] >= 2
                else "STATISTICALLY_INFEASIBLE"
            ),
            rank=lambda current, candidate: (
                -(
                    sum(value * cost for value, cost in zip(candidate, (5.0, 1.0)))
                    - sum(value * cost for value, cost in zip(current, (5.0, 1.0)))
                )
            ),
        )

    left_problem = make()
    right_problem = make()
    options = dict(
        maximum_configuration_evaluations=20,
        maximum_restarts=3,
        maximum_no_improvement=3,
        maximum_vns_neighborhood=1,
        candidate_width=2,
        random_seed=19,
        collect_trace=True,
    )
    left = ScalableReplicaOptimizer(**options).optimize(left_problem)
    right = ScalableReplicaOptimizer(**options).optimize(right_problem)

    assert left.configuration == right.configuration == (2, 2)
    assert left.objective_value == right.objective_value == 12
    assert left.lower_bound == 11
    assert left.relative_bound_gap == pytest.approx(1 / 11)
    assert all(
        value >= minimum
        for state in left_problem.evaluation_calls
        for value, minimum in zip(state, (2, 1))
    )
    assert max(left_problem.evaluation_calls.values()) == 1
    assert left.trace == right.trace


class RestartReplicaProblem:
    def __init__(self):
        self.calls = {}

    def initial_state(self):
        return (1, 1)

    def stable_configuration(self):
        return (1, 1)

    def replica_costs(self):
        return (5.0, 1.0)

    def configuration_cost(self, state):
        return 5.0 * state[0] + state[1]

    def neighbors(self, state):
        return tuple(
            candidate
            for candidate in (
                (2, 1) if state == (1, 1) else None,
                (1, 2) if state == (1, 1) else None,
            )
            if candidate is not None
        )

    def rank_replica_addition(self, _current, _candidate):
        return 0.0

    def evaluate_and_check(self, state):
        self.calls[state] = self.calls.get(state, 0) + 1
        feasible = state != (1, 1)
        return (
            _evaluation(
                feasible,
                0.5 if feasible else 1.5,
                "STATISTICALLY_FEASIBLE" if feasible else "STATISTICALLY_INFEASIBLE",
            ),
            feasible,
        )


def test_multiple_starts_find_a_cheaper_basin_and_never_worsen_incumbent():
    one = ScalableReplicaOptimizer(
        maximum_configuration_evaluations=10,
        candidate_width=2,
        maximum_restarts=1,
        random_seed=0,
    ).optimize(RestartReplicaProblem())
    many = ScalableReplicaOptimizer(
        maximum_configuration_evaluations=10,
        candidate_width=2,
        maximum_restarts=3,
        maximum_no_improvement=3,
        random_seed=0,
        collect_trace=True,
    ).optimize(RestartReplicaProblem())

    incumbent_costs = [
        entry.objective_value for entry in many.trace if entry.action == "new-incumbent"
    ]
    assert one.configuration == (2, 1)
    assert many.configuration == (1, 2)
    assert many.objective_value < one.objective_value
    assert incumbent_costs == sorted(incumbent_costs, reverse=True)


def test_time_budget_failure_has_an_explicit_reason():
    class StepClock:
        def __init__(self):
            self.value = 0.0

        def __call__(self):
            self.value += 1.0
            return self.value

    with pytest.raises(NoFeasibleConfigurationError) as captured:
        ScalableReplicaOptimizer(
            maximum_configuration_evaluations=10,
            time_budget_seconds=0.5,
            clock=StepClock(),
        ).optimize(_counterexample_problem())

    assert captured.value.termination_reason.value == "TIME_BUDGET_EXHAUSTED"


def test_auto_replica_selection_returns_method_reason_and_exact_certificate():
    problem = FunctionalReplicaProblem(
        stable=(1, 1),
        maximum=(2, 2),
        status=lambda state: (
            "STATISTICALLY_FEASIBLE" if state == (1, 2) else "STATISTICALLY_INFEASIBLE"
        ),
    )

    result = optimize_replica_configuration(
        problem,
        mode=OptimizationMode.AUTO,
        exact_configuration_limit=10,
    )

    assert result.method == "UNIFORM_COST_EXACT"
    assert result.global_optimal
    assert result.lower_bound == result.objective_value
    assert result.absolute_bound_gap == 0
    assert "AUTO selected EXACT" in result.selection_reason

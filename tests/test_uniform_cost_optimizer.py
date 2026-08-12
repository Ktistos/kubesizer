from types import SimpleNamespace

import pytest

import statistical_workflow
from metrics import Metrics
from optimization import (
    CalibratedReplicaSizingProblem,
    GraphSearchProblem,
    OptimizationMode,
    ScalableReplicaSearchOptions,
    SimulationClassification,
    UniformCostReplicaOptimizer,
    zero_heuristic,
)


class SearchProblem(GraphSearchProblem):
    def __init__(self, graph, evaluations, *, state_ordering=None):
        self.graph = graph
        self.evaluations = evaluations
        self.state_ordering = state_ordering or {}
        self.evaluation_order = []

    def initial_state(self):
        return "start"

    def neighbors(self, state):
        return tuple(neighbor for neighbor, _ in self.graph[state])

    def step_cost(self, current, neighbor):
        return dict(self.graph[current])[neighbor]

    def heuristic(self, state):
        raise AssertionError("Uniform-cost search must not request an A* heuristic.")

    def search_tie_breaker(self, state):
        return self.state_ordering.get(state, 0.0)

    def evaluate(self, configuration):
        self.evaluation_order.append(configuration)
        return self.evaluations[configuration]

    def evaluate_and_check(self, configuration):
        evaluation = self.evaluate(configuration)
        return evaluation, evaluation.feasible

    def is_feasible(self, configuration):
        return self.evaluations[configuration].feasible


def evaluation(*, feasible=False, tie_breaker=0.0, classification=None):
    return SimpleNamespace(
        feasible=feasible,
        tie_breaker=tie_breaker,
        classification=classification,
    )


def test_uniform_cost_finds_the_lowest_actual_cost_path_without_a_heuristic():
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
    assert result.unresolved_lower_cost_states == ()


def test_same_cost_ordering_changes_speed_not_minimum_cost_or_goal_ranking():
    problem = SearchProblem(
        {
            "start": (("high-congestion", 1), ("low-congestion", 1)),
            "high-congestion": (),
            "low-congestion": (),
        },
        {
            "start": evaluation(),
            "high-congestion": evaluation(feasible=True, tie_breaker=0.2),
            "low-congestion": evaluation(feasible=True, tie_breaker=0.5),
        },
        state_ordering={"high-congestion": 0.9, "low-congestion": 0.1},
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert problem.evaluation_order == [
        "start",
        "low-congestion",
        "high-congestion",
    ]
    assert result.configuration == "high-congestion"
    assert result.cost == 1
    assert result.metrics.tie_breaker == 0.2


def test_lower_cost_inconclusive_state_prevents_a_proven_minimum_claim():
    problem = SearchProblem(
        {
            "start": (("uncertain", 1), ("bridge", 1)),
            "uncertain": (),
            "bridge": (("goal", 1),),
            "goal": (),
        },
        {
            "start": evaluation(
                classification=SimulationClassification.STATISTICALLY_INFEASIBLE
            ),
            "uncertain": evaluation(
                classification=SimulationClassification.INCONCLUSIVE
            ),
            "bridge": evaluation(
                classification=SimulationClassification.STATISTICALLY_INFEASIBLE
            ),
            "goal": evaluation(
                feasible=True,
                classification=SimulationClassification.STATISTICALLY_FEASIBLE,
            ),
        },
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.configuration == "goal"
    assert result.cost == 2
    assert not result.minimality_proven
    assert result.unresolved_lower_cost_states == ("uncertain",)


def test_definitively_infeasible_lower_states_preserve_minimality_proof():
    problem = SearchProblem(
        {
            "start": (("infeasible", 1),),
            "infeasible": (("goal", 1),),
            "goal": (),
        },
        {
            "start": evaluation(
                classification=SimulationClassification.STATISTICALLY_INFEASIBLE
            ),
            "infeasible": evaluation(
                classification=SimulationClassification.STATISTICALLY_INFEASIBLE
            ),
            "goal": evaluation(
                feasible=True,
                classification=SimulationClassification.STATISTICALLY_FEASIBLE,
            ),
        },
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.minimality_proven
    assert result.unresolved_lower_cost_states == ()


class FakeService:
    def __init__(self, rate):
        self.rate = rate

    def get_mu(self):
        return self.rate


class OrderingSystem:
    def __init__(self):
        self.configuration = (1, 1)
        self.services = {
            "busy": FakeService(10.0),
            "light": FakeService(10.0),
        }

    def get_services(self):
        return self.services

    def configure_system_replicas(self, configuration):
        self.configuration = tuple(configuration)

    def solve(self):
        utilization = (
            8.0 / (10.0 * self.configuration[0]),
            2.0 / (10.0 * self.configuration[1]),
        )
        return Metrics(utilization, (0.1, 0.1), True)


def test_calibrated_problem_orders_equal_cost_states_by_bottleneck_utilization():
    problem = CalibratedReplicaSizingProblem(
        OrderingSystem(),
        SimpleNamespace(
            minimum_replica_configuration=(1, 1),
            maximum_replica_configuration=(3, 3),
        ),
        heuristic=zero_heuristic,
        evaluator=object(),
    )

    assert problem.search_tie_breaker((2, 1)) == pytest.approx(0.4)
    assert problem.search_tie_breaker((1, 2)) == pytest.approx(0.8)


def test_public_calibrated_workflow_uses_uniform_cost_by_default(monkeypatch):
    raw_result = SimpleNamespace(
        configuration=(2,),
        cost=1.0,
        metrics=object(),
        explored_states=2,
        path=((1,), (2,)),
        minimality_proven=False,
        unresolved_lower_cost_states=((1,),),
    )
    calls = []

    class FakeProblem:
        def __init__(self, *_args, **_kwargs):
            self.configuration_evaluations = {(1,): "inconclusive", (2,): "feasible"}

    class FakeUniformCost:
        def optimize(self, problem):
            calls.append(problem)
            return raw_result

    monkeypatch.setattr(
        statistical_workflow,
        "validate_profile_for_experiment",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "CalibratedReplicaSizingProblem",
        FakeProblem,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "UniformCostReplicaOptimizer",
        FakeUniformCost,
    )
    profile = SimpleNamespace(
        slos={"checkout": 1.0},
        minimum_replica_configuration=(1,),
        maximum_replica_configuration=(2,),
        profile_fingerprint="profile",
    )

    result = statistical_workflow.optimize_with_calibrated_simulation(
        object(),
        profile,
    )

    assert len(calls) == 1
    assert result.configuration == (2,)
    assert not result.minimality_proven
    assert result.unresolved_lower_cost_states == ((1,),)


def test_public_calibrated_workflow_uses_runtime_adaptive_scalable_mode(monkeypatch):
    raw_result = SimpleNamespace(
        configuration=(2,),
        cost=1.0,
        metrics=object(),
        explored_states=2,
        path=((1,), (2,)),
        minimality_proven=False,
        unresolved_lower_cost_states=(),
        solution_mode="SCALABLE",
        budget_exhausted=False,
        locally_minimal=True,
        optimality_proven=False,
    )
    captured = {}

    class FakeProblem:
        def __init__(self, *_args, **_kwargs):
            self.configuration_evaluations = {(1,): "infeasible", (2,): "feasible"}

    class FakeScalable:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def optimize(self, _problem):
            return raw_result

    monkeypatch.setattr(
        statistical_workflow,
        "validate_profile_for_experiment",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "CalibratedReplicaSizingProblem",
        FakeProblem,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "ScalableReplicaOptimizer",
        FakeScalable,
    )
    profile = SimpleNamespace(
        slos={"checkout": 1.0},
        minimum_replica_configuration=(1,),
        maximum_replica_configuration=(10,),
        profile_fingerprint="profile",
    )

    runtime_limit_calibration = SimpleNamespace(
        calibration_profile_fingerprint="profile",
        configuration_count=10,
        effective_configuration_limit=1,
    )
    result = statistical_workflow.optimize_with_calibrated_simulation(
        object(),
        profile,
        mode=OptimizationMode.AUTO,
        runtime_limit_calibration=runtime_limit_calibration,
        scalable_options=ScalableReplicaSearchOptions(
            maximum_configuration_evaluations=12,
            time_budget_seconds=45,
            candidate_width=3,
        ),
    )

    assert result.solution_mode == "SCALABLE"
    assert result.runtime_limit_calibration is runtime_limit_calibration
    assert not result.optimality_proven
    assert result.locally_minimal
    assert captured == {
        "maximum_configuration_evaluations": 12,
        "time_budget_seconds": 45,
        "candidate_width": 3,
    }

    mismatched = SimpleNamespace(
        calibration_profile_fingerprint="different-profile",
        configuration_count=10,
        effective_configuration_limit=1,
    )
    with pytest.raises(ValueError, match="different calibration profile"):
        statistical_workflow.optimize_with_calibrated_simulation(
            object(),
            profile,
            runtime_limit_calibration=mismatched,
        )

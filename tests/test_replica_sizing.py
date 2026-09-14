from collections import Counter
import math
from types import SimpleNamespace

import pytest

from metrics import Metrics
from optimization import (
    ReplicaSizingEvaluation,
    ReplicaSizingProblem,
    RequestClassMetricsUnavailableError,
    UniformCostReplicaOptimizer,
)


class FakeService:
    def __init__(self, service_rate):
        self.__service_rate = service_rate

    def get_mu(self):
        return self.__service_rate


class FakeSystem:
    def __init__(self, arrival_rates, service_rates, p95_function):
        self.arrival_rates = tuple(arrival_rates)
        self.service_rates = tuple(service_rates)
        self.services = {
            f"service-{index}": FakeService(service_rate)
            for index, service_rate in enumerate(service_rates)
        }
        self.configuration = tuple(1 for _ in service_rates)
        self.p95_function = p95_function
        self.solve_calls = Counter()
        self.simulate_calls = Counter()
        self.simulation_timelines = []

    def get_services(self):
        return self.services

    def get_effective_arrival_rates(self):
        return list(self.arrival_rates)

    def configure_system_replicas(self, configuration):
        self.configuration = tuple(configuration)

    def solve(self):
        self.solve_calls[self.configuration] += 1
        utilizations = [
            arrival_rate / (replicas * service_rate)
            for arrival_rate, replicas, service_rate in zip(
                self.arrival_rates,
                self.configuration,
                self.service_rates,
            )
        ]
        stable = all(utilization < 1 for utilization in utilizations)
        latencies = [
            0.1 if utilization < 1 else float("inf") for utilization in utilizations
        ]
        return Metrics(utilizations, latencies, stable)

    def simulate_measurement_replications(
        self,
        *,
        warmup_duration,
        measurement_duration,
        seeds,
        workers,
    ):
        self.simulate_calls[self.configuration] += 1
        seeds = tuple(seeds)
        self.simulation_timelines.append(
            (warmup_duration, measurement_duration, seeds, workers)
        )
        request_class_p95 = self.p95_function(self.configuration)
        return tuple(
            SimpleNamespace(
                seed=seed,
                replica_configuration=self.configuration,
                warmup_duration=warmup_duration,
                measurement_duration=measurement_duration,
                measured_request_counts={
                    request_class: 2_000 for request_class in request_class_p95
                },
                request_class_p95_latencies={
                    request_class: (
                        value[index] if isinstance(value, (list, tuple)) else value
                    )
                    for request_class, value in request_class_p95.items()
                },
            )
            for index, seed in enumerate(seeds)
        )


def make_problem(
    arrival_rates=(1, 1),
    service_rates=(10, 10),
    p95_function=lambda configuration: {"checkout": 0.5},
    slo=1.0,
    max_replicas=(4, 4),
):
    system = FakeSystem(arrival_rates, service_rates, p95_function)
    problem = ReplicaSizingProblem(
        system,
        simulation_duration=10,
        request_class_slos={"checkout": slo},
        max_replicas=max_replicas,
    )
    return system, problem


def test_initial_state_is_stable_and_neighbors_add_one_replica():
    _, problem = make_problem()

    initial = problem.initial_state()
    neighbors = problem.neighbors(initial)

    assert initial == (1, 1)
    assert set(neighbors) == {(2, 1), (1, 2)}
    for neighbor in neighbors:
        differences = [
            neighbor_value - initial_value
            for initial_value, neighbor_value in zip(initial, neighbor)
        ]
        assert differences.count(1) == 1
        assert differences.count(0) == len(initial) - 1
        assert problem.step_cost(initial, neighbor) == 1


def test_unstable_configuration_does_not_invoke_simulator():
    system, problem = make_problem(
        arrival_rates=(10,),
        service_rates=(5,),
        max_replicas=(3,),
    )

    evaluation = problem.evaluate((1,))

    assert not evaluation.analytical_metrics.get_stability()
    assert evaluation.simulation_metrics is None
    assert not evaluation.feasible
    assert evaluation.cost == math.inf
    assert evaluation.tie_breaker == math.inf
    assert not problem.is_feasible((1,))
    assert system.solve_calls[(1,)] == 1
    assert system.simulate_calls[(1,)] == 0


def test_stable_configuration_is_simulated_only_once_due_to_caching():
    system, problem = make_problem()

    first = problem.evaluate((1, 1))
    second = problem.evaluate((1, 1))

    assert first is second
    assert problem.is_feasible((1, 1))
    assert first.feasible
    assert first.cost == 2
    assert first.tie_breaker == 0.5
    assert system.solve_calls[(1, 1)] == 1
    assert system.simulate_calls[(1, 1)] == 1


def test_optimizer_evaluation_runs_warmup_before_measurement():
    system = FakeSystem(
        (1, 1),
        (10, 10),
        lambda configuration: {"checkout": 0.5},
    )
    problem = ReplicaSizingProblem(
        system,
        simulation_duration=20.0,
        warmup_duration=5.0,
        request_class_slos={"checkout": 1.0},
        max_replicas=(4, 4),
    )

    problem.evaluate(problem.initial_state())

    assert problem.initial_state() == (1, 1)
    warmup, measurement, seeds, workers = system.simulation_timelines[0]
    assert warmup == 5.0
    assert measurement == 20.0
    assert len(seeds) == 30
    assert workers == 1


def test_configuration_violating_request_class_p95_slo_is_rejected():
    _, problem = make_problem(
        p95_function=lambda configuration: {"checkout": 1.5},
        slo=1.0,
    )

    evaluation = problem.evaluate((1, 1))

    assert not evaluation.feasible
    assert evaluation.cost == math.inf
    assert evaluation.simulation_metrics is not None


def test_missing_end_to_end_request_class_metric_is_not_fabricated():
    system, problem = make_problem(p95_function=lambda configuration: {})

    with pytest.raises(RequestClassMetricsUnavailableError):
        problem.is_feasible((1, 1))
    with pytest.raises(RequestClassMetricsUnavailableError):
        problem.is_feasible((1, 1))

    assert system.solve_calls[(1, 1)] == 1
    assert system.simulate_calls[(1, 1)] == 1


def test_configuration_is_feasible_only_when_every_request_class_slo_passes():
    system = FakeSystem(
        (1, 1),
        (10, 10),
        lambda configuration: {"checkout": 0.8, "search": 2.1},
    )
    problem = ReplicaSizingProblem(
        system,
        simulation_duration=10,
        request_class_slos={"checkout": 1.0, "search": 2.0},
        max_replicas=(2, 2),
    )

    assert not problem.evaluate((1, 1)).feasible

    system.p95_function = lambda configuration: {"checkout": 0.8, "search": 1.9}
    uncached_problem = ReplicaSizingProblem(
        system,
        simulation_duration=10,
        request_class_slos={"checkout": 1.0, "search": 2.0},
        max_replicas=(2, 2),
    )
    evaluation = uncached_problem.evaluate((1, 1))

    assert evaluation.feasible
    assert evaluation.cost == 2
    assert evaluation.tie_breaker == 0.95


def test_replica_cost_precedes_latency_and_latency_breaks_only_equal_cost_ties():
    system = FakeSystem(
        (1, 1),
        (10, 10),
        lambda configuration: {
            "checkout": {
                (1, 1): 0.9,
                (2, 1): 0.8,
                (1, 2): 0.4,
            }[configuration]
        },
    )
    problem = ReplicaSizingProblem(
        system,
        simulation_duration=10,
        request_class_slos={"checkout": 1.0},
        max_replicas=(2, 2),
    )

    smaller = problem.evaluate((1, 1))
    equal_cost_slower = problem.evaluate((2, 1))
    equal_cost_faster = problem.evaluate((1, 2))

    assert smaller.cost == 2
    assert equal_cost_faster.cost == equal_cost_slower.cost == 3
    assert smaller.ranking_key() < equal_cost_faster.ranking_key()
    assert equal_cost_faster.ranking_key() < equal_cost_slower.ranking_key()


def test_exact_search_uses_normalized_p95_only_to_break_equal_replica_ties():
    system = FakeSystem(
        (1, 1),
        (10, 10),
        lambda configuration: {
            "checkout": {
                (1, 1): 1.5,
                (2, 1): 0.8,
                (1, 2): 0.4,
            }[configuration]
        },
    )
    problem = ReplicaSizingProblem(
        system,
        simulation_duration=10,
        request_class_slos={"checkout": 1.0},
        max_replicas=(2, 2),
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.configuration == (1, 2)
    assert result.metrics.cost == 3
    assert result.metrics.tie_breaker == 0.4


def test_uncertain_confidence_interval_is_not_treated_as_feasible():
    _, problem = make_problem(
        p95_function=lambda _configuration: {
            "checkout": [0.9, 1.1] * 15,
        },
        slo=1.0,
    )

    evaluation = problem.evaluate((1, 1))

    assert evaluation.reliability_status.value == "uncertain"
    assert not evaluation.feasible
    assert evaluation.cost == math.inf
    interval = evaluation.reliability.request_class_intervals["checkout"]
    assert interval.lower_bound < 1.0 < interval.upper_bound


def test_configuration_seed_blocks_are_reproducible_and_disjoint():
    _, problem = make_problem(max_replicas=(3, 2))

    first = problem.evaluation_seeds((1, 1))
    repeated = problem.evaluation_seeds((1, 1))
    neighbor = problem.evaluation_seeds((1, 2))
    other_neighbor = problem.evaluation_seeds((2, 1))

    assert first == repeated
    assert len(first) == 30
    assert set(first).isdisjoint(neighbor)
    assert set(first).isdisjoint(other_neighbor)
    assert set(neighbor).isdisjoint(other_neighbor)


def build_search_problem():
    p95_function = lambda configuration: {
        "checkout": 1 / configuration[0] + 1 / configuration[1]
    }
    return make_problem(
        arrival_rates=(10, 1),
        service_rates=(5, 10),
        p95_function=p95_function,
        slo=0.9,
        max_replicas=(5, 3),
    )


def test_uniform_cost_returns_minimum_replica_configuration_and_full_path():
    _, problem = build_search_problem()

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.configuration == (3, 2)
    assert result.cost == 1
    assert isinstance(result.metrics, ReplicaSizingEvaluation)
    assert result.metrics.cost == 5
    assert result.metrics.feasible
    assert result.path[0] == (3, 1)
    assert result.path[-1] == result.configuration
    assert all(
        sum(next_value - value for value, next_value in zip(current, neighbor)) == 1
        for current, neighbor in zip(result.path, result.path[1:])
    )


def test_structural_infeasibility_skips_expensive_simulation():
    system = FakeSystem(
        (1, 1),
        (10, 10),
        lambda _configuration: {"checkout": 0.5},
    )
    problem = ReplicaSizingProblem(
        system,
        simulation_duration=10,
        request_class_slos={"checkout": 1.0},
        max_replicas=(3, 3),
        structural_feasibility=lambda configuration: sum(configuration) <= 2,
    )

    evaluation = problem.evaluate((2, 1))

    assert not evaluation.feasible
    assert evaluation.classification.value == "STRUCTURALLY_INFEASIBLE"
    assert evaluation.simulation_metrics is None
    assert system.simulate_calls[(2, 1)] == 0

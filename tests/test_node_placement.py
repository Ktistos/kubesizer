import math
import random

import pytest

from optimization import (
    BranchAndBoundNodePlacementOptimizer,
    NoFeasibleConfigurationError,
    NodePlacementProblem,
    OptimizationMode,
    ScalableNodePlacementOptimizer,
    ScalablePlacementSearchOptions,
    optimize_node_placement,
)


class FakeEdge:
    def __init__(self, target, route_value):
        self.__target = target
        self.__route_value = route_value

    def get_target(self):
        return self.__target

    def get_routing_prob(self):
        return self.__route_value


class FakeService:
    def __init__(self, service_id, name, gamma, replicas):
        self.__id = service_id
        self.__name = name
        self.__gamma = gamma
        self.__replicas = replicas
        self.__edges = []

    def add_edge(self, target, route_value):
        self.__edges.append(FakeEdge(target, route_value))

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.__name

    def get_gamma(self, _request_class):
        return self.__gamma

    def get_replicas(self):
        return self.__replicas

    def get_adjacency_list(self, _request_class):
        return tuple(self.__edges)


class FakeSystem:
    def __init__(self, service_specs):
        self.__services = {
            name: FakeService(
                service_id,
                name,
                spec["gamma"],
                spec["replicas"],
            )
            for service_id, (name, spec) in enumerate(service_specs.items())
        }
        for source_name, spec in service_specs.items():
            source = self.__services[source_name]
            for target_name, route_value in spec.get("routes", {}).items():
                source.add_edge(self.__services[target_name], route_value)

    def get_type(self):
        return "open"

    def get_services(self):
        return self.__services

    def get_request_classes(self):
        return ("default",)

    def get_effective_arrival_rates(self):
        services = tuple(self.__services.values())
        rates = [service.get_gamma("default") for service in services]

        for _ in range(10_000):
            updated = [service.get_gamma("default") for service in services]
            for source in services:
                source_rate = rates[source.get_id()]
                for edge in source.get_adjacency_list("default"):
                    updated[edge.get_target().get_id()] += (
                        source_rate * edge.get_routing_prob()
                    )
            if (
                max(
                    abs(new_rate - old_rate)
                    for new_rate, old_rate in zip(updated, rates)
                )
                < 1e-14
            ):
                return updated
            rates = updated

        raise ArithmeticError("Traffic fixed-point iteration did not converge.")

    def get_request_class_effective_arrival_rates(self):
        return {"default": self.get_effective_arrival_rates()}


class FakeNode:
    def __init__(
        self,
        name,
        latencies,
        allocatable_cpu=100_000,
        allocatable_memory=100_000_000,
    ):
        self.__name = name
        self.__latencies = latencies
        self.__allocatable_cpu = allocatable_cpu
        self.__allocatable_memory = allocatable_memory

    def get_name(self):
        return self.__name

    def get_node_communication_latencies(self):
        return self.__latencies

    def get_allocatable_cpu(self):
        return self.__allocatable_cpu

    def get_allocatable_memory(self):
        return self.__allocatable_memory


class FakeCluster:
    def __init__(self, system, nodes):
        self.__system = system
        self.__nodes = nodes

    def get_system(self):
        return self.__system

    def get_nodes(self):
        return self.__nodes


CURRENT_SERVICE_SPECS = {
    "api": {
        "gamma": 20.0,
        "replicas": 3,
        "routes": {
            "auth": 0.20,
            "db": 0.60,
            "cache": 0.10,
            "worker": 0.05,
        },
    },
    "auth": {
        "gamma": 0.0,
        "replicas": 2,
        "routes": {"db": 0.80, "cache": 0.05},
    },
    "db": {
        "gamma": 0.0,
        "replicas": 5,
        "routes": {"cache": 0.05},
    },
    "cache": {
        "gamma": 0.0,
        "replicas": 2,
        "routes": {"worker": 0.05},
    },
    "worker": {
        "gamma": 0.0,
        "replicas": 2,
        "routes": {"db": 0.80, "cache": 0.05},
    },
}

CURRENT_NODE_LATENCIES = {
    "skube01": {"skube02": 0.0012, "skube03": 0.0015},
    "skube02": {"skube01": 0.0014, "skube03": 0.0009},
    "skube03": {"skube01": 0.0010, "skube02": 0.0009},
}


def build_problem(
    service_specs,
    node_latencies,
    *,
    cost_weights=None,
    service_resource_requests=None,
    node_capacities=None,
    hard_constraint=None,
):
    system = FakeSystem(service_specs)
    node_capacities = node_capacities or {}
    nodes = {}
    for node_name, latencies in node_latencies.items():
        cpu, memory = node_capacities.get(
            node_name,
            (100_000, 100_000_000),
        )
        nodes[node_name] = FakeNode(
            node_name,
            latencies,
            allocatable_cpu=cpu,
            allocatable_memory=memory,
        )
    return NodePlacementProblem(
        FakeCluster(system, nodes),
        cost_weights=cost_weights,
        service_resource_requests=service_resource_requests,
        hard_constraint=hard_constraint,
    )


def brute_force_optimum(problem):
    """Small-instance oracle kept in tests, not as a production optimizer."""
    feasible = tuple(
        evaluation
        for configuration in problem.iter_configurations()
        if (evaluation := problem.evaluate(configuration)).valid
    )
    if not feasible:
        raise NoFeasibleConfigurationError(
            "No feasible fixed-replica node placement exists."
        )
    return min(
        feasible,
        key=lambda evaluation: (
            evaluation.total_cost,
            evaluation.configuration,
        ),
    )


def test_derives_current_workload_arrival_and_edge_traffic_rates():
    problem = build_problem(
        CURRENT_SERVICE_SPECS,
        CURRENT_NODE_LATENCIES,
    )

    assert problem.get_service_arrival_rates() == pytest.approx(
        {
            "api": 20.000,
            "auth": 4.000,
            "db": 16.123,
            "cache": 3.064,
            "worker": 1.153,
        },
        abs=0.0005,
    )
    assert problem.get_edge_traffic_rates() == pytest.approx(
        {
            ("api", "db"): 12.000,
            ("api", "auth"): 4.000,
            ("auth", "db"): 3.200,
            ("api", "cache"): 2.000,
            ("api", "worker"): 1.000,
            ("worker", "db"): 0.923,
            ("db", "cache"): 0.806,
            ("auth", "cache"): 0.200,
            ("cache", "worker"): 0.153,
            ("worker", "cache"): 0.058,
        },
        abs=0.0005,
    )


def test_route_values_are_expected_call_counts_and_allow_fan_out():
    problem = build_problem(
        {
            "source": {
                "gamma": 10.0,
                "replicas": 1,
                "routes": {
                    "destination-a": 1.0,
                    "destination-b": 1.0,
                },
            },
            "destination-a": {"gamma": 0.0, "replicas": 1},
            "destination-b": {"gamma": 0.0, "replicas": 1},
        },
        {"node-a": {}},
    )

    assert problem.get_service_arrival_rates() == pytest.approx(
        {
            "source": 10.0,
            "destination-a": 10.0,
            "destination-b": 10.0,
        }
    )
    assert problem.get_edge_traffic_rates() == pytest.approx(
        {
            ("source", "destination-a"): 10.0,
            ("source", "destination-b"): 10.0,
        }
    )


def test_evaluates_supplied_current_placement_and_reports_unchecked_resources():
    problem = build_problem(
        CURRENT_SERVICE_SPECS,
        CURRENT_NODE_LATENCIES,
    )
    placement = {
        "skube01": {
            "api": 2,
            "auth": 1,
            "db": 2,
            "cache": 1,
            "worker": 1,
        },
        "skube02": {
            "api": 1,
            "auth": 1,
            "db": 2,
            "cache": 1,
        },
        "skube03": {"db": 1, "worker": 1},
    }

    evaluation = problem.evaluate(placement)

    assert evaluation.valid
    assert not evaluation.resource_feasibility_checked
    assert evaluation.replica_spreading_penalty == pytest.approx(1 / 15)
    assert evaluation.network_cost_normalized == pytest.approx(
        evaluation.network_cost_raw
        / (0.0015 * sum(problem.get_edge_traffic_rates().values()))
    )
    assert evaluation.average_network_delay_seconds == pytest.approx(
        evaluation.network_cost_raw / 20.0
    )
    assert evaluation.total_cost == pytest.approx(
        0.7 * evaluation.network_cost_normalized
        + 0.3 * evaluation.replica_spreading_penalty
    )
    assert evaluation.to_dict()["resource_feasibility_checked"] is False


def test_network_cost_uses_directional_latency():
    service_specs = {
        "source": {
            "gamma": 10.0,
            "replicas": 1,
            "routes": {"destination": 0.5},
        },
        "destination": {"gamma": 0.0, "replicas": 1},
    }
    node_latencies = {
        "node-a": {"node-b": 0.2},
        "node-b": {"node-a": 0.1},
    }
    problem = build_problem(
        service_specs,
        node_latencies,
        cost_weights={"network": 1.0, "replica_spreading": 0.0},
    )

    forward = problem.evaluate(
        {
            "node-a": {"source": 1},
            "node-b": {"destination": 1},
        }
    )
    reverse = problem.evaluate(
        {
            "node-a": {"destination": 1},
            "node-b": {"source": 1},
        }
    )

    assert forward.network_cost_raw == pytest.approx(1.0)
    assert forward.network_cost_normalized == pytest.approx(1.0)
    assert forward.average_network_delay_seconds == pytest.approx(0.1)
    assert reverse.network_cost_raw == pytest.approx(0.5)
    assert reverse.network_cost_normalized == pytest.approx(0.5)
    assert reverse.average_network_delay_seconds == pytest.approx(0.05)


@pytest.mark.parametrize(
    ("counts", "expected_penalty"),
    [
        ((2, 2, 1), 0.0),
        ((3, 1, 1), 0.125),
        ((5, 0, 0), 1.0),
    ],
)
def test_five_replica_spreading_examples(counts, expected_penalty):
    problem = build_problem(
        {"db": {"gamma": 0.0, "replicas": 5}},
        {
            "node-a": {"node-b": 0.1, "node-c": 0.1},
            "node-b": {"node-a": 0.1, "node-c": 0.1},
            "node-c": {"node-a": 0.1, "node-b": 0.1},
        },
        cost_weights={"network": 0.0, "replica_spreading": 1.0},
    )
    placement = {
        node_name: {"db": count}
        for node_name, count in zip(
            ("node-a", "node-b", "node-c"),
            counts,
        )
        if count
    }

    evaluation = problem.evaluate(placement)

    assert evaluation.valid
    assert evaluation.replica_spreading_penalty == pytest.approx(expected_penalty)
    assert evaluation.total_cost == pytest.approx(expected_penalty)


def test_rejects_unknown_names_invalid_counts_and_changed_replica_totals():
    problem = build_problem(
        {
            "api": {"gamma": 0.0, "replicas": 2},
            "worker": {"gamma": 0.0, "replicas": 1},
        },
        {
            "node-a": {"node-b": 0.1},
            "node-b": {"node-a": 0.1},
        },
    )

    evaluation = problem.evaluate(
        {
            "node-a": {"api": 1, "ghost": 1},
            "node-b": {"api": True, "worker": -1},
            "unknown-node": {"api": 1},
        }
    )

    assert not evaluation.valid
    assert math.isinf(evaluation.total_cost)
    assert any(
        "Unknown node 'unknown-node'" in violation
        for violation in evaluation.constraint_violations
    )
    assert any(
        "Unknown service 'ghost'" in violation
        for violation in evaluation.constraint_violations
    )
    assert any(
        "must have exactly 2 replicas" in violation
        for violation in evaluation.constraint_violations
    )


def test_enforces_cpu_and_memory_when_all_service_requests_are_supplied():
    service_specs = {"api": {"gamma": 0.0, "replicas": 2}}
    node_latencies = {
        "node-a": {"node-b": 0.1},
        "node-b": {"node-a": 0.1},
    }
    problem = build_problem(
        service_specs,
        node_latencies,
        service_resource_requests={
            "api": {
                "cpu_request_millicores": 600,
                "memory_request_bytes": 700,
            }
        },
        node_capacities={
            "node-a": (1_000, 1_000),
            "node-b": (1_000, 1_000),
        },
    )

    invalid = problem.evaluate({"node-a": {"api": 2}})
    valid = problem.evaluate({"node-a": {"api": 1}, "node-b": {"api": 1}})

    assert invalid.resource_feasibility_checked
    assert not invalid.valid
    assert any(
        "CPU request 1200m exceeds" in violation
        for violation in invalid.constraint_violations
    )
    assert any(
        "memory request 1400 bytes exceeds" in violation
        for violation in invalid.constraint_violations
    )
    assert valid.valid


def test_rejects_partial_service_resource_requests():
    with pytest.raises(
        ValueError,
        match="Resource requests must be supplied for every service",
    ):
        build_problem(
            {
                "api": {"gamma": 0.0, "replicas": 1},
                "worker": {"gamma": 0.0, "replicas": 1},
            },
            {"node-a": {}},
            service_resource_requests={
                "api": {
                    "cpu_request_millicores": 100,
                    "memory_request_bytes": 100,
                }
            },
        )


def test_current_workload_has_expected_exact_search_space_size():
    problem = build_problem(
        CURRENT_SERVICE_SPECS,
        CURRENT_NODE_LATENCIES,
    )

    assert problem.configuration_count() == 45_360


def test_partial_cost_bound_does_not_exceed_any_completion():
    problem = build_problem(
        {
            "source": {
                "gamma": 10.0,
                "replicas": 2,
                "routes": {"destination": 0.5},
            },
            "destination": {"gamma": 0.0, "replicas": 2},
        },
        {
            "node-a": {"node-b": 0.2},
            "node-b": {"node-a": 0.1},
        },
    )
    partial = {"source": (2, 0)}

    lower_bound = problem.partial_cost_lower_bound(partial)
    completion_costs = [
        problem.evaluate(
            problem.build_configuration(
                {
                    **partial,
                    "destination": destination_distribution,
                }
            )
        ).total_cost
        for destination_distribution in problem.get_replica_distribution_options(
            "destination"
        )
    ]

    assert lower_bound <= min(completion_costs) + 1e-15


def test_branch_and_bound_matches_brute_force_on_current_workload():
    problem = build_problem(
        CURRENT_SERVICE_SPECS,
        CURRENT_NODE_LATENCIES,
    )

    optimum = brute_force_optimum(problem)
    bounded = BranchAndBoundNodePlacementOptimizer().optimize(problem)

    assert bounded.configuration == optimum.configuration
    assert bounded.cost == pytest.approx(optimum.total_cost)
    assert bounded.metrics == optimum
    assert bounded.optimality_proven
    assert bounded.total_configurations == problem.configuration_count()
    assert (
        bounded.complete_states + bounded.pruned_complete_placements
        == bounded.total_configurations
    )
    assert bounded.complete_states < bounded.total_configurations
    assert bounded.pruned_complete_placements > 0


def test_branch_and_bound_prunes_resource_infeasible_partial_placements():
    problem = build_problem(
        {
            "api": {"gamma": 0.0, "replicas": 2},
            "worker": {"gamma": 0.0, "replicas": 2},
        },
        {
            "node-a": {"node-b": 0.1},
            "node-b": {"node-a": 0.1},
        },
        service_resource_requests={
            "api": {
                "cpu_request_millicores": 1,
                "memory_request_bytes": 1,
            },
            "worker": {
                "cpu_request_millicores": 1,
                "memory_request_bytes": 1,
            },
        },
        node_capacities={
            "node-a": (2, 2),
            "node-b": (2, 2),
        },
    )

    result = BranchAndBoundNodePlacementOptimizer().optimize(problem)

    assert result.metrics.valid
    assert result.complete_states < result.total_configurations
    assert result.pruned_complete_placements > 0


def test_branch_and_bound_raises_when_capacity_rejects_every_placement():
    problem = build_problem(
        {"api": {"gamma": 0.0, "replicas": 1}},
        {
            "node-a": {"node-b": 0.1},
            "node-b": {"node-a": 0.1},
        },
        service_resource_requests={
            "api": {
                "cpu_request_millicores": 2,
                "memory_request_bytes": 1,
            }
        },
        node_capacities={
            "node-a": (1, 100),
            "node-b": (1, 100),
        },
    )

    with pytest.raises(NoFeasibleConfigurationError):
        BranchAndBoundNodePlacementOptimizer().optimize(problem)


@pytest.mark.parametrize("seed", range(10))
def test_branch_and_bound_matches_brute_force_across_small_problems(seed):
    random_source = random.Random(seed)
    service_names = ("service-a", "service-b", "service-c")
    service_specs = {}
    for source_index, source_name in enumerate(service_names):
        routes = {
            destination_name: random_source.uniform(0.0, 0.45)
            for destination_name in service_names[source_index + 1 :]
            if random_source.random() < 0.7
        }
        service_specs[source_name] = {
            "gamma": (random_source.uniform(1.0, 10.0) if source_index == 0 else 0.0),
            "replicas": random_source.randint(1, 2),
            "routes": routes,
        }

    node_names = tuple(f"node-{index}" for index in range(random_source.randint(2, 3)))
    node_latencies = {
        source_name: {
            destination_name: random_source.uniform(0.001, 0.1)
            for destination_name in node_names
            if destination_name != source_name
        }
        for source_name in node_names
    }
    network_weight = random_source.random()
    problem = build_problem(
        service_specs,
        node_latencies,
        cost_weights={
            "network": network_weight,
            "replica_spreading": 1.0 - network_weight,
        },
    )

    optimum = brute_force_optimum(problem)
    bounded = BranchAndBoundNodePlacementOptimizer().optimize(problem)

    assert bounded.configuration == optimum.configuration
    assert bounded.cost == pytest.approx(optimum.total_cost)


def test_scalable_placement_returns_feasible_locally_improved_result():
    problem = build_problem(
        CURRENT_SERVICE_SPECS,
        CURRENT_NODE_LATENCIES,
    )

    result = ScalableNodePlacementOptimizer(
        beam_width=8,
        candidate_distributions_per_service=16,
        maximum_partial_states=10_000,
        maximum_complete_evaluations=1_000,
        maximum_local_iterations=20,
        time_budget_seconds=10,
    ).optimize(problem)

    assert result.metrics.valid
    assert result.solution_mode == "SCALABLE"
    assert not result.optimality_proven
    assert result.complete_states <= 1_000
    assert result.total_configurations == problem.configuration_count()


def test_auto_placement_uses_scalable_mode_above_exact_space_limit():
    problem = build_problem(
        CURRENT_SERVICE_SPECS,
        CURRENT_NODE_LATENCIES,
    )

    result = optimize_node_placement(
        problem,
        mode=OptimizationMode.AUTO,
        exact_configuration_limit=1,
        scalable_options=ScalablePlacementSearchOptions(
            beam_width=4,
            candidate_distributions_per_service=8,
            maximum_partial_states=5_000,
            maximum_complete_evaluations=500,
            maximum_local_iterations=10,
            time_budget_seconds=10,
        ),
    )

    assert result.solution_mode == "SCALABLE"
    assert result.metrics.valid
    assert not result.optimality_proven
    assert "AUTO selected SCALABLE" in result.selection_reason


def test_placement_selection_reports_exact_and_scalable_progress():
    problem = build_problem(CURRENT_SERVICE_SPECS, CURRENT_NODE_LATENCIES)
    exact_events = []
    scalable_events = []

    optimize_node_placement(
        problem,
        mode=OptimizationMode.EXACT,
        progress_callback=(
            lambda event, details: exact_events.append((event, details))
        ),
    )
    optimize_node_placement(
        problem,
        mode=OptimizationMode.SCALABLE,
        scalable_options=ScalablePlacementSearchOptions(
            beam_width=4,
            candidate_distributions_per_service=8,
            maximum_partial_states=5_000,
            maximum_complete_evaluations=500,
            maximum_local_iterations=10,
            time_budget_seconds=10,
        ),
        progress_callback=(
            lambda event, details: scalable_events.append((event, details))
        ),
    )

    assert exact_events[0][0] == "placement_search_started"
    assert exact_events[-1][0] == "placement_search_completed"
    assert any(event == "placement_search_progress" for event, _ in exact_events)
    assert scalable_events[0][0] == "placement_search_started"
    assert scalable_events[-1][0] == "placement_search_completed"
    assert any(event == "placement_search_progress" for event, _ in scalable_events)


def _swap_fixture():
    services = {
        "A": {"gamma": 10.0, "replicas": 1, "routes": {"B": 1.0}},
        "B": {"gamma": 0.0, "replicas": 1},
        "C": {"gamma": 10.0, "replicas": 1, "routes": {"D": 1.0}},
        "D": {"gamma": 0.0, "replicas": 1},
    }
    latencies = {
        "node-a": {"node-b": 1.0},
        "node-b": {"node-a": 1.0},
    }
    requests = {
        name: {
            "cpu_request_millicores": 1,
            "memory_request_bytes": 1,
        }
        for name in services
    }
    problem = build_problem(
        services,
        latencies,
        cost_weights={"network": 1.0, "replica_spreading": 0.0},
        service_resource_requests=requests,
        node_capacities={"node-a": (2, 2), "node-b": (2, 2)},
    )
    initial = {
        "node-a": {"A": 1, "C": 1},
        "node-b": {"B": 1, "D": 1},
    }
    return problem, initial


def test_pairwise_swap_escapes_full_node_relocation_local_placement():
    problem, initial = _swap_fixture()

    result = ScalableNodePlacementOptimizer(
        initial_placement=initial,
        beam_width=4,
        maximum_partial_states=1_000,
        maximum_complete_evaluations=100,
        maximum_restarts=1,
        maximum_no_improvement=1,
        lns_neighborhood_sizes=(3,),
        lns_subset_attempts=1,
        lns_subproblem_budget=50,
        random_seed=11,
        collect_trace=True,
    ).optimize(problem)

    assert result.metrics.valid
    assert result.objective_value == pytest.approx(0.0)
    assert result.relocation_local_certified
    assert result.swap_local_certified
    assert result.locally_optimal
    assert not result.global_optimal
    assert result.lower_bound == pytest.approx(0.0)
    assert result.relative_bound_gap is None
    assert any(entry.action == "accepted-swap" for entry in result.trace)
    colocated = [set(services) for services in result.placement.values()]
    assert {"A", "B"} in colocated
    assert {"C", "D"} in colocated


def test_budget_stopped_placement_does_not_claim_local_certificate():
    problem, initial = _swap_fixture()

    result = ScalableNodePlacementOptimizer(
        initial_placement=initial,
        beam_width=2,
        maximum_partial_states=100,
        maximum_complete_evaluations=1,
        maximum_restarts=1,
        random_seed=4,
    ).optimize(problem)

    assert result.metrics.valid
    assert result.budget_exhausted
    assert not result.relocation_local_certified
    assert not result.swap_local_certified
    assert not result.locally_optimal
    assert result.termination_reason.value in {
        "TOTAL_CALL_BUDGET_EXHAUSTED",
        "EVALUATION_BUDGET_EXHAUSTED",
    }


def test_beam_failure_is_not_reported_as_proof_of_infeasibility():
    problem = build_problem(
        {"api": {"gamma": 0.0, "replicas": 1}},
        {"node-a": {"node-b": 0.1}, "node-b": {"node-a": 0.1}},
        service_resource_requests={
            "api": {
                "cpu_request_millicores": 3,
                "memory_request_bytes": 1,
            }
        },
        node_capacities={"node-a": (2, 2), "node-b": (2, 2)},
    )

    with pytest.raises(
        NoFeasibleConfigurationError,
        match="not proof",
    ) as captured:
        ScalableNodePlacementOptimizer(
            maximum_restarts=2,
            random_seed=5,
        ).optimize(problem)

    assert captured.value.termination_reason.value == ("NO_FEASIBLE_SOLUTION_FOUND")
    assert captured.value.beam_failures == 2


def test_placement_seed_is_reproducible_and_result_respects_hard_constraint():
    def separate_a_b(placement, *, complete):
        if not complete:
            return True
        return not any(
            "A" in services and "B" in services for services in placement.values()
        )

    services = {
        "A": {"gamma": 5.0, "replicas": 1, "routes": {"B": 1.0}},
        "B": {"gamma": 0.0, "replicas": 1},
        "C": {"gamma": 0.0, "replicas": 1},
    }
    latencies = {
        "node-a": {"node-b": 0.2, "node-c": 0.3},
        "node-b": {"node-a": 0.2, "node-c": 0.1},
        "node-c": {"node-a": 0.3, "node-b": 0.1},
    }

    def run():
        problem = build_problem(
            services,
            latencies,
            hard_constraint=separate_a_b,
        )
        return problem, ScalableNodePlacementOptimizer(
            beam_width=6,
            diversity_per_signature=2,
            maximum_restarts=2,
            maximum_no_improvement=2,
            maximum_complete_evaluations=100,
            random_seed=23,
            collect_trace=True,
            lns_neighborhood_sizes=(2,),
        ).optimize(problem)

    left_problem, left = run()
    right_problem, right = run()

    assert left.configuration == right.configuration
    assert left.objective_value == pytest.approx(right.objective_value)
    assert left.trace == right.trace
    assert left_problem.evaluate(left.placement).valid
    assert not any(
        "A" in services and "B" in services for services in left.placement.values()
    )


def test_small_scalable_placement_reports_true_gap_to_exact_for_test_only():
    problem, _ = _swap_fixture()
    exact = BranchAndBoundNodePlacementOptimizer().optimize(problem)
    heuristic = ScalableNodePlacementOptimizer(
        beam_width=4,
        maximum_restarts=1,
        maximum_complete_evaluations=100,
        random_seed=9,
        lns_neighborhood_sizes=(2,),
    ).optimize(problem)

    observed_test_gap = heuristic.objective_value - exact.objective_value
    assert observed_test_gap >= -1e-15
    assert heuristic.metrics.valid
    assert not heuristic.global_optimal

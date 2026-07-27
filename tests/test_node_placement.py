import math

import pytest

from optimization import (
    ExhaustiveNodePlacementOptimizer,
    NoFeasibleConfigurationError,
    NodePlacementProblem,
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

    def get_gamma(self):
        return self.__gamma

    def get_replicas(self):
        return self.__replicas

    def get_adjacency_list(self):
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

    def get_effective_arrival_rates(self):
        services = tuple(self.__services.values())
        rates = [service.get_gamma() for service in services]

        for _ in range(10_000):
            updated = [service.get_gamma() for service in services]
            for source in services:
                source_rate = rates[source.get_id()]
                for edge in source.get_adjacency_list():
                    updated[edge.get_target().get_id()] += (
                        source_rate * edge.get_routing_prob()
                    )
            if max(
                abs(new_rate - old_rate)
                for new_rate, old_rate in zip(updated, rates)
            ) < 1e-14:
                return updated
            rates = updated

        raise ArithmeticError("Traffic fixed-point iteration did not converge.")


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
        / (
            0.0015
            * sum(problem.get_edge_traffic_rates().values())
        )
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
    assert evaluation.replica_spreading_penalty == pytest.approx(
        expected_penalty
    )
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
    valid = problem.evaluate(
        {"node-a": {"api": 1}, "node-b": {"api": 1}}
    )

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


def test_exhaustive_optimizer_finds_global_network_minimum():
    problem = build_problem(
        {
            "source": {
                "gamma": 10.0,
                "replicas": 1,
                "routes": {"destination": 0.5},
            },
            "destination": {"gamma": 0.0, "replicas": 1},
        },
        {
            "node-a": {"node-b": 0.2},
            "node-b": {"node-a": 0.1},
        },
        cost_weights={"network": 1.0, "replica_spreading": 0.0},
    )

    result = ExhaustiveNodePlacementOptimizer().optimize(problem)

    source_node = next(
        node_name
        for node_name, services in result.placement.items()
        if services.get("source") == 1
    )
    destination_node = next(
        node_name
        for node_name, services in result.placement.items()
        if services.get("destination") == 1
    )

    assert source_node == destination_node
    assert result.cost == pytest.approx(0.0)
    assert result.metrics.network_cost_raw == pytest.approx(0.0)
    assert result.explored_states == 4
    assert result.feasible_states == 4


def test_exhaustive_optimizer_raises_when_capacity_rejects_every_placement():
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
        ExhaustiveNodePlacementOptimizer().optimize(problem)

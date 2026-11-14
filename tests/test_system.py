import math
import random
import pytest

from performance_models import QueueingNetworkType, System


def test_get_service_by_id_returns_matching_service_or_none():
    workload = {
        "read": {
            "api": {"gamma": 1.0, "mu": 10.0},
            "worker": {"gamma": 0.0, "mu": 5.0},
        }
    }
    system = System("test", workload, QueueingNetworkType.OPEN)

    assert system.get_service_by_id(0) is system.get_service("api")
    assert system.get_service_by_id(1) is system.get_service("worker")
    assert system.get_service_by_id(2) is None

    second_system = System("second", workload, QueueingNetworkType.OPEN)
    assert second_system.get_service_by_id(0).get_name() == "api"


def test_workload_spec_is_returned_as_an_isolated_copy():
    workload = {"read": {"api": {"gamma": 1.0, "mu": 10.0}}}
    system = System("test", workload, QueueingNetworkType.OPEN)

    copied = system.get_workload_spec()
    copied["read"]["api"]["gamma"] = 99.0

    assert system.get_workload_spec()["read"]["api"]["gamma"] == 1.0


def test_class_traffic_equations_are_solved_then_aggregated():
    workload = {
        "read": {
            "api": {
                "gamma": 2.0,
                "mu": 10.0,
                "routes": {"worker": 0.5},
            },
            "worker": {"gamma": 0.0, "mu": 5.0},
        },
        "write": {
            "api": {
                "gamma": 3.0,
                "mu": 10.0,
                "routes": {"worker": 0.2},
            },
        },
    }
    system = System("test", workload, QueueingNetworkType.OPEN)

    assert tuple(system.get_services()) == ("api", "worker")
    assert system.get_request_classes() == ("read", "write")

    class_rates = system.get_request_class_effective_arrival_rates()
    assert class_rates["read"] == pytest.approx([2.0, 1.0])
    assert class_rates["write"] == pytest.approx([3.0, 0.6])
    assert system.get_effective_arrival_rates() == pytest.approx([5.0, 1.6])

    worker = system.get_service("worker")
    assert worker.get_gamma("write") == 0.0
    assert worker.get_adjacency_list("write") == ()

    metrics = system.solve()
    assert metrics.get_mean_utilizations() == pytest.approx([0.5, 0.32])
    assert metrics.get_stability()
    assert metrics.get_mean_latencies() == pytest.approx([0.2, 1 / 3.4])


def test_combined_request_class_load_determines_stability():
    workload = {
        "read": {"db": {"gamma": 6.0, "mu": 10.0}},
        "write": {"db": {"gamma": 6.0, "mu": 10.0}},
    }
    system = System("test", workload, QueueingNetworkType.OPEN)

    class_rates = system.get_request_class_effective_arrival_rates()
    assert class_rates["read"] == pytest.approx([6.0])

    assert class_rates["write"] == pytest.approx([6.0])
    assert system.get_effective_arrival_rates() == pytest.approx([12.0])

    metrics = system.solve()
    assert metrics.get_mean_utilizations() == pytest.approx([1.2])
    assert not metrics.get_stability()
    assert math.isinf(metrics.get_mean_latencies()[0])


def test_minimum_stable_replicas_use_strict_capacity_bound():
    workload = {
        "read": {
            "api": {
                "gamma": 10.0,
                "mu": 5.0,
                "routes": {"worker": 0.5},
            },
            "worker": {"gamma": 0.0, "mu": 10.0},
        }
    }
    system = System("test", workload, QueueingNetworkType.OPEN)

    minimum_replicas = system.get_minimum_stable_replicas()

    # The API load exactly fills two replicas, so strict stability needs three.
    assert minimum_replicas == [3, 1]
    system.configure_system_replicas(minimum_replicas)
    assert system.solve().get_stability()

    system.configure_system_replicas([2, 1])
    assert not system.solve().get_stability()


def test_parser_rejects_inconsistent_shared_service_rates():
    workload = {
        "read": {"api": {"gamma": 1.0, "mu": 10.0}},
        "write": {"api": {"gamma": 1.0, "mu": 20.0}},
    }
    with pytest.raises(ValueError, match="same mu"):
        System("test", workload, QueueingNetworkType.OPEN)


def test_parser_rejects_routes_to_unknown_services():
    workload = {
        "read": {
            "api": {
                "gamma": 1.0,
                "mu": 10.0,
                "routes": {"missing": 0.5},
            }
        }
    }
    with pytest.raises(ValueError, match="unknown service 'missing'"):
        System("test", workload, QueueingNetworkType.OPEN)


def test_simulation_reports_p95_by_request_class():
    random.seed(42)
    workload = {
        "read": {"api": {"gamma": 2.0, "mu": 100.0}},
        "write": {"api": {"gamma": 2.0, "mu": 100.0}},
    }
    system = System("test", workload, QueueingNetworkType.OPEN)

    metrics = system.simulate(10)
    request_class_p95 = metrics.get_request_class_p95_latencies()

    assert set(request_class_p95) == {"read", "write"}
    assert all(latency > 0 for latency in request_class_p95.values())


def test_simulation_can_exclude_pre_warmup_latency_without_skipping_the_run():
    random.seed(42)
    system = System(
        "test",
        {"read": {"api": {"gamma": 2.0, "mu": 100.0}}},
        QueueingNetworkType.OPEN,
    )

    metrics = system.simulate(10.0, warmup_duration=10.0)

    assert metrics.get_request_class_p95_latencies() == {}
    assert metrics.get_mean_utilizations()[0] > 0
    with pytest.raises(ValueError, match="warmup_duration"):
        system.simulate(10.0, warmup_duration=11.0)

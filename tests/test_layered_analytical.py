import copy
import math

import pytest

from analytical_workflow import optimize_with_analytical_model
from performance_models import QueueingNetworkType, System
from performance_models.layered import (
    LayeredAnalyticalSystem,
    Moments,
    independent_join_moments,
)


def _system(*, rate=0.1, mode="sequential", blocking=0.0, queue="pooled"):
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "calls": {"parent": {"child": 1.0}},
                "call_execution": mode,
                "fixed_latency_overhead_seconds": 0.02,
                "end_to_end_latency_slos": {"mean_seconds": 0.8},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"parent": {"distribution": "exponential", "rate": rate}}
            },
            "service_times": {
                "parent": {
                    "distribution": "deterministic",
                    "mean": 0.1,
                    "concurrency_limit_per_replica": 1,
                    "replica_queue_model": queue,
                },
                "child": {"distribution": "deterministic", "mean": 0.2},
            },
        },
    }
    if blocking:
        workload["traffic_model"]["service_times"]["child"]["non_cpu_blocking_time"] = {
            "distribution": "deterministic",
            "mean": blocking,
        }
    return System("test", workload, QueueingNetworkType.OPEN)


def test_deterministic_join_is_max_not_sum():
    assert independent_join_moments([Moments(0.1), Moments(0.2)]) == Moments(0.2)


def test_variable_join_is_between_max_means_and_sum():
    join = independent_join_moments([Moments(0.1, 0.01), Moments(0.1, 0.01)])
    assert 0.1 < join.mean < 0.2
    assert join.variance > 0.0


def test_single_cpu_queue_recovers_existing_mg1_mean():
    source = _system(rate=2)
    workload = source.get_workload_spec()
    workload["request_classes"]["request"]["calls"] = {}
    workload["traffic_model"]["service_times"].pop("child")
    system = System("single", workload, QueueingNetworkType.OPEN)
    layered = LayeredAnalyticalSystem(system)
    assert layered.get_request_class_mean_latencies() == pytest.approx(
        system.get_request_class_mean_latencies()
    )


def test_child_wait_holds_parent_pool_without_counting_child_twice():
    system = _system(rate=1e-8)
    layered = LayeredAnalyticalSystem(system)
    assert layered.get_request_class_mean_latencies()["request"] == pytest.approx(
        0.32, abs=1e-8
    )
    details = layered.get_layered_model_details()["services"]["parent"]
    assert details["holding_mean_seconds"] == pytest.approx(0.3, abs=1e-8)


def test_parent_occupancy_detects_bottleneck_cpu_misses():
    system = _system(rate=4)
    assert system.solve().get_stability()
    layered = LayeredAnalyticalSystem(system)
    assert not layered.solve().get_stability()
    assert math.isinf(layered.get_request_class_mean_latencies()["request"])
    assert (
        layered.get_layered_model_details()["services"]["parent"]["holding_utilization"]
        > 1
    )


def test_blocking_releases_cpu_but_occupies_pool():
    system = _system(rate=1, blocking=0.9)
    layered = LayeredAnalyticalSystem(system)
    details = layered.get_layered_model_details()["services"]["child"]
    assert details["cpu_utilization"] == pytest.approx(0.2)
    assert details["holding_utilization"] == pytest.approx(1.1)
    assert not layered.solve().get_stability()


def test_independent_replica_queues_do_not_become_one_pooled_queue():
    system = _system(rate=3, queue="independent_random")
    system.configure_system_replicas((2, 2))
    independent = LayeredAnalyticalSystem(system).get_request_class_mean_latencies()[
        "request"
    ]
    workload = copy.deepcopy(system.get_workload_spec())
    workload["traffic_model"]["service_times"]["parent"][
        "replica_queue_model"
    ] = "pooled"
    pooled = System("pooled", workload, QueueingNetworkType.OPEN)
    pooled.configure_system_replicas((2, 2))
    assert (
        independent
        > LayeredAnalyticalSystem(pooled).get_request_class_mean_latencies()["request"]
    )


def test_parallel_join_follows_critical_path_at_low_load():
    base = _system(rate=1e-8)
    workload = base.get_workload_spec()
    workload["request_classes"]["request"]["call_execution"] = "parallel"
    workload["request_classes"]["request"]["calls"]["parent"]["other"] = 1
    workload["traffic_model"]["service_times"]["other"] = {
        "distribution": "deterministic",
        "mean": 0.15,
    }
    layered = LayeredAnalyticalSystem(
        System("parallel", workload, QueueingNetworkType.OPEN)
    )
    assert layered.get_request_class_mean_latencies()["request"] == pytest.approx(
        0.32, abs=1e-7
    )


def test_fractional_parallel_calls_abstain_instead_of_inventing_membership():
    system = _system(mode="parallel")
    workload = system.get_workload_spec()
    workload["request_classes"]["request"]["calls"]["parent"]["child"] = 0.5
    with pytest.raises(ValueError, match="fork membership"):
        LayeredAnalyticalSystem(System("fraction", workload, QueueingNetworkType.OPEN))


def test_exact_search_can_cross_layered_unstable_cpu_stable_states():
    system = _system(rate=4)
    result = optimize_with_analytical_model(
        system, maximum_replicas=6, analytical_semantics="experimental-layered"
    )
    assert result.metrics.feasible
    assert (
        result.metrics.evaluation_basis == "EXPERIMENTAL_LAYERED_MOMENT_APPROXIMATION"
    )
    assert result.configuration[system.get_service("parent").get_id()] > 1
    assert result.global_optimal


def test_deadline_still_rejected_in_experimental_mode():
    workload = _system().get_workload_spec()
    workload["request_classes"]["request"]["end_to_end_timeout_seconds"] = 1
    system = System("deadline", workload, QueueingNetworkType.OPEN)
    with pytest.raises(ValueError, match="terminal timeouts"):
        optimize_with_analytical_model(
            system, maximum_replicas=3, analytical_semantics="experimental-layered"
        )

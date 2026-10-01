import copy

import pytest

from performance_models import QueueingNetworkType, System, parse_workload_spec
from analytical_workflow import optimize_with_analytical_model


def _workload(*, blocking_mean=0.3, concurrency=2):
    service = {
        "execution_model": "processor_sharing",
        "concurrency_limit_per_replica": concurrency,
        "distribution": "deterministic",
        "mean": 0.1,
    }
    if blocking_mean is not None:
        service["non_cpu_blocking_time"] = {
            "distribution": "deterministic",
            "mean": blocking_mean,
        }
    return {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "end_to_end_latency_slos": {"mean_seconds": 2.0},
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "deterministic", "rate": 1.0}}
            },
            "service_times": {"api": service},
        },
    }


def test_strict_analytical_optimizer_rejects_blocking_resource_semantics():
    system = System("blocking", _workload(), QueueingNetworkType.OPEN)

    with pytest.raises(ValueError, match="non-CPU blocking"):
        optimize_with_analytical_model(system, maximum_replicas=2)


@pytest.mark.parametrize(
    "blocking",
    (
        0.0,
        -0.1,
        {"distribution": "deterministic", "mean": 0.0},
        {"unknown": {"distribution": "deterministic", "mean": 0.1}},
    ),
)
def test_invalid_non_cpu_blocking_time_is_rejected(blocking):
    workload = _workload()
    workload["traffic_model"]["service_times"]["api"]["non_cpu_blocking_time"] = (
        copy.deepcopy(blocking)
    )

    with pytest.raises(ValueError, match="non_cpu_blocking_time"):
        parse_workload_spec(workload)


def test_blocking_concurrency_is_part_of_the_stability_lower_bound():
    system = System(
        "blocking-stability",
        _workload(blocking_mean=3.0, concurrency=2),
        QueueingNetworkType.OPEN,
    )

    # CPU utilization is only 0.1, but one replica offers 3.1 concurrency
    # units to a pool of size 2.
    assert system.solve().get_mean_utilizations() == pytest.approx([0.1])
    assert system.get_concurrency_utilization_lower_bounds() == pytest.approx([1.55])
    assert system.solve().get_stability() is False
    assert system.get_minimum_stable_replicas() == [2]

    system.configure_system_replicas((2,))
    assert system.solve().get_stability() is True

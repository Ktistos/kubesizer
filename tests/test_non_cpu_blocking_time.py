import copy

import pytest

from calibration.profile import experiment_fingerprints
from native_backend import get_native_backend
from performance_models import QueueingNetworkType, System, parse_workload_spec
from statistical_workflow import optimize_with_analytical_model


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
                "request": {
                    "api": {"distribution": "deterministic", "rate": 1.0}
                }
            },
            "service_times": {"api": service},
        },
    }


def test_non_cpu_blocking_time_is_parsed_exposed_and_serialized():
    workload = _workload()
    parsed = parse_workload_spec(workload)
    system = System("blocking", workload, QueueingNetworkType.OPEN)

    blocking = parsed.traffic_model.service_blocking_times["api"]
    assert blocking.mean == pytest.approx(0.3)
    assert (
        parsed.traffic_model.service_blocking_times_by_request_class["request"][
            "api"
        ]
        is blocking
    )
    assert system.get_service_blocking_time_distribution_specs()["api"].mean == (
        pytest.approx(0.3)
    )
    serialized = get_native_backend().serialize_system(system)
    assert serialized["services"][0]["blocking_time_distributions"] == [
        {"distribution": "deterministic", "mean": 0.3, "scv": 0.0}
    ]


def test_class_specific_non_cpu_blocking_time_can_be_omitted_for_other_classes():
    workload = _workload()
    workload["request_classes"] = {
        "read": {"concurrency_weight": 0.5, "routing": {}},
        "write": {"concurrency_weight": 0.5, "routing": {}},
    }
    workload["traffic_model"]["entry_services"] = {
        request_class: {
            "api": {"distribution": "deterministic", "rate": 1.0}
        }
        for request_class in ("read", "write")
    }
    workload["traffic_model"]["service_times"]["api"][
        "non_cpu_blocking_time"
    ] = {
        "write": {"distribution": "deterministic", "mean": 0.4},
    }

    parsed = parse_workload_spec(workload)
    system = System("class-blocking", workload, QueueingNetworkType.OPEN)

    assert parsed.traffic_model.service_blocking_times == {}
    assert parsed.traffic_model.service_blocking_times_by_request_class["read"] == {}
    assert parsed.traffic_model.service_blocking_times_by_request_class["write"][
        "api"
    ].mean == pytest.approx(0.4)
    assert get_native_backend().serialize_system(system)["services"][0][
        "blocking_time_distributions"
    ][0] is None


def test_blocking_time_changes_workload_fingerprint_and_analytical_mean():
    without = _workload(blocking_mean=None)
    with_blocking = _workload(blocking_mean=0.3)
    without_system = System("without", without, QueueingNetworkType.OPEN)
    blocking_system = System(
        "with-blocking",
        with_blocking,
        QueueingNetworkType.OPEN,
    )

    assert experiment_fingerprints(without_system)[0] == (
        experiment_fingerprints(blocking_system)[0]
    )
    assert experiment_fingerprints(without_system)[1] != (
        experiment_fingerprints(blocking_system)[1]
    )
    assert blocking_system.get_request_class_mean_latencies()["request"] == (
        pytest.approx(
            without_system.get_request_class_mean_latencies()["request"] + 0.3
        )
    )


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
    workload["traffic_model"]["service_times"]["api"][
        "non_cpu_blocking_time"
    ] = copy.deepcopy(blocking)

    with pytest.raises(ValueError, match="non_cpu_blocking_time"):
        parse_workload_spec(workload)


def test_cpu_capacity_is_released_while_blocking_but_concurrency_is_held():
    system = System(
        "blocking-capacity",
        _workload(blocking_mean=3.0, concurrency=2),
        QueueingNetworkType.OPEN,
    )

    # Invoke the kernel directly because this intentionally overloaded finite
    # prefix is a transition fixture, not a valid steady-state experiment.
    result = get_native_backend().measurement_replications(
        system,
        warmup_duration=0.0,
        measurement_duration=2.5,
        seeds=(15001,),
        workers=1,
        maximum_drain_time=20.0,
    )[0]

    # Deterministic arrivals occur at t=1 and t=2. The first visit releases CPU
    # at 1.1 while retaining one of two concurrency slots, so the second visit
    # completes its CPU work at 2.1 instead of waiting for the first I/O delay.
    assert result.request_class_latencies["request"] == pytest.approx((3.1, 3.1))


def test_blocking_time_queues_when_the_service_concurrency_pool_is_full():
    system = System(
        "blocking-concurrency",
        _workload(blocking_mean=2.0, concurrency=1),
        QueueingNetworkType.OPEN,
    )

    result = get_native_backend().measurement_replications(
        system,
        warmup_duration=0.0,
        measurement_duration=2.5,
        seeds=(15002,),
        workers=1,
        maximum_drain_time=20.0,
    )[0]

    assert result.request_class_latencies["request"] == pytest.approx((2.1, 3.2))


def test_blocking_concurrency_is_part_of_the_stability_lower_bound():
    system = System(
        "blocking-stability",
        _workload(blocking_mean=3.0, concurrency=2),
        QueueingNetworkType.OPEN,
    )

    # CPU utilization is only 0.1, but one replica offers 3.1 concurrency
    # units to a pool of size 2.
    assert system.solve().get_mean_utilizations() == pytest.approx([0.1])
    assert system.get_concurrency_utilization_lower_bounds() == pytest.approx(
        [1.55]
    )
    assert system.solve().get_stability() is False
    assert system.get_minimum_stable_replicas() == [2]

    system.configure_system_replicas((2,))
    assert system.solve().get_stability() is True

"""Quota conservation, burst completion, replica isolation and strict abstention."""

import copy

import pytest

from calibration.profile import experiment_fingerprints
from native_backend import get_native_backend
from native_backend.backend import NativeBackendError
from performance_models import QueueingNetworkType, System, parse_workload_spec


def workload(*, demand=0.06, quota=0.1, parallelism=2, classes=2):
    return {
        "request_classes": {
            f"r{i}": {"concurrency_weight": 1 / classes, "routing": {}}
            for i in range(classes)
        },
        "traffic_model": {
            "entry_services": {
                f"r{i}": {"cpu": {"distribution": "deterministic", "rate": 1}}
                for i in range(classes)
            },
            "service_times": {
                "cpu": {
                    "execution_model": "processor_sharing",
                    "replica_queue_model": "independent_round_robin",
                    # Existing service-time units are CPU work divided by the
                    # long-run cores available to a replica (quota / period).
                    "distribution": "deterministic",
                    "mean": demand / (quota / 0.1),
                    "cpu_bandwidth": {
                        "quota_seconds": quota,
                        "period_seconds": 0.1,
                        "parallelism": parallelism,
                    },
                }
            },
        },
    }


def simulate(specification, replicas=1):
    system = System("cpu-budget", specification, QueueingNetworkType.OPEN)
    system.configure_system_replicas((replicas,))
    result = system.simulate_measurement_replications(
        warmup_duration=0,
        measurement_duration=1.001,
        maximum_drain_time=1,
        seeds=(701,),
    )[0]
    assert result.tagged_requests_in_flight_at_end == 0
    return result


@pytest.mark.parametrize(
    "demand,quota,parallelism,expected",
    [
        (0.02, 0.1, 2, 0.02),  # Parallel progress before the budget is exhausted.
        (0.06, 0.1, 2, 0.11),  # Exhaust, wait for refill, then finish.
        (0.06, 0.05, 2, 0.21),  # Fractional CPU, correctly normalized work.
        (0.16, 0.1, 2, 0.31),  # Several refills without losing CPU work.
        (0.06, 0.1, 1, 0.12),  # A single physical CPU gives continuous PS here.
    ],
)
def test_known_simultaneous_completions(demand, quota, parallelism, expected):
    result = simulate(workload(demand=demand, quota=quota, parallelism=parallelism))
    for values in result.request_class_latencies.values():
        assert values == pytest.approx((expected,), abs=1e-9)


def test_each_replica_has_its_own_budget():
    result = simulate(workload(classes=4), replicas=2)
    assert all(
        v == pytest.approx((0.11,)) for v in result.request_class_latencies.values()
    )


def test_opt_out_preserves_continuous_processor_sharing():
    value = workload()
    del value["traffic_model"]["service_times"]["cpu"]["cpu_bandwidth"]
    result = simulate(value)
    assert all(
        v == pytest.approx((0.12,)) for v in result.request_class_latencies.values()
    )


def test_earlier_completion_invalidates_old_budget_events():
    spec = workload()
    service = spec["traffic_model"]["service_times"]["cpu"]
    service.pop("distribution")
    service.pop("mean")
    service.update(
        r0={"distribution": "deterministic", "mean": 0.02},
        r1={"distribution": "deterministic", "mean": 0.08},
    )
    result = simulate(spec)
    assert result.request_class_latencies["r0"] == pytest.approx((0.02,))
    assert result.request_class_latencies["r1"] == pytest.approx((0.08,))


def test_blocking_does_not_spend_cpu_budget():
    spec = workload(demand=0.02)
    spec["traffic_model"]["service_times"]["cpu"]["non_cpu_blocking_time"] = {
        "distribution": "deterministic",
        "mean": 0.2,
    }
    result = simulate(spec)
    assert all(
        v == pytest.approx((0.22,)) for v in result.request_class_latencies.values()
    )


def test_parallelism_limit_shares_cpus_before_budget_exhaustion():
    result = simulate(workload(demand=0.02, classes=4, parallelism=2))
    assert all(
        v == pytest.approx((0.04,)) for v in result.request_class_latencies.values()
    )


def test_stages_reusing_a_cpu_share_its_budget():
    spec = workload()
    for request_class in spec["request_classes"].values():
        request_class.pop("routing")
        request_class["stage_workflow"] = {
            "entries": {"cpu": "first"},
            "stages": {
                "first": {
                    "resource": "cpu",
                    "service_time": {
                        "distribution": "deterministic",
                        "mean": 0.03,
                    },
                    "next": "last",
                },
                "last": {
                    "resource": "cpu",
                    "service_time": {
                        "distribution": "deterministic",
                        "mean": 0.03,
                    },
                },
            },
        }
    result = simulate(spec)
    assert all(
        v == pytest.approx((0.11,)) for v in result.request_class_latencies.values()
    )


def test_quota_semantics_abstain_analytically_and_change_calibration_identity():
    first = System("cpu", workload(), QueueingNetworkType.OPEN)
    changed = workload()
    changed["traffic_model"]["service_times"]["cpu"]["cpu_bandwidth"]["parallelism"] = 4
    second = System("cpu", changed, QueueingNetworkType.OPEN)
    assert any(
        "periodic CPU quota" in r
        for r in first.get_strict_analytical_incompatibilities()
    )
    with pytest.raises(ValueError):
        first.get_request_class_mean_latencies()
    assert experiment_fingerprints(first)[0] != experiment_fingerprints(second)[0]
    isolated = first.get_service_cpu_bandwidths()
    isolated["cpu"]["period_seconds"] = 999
    assert first.get_service_cpu_bandwidths()["cpu"]["period_seconds"] == 0.1


@pytest.mark.parametrize(
    "field,value",
    [
        ("quota_seconds", 0),
        ("quota_seconds", 1e-300),
        ("quota_seconds", float("nan")),
        ("quota_seconds", 1),
        ("period_seconds", float("inf")),
        ("period_seconds", 0),
        ("parallelism", True),
        ("parallelism", 0),
        ("parallelism", 4097),
    ],
)
def test_invalid_bandwidth_is_rejected(field, value):
    spec = workload()
    spec["traffic_model"]["service_times"]["cpu"]["cpu_bandwidth"][field] = value
    with pytest.raises(ValueError):
        parse_workload_spec(spec)


@pytest.mark.parametrize(
    "field,value",
    [("execution_model", "fixed_slots"), ("replica_queue_model", "pooled")],
)
def test_conflicting_resource_semantics_are_rejected(field, value):
    spec = workload()
    spec["traffic_model"]["service_times"]["cpu"][field] = value
    with pytest.raises(ValueError, match="independent replica queues"):
        parse_workload_spec(spec)


def test_native_protocol_rejects_invalid_budget_without_python_parser():
    backend = get_native_backend()
    system = System("cpu", workload(), QueueingNetworkType.OPEN)
    payload = copy.deepcopy(backend.serialize_system(system))
    payload["services"][0]["cpu_bandwidth"]["quota_seconds"] = -1
    with pytest.raises(NativeBackendError, match="CPU bandwidth"):
        backend._invoke("measurement_replications", system, serialized_workload=payload)


def test_quota_stays_occupied_after_request_deadline():
    spec = workload(demand=0.16)
    for request_class in spec["request_classes"].values():
        request_class["end_to_end_timeout_seconds"] = 0.08
    # Outstanding visits must survive failure without dangling boundary events.
    result = simulate(spec)
    assert sum(result.failed_measured_request_counts.values()) == 2

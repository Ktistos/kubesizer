"""Deterministic queue, rate, failure and warm/cold resource semantics."""

import pytest

from calibration.profile import experiment_fingerprints
from performance_models import System, QueueingNetworkType


def workload(classes=2, **resource):
    return dict(
        request_classes={
            f"r{i}": dict(concurrency_weight=1 / classes, routing={})
            for i in range(classes)
        },
        traffic_model=dict(
            entry_services={
                f"r{i}": {"worker": dict(distribution="deterministic", rate=1)}
                for i in range(classes)
            },
            service_times={
                "worker": dict(
                    replica_queue_model="independent_round_robin",
                    distribution="deterministic",
                    mean=0.2,
                    **resource,
                )
            },
        ),
    )


def simulate(raw, *, replicas=1, duration=1.001):
    network = System("component", raw, QueueingNetworkType.OPEN)
    network.configure_system_replicas((replicas,))
    return network.simulate_measurement_replications(
        warmup_duration=0,
        measurement_duration=duration,
        maximum_drain_time=5,
        seeds=(321,),
    )[0]


def test_bounded_fifo_rejects_only_the_overflow():
    result = simulate(workload(classes=3, admission_queue={"capacity_per_replica": 1}))
    assert result.request_class_latencies["r0"] == pytest.approx((0.2,))
    assert result.request_class_latencies["r1"] == pytest.approx((0.4,))
    assert result.failed_measured_request_counts == {"r0": 0, "r1": 0, "r2": 1}


def test_independent_replicas_do_not_share_pending_capacity():
    result = simulate(
        workload(classes=4, admission_queue={"capacity_per_replica": 0}), replicas=2
    )
    assert result.failed_measured_request_counts == {"r0": 0, "r1": 0, "r2": 1, "r3": 1}


def test_admission_timeout_does_not_cancel_accepted_work():
    result = simulate(workload(admission_queue={"timeout_seconds": 0.05}))
    assert result.request_class_latencies["r0"] == pytest.approx((0.2,))
    assert result.request_class_latencies["r1"] == pytest.approx((0.05,))
    assert result.failed_measured_request_counts["r1"] == 1


def test_superseded_timeout_survives_visit_slot_reuse():
    raw = workload(admission_queue={"timeout_seconds": 0.5})
    result = simulate(raw, duration=4.001)
    assert all(v == 0 for v in result.failed_measured_request_counts.values())
    assert result.request_class_latencies["r1"] == pytest.approx((0.4,) * 4)


def test_token_bucket_waits_fifo_and_refills_without_a_cpu_queue():
    raw = workload(
        classes=3,
        execution_model="delay",
        rate_limit={"rate_per_second": 2, "burst": 1, "action": "wait"},
    )
    result = simulate(raw)
    for i in range(3):
        assert result.request_class_latencies[f"r{i}"] == pytest.approx(
            (0.2 + 0.5 * i,)
        )


def test_token_bucket_rejects_without_spending_service_work():
    result = simulate(
        workload(rate_limit={"rate_per_second": 2, "burst": 1, "action": "reject"})
    )
    assert result.failed_measured_request_counts == {"r0": 0, "r1": 1}
    assert result.request_class_latencies["r1"] == (0,)


def test_delay_is_not_misinterpreted_as_cpu_capacity():
    raw = workload(classes=3, execution_model="delay")
    raw["traffic_model"]["service_times"]["worker"]["mean"] = 2
    network = System("delay", raw, QueueingNetworkType.OPEN)
    assert network.get_request_class_mean_latencies() == pytest.approx(
        {f"r{i}": 2 for i in range(3)}
    )
    assert network.get_minimum_stable_replicas() == [1]
    result = simulate(raw)
    assert all(
        v == pytest.approx((2,)) for v in result.request_class_latencies.values()
    )


@pytest.mark.parametrize(
    "warm,timeout,expected", [(False, 5, 0.5), (True, 5, 0.2), (True, 0.1, 0.5)]
)
def test_first_invocation_cold_start_depends_on_idle_state(warm, timeout, expected):
    result = simulate(
        workload(
            classes=1,
            lifecycle=dict(
                startup_delay=dict(distribution="deterministic", mean=0.3),
                idle_timeout_seconds=timeout,
                initially_warm=warm,
            ),
        )
    )
    assert result.request_class_latencies["r0"] == pytest.approx((expected,))


def test_instance_cools_after_a_completed_invocation():
    result = simulate(
        workload(
            classes=1,
            lifecycle=dict(
                startup_delay=dict(distribution="deterministic", mean=0.3),
                idle_timeout_seconds=0.1,
                initially_warm=False,
            ),
        ),
        duration=2.001,
    )
    assert result.request_class_latencies["r0"] == pytest.approx((0.5, 0.5))


def test_fixed_failure_probability_marks_terminal_failures():
    result = simulate(workload(failure_probability=1))
    assert all(v == 1 for v in result.failed_measured_request_counts.values())


def test_policies_invalidate_calibration_identity_and_abstain_analytically():
    first = System("a", workload(), QueueingNetworkType.OPEN)
    second = System(
        "a",
        workload(admission_queue={"capacity_per_replica": 0}),
        QueueingNetworkType.OPEN,
    )
    assert experiment_fingerprints(first) != experiment_fingerprints(second)
    assert second.get_strict_analytical_incompatibilities()


@pytest.mark.parametrize(
    "policy",
    [
        {"admission_queue": {"capacity_per_replica": -1}},
        {"rate_limit": {"rate_per_second": 0, "burst": 1, "action": "wait"}},
        {"failure_probability": float("nan")},
        {
            "lifecycle": {
                "startup_delay": {"distribution": "deterministic", "mean": 1},
                "idle_timeout_seconds": 0,
                "initially_warm": True,
            }
        },
    ],
)
def test_invalid_policy_fails_before_simulation(policy):
    with pytest.raises(ValueError):
        System("bad", workload(**policy), QueueingNetworkType.OPEN)

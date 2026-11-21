"""Retries retain timed-out work and job boundaries include background work."""

import copy

import pytest

from performance_models import QueueingNetworkType, System


def workload(
    *,
    attempts=3,
    timeout=0,
    duration=0.2,
    failure=1,
    boundary="client_response",
    execution="fixed_slots"
):
    return {
        "request_classes": {
            "r": {
                "concurrency_weight": 1,
                "completion_boundary": boundary,
                "stage_workflow": {
                    "entries": {"worker": "retry"},
                    "stages": {
                        "retry": {
                            "calls": {"attempt": 1},
                            "retry": {
                                "max_attempts": attempts,
                                "attempt_timeout_seconds": timeout,
                            },
                        },
                        "attempt": {
                            "resource": "worker",
                            "service_time": {
                                "distribution": "deterministic",
                                "mean": duration,
                            },
                        },
                    },
                },
            }
        },
        "traffic_model": {
            "entry_services": {
                "r": {"worker": {"distribution": "deterministic", "rate": 1}}
            },
            "service_times": {
                "worker": {
                    "execution_model": execution,
                    "replica_queue_model": "independent_round_robin",
                    "failure_probability": failure,
                }
            },
        },
    }


def simulate(raw, duration=1.001):
    return System(
        "retry", raw, QueueingNetworkType.OPEN
    ).simulate_measurement_replications(
        warmup_duration=0,
        measurement_duration=duration,
        maximum_drain_time=30,
        seeds=(67,),
    )[
        0
    ]


def test_retries_spend_cpu_on_every_attempt_and_backoff():
    raw = workload()
    raw["request_classes"]["r"]["stage_workflow"]["stages"]["retry"]["retry"][
        "backoff"
    ] = {"distribution": "deterministic", "mean": 0.1}
    result = simulate(raw)
    assert result.request_class_latencies["r"] == pytest.approx((0.8,))
    assert result.failed_measured_request_counts["r"] == 1


@pytest.mark.parametrize(
    "execution,work_latency", [("fixed_slots", 1.0), ("delay", 0.6)]
)
def test_attempt_timeouts_do_not_cancel_old_resource_work(execution, work_latency):
    raw = workload(
        attempts=2, timeout=0.1, duration=0.5, failure=0, execution=execution
    )
    client = simulate(raw)
    assert client.request_class_latencies["r"] == pytest.approx((0.2,))
    assert client.failed_measured_request_counts["r"] == 1
    raw["request_classes"]["r"]["completion_boundary"] = "all_work"
    job = simulate(raw)
    assert job.request_class_latencies["r"] == pytest.approx((work_latency,))
    assert job.failed_measured_request_counts["r"] == 1


def test_old_attempt_deadlines_do_not_touch_reused_visit_slots():
    result = simulate(workload(timeout=10, duration=0.01, failure=0), duration=12.001)
    assert result.request_class_latencies["r"] == pytest.approx((0.01,) * 12)
    assert result.failed_measured_request_counts["r"] == 0


def test_failed_retry_skips_its_success_continuation():
    raw = workload(attempts=1)
    nodes = raw["request_classes"]["r"]["stage_workflow"]["stages"]
    nodes["retry"]["next"] = "only_on_success"
    nodes["only_on_success"] = {
        "resource": "worker",
        "service_time": {"distribution": "deterministic", "mean": 5},
    }
    assert simulate(raw).request_class_latencies["r"] == pytest.approx((0.2,))


def test_successful_retry_runs_its_continuation_once():
    raw = workload(failure=0)
    nodes = raw["request_classes"]["r"]["stage_workflow"]["stages"]
    nodes["retry"]["next"] = "then"
    nodes["then"] = {
        "resource": "worker",
        "service_time": {"distribution": "deterministic", "mean": 0.1},
    }
    assert simulate(raw).request_class_latencies["r"] == pytest.approx((0.3,))


def test_retry_failures_have_the_declared_independent_probability():
    raw = workload(duration=0.001, failure=0.5)
    system = System("retry", raw, QueueingNetworkType.OPEN)
    repeats = system.simulate_measurement_replications(
        warmup_duration=1,
        measurement_duration=100,
        maximum_drain_time=5,
        seeds=tuple(range(20)),
    )
    failures = sum(r.failed_measured_request_counts["r"] for r in repeats)
    counts = sum(r.measured_request_counts["r"] for r in repeats)
    assert failures / counts == pytest.approx(0.5**3, abs=0.025)


@pytest.mark.parametrize(
    "boundary,latency,failures", [("client_response", 0.01, 0), ("all_work", 0.21, 1)]
)
def test_background_job_completion_has_its_own_boundary(boundary, latency, failures):
    raw = {
        "request_classes": {
            "r": {
                "concurrency_weight": 1,
                "completion_boundary": boundary,
                "calls": {"publisher": {"consumer": 1}},
                "call_execution": "parallel",
                "call_policies": {"publisher": {"completion": "background"}},
            }
        },
        "traffic_model": {
            "entry_services": {
                "r": {"publisher": {"distribution": "deterministic", "rate": 1}}
            },
            "service_times": {
                "publisher": {"distribution": "deterministic", "mean": 0.01},
                "consumer": {
                    "distribution": "deterministic",
                    "mean": 0.2,
                    "failure_probability": 1,
                },
            },
        },
    }
    result = simulate(raw)
    assert result.request_class_latencies["r"] == pytest.approx((latency,))
    assert result.failed_measured_request_counts["r"] == failures


@pytest.mark.parametrize(
    "change",
    [
        {"max_attempts": 0},
        {"max_attempts": 101},
        {"max_attempts": 2, "attempt_timeout_seconds": -1},
    ],
)
def test_invalid_retry_policy_is_rejected(change):
    raw = workload()
    raw["request_classes"]["r"]["stage_workflow"]["stages"]["retry"]["retry"] = change
    with pytest.raises(ValueError):
        System("bad", raw, QueueingNetworkType.OPEN)

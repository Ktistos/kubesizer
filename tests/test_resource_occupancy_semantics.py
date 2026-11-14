"""Known-phase fixtures: response duration is not admission-slot occupancy."""

import copy

import pytest

from calibration.profile import experiment_fingerprints
from native_backend import get_native_backend
from optimization import AnalyticalReplicaSizingProblem
from performance_models import QueueingNetworkType, System, parse_workload_spec


def _duration(mean):
    return {"distribution": "deterministic", "mean": mean}


def _workload(*, classes=("request",), services=("api",)):
    return {
        "request_classes": {
            name: {"concurrency_weight": 1.0 / len(classes), "routing": {}}
            for name in classes
        },
        "traffic_model": {
            "entry_services": {
                name: {"api": {"distribution": "deterministic", "rate": 0.5}}
                for name in classes
            },
            "service_times": {
                name: {**_duration(0.1), "concurrency_limit_per_replica": 1}
                for name in services
            },
        },
    }


def _system(workload):
    return System("occupancy", workload, QueueingNetworkType.OPEN)


def _measure(workload, *, seeds=(8123,), duration=2.1):
    return _system(workload).simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=duration,
        seeds=seeds,
        maximum_drain_time=10.0,
    )


@pytest.mark.parametrize("value", [None, {}, {"request": _duration(0.1)}])
def test_post_release_delay_requires_one_service_level_distribution(value):
    workload = _workload()
    workload["traffic_model"]["service_times"]["api"]["post_release_delay"] = value
    with pytest.raises(ValueError, match="post_release_delay"):
        parse_workload_spec(workload)


@pytest.mark.parametrize("mean", [-1.0, 0.0, float("nan"), float("inf"), True])
def test_post_release_delay_rejects_invalid_means(mean):
    workload = _workload()
    workload["traffic_model"]["service_times"]["api"]["post_release_delay"] = _duration(
        mean
    )
    with pytest.raises(ValueError, match="post_release_delay"):
        parse_workload_spec(workload)


def test_post_release_delay_is_serialized_and_invalidates_workload_fingerprint():
    workloads = [_workload() for _ in range(3)]
    for workload, mean in zip(workloads[1:], (0.4, 0.5)):
        workload["traffic_model"]["service_times"]["api"]["post_release_delay"] = (
            _duration(mean)
        )
    systems = [_system(workload) for workload in workloads]
    fingerprints = [experiment_fingerprints(system) for system in systems]
    assert len({value[0] for value in fingerprints}) == 1
    assert len({value[1] for value in fingerprints}) == 3
    backend = get_native_backend()
    assert (
        backend.serialize_system(systems[0])["services"][0]["post_release_delay"]
        is None
    )
    assert (
        backend.serialize_system(systems[1])["services"][0]["post_release_delay"][
            "mean"
        ]
        == 0.4
    )


@pytest.mark.parametrize("execution_model", ["fixed_slots", "processor_sharing"])
@pytest.mark.parametrize("queue_model", ["pooled", "independent_random"])
def test_post_release_delay_frees_own_slot_unlike_blocking(
    execution_model, queue_model
):
    workload = _workload(classes=("left", "right"))
    service = workload["traffic_model"]["service_times"]["api"]
    service.update(execution_model=execution_model, replica_queue_model=queue_model)
    service["post_release_delay"] = _duration(0.4)
    (outside,) = _measure(workload)
    held_workload = copy.deepcopy(workload)
    service = held_workload["traffic_model"]["service_times"]["api"]
    service["non_cpu_blocking_time"] = service.pop("post_release_delay")
    (held,) = _measure(held_workload)

    assert sorted(
        value[0] for value in outside.request_class_latencies.values()
    ) == pytest.approx([0.5, 0.6])
    assert sorted(
        value[0] for value in held.request_class_latencies.values()
    ) == pytest.approx([0.5, 1.0])
    assert outside.tagged_requests_in_flight_at_end == 0


def test_service_latency_includes_released_delay_but_cpu_utilization_does_not():
    workload = _workload(classes=("left", "right"))
    workload["traffic_model"]["service_times"]["api"]["post_release_delay"] = _duration(
        0.4
    )
    result = get_native_backend().simulate(
        _system(workload),
        seed=8123,
        horizon=3.1,
        warmup_duration=0.0,
        capture_state=True,
    )
    assert result["metrics"]["mean_latencies"] == pytest.approx([0.55])
    assert result["metrics"]["mean_utilizations"] == pytest.approx([0.2 / 3.1])
    assert result["busy_replicas_at_end"] == [0]
    assert result["queue_lengths_at_end"] == [0]


def test_child_delay_holds_callers_slot_until_child_responds():
    workload = _workload(classes=("left", "right"), services=("api", "db"))
    for request_class in workload["request_classes"].values():
        del request_class["routing"]
        request_class.update(calls={"api": {"db": 1.0}}, call_execution="sequential")
    service_times = workload["traffic_model"]["service_times"]
    service_times["db"]["post_release_delay"] = _duration(0.4)
    service_times["api"]["post_release_delay"] = _duration(0.05)
    (result,) = _measure(workload)
    # Parent holds its slot for .1 CPU + .1 child CPU + .4 child response.
    # Its own .05 response delay does not keep the next parent queued.
    assert sorted(
        value[0] for value in result.request_class_latencies.values()
    ) == pytest.approx([0.65, 1.25])


@pytest.mark.parametrize("mode,expected", [("sequential", 0.95), ("parallel", 0.65)])
def test_call_execution_includes_delay_once_on_each_response_path(mode, expected):
    workload = _workload(services=("api", "fast", "slow"))
    request = workload["request_classes"]["request"]
    del request["routing"]
    request.update(calls={"api": {"fast": 1.0, "slow": 1.0}}, call_execution=mode)
    for service, mean in (("api", 0.05), ("fast", 0.2), ("slow", 0.4)):
        workload["traffic_model"]["service_times"][service]["post_release_delay"] = (
            _duration(mean)
        )
    (result,) = _measure(workload)
    assert result.request_class_latencies["request"] == pytest.approx([expected])


def test_routing_waits_for_response_before_next_visit_and_adds_external_delay_once():
    workload = _workload(services=("api", "db"))
    request = workload["request_classes"]["request"]
    request.update(
        routing={"api": {"db": 1.0}},
        latency_boundary="external_client",
        latency_components={"client_to_entry": _duration(0.03)},
    )
    for service in workload["traffic_model"]["service_times"].values():
        service["post_release_delay"] = _duration(0.4)
    workload["traffic_model"]["communication_latency"] = {
        "same_node_one_way": _duration(0.01),
        "cross_node_one_way": _duration(0.01),
        "cross_node_probability": 0.5,
    }
    (result,) = _measure(workload)
    assert result.request_class_latencies["request"] == pytest.approx([1.04])


def test_repeated_workflow_visits_each_apply_blocking_and_post_release_delay():
    workload = _workload()
    request = workload["request_classes"]["request"]
    del request["routing"]
    request["workflow"] = [{"service": "api", "mean_visits": 2.0}]
    service = workload["traffic_model"]["service_times"]["api"]
    service["non_cpu_blocking_time"] = _duration(0.2)
    service["post_release_delay"] = _duration(0.4)
    (result,) = _measure(workload, duration=20.1)
    assert result.request_class_latencies["request"] == pytest.approx([1.4] * 10)
    assert result.tagged_requests_in_flight_at_end == 0


@pytest.mark.parametrize("calls", [False, True])
def test_timeout_during_released_delay_preserves_ownership_and_failure_counts(calls):
    workload = _workload(services=("api", "db") if calls else ("api",))
    request = workload["request_classes"]["request"]
    request["end_to_end_timeout_seconds"] = 0.3
    if calls:
        del request["routing"]
        request.update(calls={"api": {"db": 1.0}}, call_execution="sequential")
    service = workload["traffic_model"]["service_times"]["db" if calls else "api"]
    service["post_release_delay"] = _duration(0.4)
    for result in _measure(workload, seeds=(8123, 8124), duration=20.1):
        assert result.measured_request_counts == {"request": 10}
        assert result.failed_measured_request_counts == {"request": 10}
        assert result.request_class_latencies["request"] == pytest.approx([0.3] * 10)
        assert result.tagged_requests_in_flight_at_end == 0


def test_empirical_post_release_delay_is_sampled_reproducibly():
    workload = _workload()
    workload["traffic_model"]["service_times"]["api"]["post_release_delay"] = {
        "distribution": "empirical",
        "samples": [0.2, 0.4],
        "sampling": "iid_with_replacement",
    }
    first = _measure(workload, duration=40.1)
    assert first == _measure(workload, duration=40.1)
    assert {
        round(value, 6) for value in first[0].request_class_latencies["request"]
    } == {0.3, 0.5}


def test_analytical_mean_diagnostic_accounts_for_delay_but_certification_abstains():
    workload = _workload()
    before = _system(workload)
    workload["traffic_model"]["service_times"]["api"]["post_release_delay"] = _duration(
        0.4
    )
    after = _system(workload)
    assert after.get_request_class_mean_latencies()["request"] == pytest.approx(
        before.get_request_class_mean_latencies()["request"] + 0.4
    )
    assert any(
        "post-release" in item
        for item in after.get_strict_analytical_incompatibilities()
    )
    assert before.get_minimum_stable_replicas() == after.get_minimum_stable_replicas()
    assert (
        before.get_concurrency_utilization_lower_bounds()
        == after.get_concurrency_utilization_lower_bounds()
    )
    with pytest.raises(ValueError, match="post-release"):
        AnalyticalReplicaSizingProblem(after, {"request": 1.0}, max_replicas=3)

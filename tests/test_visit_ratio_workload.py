import copy

import pytest

from calibration.profile import experiment_fingerprints
from performance_models import QueueingNetworkType, System, parse_workload_spec
from statistical_workflow import optimize_with_analytical_model


def _visit_workload(*, visits=None, fixed_overhead=0.01):
    if visits is None:
        visits = {
            "api": 1.0,
            "auth": 3.0,
            "search": 0.75,
        }
    return {
        "request_classes": {
            "metadata_read": {
                "concurrency_weight": 1.0,
                "end_to_end_latency_slos": {"mean_seconds": 1.0},
                "visits": visits,
                "fixed_latency_overhead_seconds": fixed_overhead,
            }
        },
        "traffic_model": {
            "entry_services": {
                "metadata_read": {"api": {"distribution": "exponential", "rate": 4.0}}
            },
            "service_times": {
                "api": {"distribution": "exponential", "rate": 20.0},
                "auth": {"distribution": "exponential", "rate": 20.0},
                "search": {"distribution": "exponential", "rate": 10.0},
            },
        },
    }


def test_visit_ratios_support_fanout_and_repeated_calls():
    workload = _visit_workload()
    parsed = parse_workload_spec(workload)
    system = System("visit-ratios", workload, QueueingNetworkType.OPEN)

    assert dict(parsed.request_classes["metadata_read"]) == {}
    assert dict(parsed.request_class_visit_ratios["metadata_read"]) == {
        "api": 1.0,
        "auth": 3.0,
        "search": 0.75,
    }
    assert parsed.request_class_fixed_latency_overheads == {"metadata_read": 0.01}
    assert system.get_request_class_effective_arrival_rates() == {
        "metadata_read": pytest.approx([4.0, 12.0, 3.0])
    }
    assert system.get_effective_arrival_rates() == pytest.approx([4.0, 12.0, 3.0])
    assert system.get_minimum_stable_replicas() == [1, 1, 1]


def test_visit_ratio_mean_is_visit_weighted_and_includes_fixed_overhead():
    system = System(
        "visit-mean",
        _visit_workload(),
        QueueingNetworkType.OPEN,
    )

    service_means = system.solve().get_mean_latencies()
    expected = (
        service_means[0] + 3.0 * service_means[1] + 0.75 * service_means[2] + 0.01
    )

    assert system.get_request_class_mean_latencies() == {
        "metadata_read": pytest.approx(expected)
    }
    assert all(
        not details["exact"]
        for details in system.get_analytical_model_details().values()
    )


def test_visit_ratios_use_the_weighted_external_class_rate():
    workload = {
        "request_classes": {
            "read": {
                "concurrency_weight": 0.25,
                "visits": {"api": 1.0, "auth": 2.0},
            },
            "write": {
                "concurrency_weight": 0.75,
                "routing": {},
            },
        },
        "traffic_model": {
            "entry_services": {
                "read": {"api": {"distribution": "exponential", "rate": 8.0}},
                "write": {"api": {"distribution": "exponential", "rate": 2.0}},
            },
            "service_times": {
                "api": {"distribution": "exponential", "rate": 20.0},
                "auth": {"distribution": "exponential", "rate": 10.0},
            },
        },
    }
    system = System("weighted-visits", workload, QueueingNetworkType.OPEN)

    assert system.get_request_class_effective_arrival_rates() == {
        "read": pytest.approx([2.5, 5.0]),
        "write": pytest.approx([7.5, 0.0]),
    }
    assert system.get_effective_arrival_rates() == pytest.approx([10.0, 5.0])


def test_visit_ratios_and_routing_are_mutually_exclusive():
    workload = _visit_workload()
    workload["request_classes"]["metadata_read"]["routing"] = {}

    with pytest.raises(
        ValueError,
        match="exactly one of routing, visits, workflow, or calls",
    ):
        parse_workload_spec(workload)


def test_fixed_latency_overhead_is_supported_for_routing_workloads():
    workload = _visit_workload()
    request_class = workload["request_classes"]["metadata_read"]
    request_class.pop("visits")
    request_class["routing"] = {}

    parsed = parse_workload_spec(workload)

    assert parsed.request_class_fixed_latency_overheads == {
        "metadata_read": 0.01
    }
    assert parsed.request_class_latency_overhead_specs[
        "metadata_read"
    ].distribution == "deterministic"


@pytest.mark.parametrize(
    ("visits", "message"),
    (
        ({}, "nonempty mapping"),
        ({"api": 1.0, "missing": 1.0}, "unknown services"),
        ({"api": 1.0, "auth": 0.0}, "greater than zero"),
        ({"api": 1.0, "auth": -1.0}, "greater than zero"),
    ),
)
def test_invalid_visit_ratios_fail_explicitly(visits, message):
    workload = _visit_workload(visits=visits)

    with pytest.raises(ValueError, match=message):
        parse_workload_spec(workload)


def test_visit_ratios_must_include_the_external_entry_service():
    workload = _visit_workload(visits={"auth": 3.0})

    with pytest.raises(ValueError, match="external entry service"):
        parse_workload_spec(workload)


def test_visit_only_workload_is_rejected_before_native_simulation():
    system = System(
        "analytical-only-visits",
        _visit_workload(),
        QueueingNetworkType.OPEN,
    )

    with pytest.raises(ValueError, match="Simulation requires sequential routing"):
        system.simulate(10.0)
    with pytest.raises(ValueError, match="Simulation requires sequential routing"):
        system.run_warmup_ensemble(
            seeds=(1,),
            horizon=10.0,
            bucket_width=1.0,
        )
    with pytest.raises(ValueError, match="Simulation requires sequential routing"):
        system.simulate_measurement_replications(
            warmup_duration=1.0,
            measurement_duration=2.0,
            seeds=(1,),
        )


def test_executable_workflow_derives_visits_and_runs_in_native_backend():
    workload = _visit_workload(fixed_overhead=0.1)
    request_class = workload["request_classes"]["metadata_read"]
    del request_class["visits"]
    request_class["workflow"] = [
        {"service": "api"},
        {"service": "auth", "mean_visits": 2.5},
        {"service": "search", "mean_visits": 0.75},
    ]
    workload["traffic_model"]["entry_services"]["metadata_read"]["api"] = {
        "distribution": "deterministic",
        "rate": 1.0,
    }
    workload["traffic_model"]["service_times"] = {
        service: {"distribution": "deterministic", "mean": 0.01}
        for service in ("api", "auth", "search")
    }

    parsed = parse_workload_spec(workload)
    system = System("workflow", workload, QueueingNetworkType.OPEN)
    replication = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=20.5,
        seeds=(101,),
    )[0]

    assert dict(parsed.request_class_visit_ratios["metadata_read"]) == {
        "api": 1.0,
        "auth": 2.5,
        "search": 0.75,
    }
    assert tuple(
        (visit.service, visit.mean_visits)
        for visit in parsed.request_class_workflows["metadata_read"]
    ) == (("api", 1.0), ("auth", 2.5), ("search", 0.75))
    assert replication.measured_request_counts == {"metadata_read": 20}
    assert replication.completed_measured_request_counts == {
        "metadata_read": 20
    }
    assert all(
        any(latency == pytest.approx(expected) for expected in (0.13, 0.14, 0.15))
        for latency in replication.request_class_latencies["metadata_read"]
    )


def test_workflow_must_start_at_its_only_external_entry_service():
    workload = _visit_workload()
    request_class = workload["request_classes"]["metadata_read"]
    del request_class["visits"]
    request_class["workflow"] = [{"service": "auth"}]

    with pytest.raises(ValueError, match="workflow must start"):
        parse_workload_spec(workload)


def test_analytical_optimizer_uses_visit_ratio_load_and_latency():
    workload = _visit_workload(
        visits={"api": 1.0, "auth": 3.0},
        fixed_overhead=0.0,
    )
    workload["request_classes"]["metadata_read"]["end_to_end_latency_slos"] = {
        "mean_seconds": 0.35
    }
    system = System("visit-optimization", workload, QueueingNetworkType.OPEN)

    result = optimize_with_analytical_model(
        system,
        maximum_replicas=4,
    )

    assert result.configuration == (1, 2)
    assert dict(result.metrics.request_class_mean_latencies)["metadata_read"] <= 0.35


def test_visits_and_fixed_overhead_change_experiment_fingerprints():
    original = _visit_workload()
    changed_visits = copy.deepcopy(original)
    changed_visits["request_classes"]["metadata_read"]["visits"]["auth"] = 2.5
    changed_overhead = copy.deepcopy(original)
    changed_overhead["request_classes"]["metadata_read"][
        "fixed_latency_overhead_seconds"
    ] = 0.02

    original_fingerprints = experiment_fingerprints(
        System("original", original, QueueingNetworkType.OPEN)
    )
    visit_fingerprints = experiment_fingerprints(
        System("changed-visits", changed_visits, QueueingNetworkType.OPEN)
    )
    overhead_fingerprints = experiment_fingerprints(
        System("changed-overhead", changed_overhead, QueueingNetworkType.OPEN)
    )

    assert original_fingerprints[0] != visit_fingerprints[0]
    assert original_fingerprints[1] == visit_fingerprints[1]
    assert original_fingerprints[0] == overhead_fingerprints[0]
    assert original_fingerprints[1] != overhead_fingerprints[1]

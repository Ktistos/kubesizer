import copy

import pytest

from calibration.profile import experiment_fingerprints
from performance_models import QueueingNetworkType, System, parse_workload_spec
from optimization import OptimizationMode
from statistical_workflow import optimize_with_analytical_model


def _workload():
    return {
        "request_classes": {
            "fast": {
                "concurrency_weight": 0.5,
                "visits": {"api": 1.0},
            },
            "slow": {
                "concurrency_weight": 0.5,
                "visits": {"api": 1.0},
            },
        },
        "traffic_model": {
            "entry_services": {
                "fast": {
                    "api": {"distribution": "exponential", "rate": 1.0}
                },
                "slow": {
                    "api": {"distribution": "exponential", "rate": 1.0}
                },
            },
            "service_times": {
                "api": {
                    "fast": {"distribution": "deterministic", "mean": 0.1},
                    "slow": {"distribution": "deterministic", "mean": 0.3},
                }
            },
        },
    }


def test_parser_accepts_explicit_request_class_service_distributions():
    parsed = parse_workload_spec(_workload())

    by_class = parsed.traffic_model.service_times_by_request_class
    assert by_class["fast"]["api"].mean == pytest.approx(0.1)
    assert by_class["slow"]["api"].mean == pytest.approx(0.3)


def test_multiclass_mixture_uses_workload_for_stability_and_variability():
    system = System("multiclass", _workload(), QueueingNetworkType.OPEN)

    metrics = system.solve()

    assert metrics.get_mean_utilizations() == pytest.approx([0.4])
    assert system.get_service_time_scvs() == pytest.approx([0.25])
    assert system.get_minimum_stable_replicas() == [1]
    assert system.get_analytical_model_details()["api"]["model"].startswith("G/G/c")


def test_each_class_gets_its_own_service_mean_and_the_shared_fcfs_wait():
    system = System("multiclass", _workload(), QueueingNetworkType.OPEN)

    latencies = system.get_request_class_mean_latencies()

    # Mixture mean=0.2, mixture SCV=0.25, ca^2=1 and rho=0.4.
    # Kingman wait = ((1 + .25)/2) * .2 * .4 / .6 = 1/12.
    assert latencies["fast"] == pytest.approx(0.1 + 1.0 / 12.0)
    assert latencies["slow"] == pytest.approx(0.3 + 1.0 / 12.0)


def test_missing_reachable_class_service_distribution_fails():
    workload = _workload()
    del workload["traffic_model"]["service_times"]["api"]["slow"]

    with pytest.raises(ValueError, match="no service-time distribution"):
        parse_workload_spec(workload)


def test_native_simulation_uses_request_class_specific_service_times():
    workload = _workload()
    for request_class in workload["request_classes"].values():
        request_class["workflow"] = [{"service": "api"}]
        del request_class["visits"]
    for streams in workload["traffic_model"]["entry_services"].values():
        streams["api"] = {"distribution": "deterministic", "rate": 1.0}
    system = System("multiclass", workload, QueueingNetworkType.OPEN)
    system.get_service("api").set_replicas(2)

    replications = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=5.5,
        seeds=(17,),
    )

    assert replications[0].request_class_latencies["fast"] == pytest.approx(
        (0.1,) * 5
    )
    assert replications[0].request_class_latencies["slow"] == pytest.approx(
        (0.3,) * 5
    )


def test_analytical_optimizer_enforces_every_class_specific_mean_slo():
    workload = _workload()
    workload["request_classes"]["fast"]["end_to_end_latency_slos"] = {
        "mean_seconds": 0.2
    }
    workload["request_classes"]["slow"]["end_to_end_latency_slos"] = {
        "mean_seconds": 0.35
    }
    system = System("multiclass", workload, QueueingNetworkType.OPEN)

    result = optimize_with_analytical_model(
        system,
        maximum_replicas=3,
        mode=OptimizationMode.EXACT,
    )

    assert result.configuration == (2,)
    latencies = dict(result.metrics.request_class_mean_latencies)
    assert latencies["fast"] <= 0.2
    assert latencies["slow"] <= 0.35


def test_class_specific_service_demand_changes_workload_fingerprint():
    original = _workload()
    changed = copy.deepcopy(original)
    changed["traffic_model"]["service_times"]["api"]["slow"]["mean"] = 0.31

    original_fingerprints = experiment_fingerprints(
        System("original", original, QueueingNetworkType.OPEN)
    )
    changed_fingerprints = experiment_fingerprints(
        System("changed", changed, QueueingNetworkType.OPEN)
    )

    assert original_fingerprints[0] == changed_fingerprints[0]
    assert original_fingerprints[1] != changed_fingerprints[1]

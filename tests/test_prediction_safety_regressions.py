"""Regressions for the prediction/safety defects found in the repository review."""

import math
import statistics
from itertools import accumulate
from pathlib import Path

import pytest
import yaml

from characterization import (
    build_analytical_workload_variants,
    build_characterized_system_variants,
    characterize_deployment,
    parse_portable_sizing_inputs,
)
from characterization.simulation import _downstream_response_mean
from optimization import AnalyticalReplicaSizingProblem
from performance_models import AnalyticalQueueingModel, QueueingNetworkType, System
from predictive_validation import (
    PredictiveValidationCase,
    PredictiveValidationOptions,
    PredictiveValidationStatus,
    _model_structure_fingerprint,
    validate_simulator_predictions,
)
from statistical_workflow import optimize_with_analytical_model


def _system(*, service=None, arrival=None, replicas=1, slo=None):
    request = {"concurrency_weight": 1.0, "routing": {}}
    if slo is not None:
        request["end_to_end_latency_slos"] = {"mean_seconds": slo}
    service_spec = {
        "distribution": "deterministic",
        "mean": 0.01,
        "replica_queue_model": "pooled",
        **(service or {}),
    }
    if service_spec["distribution"] == "exponential":
        service_spec["rate"] = 1.0 / service_spec.pop("mean")
    system = System(
        "safety-regression",
        {
            "request_classes": {"r": request},
            "traffic_model": {
                "entry_services": {
                    "r": {
                        "api": arrival or {"distribution": "exponential", "rate": 60.0}
                    }
                },
                "service_times": {"api": service_spec},
            },
        },
        QueueingNetworkType.OPEN,
    )
    system.configure_system_replicas((replicas,))
    return system


@pytest.mark.parametrize("block_duration", [0.5, 9.0, 10.0, 60.0, 1000.0, 1e300])
@pytest.mark.parametrize("gaps", [[1.0, 9.0] * 100, [0.0, 0.0, 1.0, 9.0] * 50])
def test_trace_bootstrap_preserves_offered_rate_across_block_lengths(
    block_duration, gaps
):
    system = _system(
        arrival={
            "distribution": "arrival_trace",
            "timestamps": list(accumulate(gaps, initial=0.0)),
            "sampling": "block_bootstrap",
            "block_duration": block_duration,
        }
    )
    seeds = (101, 102, 103, 104)
    horizon = 50_000.0
    ensemble = system.run_warmup_ensemble(
        seeds=seeds,
        horizon=horizon,
        bucket_width=1000.0,
        workers=2,
    )
    declared_rate = system.get_external_arrival_distribution_specs()["r"]["api"].rate
    generated_rate = ensemble.external_arrival_count / (len(seeds) * horizon)
    assert generated_rate == pytest.approx(declared_rate, rel=0.02)


@pytest.mark.parametrize(
    "shape",
    [
        {"distribution": "deterministic"},
        {"distribution": "gamma", "scv": 4.0},
        {"distribution": "lognormal", "scv": 2.0},
    ],
)
@pytest.mark.parametrize("queue", ["pooled", "independent_random"])
@pytest.mark.parametrize("replicas", [1, 2])
def test_poisson_processor_sharing_uses_insensitive_mean(shape, queue, replicas):
    system = _system(
        service={
            **shape,
            "execution_model": "processor_sharing",
            "replica_queue_model": queue,
        },
        arrival={"distribution": "exponential", "rate": 60.0 * replicas},
        replicas=replicas,
    )
    expected = 0.015625 if replicas == 2 and queue == "pooled" else 0.025
    assert (
        system.get_analytical_queueing_models()["api"] is AnalyticalQueueingModel.MGC_PS
    )
    assert system.get_strict_analytical_incompatibilities() == ()
    assert system.get_request_class_mean_latencies()["r"] == pytest.approx(expected)


@pytest.mark.parametrize(
    "queue, expected", [("pooled", 0.015625), ("independent_random", 0.025)]
)
@pytest.mark.parametrize(
    "shape",
    [
        {"distribution": "deterministic"},
        {"distribution": "gamma", "scv": 4.0},
    ],
)
def test_multireplica_ps_native_mean_matches_corrected_analytical_model(
    queue, expected, shape
):
    system = _system(
        service={
            **shape,
            "execution_model": "processor_sharing",
            "replica_queue_model": queue,
        },
        arrival={"distribution": "exponential", "rate": 120.0},
        replicas=2,
    )
    records = system.simulate_measurement_replications(
        warmup_duration=50.0,
        measurement_duration=200.0,
        seeds=tuple(range(73101, 73109)),
        workers=2,
        maximum_drain_time=30.0,
    )
    means = [
        statistics.fmean(record.request_class_latencies["r"]) for record in records
    ]
    se = statistics.stdev(means) / math.sqrt(len(means))
    assert statistics.fmean(means) == pytest.approx(
        expected, abs=max(4 * se, 0.04 * expected)
    )


def test_processor_sharing_no_longer_accepts_unsafe_single_replica():
    system = _system(service={"execution_model": "processor_sharing"}, slo=0.020)
    result = optimize_with_analytical_model(system, maximum_replicas=3)
    assert result.configuration == (2,)


def test_processor_sharing_scales_latency_by_each_class_cpu_work():
    workload = {
        "request_classes": {
            name: {"concurrency_weight": 0.5, "routing": {}}
            for name in ("short", "long")
        },
        "traffic_model": {
            "entry_services": {
                name: {"api": {"distribution": "exponential", "rate": 20.0}}
                for name in ("short", "long")
            },
            "service_times": {
                "api": {
                    "execution_model": "processor_sharing",
                    "replica_queue_model": "pooled",
                    "short": {"distribution": "deterministic", "mean": 0.005},
                    "long": {"distribution": "deterministic", "mean": 0.025},
                }
            },
        },
    }
    system = System("class-ps", workload, QueueingNetworkType.OPEN)
    # Aggregate rho=.6; both classes have slowdown 1/(1-rho)=2.5.
    expected = {"short": 0.0125, "long": 0.0625}
    assert system.get_request_class_mean_latencies() == pytest.approx(expected)
    records = system.simulate_measurement_replications(
        warmup_duration=50,
        measurement_duration=300,
        seeds=tuple(range(73101, 73109)),
        workers=2,
        maximum_drain_time=30,
    )
    for name, mean in expected.items():
        values = [
            statistics.fmean(record.request_class_latencies[name]) for record in records
        ]
        se = statistics.stdev(values) / math.sqrt(len(values))
        assert statistics.fmean(values) == pytest.approx(
            mean, abs=max(4 * se, 0.04 * mean)
        )


def test_independent_random_replica_retains_its_own_processing_slots():
    system = _system(
        service={
            "distribution": "exponential",
            "mean": 1.0,
            "processing_slots_per_replica": 2,
            "replica_queue_model": "independent_random",
        },
        arrival={"distribution": "exponential", "rate": 3.0},
        replicas=2,
    )
    assert system.get_request_class_mean_latencies()["r"] == pytest.approx(16.0 / 7.0)


@pytest.mark.parametrize(
    "queue, expected, replicas_needed",
    [
        ("pooled", 16.0 / 7.0, 2),
        ("independent_random", 4.0, 3),
        ("independent_round_robin", 3.25, 3),
    ],
)
def test_replica_dispatch_changes_latency_and_sizing(queue, expected, replicas_needed):
    system = _system(
        service={
            "distribution": "exponential",
            "mean": 1.0,
            "replica_queue_model": queue,
        },
        arrival={"distribution": "exponential", "rate": 1.5},
        replicas=2,
        slo=3.0,
    )
    assert system.get_request_class_mean_latencies()["r"] == pytest.approx(expected)
    if queue == "independent_round_robin":
        assert not system.get_analytical_model_details()["api"]["exact"]
    result = optimize_with_analytical_model(system, maximum_replicas=4)
    assert result.configuration == (replicas_needed,)


@pytest.mark.parametrize(
    "service, arrival",
    [
        (
            {"concurrency_limit_per_replica": 1},
            {"distribution": "exponential", "rate": 1.0},
        ),
        ({}, {"distribution": "deterministic", "rate": 1.0}),
        (
            {"replica_queue_model": "independent_round_robin"},
            {"distribution": "exponential", "rate": 1.0},
        ),
    ],
)
def test_unsupported_ps_semantics_cannot_be_certified(service, arrival):
    system = _system(
        service={"execution_model": "processor_sharing", **service}, arrival=arrival
    )
    assert system.get_strict_analytical_incompatibilities()
    assert math.isinf(system.get_request_class_mean_latencies()["r"])
    with pytest.raises(ValueError, match="cannot certify"):
        optimize_with_analytical_model(system, maximum_replicas=3)
    with pytest.raises(ValueError, match="cannot certify"):
        AnalyticalReplicaSizingProblem(system, {}, max_replicas=3)
    # Unsupported analytical latency must not prevent native simulation.
    (record,) = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=10.0,
        seeds=(91,),
    )
    assert record.request_class_latencies["r"]


@pytest.mark.parametrize("local_calls", [{}, {"front": {"child": 0.0}}])
def test_downstream_average_includes_local_only_visits(local_calls):
    downstream, issue = _downstream_response_mean(
        "front",
        class_calls={"local": local_calls, "remote": {"front": {"child": 1.0}}},
        class_modes={"local": "parallel", "remote": "sequential"},
        visits_by_class={
            "local": {"front": 1.0},
            "remote": {"front": 1.0, "child": 1.0},
        },
        observed_class_rates={"local": 10.0, "remote": 10.0},
        service_response_means={"child": 0.1},
        communication_round_trip_mean=0.0,
    )
    assert issue is None
    assert downstream == pytest.approx(0.05)


def test_analytical_characterization_preserves_explicit_blocking_and_refuses_sizing():
    base = Path(__file__).parents[1] / "examples" / "portable_evidence"
    workload = yaml.safe_load((base / "workload.yaml").read_text())
    workload["service_blocking_times"] = {
        "api": {"distribution": "deterministic", "mean": 0.001},
        "database": {"distribution": "deterministic", "mean": 1.0},
    }
    inputs = parse_portable_sizing_inputs(
        workload,
        yaml.safe_load((base / "measurements.yaml").read_text()),
        base_path=base,
    )
    characterization = characterize_deployment(inputs)
    (variant,) = build_analytical_workload_variants(inputs, characterization)
    assert (
        variant.workload["traffic_model"]["service_times"]["database"][
            "non_cpu_blocking_time"
        ]["mean"]
        == 1.0
    )
    ((_, system),) = build_characterized_system_variants(
        inputs, characterization, evaluation_mode="analytical"
    )
    with pytest.raises(ValueError, match="non-CPU blocking"):
        optimize_with_analytical_model(system, maximum_replicas=10)


def _validation_case(name, system):
    return PredictiveValidationCase.from_mappings(
        name,
        system,
        tuple(system.get_system_replicas()),
        {"r": 0.01},
        observed_request_class_error_rates={"r": 0.0},
    )


@pytest.mark.parametrize(
    "original, changed",
    [
        ({"concurrency_limit_per_replica": 1}, {"concurrency_limit_per_replica": 100}),
        (
            {
                "execution_model": "processor_sharing",
                "concurrency_limit_per_replica": 1,
            },
            {"execution_model": "processor_sharing"},  # Unlimited, not a limit of one.
        ),
        (
            {"processing_slots_per_replica": 1, "concurrency_limit_per_replica": 3},
            {"processing_slots_per_replica": 2, "concurrency_limit_per_replica": 3},
        ),
    ],
)
def test_prediction_validation_rejects_changed_per_replica_limits(original, changed):
    arrival = {"distribution": "deterministic", "rate": 1.0}
    target = _system(service=original, arrival=arrival)
    supporting = _system(service=changed, arrival=arrival)
    assert _model_structure_fingerprint(target) != _model_structure_fingerprint(
        supporting
    )
    result = validate_simulator_predictions(
        target,
        [
            _validation_case("target", target),
            _validation_case("wrong-limits", supporting),
        ],
        options=PredictiveValidationOptions(
            warmup_duration=0.0,
            measurement_duration=10.5,
            replication_count=3,
            bootstrap_resample_count=200,
        ),
    )
    assert result.status is PredictiveValidationStatus.REJECTED


def test_prediction_validation_allows_replica_changes_with_identical_per_replica_limits():
    arrival = {"distribution": "deterministic", "rate": 1.0}
    service = {"concurrency_limit_per_replica": 3, "processing_slots_per_replica": 2}
    target = _system(service=service, arrival=arrival)
    supporting = _system(service=service, arrival=arrival, replicas=2)
    assert _model_structure_fingerprint(target) == _model_structure_fingerprint(
        supporting
    )
    result = validate_simulator_predictions(
        target,
        [
            _validation_case("target", target),
            _validation_case("extra-replica", supporting),
        ],
        options=PredictiveValidationOptions(
            warmup_duration=0.0,
            measurement_duration=10.5,
            replication_count=3,
            bootstrap_resample_count=200,
        ),
    )
    assert result.status is PredictiveValidationStatus.VALIDATED

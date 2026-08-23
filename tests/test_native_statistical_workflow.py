from calibration import CalibrationOptions, build_calibration_profile
import pytest
from optimization import FinalValidationClassification, SimulationClassification
from performance_models import QueueingNetworkType, System
from statistical_workflow import (
    evaluate_configuration,
    optimize_with_calibrated_simulation,
    validate_final_configuration,
)


def test_public_statistical_workflow_runs_end_to_end_on_native_backend():
    system = System(
        "native-statistical-integration",
        {"checkout": {"api": {"gamma": 2.0, "mu": 10.0}}},
        QueueingNetworkType.OPEN,
    )
    options = CalibrationOptions(
        warmup_replications=2,
        warmup_validation_replications=2,
        measurement_calibration_replications=2,
        target_samples_per_class=2,
        minimum_eligible_samples_per_class=2,
        minimum_nonempty_replications_per_class=2,
        minimum_optimization_replications=2,
        maximum_optimization_replications=4,
        replication_batch_size=2,
        final_validation_replications=2,
        bootstrap_resample_count=1_000,
    )
    minimum = tuple(system.get_minimum_stable_replicas())
    maximum = (2,)
    profile = build_calibration_profile(
        system,
        maximum_configuration=maximum,
        request_class_slos={"checkout": 100.0},
        warmup_duration=0.0,
        measurement_duration=5.0,
        measurement_duration_candidates=(5.0, 10.0),
        warmup_bucket_width=0.5,
        representative_configurations=(minimum, maximum),
        calibration_diagnostics={"integration_test": True},
        options=options,
        creation_timestamp="2026-08-11T00:00:00+00:00",
    )

    evaluation = evaluate_configuration(system, minimum, profile, workers=2)
    optimization = optimize_with_calibrated_simulation(
        system,
        profile,
        workers=2,
    )
    final = validate_final_configuration(
        system,
        optimization.configuration,
        profile,
        workers=2,
    )

    assert evaluation.classification == SimulationClassification.STATISTICALLY_FEASIBLE
    assert evaluation.replications_used == 2
    assert optimization.configuration == minimum
    assert optimization.metrics.classification == (
        SimulationClassification.STATISTICALLY_FEASIBLE
    )
    assert final.classification == FinalValidationClassification.VALIDATED_FEASIBLE
    assert final.passed
    assert set(evaluation.seeds_used).isdisjoint(final.seeds)


def test_native_timeout_outcomes_make_mean_slo_evaluation_infeasible():
    workload = {
        "request_classes": {
            "checkout": {
                "concurrency_weight": 1.0,
                "end_to_end_latency_slos": {"mean_seconds": 0.04},
                "end_to_end_timeout_seconds": 0.05,
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "checkout": {
                    "api": {"distribution": "deterministic", "rate": 1.0}
                }
            },
            "service_times": {
                "api": {"distribution": "deterministic", "mean": 0.1}
            },
        },
    }
    system = System("native-timeout-evaluation", workload, QueueingNetworkType.OPEN)
    options = CalibrationOptions(
        warmup_replications=2,
        warmup_validation_replications=2,
        measurement_calibration_replications=2,
        target_samples_per_class=2,
        minimum_eligible_samples_per_class=2,
        minimum_nonempty_replications_per_class=2,
        minimum_optimization_replications=2,
        maximum_optimization_replications=2,
        replication_batch_size=2,
        final_validation_replications=2,
        bootstrap_resample_count=1_000,
    )
    profile = build_calibration_profile(
        system,
        maximum_configuration=(1,),
        request_class_slos={"checkout": 0.04},
        warmup_duration=0.0,
        measurement_duration=3.0,
        measurement_duration_candidates=(3.0, 6.0),
        warmup_bucket_width=0.5,
        representative_configurations=((1,),),
        calibration_diagnostics={"integration_test": True},
        options=options,
        creation_timestamp="2026-08-11T00:00:00+00:00",
        latency_slo_kind="mean",
    )

    evaluation = evaluate_configuration(system, (1,), profile, workers=2)

    assert evaluation.classification == SimulationClassification.STATISTICALLY_INFEASIBLE
    assert evaluation.intervals["checkout"].estimate == pytest.approx(0.05)
    assert dict(evaluation.simulation_diagnostics)[
        "failed_measured_request_count"
    ] > 0

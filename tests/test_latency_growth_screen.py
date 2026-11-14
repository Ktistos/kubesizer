from types import SimpleNamespace

import pytest

from calibration import CalibrationOptions, build_calibration_profile
from optimization import (
    CalibratedConfigurationEvaluator,
    CalibratedReplicaSizingProblem,
    FinalValidationClassification,
    SimulationClassification,
    UniformCostReplicaOptimizer,
    validate_final_configuration,
)
from optimization.replication_evidence import (
    LatencyGrowthStatus,
    latency_growth_diagnostics,
)
from performance_models import QueueingNetworkType, System


def replication(early, late, *, seed=1, warmup=5.0, measurement=10.0):
    count = len(early) + len(late)
    arrivals = tuple(
        warmup + (index + 0.5) * measurement / count for index in range(count)
    )
    latencies = tuple(early) + tuple(late)
    return SimpleNamespace(
        seed=seed,
        request_class_arrival_times={"request": arrivals},
        request_class_latencies={"request": latencies},
        request_class_failures={
            "request": tuple(False for _ in latencies)
        },
        processed_event_count=100,
        external_arrival_count=count,
        completed_request_count=count,
        stop_time=warmup + measurement,
    )


def test_latency_growth_screen_uses_independent_replication_contrasts():
    replications = tuple(
        replication((0.10,) * 40, (0.40,) * 40, seed=seed)
        for seed in (1, 2, 3)
    )

    (diagnostic,) = latency_growth_diagnostics(
        replications,
        ("request",),
        warmup_duration=5.0,
        measurement_duration=10.0,
    )

    assert diagnostic.status == LatencyGrowthStatus.GROWTH_DETECTED
    assert diagnostic.replication_count == 3
    assert diagnostic.early_mean_seconds == pytest.approx(0.10)
    assert diagnostic.late_mean_seconds == pytest.approx(0.40)


def test_latency_growth_screen_can_find_no_material_growth():
    replications = tuple(
        replication((0.10,) * 40, (0.105,) * 40, seed=seed)
        for seed in (1, 2, 3)
    )

    (diagnostic,) = latency_growth_diagnostics(
        replications,
        ("request",),
        warmup_duration=5.0,
        measurement_duration=10.0,
    )

    assert diagnostic.status == LatencyGrowthStatus.NO_MATERIAL_GROWTH_DETECTED


def test_latency_growth_screen_is_inconclusive_without_raw_cohorts():
    replications = tuple(
        SimpleNamespace(request_class_latencies={"request": (0.1,)})
        for _ in range(30)
    )

    (diagnostic,) = latency_growth_diagnostics(
        replications,
        ("request",),
        warmup_duration=5.0,
        measurement_duration=10.0,
    )

    assert diagnostic.status == LatencyGrowthStatus.INCONCLUSIVE
    assert "unavailable" in diagnostic.reason


def make_system_and_profile():
    system = System(
        "growth-screen",
        {"request": {"api": {"gamma": 1.0, "mu": 10.0}}},
        QueueingNetworkType.OPEN,
    )
    options = CalibrationOptions(
        warmup_replications=2,
        warmup_validation_replications=2,
        measurement_calibration_replications=3,
        target_samples_per_class=2,
        minimum_eligible_samples_per_class=4,
        minimum_nonempty_replications_per_class=2,
        minimum_optimization_replications=3,
        maximum_optimization_replications=3,
        replication_batch_size=1,
        final_validation_replications=3,
        bootstrap_resample_count=1_000,
        minimum_bootstrap_tail_draws=5,
    )
    minimum = tuple(system.get_minimum_stable_replicas())
    maximum = tuple(value + 1 for value in minimum)
    profile = build_calibration_profile(
        system,
        maximum_configuration=maximum,
        request_class_slos={"request": 1.0},
        warmup_duration=5.0,
        measurement_duration=10.0,
        measurement_duration_candidates=(10.0, 20.0),
        warmup_bucket_width=1.0,
        representative_configurations=(minimum, maximum),
        calibration_diagnostics={},
        options=options,
        creation_timestamp="2026-10-31T00:00:00+00:00",
        latency_slo_kind="mean",
    )
    return system, profile


class GrowingSystem:
    def __init__(self, system):
        self.system = system

    def __getattr__(self, name):
        return getattr(self.system, name)

    def simulate_measurement_replications(self, **kwargs):
        return tuple(
            replication(
                (0.10,) * 40,
                (0.40,) * 40,
                seed=seed,
                warmup=kwargs["warmup_duration"],
                measurement=kwargs["measurement_duration"],
            )
            for seed in kwargs["seeds"]
        )


def test_candidate_growth_is_not_presented_as_a_finite_mean_prediction():
    raw, profile = make_system_and_profile()
    result = CalibratedConfigurationEvaluator(GrowingSystem(raw), profile).evaluate(
        profile.minimum_replica_configuration
    )

    assert result.classification == SimulationClassification.MODELED_NONSTATIONARY
    assert not result.feasible
    assert "not a stationary mean" in result.failure_reason
    assert result.latency_growth_diagnostics[0].status == (
        LatencyGrowthStatus.GROWTH_DETECTED
    )


def test_final_validation_rejects_growth_even_when_finite_mean_meets_slo():
    raw, profile = make_system_and_profile()
    result = validate_final_configuration(
        GrowingSystem(raw),
        profile.minimum_replica_configuration,
        profile,
    )

    assert result.classification == (
        FinalValidationClassification.MODELED_NONSTATIONARY
    )
    assert not result.passed
    assert result.intervals["request"].estimate < 1.0
    assert result.latency_growth_diagnostics[0].status == (
        LatencyGrowthStatus.GROWTH_DETECTED
    )


def test_optimizer_can_move_past_a_modeled_nonstationary_candidate():
    raw, profile = make_system_and_profile()

    class Evaluator:
        def evaluate(self, configuration):
            if configuration == profile.minimum_replica_configuration:
                return SimpleNamespace(
                    classification=SimulationClassification.MODELED_NONSTATIONARY,
                    intervals={},
                    replications_used=3,
                    failure_reason="latency growth",
                )
            interval = SimpleNamespace(upper_bound=0.5, slo=1.0, p95_estimate=0.4)
            return SimpleNamespace(
                classification=SimulationClassification.STATISTICALLY_FEASIBLE,
                intervals={"request": interval},
                replications_used=3,
                failure_reason=None,
            )

    problem = CalibratedReplicaSizingProblem(raw, profile, evaluator=Evaluator())

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.configuration == profile.maximum_replica_configuration
    assert problem.configuration_evaluations[
        profile.minimum_replica_configuration
    ].classification == SimulationClassification.MODELED_NONSTATIONARY

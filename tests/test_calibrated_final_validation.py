from types import SimpleNamespace

import pytest

from calibration import CalibrationOptions, build_calibration_profile
from optimization import FinalValidationClassification, validate_final_configuration
from performance_models import QueueingNetworkType, System


def make_system(gamma=1.0):
    return System(
        "final-validation",
        {"checkout": {"api": {"gamma": gamma, "mu": 10.0}}},
        QueueingNetworkType.OPEN,
    )


def make_profile(
    system,
    *,
    latency_slo_kind="percentile",
    failure_rate_slos=None,
    replication_count=2,
):
    options = CalibrationOptions(
        warmup_replications=2,
        warmup_validation_replications=2,
        measurement_calibration_replications=2,
        target_samples_per_class=2,
        minimum_eligible_samples_per_class=4,
        minimum_nonempty_replications_per_class=2,
        minimum_optimization_replications=replication_count,
        maximum_optimization_replications=max(4, replication_count),
        replication_batch_size=2,
        final_validation_replications=replication_count,
        bootstrap_resample_count=1_000,
    )
    minimum = tuple(system.get_minimum_stable_replicas())
    maximum = tuple(value + 1 for value in minimum)
    return build_calibration_profile(
        system,
        maximum_configuration=maximum,
        request_class_slos={"checkout": 1.0},
        warmup_duration=5.0,
        measurement_duration=10.0,
        measurement_duration_candidates=(10.0, 20.0),
        warmup_bucket_width=1.0,
        representative_configurations=(minimum, maximum),
        calibration_diagnostics={},
        options=options,
        creation_timestamp="2026-08-11T00:00:00+00:00",
        latency_slo_kind=latency_slo_kind,
        request_class_failure_rate_slos=failure_rate_slos,
    )


class FakeFinalSystem:
    def __init__(self, system, values):
        self.system = system
        self.values = tuple(values)
        self.calls = []

    def __getattr__(self, name):
        return getattr(self.system, name)

    def simulate_measurement_replications(self, **kwargs):
        seeds = tuple(kwargs["seeds"])
        configuration = tuple(self.get_system_replicas())
        self.calls.append((configuration, seeds, kwargs))
        return tuple(
            SimpleNamespace(
                seed=seed,
                request_class_latencies={"checkout": self.values},
                request_class_failures={"checkout": tuple(False for _ in self.values)},
                processed_event_count=10,
                external_arrival_count=5,
                completed_request_count=5,
            )
            for seed in seeds
        )


def test_final_validation_uses_one_fixed_fresh_seed_experiment():
    raw = make_system()
    profile = make_profile(raw)
    system = FakeFinalSystem(raw, (0.5, 0.5))

    result = validate_final_configuration(system, (1,), profile, workers=2)

    assert result.classification == FinalValidationClassification.VALIDATED_FEASIBLE
    assert result.passed
    assert result.seeds == profile.final_validation_seeds
    assert set(result.seeds).isdisjoint(profile.optimization_seeds())
    assert len(system.calls) == 1
    assert system.calls[0][2]["warmup_duration"] == profile.warmup_duration
    assert system.calls[0][2]["measurement_duration"] == profile.measurement_duration
    assert result.intervals["checkout"].upper_bound == pytest.approx(0.5)
    assert result.intervals["checkout"].upper_tail_probability == pytest.approx(0.975)


def test_final_validation_uses_mean_interval_for_mean_slo_profile():
    raw = make_system()
    profile = make_profile(raw, latency_slo_kind="mean")
    system = FakeFinalSystem(raw, (0.2, 0.4))

    result = validate_final_configuration(system, (1,), profile)

    assert result.passed
    assert result.intervals["checkout"].latency_statistic == "mean"
    assert result.intervals["checkout"].estimate == pytest.approx(0.3)


def test_pareto_validation_allocates_confidence_across_candidates():
    raw = make_system()
    profile = make_profile(raw)
    system = FakeFinalSystem(raw, (0.5, 0.5))

    result = validate_final_configuration(
        system, (1,), profile, validation_candidate_count=10
    )

    assert result.passed
    assert result.seeds == profile.final_validation_seeds
    assert result.intervals["checkout"].upper_tail_probability == pytest.approx(0.9975)


def test_final_validation_requires_terminal_failure_rate_bound_too():
    raw = make_system()
    profile = make_profile(
        raw,
        failure_rate_slos={"checkout": 0.10},
        replication_count=200,
    )
    system = FakeFinalSystem(raw, (0.5, 0.5))

    result = validate_final_configuration(system, (1,), profile)

    assert result.passed
    assert 0.0 < result.failure_intervals["checkout"].upper_bound <= 0.10


def test_final_validation_reports_fresh_replication_progress():
    raw = make_system()
    profile = make_profile(raw)
    system = FakeFinalSystem(raw, (0.5, 0.5))
    events = []

    validate_final_configuration(
        system,
        (1,),
        profile,
        progress_callback=lambda event, details: events.append((event, details)),
    )

    assert tuple(event for event, _ in events) == (
        "final_validation_started",
        "final_validation_replications_completed",
    )
    assert events[-1][1]["completed_replications"] == len(
        profile.final_validation_seeds
    )
    assert events[-1][1]["total_replications"] == len(profile.final_validation_seeds)


def test_final_validation_failure_is_structured_and_not_reported_as_feasible():
    raw = make_system()
    profile = make_profile(raw)
    system = FakeFinalSystem(raw, (1.5, 1.5))

    result = validate_final_configuration(system, (1,), profile)

    assert result.classification == FinalValidationClassification.FAILED_SLO_VALIDATION
    assert not result.passed
    assert "upper bound" in result.failure_reason


def test_final_validation_rejects_insufficient_final_cohort():
    raw = make_system()
    profile = make_profile(raw)
    system = FakeFinalSystem(raw, ())

    result = validate_final_configuration(system, (1,), profile)

    assert result.classification == FinalValidationClassification.SIMULATION_FAILURE
    assert "sample requirements" in result.failure_reason


def test_final_validation_rejects_profile_after_workload_change():
    source = make_system()
    profile = make_profile(source)
    changed = FakeFinalSystem(make_system(gamma=2.0), (0.5, 0.5))

    result = validate_final_configuration(changed, (1,), profile)

    assert result.classification == FinalValidationClassification.CALIBRATION_FAILURE
    assert "workload" in result.failure_reason
    assert changed.calls == []

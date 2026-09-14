from types import SimpleNamespace

import pytest

import statistical_workflow
from calibration import CalibrationOptions
from performance_models import QueueingNetworkType, System


def make_system():
    return System(
        "public-workflow",
        {"checkout": {"api": {"gamma": 1.0, "mu": 10.0}}},
        QueueingNetworkType.OPEN,
    )


def options():
    return CalibrationOptions(
        warmup_replications=2,
        warmup_validation_replications=2,
        measurement_calibration_replications=2,
        target_samples_per_class=2,
        minimum_eligible_samples_per_class=4,
        minimum_nonempty_replications_per_class=2,
        minimum_optimization_replications=2,
        maximum_optimization_replications=4,
        replication_batch_size=2,
        final_validation_replications=2,
        bootstrap_resample_count=1_000,
    )


def test_public_calibrate_runs_warmup_then_measurement_and_freezes_profile(monkeypatch):
    system = make_system()
    selected_parameters = SimpleNamespace(
        minimum_replica_configuration=(1,),
        calibration_seeds=(100_000, 100_001),
    )
    validation_configuration = SimpleNamespace(seeds=(1_000_000, 1_000_001))
    validation = SimpleNamespace(
        representative_configurations=((1,), (2,)),
        rounds=(SimpleNamespace(configurations=(validation_configuration,)),),
    )
    warmup = SimpleNamespace(
        initial_parameters=selected_parameters,
        validation=validation,
        global_warmup=7.0,
        bucket_width=0.5,
    )
    measurement = SimpleNamespace(
        global_measurement_duration=20.0,
        candidate_durations=(10.0, 20.0, 40.0),
        seeds=(10_000_000, 10_000_001),
    )
    calls = []

    monkeypatch.setattr(
        statistical_workflow,
        "select_warmup_pilot_parameters",
        lambda selected, **kwargs: (
            calls.append(("warmup-parameters", kwargs)) or selected_parameters
        ),
    )
    monkeypatch.setattr(
        statistical_workflow,
        "calibrate_global_warmup",
        lambda selected, **kwargs: (
            calls.append(("warmup", kwargs)) or warmup
        ),
    )
    monkeypatch.setattr(
        statistical_workflow,
        "calibrate_measurement_duration",
        lambda selected, **kwargs: (
            calls.append(("measurement", kwargs)) or measurement
        ),
    )
    monkeypatch.setattr(
        statistical_workflow,
        "calibration_diagnostics_summary",
        lambda *_args: {"warmup": {}, "measurement": {}, "sequential_policy": {}},
    )

    profile = statistical_workflow.calibrate(
        system,
        request_class_slos={"checkout": 1.0},
        maximum_replicas=2,
        options=options(),
        workers=3,
    )

    assert [name for name, _ in calls] == [
        "warmup-parameters",
        "warmup",
        "measurement",
    ]
    assert calls[1][1]["horizon_sensitivity_tolerance"] == pytest.approx(0.10)
    assert calls[1][1]["max_buckets_per_series"] == 262_144
    assert calls[2][1]["warmup_duration"] == 7.0
    assert calls[2][1]["representative_configurations"] == ((1,), (2,))
    assert profile.warmup_duration == 7.0
    assert profile.measurement_duration == 20.0
    assert profile.minimum_optimization_replications == 2
    assert profile.maximum_optimization_replications == 4
    assert profile.calibration_seeds == (
        100_000,
        100_001,
        1_000_000,
        1_000_001,
        10_000_000,
        10_000_001,
    )


def test_global_warmup_bucket_budget_scales_down_for_large_topologies():
    large_system = SimpleNamespace(
        get_services=lambda: {str(index): object() for index in range(20)}
    )

    assert statistical_workflow._warmup_bucket_cap(
        large_system,
        CalibrationOptions(),
    ) == 49_382


def test_public_calibrate_defaults_to_workload_p95_slos(monkeypatch):
    system = make_system()
    expected = object()
    captured = {}
    monkeypatch.setattr(
        system,
        "get_request_class_percentile_latency_limits",
        lambda *, quantile: {"checkout": 1.75},
    )

    def fake_calibrate_one(selected, **kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(statistical_workflow, "_calibrate_one", fake_calibrate_one)

    result = statistical_workflow.calibrate(system, maximum_replicas=2)

    assert result is expected
    assert captured["request_class_slos"] == {"checkout": 1.75}


def test_public_calibrate_passes_a_statistical_stability_floor(monkeypatch):
    system = make_system()
    captured = {}

    def fake_calibrate_one(selected, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(statistical_workflow, "_calibrate_one", fake_calibrate_one)

    statistical_workflow.calibrate(
        system,
        request_class_slos={"checkout": 1.0},
        maximum_replicas=3,
        minimum_configuration=(2,),
    )

    assert captured["minimum_configuration"] == (2,)


def test_public_analytical_optimization_respects_a_statistical_floor():
    result = statistical_workflow.optimize_with_analytical_model(
        make_system(),
        request_class_mean_latency_limits={},
        maximum_replicas=3,
        minimum_configuration=(2,),
    )

    assert result.configuration == (2,)


def test_simulation_calibration_accepts_workload_mean_slos(monkeypatch):
    system = make_system()
    expected = object()
    captured = {}
    system.get_request_class_latency_slo_kind = lambda: "mean"
    system.get_request_class_mean_latency_limits = lambda: {"checkout": 1.0}

    def fake_calibrate_one(selected, **kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(statistical_workflow, "_calibrate_one", fake_calibrate_one)

    result = statistical_workflow.calibrate(system, maximum_replicas=2)

    assert result is expected
    assert captured["request_class_slos"] == {"checkout": 1.0}
    assert captured["latency_slo_kind"] == "mean"


def test_public_evaluate_delegates_to_calibrated_evaluator_and_reporting(monkeypatch):
    expected = object()
    saved = []

    class FakeEvaluator:
        def __init__(self, system, profile, workers):
            assert workers == 4

        def evaluate(self, configuration):
            assert configuration == (2,)
            return expected

    monkeypatch.setattr(
        statistical_workflow,
        "CalibratedConfigurationEvaluator",
        FakeEvaluator,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "save_configuration_evaluation",
        lambda result, directory: saved.append((result, directory)),
    )

    result = statistical_workflow.evaluate_configuration(
        object(),
        (2,),
        object(),
        workers=4,
        report_directory="evidence",
    )

    assert result is expected
    assert saved == [(expected, "evidence")]


def test_public_optimize_returns_all_run_classifications(monkeypatch):
    raw_result = SimpleNamespace(
        configuration=(2,),
        cost=1,
        metrics=object(),
        explored_states=2,
        path=((1,), (2,)),
    )
    evaluations = {(1,): "inconclusive", (2,): "feasible"}

    class FakeProblem:
        def __init__(self, *_args, **_kwargs):
            self.configuration_evaluations = evaluations

    class FakeOptimizer:
        def __init__(self, **_kwargs):
            pass

        def optimize(self, problem):
            return raw_result

    monkeypatch.setattr(
        statistical_workflow,
        "validate_profile_for_experiment",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "CalibratedReplicaSizingProblem",
        FakeProblem,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "UniformCostReplicaOptimizer",
        FakeOptimizer,
    )
    profile = SimpleNamespace(
        slos={"checkout": 1.0},
        minimum_replica_configuration=(1,),
        maximum_replica_configuration=(2,),
        profile_fingerprint="profile",
    )

    result = statistical_workflow.optimize_with_calibrated_simulation(
        object(),
        profile,
    )

    assert result.configuration == (2,)
    assert result.evaluations == evaluations
    assert result.calibration_profile_fingerprint == "profile"


def test_public_final_validation_delegates_and_saves(monkeypatch):
    expected = object()
    saved = []
    monkeypatch.setattr(
        statistical_workflow,
        "_validate_final_configuration",
        lambda *_args, **_kwargs: expected,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "save_final_validation",
        lambda result, directory: saved.append((result, directory)),
    )

    result = statistical_workflow.validate_final_configuration(
        object(),
        (2,),
        object(),
        report_directory="holdout",
    )

    assert result is expected
    assert saved == [(expected, "holdout")]

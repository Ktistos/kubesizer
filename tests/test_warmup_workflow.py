from types import SimpleNamespace

import pytest

from calibration import (
    WarmupCalibrationError,
    WarmupHorizonAnalysis,
    analyze_warmup_horizon,
    calibrate_minimum_configuration,
    parameters_with_pilot_horizon,
    select_warmup_pilot_parameters,
    window_length_for_bucket_width,
)
from performance_models import QueueingNetworkType, System


def make_parameters():
    system = System(
        "workflow",
        {"read": {"api": {"gamma": 1.0, "mu": 4.0}}},
        QueueingNetworkType.OPEN,
    )
    parameters = select_warmup_pilot_parameters(system, replication_count=2)
    return system, parameters


def successful_analysis(horizon, warmup):
    cutoff = SimpleNamespace(warmup_duration=warmup)
    selected = SimpleNamespace(cutoff=cutoff)
    analysis = WarmupHorizonAnalysis(
        pilot_horizon=horizon,
        bucket_width_analyses=(),
        bucket_width_sensitivity=SimpleNamespace(passed=True),
        selected_bucket_width=0.5,
        passed=True,
    )
    object.__setattr__(analysis, "selected_analysis", lambda: selected)
    return analysis


def test_parameters_with_horizon_aligns_all_candidate_bucket_counts():
    _, parameters = make_parameters()
    changed = parameters_with_pilot_horizon(parameters, 503.0)

    assert changed.pilot_horizon == 505.0
    assert changed.raw_pilot_horizon == 503.0
    assert changed.bucket_counts == (808, 404, 202)
    assert parameters.pilot_horizon == 1000.0


def test_window_length_scales_to_preserve_physical_duration():
    lengths = tuple(
        window_length_for_bucket_width(
            reference_bucket_width=1.0,
            reference_window_length=10,
            bucket_width=bucket_width,
        )
        for bucket_width in (0.5, 1.0, 2.0)
    )

    assert lengths == (20, 10, 5)
    assert tuple(
        length * bucket_width
        for length, bucket_width in zip(lengths, (0.5, 1.0, 2.0))
    ) == (10.0, 10.0, 10.0)


def test_window_length_rejects_unaligned_candidate_width():
    with pytest.raises(ValueError, match="must align"):
        window_length_for_bucket_width(
            reference_bucket_width=1.0,
            reference_window_length=10,
            bucket_width=3.0,
        )


def test_horizon_analysis_scales_window_length_for_each_candidate(monkeypatch):
    parameters = SimpleNamespace(
        initial_bucket_width=1.0,
        bucket_width_candidates=(0.5, 1.0, 2.0),
        pilot_horizon=100.0,
    )
    observed_window_lengths = []

    monkeypatch.setattr(
        "calibration.workflow.run_warmup_pilot_replications",
        lambda *_args, **_kwargs: (object(),),
    )
    monkeypatch.setattr(
        "calibration.workflow.rebucket_warmup_replication",
        lambda replication, _bucket_width: replication,
    )
    monkeypatch.setattr(
        "calibration.workflow.average_warmup_replications",
        lambda _replications: object(),
    )

    def fake_detect(_ensemble, **options):
        observed_window_lengths.append(options["window_length"])
        return SimpleNamespace(warmup_duration=20.0)

    monkeypatch.setattr(
        "calibration.workflow.detect_ensemble_warmup_cutoff",
        fake_detect,
    )
    monkeypatch.setattr(
        "calibration.workflow.check_horizon_adequacy",
        lambda *_args, **_kwargs: SimpleNamespace(passed=True),
    )
    monkeypatch.setattr(
        "calibration.workflow.check_bucket_width_sensitivity",
        lambda *_args, **_kwargs: SimpleNamespace(
            passed=True,
            selected_bucket_width=0.5,
        ),
    )

    result = analyze_warmup_horizon(object(), parameters)

    assert observed_window_lengths == [20, 10, 5]
    assert result.passed


def test_minimum_calibration_requires_two_consecutive_agreeing_horizons(monkeypatch):
    system, parameters = make_parameters()
    calls = []
    simulated_horizons = []

    def fake_analysis(_system, current_parameters, **_options):
        calls.append(current_parameters.pilot_horizon)
        warmup = 100.0 if len(calls) == 1 else 105.0
        return successful_analysis(current_parameters.pilot_horizon, warmup)

    monkeypatch.setattr("calibration.workflow.analyze_warmup_horizon", fake_analysis)
    monkeypatch.setattr(
        "calibration.workflow.run_warmup_pilot_replications",
        lambda _system, current_parameters, **_options: (
            simulated_horizons.append(current_parameters.pilot_horizon)
            or (object(),)
        ),
    )
    result = calibrate_minimum_configuration(
        system,
        parameters,
        max_horizon_doublings=2,
    )

    assert calls == [1000.0, 2000.0]
    assert simulated_horizons == [2000.0]
    assert result.parameters.pilot_horizon == 2000.0
    assert result.warmup_duration == 105.0
    assert result.horizon_comparison.passed


def test_minimum_calibration_keeps_doubling_when_w_changes(monkeypatch):
    system, parameters = make_parameters()
    warmups = iter((50.0, 100.0, 102.0))
    simulated_horizons = []

    monkeypatch.setattr(
        "calibration.workflow.analyze_warmup_horizon",
        lambda _system, current_parameters, **_options: successful_analysis(
            current_parameters.pilot_horizon,
            next(warmups),
        ),
    )
    monkeypatch.setattr(
        "calibration.workflow.run_warmup_pilot_replications",
        lambda _system, current_parameters, **_options: (
            simulated_horizons.append(current_parameters.pilot_horizon)
            or (object(),)
        ),
    )
    result = calibrate_minimum_configuration(
        system,
        parameters,
        max_horizon_doublings=3,
    )

    assert result.parameters.pilot_horizon == 4000.0
    assert len(result.attempts) == 3
    assert simulated_horizons == [2000.0, 8000.0]


def test_minimum_calibration_reports_failure_without_stable_pair(monkeypatch):
    system, parameters = make_parameters()
    warmups = iter((10.0, 20.0, 40.0))
    simulated_horizons = []
    monkeypatch.setattr(
        "calibration.workflow.analyze_warmup_horizon",
        lambda _system, current_parameters, **_options: successful_analysis(
            current_parameters.pilot_horizon,
            next(warmups),
        ),
    )
    monkeypatch.setattr(
        "calibration.workflow.run_warmup_pilot_replications",
        lambda _system, current_parameters, **_options: (
            simulated_horizons.append(current_parameters.pilot_horizon)
            or (object(),)
        ),
    )

    with pytest.raises(WarmupCalibrationError, match="No two consecutive"):
        calibrate_minimum_configuration(
            system,
            parameters,
            max_horizon_doublings=2,
        )

    assert simulated_horizons == [2000.0, 4000.0]

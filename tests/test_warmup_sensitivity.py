from types import SimpleNamespace

import pytest

from calibration import (
    WarmupEnsembleSeries,
    check_bucket_width_sensitivity,
    check_horizon_adequacy,
    check_post_warmup_stability,
    compare_warmup_durations,
)


def make_ensemble(active, *, bucket_width=1.0):
    active = tuple(active)
    zeros = tuple(0.0 for _ in active)
    ones = tuple(1.0 for _ in active)
    return WarmupEnsembleSeries(
        service_names=("api",),
        replica_configuration=(2,),
        pilot_horizon=len(active) * bucket_width,
        bucket_width=bucket_width,
        seeds=(1, 2),
        active_requests=active,
        queue_length_by_service=(zeros,),
        busy_replicas_by_service=(ones,),
        number_in_system_by_service=(ones,),
        utilization_by_service=(tuple(value / 2 for value in ones),),
    )


def make_cutoff(width, count, warmup):
    return SimpleNamespace(
        bucket_width=width,
        bucket_count=count,
        warmup_duration=warmup,
    )


def test_horizon_adequacy_accepts_early_cutoff_and_flat_late_run():
    ensemble = make_ensemble([4.0] * 300)
    result = check_horizon_adequacy(
        ensemble,
        make_cutoff(1.0, 300, 20.0),
    )

    assert result.passed
    assert result.post_warmup_bucket_count == 280
    assert all(comparison.passed for comparison in result.plateau_comparisons)


def test_horizon_adequacy_explains_late_cutoff_and_moving_reference():
    ensemble = make_ensemble([1.0] * 240 + [5.0] * 60)
    result = check_horizon_adequacy(
        ensemble,
        make_cutoff(1.0, 300, 120.0),
    )

    assert not result.passed
    assert not result.warmup_before_first_third
    assert any("active_requests" in reason for reason in result.reasons)


def test_duration_comparison_handles_zero_and_relative_change():
    assert compare_warmup_durations(0.0, 0.0).passed
    comparison = compare_warmup_durations(90.0, 100.0)
    assert comparison.relative_difference == pytest.approx(0.10)
    assert comparison.passed
    assert not compare_warmup_durations(80.0, 100.0).passed


def test_bucket_width_sensitivity_uses_adjacent_widths_and_smallest_delta():
    result = check_bucket_width_sensitivity(
        (
            make_cutoff(2.0, 250, 100.0),
            make_cutoff(0.5, 1000, 95.0),
            make_cutoff(1.0, 500, 100.0),
        ),
        pilot_horizon=500.0,
    )

    assert result.passed
    assert result.selected_bucket_width == 0.5
    assert len(result.adjacent_comparisons) == 2


def test_bucket_width_sensitivity_rejects_cutoff_change_or_too_little_data():
    unstable = check_bucket_width_sensitivity(
        (make_cutoff(1.0, 500, 50.0), make_cutoff(2.0, 250, 100.0)),
        pilot_horizon=500.0,
    )
    short = check_bucket_width_sensitivity(
        (make_cutoff(1.0, 100, 10.0), make_cutoff(2.0, 50, 10.0)),
        pilot_horizon=100.0,
    )

    assert not unstable.passed
    assert not short.passed
    assert short.selected_bucket_width is None


def test_post_warmup_check_uses_first_complete_windows_at_or_after_w():
    ensemble = make_ensemble([0.0] * 20 + [5.0] * 180)
    result = check_post_warmup_stability(
        ensemble,
        20.0,
        window_length=10,
    )

    assert result.passed
    active = next(
        diagnostic
        for diagnostic in result.metric_diagnostics
        if diagnostic.metric_name == "active_requests"
    )
    assert active.first_window_index == 2
    assert active.window_means == pytest.approx((5.0, 5.0, 5.0))


def test_post_warmup_check_rejects_windows_that_are_still_rising():
    ensemble = make_ensemble(list(range(60)) + [100.0] * 140)
    result = check_post_warmup_stability(
        ensemble,
        20.0,
        window_length=10,
    )

    assert not result.passed

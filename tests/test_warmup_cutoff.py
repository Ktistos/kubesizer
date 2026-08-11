import pytest

from calibration import (
    WarmupCutoffNotFoundError,
    detect_metric_warmup_cutoff,
    detect_warmup_cutoff_from_series,
)


def repeated_windows(values, window_length=2):
    return tuple(
        value
        for window_value in values
        for value in [window_value] * window_length
    )


def test_rising_then_flat_series_returns_known_cutoff():
    series = repeated_windows((0.0, 3.0, 7.0, 10.0, 10.0, 10.0, 10.0, 10.0))

    diagnostic = detect_metric_warmup_cutoff(
        series,
        metric_name="active_requests",
        bucket_width=1.0,
        window_length=2,
    )

    assert diagnostic.reference_level == pytest.approx(10.0)
    assert diagnostic.accepted_window_index == 3
    assert diagnostic.cutoff_time == pytest.approx(6.0)
    assert diagnostic.accepted_slope == pytest.approx(0.0)


def test_permanently_increasing_series_returns_failure():
    series = repeated_windows(tuple(float(value) for value in range(10)))

    with pytest.raises(WarmupCutoffNotFoundError, match="active_requests"):
        detect_metric_warmup_cutoff(
            series,
            metric_name="active_requests",
            bucket_width=1.0,
            window_length=2,
        )


def test_near_zero_reference_uses_absolute_tolerance_floor():
    series = repeated_windows((0.5, 0.2, 0.05, 0.04, 0.03, 0.04, 0.03))

    diagnostic = detect_metric_warmup_cutoff(
        series,
        metric_name="queue_length[api]",
        bucket_width=1.0,
        window_length=2,
    )

    assert diagnostic.tolerance == pytest.approx(0.1)
    assert diagnostic.cutoff_time == pytest.approx(4.0)


def test_latest_metric_cutoff_is_selected_and_safety_added_once():
    metrics = {
        "active_requests": repeated_windows(
            (0.0, 5.0, 10.0, 10.0, 10.0, 10.0, 10.0)
        ),
        "queue_length[api]": repeated_windows(
            (0.0, 0.0, 2.0, 5.0, 5.0, 5.0, 5.0)
        ),
    }

    result = detect_warmup_cutoff_from_series(
        metrics,
        bucket_width=1.0,
        window_length=2,
    )

    assert result.candidate_warmup == pytest.approx(6.0)
    assert result.safety_window == pytest.approx(2.0)
    assert result.warmup_duration == pytest.approx(8.0)
    assert result.determining_metrics == ("queue_length[api]",)


def test_utilization_uses_its_smaller_absolute_tolerance():
    series = repeated_windows((0.0, 0.4, 0.5, 0.5, 0.5, 0.5))

    diagnostic = detect_metric_warmup_cutoff(
        series,
        metric_name="utilization[api]",
        bucket_width=1.0,
        window_length=2,
    )

    assert diagnostic.tolerance == pytest.approx(0.025)


def test_too_few_complete_windows_is_rejected():
    with pytest.raises(ValueError, match="enough complete windows"):
        detect_metric_warmup_cutoff(
            (1.0, 1.0, 1.0),
            metric_name="active_requests",
            bucket_width=1.0,
            window_length=2,
        )

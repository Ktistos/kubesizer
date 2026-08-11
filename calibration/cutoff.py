import math
from dataclasses import dataclass


class WarmupCutoffNotFoundError(RuntimeError):
    """Raised when an important state metric does not reach a defensible plateau."""

    def __init__(self, metric_name):
        super().__init__(f"No warm-up cutoff was found for metric '{metric_name}'.")
        self.metric_name = metric_name


@dataclass(frozen=True)
class MetricWarmupDiagnostic:
    metric_name: str
    reference_start_index: int
    reference_level: float
    tolerance: float
    window_means: tuple[float, ...]
    accepted_window_index: int
    cutoff_time: float
    accepted_slope: float
    accepted_total_fitted_change: float


@dataclass(frozen=True)
class WarmupCutoffResult:
    bucket_width: float
    bucket_count: int
    window_length: int
    reference_tail_fraction: float
    relative_tolerance: float
    safety_window: float
    candidate_warmup: float
    warmup_duration: float
    determining_metrics: tuple[str, ...]
    metric_diagnostics: tuple[MetricWarmupDiagnostic, ...]

    def diagnostic_by_metric(self):
        return {
            diagnostic.metric_name: diagnostic
            for diagnostic in self.metric_diagnostics
        }


def _linear_slope(values, spacing):
    mean_x = spacing * (len(values) - 1) / 2.0
    mean_y = sum(values) / len(values)
    numerator = sum(
        (index * spacing - mean_x) * (value - mean_y)
        for index, value in enumerate(values)
    )
    denominator = sum(
        (index * spacing - mean_x) ** 2 for index in range(len(values))
    )
    return numerator / denominator if denominator else 0.0


def default_absolute_tolerance(metric_name):
    if metric_name.startswith("utilization["):
        return 0.01
    return 0.1


def detect_metric_warmup_cutoff(
    series,
    *,
    metric_name,
    bucket_width,
    window_length=10,
    reference_tail_fraction=0.20,
    relative_tolerance=0.05,
    absolute_tolerance=None,
    required_consecutive_windows=3,
):
    """Find the first practically flat run of windows near the late reference."""
    series = tuple(float(value) for value in series)
    if not series or any(not math.isfinite(value) for value in series):
        raise ValueError("Metric series must contain finite values.")
    if not math.isfinite(bucket_width) or bucket_width <= 0:
        raise ValueError("bucket_width must be finite and greater than zero.")
    if not isinstance(window_length, int) or window_length <= 0:
        raise ValueError("window_length must be a positive integer.")
    if not 0 < reference_tail_fraction < 1:
        raise ValueError("reference_tail_fraction must be between zero and one.")
    if relative_tolerance < 0:
        raise ValueError("relative_tolerance must be non-negative.")
    if (
        not isinstance(required_consecutive_windows, int)
        or required_consecutive_windows < 2
    ):
        raise ValueError("required_consecutive_windows must be at least two.")
    if absolute_tolerance is None:
        absolute_tolerance = default_absolute_tolerance(metric_name)
    if not math.isfinite(absolute_tolerance) or absolute_tolerance < 0:
        raise ValueError("absolute_tolerance must be finite and non-negative.")

    reference_start_one_based = math.ceil(
        (1.0 - reference_tail_fraction) * len(series)
    )
    reference_start_index = max(0, reference_start_one_based - 1)
    reference_values = series[reference_start_index:]
    reference_level = sum(reference_values) / len(reference_values)
    tolerance = max(
        relative_tolerance * abs(reference_level),
        absolute_tolerance,
    )

    complete_window_count = len(series) // window_length
    if complete_window_count < required_consecutive_windows:
        raise ValueError("Metric series does not contain enough complete windows.")
    window_means = tuple(
        sum(series[index * window_length : (index + 1) * window_length])
        / window_length
        for index in range(complete_window_count)
    )
    window_duration = window_length * bucket_width

    for window_index in range(
        complete_window_count - required_consecutive_windows + 1
    ):
        candidate_means = window_means[
            window_index : window_index + required_consecutive_windows
        ]
        if any(
            abs(window_mean - reference_level) > tolerance
            for window_mean in candidate_means
        ):
            continue

        slope = _linear_slope(candidate_means, window_duration)
        total_fitted_change = abs(slope) * (
            required_consecutive_windows * window_duration
        )
        if total_fitted_change > tolerance:
            continue

        return MetricWarmupDiagnostic(
            metric_name=metric_name,
            reference_start_index=reference_start_index,
            reference_level=reference_level,
            tolerance=tolerance,
            window_means=window_means,
            accepted_window_index=window_index,
            cutoff_time=window_index * window_duration,
            accepted_slope=slope,
            accepted_total_fitted_change=total_fitted_change,
        )

    raise WarmupCutoffNotFoundError(metric_name)


def detect_warmup_cutoff_from_series(
    metric_series,
    *,
    bucket_width,
    window_length=10,
    reference_tail_fraction=0.20,
    relative_tolerance=0.05,
    absolute_tolerances=None,
    required_consecutive_windows=3,
):
    """Combine metric-specific cutoffs and add one analysis-window safety margin."""
    metric_series = dict(metric_series)
    if not metric_series:
        raise ValueError("At least one important metric series is required.")
    series_lengths = {len(series) for series in metric_series.values()}
    if len(series_lengths) != 1:
        raise ValueError("All important metric series must have equal length.")
    absolute_tolerances = dict(absolute_tolerances or {})

    diagnostics = tuple(
        detect_metric_warmup_cutoff(
            series,
            metric_name=metric_name,
            bucket_width=bucket_width,
            window_length=window_length,
            reference_tail_fraction=reference_tail_fraction,
            relative_tolerance=relative_tolerance,
            absolute_tolerance=absolute_tolerances.get(metric_name),
            required_consecutive_windows=required_consecutive_windows,
        )
        for metric_name, series in metric_series.items()
    )
    candidate_warmup = max(
        diagnostic.cutoff_time for diagnostic in diagnostics
    )
    determining_metrics = tuple(
        diagnostic.metric_name
        for diagnostic in diagnostics
        if math.isclose(
            diagnostic.cutoff_time,
            candidate_warmup,
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
    )
    safety_window = window_length * bucket_width

    return WarmupCutoffResult(
        bucket_width=bucket_width,
        bucket_count=series_lengths.pop(),
        window_length=window_length,
        reference_tail_fraction=reference_tail_fraction,
        relative_tolerance=relative_tolerance,
        safety_window=safety_window,
        candidate_warmup=candidate_warmup,
        warmup_duration=candidate_warmup + safety_window,
        determining_metrics=determining_metrics,
        metric_diagnostics=diagnostics,
    )


def detect_ensemble_warmup_cutoff(ensemble, **kwargs):
    return detect_warmup_cutoff_from_series(
        ensemble.important_metric_series(),
        bucket_width=ensemble.bucket_width,
        **kwargs,
    )

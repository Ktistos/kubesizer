import math
from dataclasses import dataclass

from .cutoff import default_absolute_tolerance


@dataclass(frozen=True)
class MetricPlateauComparison:
    metric_name: str
    mean_60_to_80: float
    mean_80_to_100: float
    tolerance: float
    passed: bool


@dataclass(frozen=True)
class HorizonAdequacyResult:
    passed: bool
    warmup_before_first_third: bool
    post_warmup_bucket_count: int
    enough_post_warmup_buckets: bool
    plateau_comparisons: tuple[MetricPlateauComparison, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class WarmupDurationComparison:
    shorter_duration: float
    longer_duration: float
    relative_difference: float
    tolerance: float
    passed: bool


@dataclass(frozen=True)
class BucketWidthRecord:
    bucket_width: float
    bucket_count: int
    warmup_duration: float
    post_warmup_bucket_count: int
    enough_data: bool


@dataclass(frozen=True)
class BucketWidthSensitivityResult:
    passed: bool
    selected_bucket_width: float | None
    records: tuple[BucketWidthRecord, ...]
    adjacent_comparisons: tuple[WarmupDurationComparison, ...]


@dataclass(frozen=True)
class PostWarmupMetricDiagnostic:
    metric_name: str
    reference_start_index: int
    reference_level: float
    tolerance: float
    first_window_index: int
    window_means: tuple[float, ...]
    slope: float
    total_fitted_change: float
    passed: bool


@dataclass(frozen=True)
class PostWarmupStabilityResult:
    warmup_duration: float
    window_length: int
    passed: bool
    metric_diagnostics: tuple[PostWarmupMetricDiagnostic, ...]


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


def _reference(series, tail_fraction):
    reference_start_one_based = math.ceil((1.0 - tail_fraction) * len(series))
    reference_start_index = max(0, reference_start_one_based - 1)
    values = series[reference_start_index:]
    return reference_start_index, sum(values) / len(values)


def check_horizon_adequacy(
    ensemble,
    cutoff,
    *,
    relative_tolerance=0.05,
    absolute_tolerances=None,
    minimum_post_warmup_buckets=50,
):
    """Apply the guide's H/3, remaining-data, and late-plateau checks."""
    if ensemble.bucket_width != cutoff.bucket_width:
        raise ValueError("Ensemble and cutoff bucket widths must match.")
    if ensemble.bucket_count != cutoff.bucket_count:
        raise ValueError("Ensemble and cutoff bucket counts must match.")
    if not isinstance(minimum_post_warmup_buckets, int) or minimum_post_warmup_buckets < 0:
        raise ValueError("minimum_post_warmup_buckets must be non-negative.")
    absolute_tolerances = dict(absolute_tolerances or {})

    before_first_third = cutoff.warmup_duration <= ensemble.pilot_horizon / 3.0
    post_bucket_count = math.floor(
        (ensemble.pilot_horizon - cutoff.warmup_duration)
        / ensemble.bucket_width
        + 1e-12
    )
    enough_post_buckets = post_bucket_count >= minimum_post_warmup_buckets

    bucket_count = ensemble.bucket_count
    start_60 = math.floor(0.60 * bucket_count)
    start_80 = math.floor(0.80 * bucket_count)
    comparisons = []
    for metric_name, raw_series in ensemble.important_metric_series().items():
        series = tuple(raw_series)
        earlier = series[start_60:start_80]
        later = series[start_80:]
        if not earlier or not later:
            raise ValueError("The ensemble is too short for late-plateau comparisons.")
        earlier_mean = sum(earlier) / len(earlier)
        later_mean = sum(later) / len(later)
        tolerance = max(
            relative_tolerance * abs(later_mean),
            absolute_tolerances.get(
                metric_name,
                default_absolute_tolerance(metric_name),
            ),
        )
        comparisons.append(
            MetricPlateauComparison(
                metric_name=metric_name,
                mean_60_to_80=earlier_mean,
                mean_80_to_100=later_mean,
                tolerance=tolerance,
                passed=abs(earlier_mean - later_mean) <= tolerance,
            )
        )

    reasons = []
    if not before_first_third:
        reasons.append("warm-up duration is later than one third of the pilot horizon")
    if not enough_post_buckets:
        reasons.append("fewer than the required post-warm-up buckets remain")
    failed_metrics = tuple(
        comparison.metric_name for comparison in comparisons if not comparison.passed
    )
    if failed_metrics:
        reasons.append(
            "60-80% and 80-100% means disagree for: " + ", ".join(failed_metrics)
        )

    return HorizonAdequacyResult(
        passed=not reasons,
        warmup_before_first_third=before_first_third,
        post_warmup_bucket_count=post_bucket_count,
        enough_post_warmup_buckets=enough_post_buckets,
        plateau_comparisons=tuple(comparisons),
        reasons=tuple(reasons),
    )


def compare_warmup_durations(shorter_duration, longer_duration, *, tolerance=0.10):
    """Compare two cutoff estimates using a symmetric relative difference."""
    if shorter_duration < 0 or longer_duration < 0:
        raise ValueError("Warm-up durations must be non-negative.")
    if tolerance < 0:
        raise ValueError("tolerance must be non-negative.")
    denominator = max(shorter_duration, longer_duration)
    relative_difference = (
        abs(shorter_duration - longer_duration) / denominator
        if denominator
        else 0.0
    )
    return WarmupDurationComparison(
        shorter_duration=shorter_duration,
        longer_duration=longer_duration,
        relative_difference=relative_difference,
        tolerance=tolerance,
        passed=relative_difference <= tolerance,
    )


def check_bucket_width_sensitivity(
    cutoffs,
    *,
    pilot_horizon,
    tolerance=0.10,
    minimum_total_buckets=200,
    minimum_post_warmup_buckets=50,
):
    """Check adjacent delta estimates and choose the smallest defensible delta."""
    cutoffs = tuple(sorted(cutoffs, key=lambda cutoff: cutoff.bucket_width))
    if len(cutoffs) < 2:
        raise ValueError("At least two bucket-width cutoff results are required.")
    if len({cutoff.bucket_width for cutoff in cutoffs}) != len(cutoffs):
        raise ValueError("Bucket widths must be unique.")
    if pilot_horizon <= 0:
        raise ValueError("pilot_horizon must be greater than zero.")

    records = tuple(
        BucketWidthRecord(
            bucket_width=cutoff.bucket_width,
            bucket_count=cutoff.bucket_count,
            warmup_duration=cutoff.warmup_duration,
            post_warmup_bucket_count=math.floor(
                (pilot_horizon - cutoff.warmup_duration) / cutoff.bucket_width
                + 1e-12
            ),
            enough_data=(
                cutoff.bucket_count >= minimum_total_buckets
                and (pilot_horizon - cutoff.warmup_duration) / cutoff.bucket_width
                >= minimum_post_warmup_buckets
            ),
        )
        for cutoff in cutoffs
    )
    comparisons = tuple(
        compare_warmup_durations(
            first.warmup_duration,
            second.warmup_duration,
            tolerance=tolerance,
        )
        for first, second in zip(cutoffs, cutoffs[1:])
    )
    passed = all(record.enough_data for record in records) and all(
        comparison.passed for comparison in comparisons
    )
    selected = records[0].bucket_width if passed else None
    return BucketWidthSensitivityResult(
        passed=passed,
        selected_bucket_width=selected,
        records=records,
        adjacent_comparisons=comparisons,
    )


def check_post_warmup_stability(
    ensemble,
    warmup_duration,
    *,
    window_length=10,
    reference_tail_fraction=0.20,
    relative_tolerance=0.05,
    absolute_tolerances=None,
    required_consecutive_windows=3,
):
    """Check the first complete windows beginning at or after a proposed W."""
    if warmup_duration < 0 or warmup_duration > ensemble.pilot_horizon:
        raise ValueError("warmup_duration must lie within the pilot horizon.")
    if not isinstance(window_length, int) or window_length <= 0:
        raise ValueError("window_length must be a positive integer.")
    if not isinstance(required_consecutive_windows, int) or required_consecutive_windows < 2:
        raise ValueError("required_consecutive_windows must be at least two.")
    if not 0 < reference_tail_fraction < 1:
        raise ValueError("reference_tail_fraction must be between zero and one.")
    absolute_tolerances = dict(absolute_tolerances or {})

    window_duration = window_length * ensemble.bucket_width
    first_window_index = math.ceil(
        max(0.0, warmup_duration - 1e-12) / window_duration
    )
    first_bucket = first_window_index * window_length
    final_bucket = first_bucket + required_consecutive_windows * window_length
    if final_bucket > ensemble.bucket_count:
        raise ValueError("Not enough complete analysis windows remain after warm-up.")

    diagnostics = []
    for metric_name, raw_series in ensemble.important_metric_series().items():
        series = tuple(raw_series)
        reference_start_index, reference_level = _reference(
            series,
            reference_tail_fraction,
        )
        tolerance = max(
            relative_tolerance * abs(reference_level),
            absolute_tolerances.get(
                metric_name,
                default_absolute_tolerance(metric_name),
            ),
        )
        window_means = tuple(
            sum(
                series[
                    first_bucket + offset * window_length :
                    first_bucket + (offset + 1) * window_length
                ]
            )
            / window_length
            for offset in range(required_consecutive_windows)
        )
        slope = _linear_slope(window_means, window_duration)
        total_change = abs(slope) * (
            required_consecutive_windows * window_duration
        )
        passed = (
            all(abs(value - reference_level) <= tolerance for value in window_means)
            and total_change <= tolerance
        )
        diagnostics.append(
            PostWarmupMetricDiagnostic(
                metric_name=metric_name,
                reference_start_index=reference_start_index,
                reference_level=reference_level,
                tolerance=tolerance,
                first_window_index=first_window_index,
                window_means=window_means,
                slope=slope,
                total_fitted_change=total_change,
                passed=passed,
            )
        )

    return PostWarmupStabilityResult(
        warmup_duration=warmup_duration,
        window_length=window_length,
        passed=all(diagnostic.passed for diagnostic in diagnostics),
        metric_diagnostics=tuple(diagnostics),
    )

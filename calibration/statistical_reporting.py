import csv
import html
import json
import math
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from pathlib import Path

from .profile import save_calibration_profile
from .reporting import save_warmup_calibration


@dataclass(frozen=True)
class StatisticalCalibrationArtifacts:
    output_directory: str
    profile_path: str
    json_path: str
    markdown_path: str
    measurement_csv_path: str
    plot_paths: tuple[str, ...]
    warmup_output_directory: str


def _json_value(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
            if field.name != "bootstrap_p95_values"
        }
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def interval_summary(interval):
    if interval is None:
        return None
    return {
        "request_class": interval.request_class,
        "latency_statistic": interval.latency_statistic,
        "replication_count": interval.replication_count,
        "nonempty_replication_count": interval.nonempty_replication_count,
        "eligible_request_count": interval.eligible_request_count,
        "estimate": interval.estimate,
        "lower_bound": interval.lower_bound,
        "upper_bound": interval.upper_bound,
        "half_width": interval.half_width,
        "relative_half_width": interval.relative_half_width,
        "slo": interval.slo,
        "lower_tail_probability": interval.lower_tail_probability,
        "upper_tail_probability": interval.upper_tail_probability,
        "bootstrap_resample_count": interval.bootstrap_resample_count,
        "bootstrap_seed": interval.bootstrap_seed,
    }


def calibration_diagnostics_summary(warmup, measurement, options):
    final_minimum = warmup.minimum_configuration_calibrations[-1]
    selected = final_minimum.final_analysis.selected_analysis()
    cutoff = selected.cutoff
    return {
        "warmup": {
            "global_warmup": warmup.global_warmup,
            "bucket_width": warmup.bucket_width,
            "maximum_buckets_per_series": (
                options.warmup_max_buckets_per_series
            ),
            "maximum_total_bucket_values": (
                options.warmup_max_total_bucket_values
            ),
            "analysis_window_length": cutoff.window_length,
            "minimum_stable_configuration": list(
                warmup.final_parameters.minimum_replica_configuration
            ),
            "pilot_horizons": [
                attempt.pilot_horizon
                for calibration in warmup.minimum_configuration_calibrations
                for attempt in calibration.attempts
            ],
            "metric_specific_cutoffs": {
                item.metric_name: {
                    "cutoff_time": item.cutoff_time,
                    "reference_level": item.reference_level,
                    "tolerance": item.tolerance,
                    "accepted_slope": item.accepted_slope,
                    "accepted_total_fitted_change": (
                        item.accepted_total_fitted_change
                    ),
                }
                for item in cutoff.metric_diagnostics
            },
            "determining_metrics": list(cutoff.determining_metrics),
            "candidate_warmup": cutoff.candidate_warmup,
            "safety_window": cutoff.safety_window,
            "horizon_comparison": _json_value(
                final_minimum.horizon_comparison
            ),
            "bucket_width_sensitivity": _json_value(
                final_minimum.final_analysis.bucket_width_sensitivity
            ),
            "representative_configurations": [
                list(item)
                for item in warmup.validation.representative_configurations
            ],
            "validation_rounds": [
                {
                    "round_index": round_result.round_index,
                    "proposed_warmup": round_result.proposed_warmup,
                    "passed": round_result.passed,
                    "configurations": [
                        {
                            "configuration": list(item.replica_configuration),
                            "required_warmup": item.required_warmup,
                            "passed": item.passed,
                            "seeds": list(item.seeds),
                        }
                        for item in round_result.configurations
                    ],
                }
                for round_result in warmup.validation.rounds
            ],
        },
        "measurement": {
            "global_measurement_duration": (
                measurement.global_measurement_duration
            ),
            "initial_duration": measurement.initial_duration,
            "candidate_durations": list(measurement.candidate_durations),
            "external_arrival_rates": dict(
                measurement.request_class_external_arrival_rates
            ),
            "replications_per_configuration": (
                measurement.replications_per_configuration
            ),
            "seeds": list(measurement.seeds),
            "configurations": [
                {
                    "configuration": list(item.replica_configuration),
                    "accepted_duration": item.accepted_duration,
                    "candidates": [
                        {
                            "duration": candidate.duration,
                            "doubled_duration": candidate.doubled_duration,
                            "accepted": candidate.accepted,
                            "request_classes": [
                                {
                                    "request_class": diagnostic.request_class,
                                    "accepted": diagnostic.accepted,
                                    "interval": interval_summary(
                                        diagnostic.interval
                                    ),
                                    "doubled_interval": interval_summary(
                                        diagnostic.doubled_interval
                                    ),
                                    "estimate_relative_change": (
                                        diagnostic.p95_relative_change
                                    ),
                                    "ci_width_relative_change": (
                                        diagnostic.ci_width_relative_change
                                    ),
                                    "ci_half_width_absolute_change": (
                                        diagnostic.ci_half_width_absolute_change
                                    ),
                                    "sample_requirement_passed": (
                                        diagnostic.sample_requirement_passed
                                    ),
                                    "nonempty_replication_requirement_passed": (
                                        diagnostic.nonempty_replication_requirement_passed
                                    ),
                                    "precision_requirement_passed": (
                                        diagnostic.precision_requirement_passed
                                    ),
                                    "estimate_stability_passed": (
                                        diagnostic.p95_stability_passed
                                    ),
                                    "ci_width_stability_passed": (
                                        diagnostic.ci_width_stability_passed
                                    ),
                                }
                                for diagnostic in candidate.request_classes
                            ],
                        }
                        for candidate in item.candidates
                    ],
                }
                for item in measurement.configurations
            ],
        },
        "sequential_policy": {
            "minimum_replications": options.minimum_optimization_replications,
            "maximum_replications": options.maximum_optimization_replications,
            "batch_size": options.replication_batch_size,
            "maximum_looks": options.maximum_sequential_looks,
            "overall_alpha": options.overall_alpha,
            "error_control": "Bonferroni across two tails, looks, and classes",
            "latency_interval": "replication-cluster bootstrap",
            "bootstrap_resample_count": options.bootstrap_resample_count,
        },
    }


def _measurement_rows(measurement):
    for configuration in measurement.configurations:
        for candidate in configuration.candidates:
            for item in candidate.request_classes:
                interval = item.interval
                yield {
                    "configuration": str(configuration.replica_configuration),
                    "request_class": item.request_class,
                    "duration": item.duration,
                    "doubled_duration": item.doubled_duration,
                    "eligible_requests": (
                        interval.eligible_request_count if interval else 0
                    ),
                    "nonempty_replications": (
                        interval.nonempty_replication_count if interval else 0
                    ),
                    "latency_statistic": (
                        interval.latency_statistic if interval else ""
                    ),
                    "estimate": interval.estimate if interval else "",
                    "lower_bound": interval.lower_bound if interval else "",
                    "upper_bound": interval.upper_bound if interval else "",
                    "ci_half_width": interval.half_width if interval else "",
                    "estimate_relative_change": item.p95_relative_change,
                    "ci_width_relative_change": item.ci_width_relative_change,
                    "ci_half_width_absolute_change": (
                        item.ci_half_width_absolute_change
                    ),
                    "accepted": item.accepted,
                }


def _save_series_svg(series, path, *, title, y_label):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    width, height = 960, 540
    left, right, top, bottom = 80, 30, 60, 70
    plot_width = width - left - right
    plot_height = height - top - bottom
    all_points = [point for _, points in series for point in points]
    x_values = [point[0] for point in all_points]
    y_values = [point[1] for point in all_points]
    x_min, x_max = min(x_values), max(x_values)
    y_min, y_max = min(0.0, min(y_values)), max(y_values)
    if x_max == x_min:
        x_max = x_min + 1.0
    if y_max == y_min:
        y_max = y_min + 1.0
    colors = ("#1f77b4", "#ff7f0e", "#2ca02c", "#9467bd", "#d62728")

    def x_coord(value):
        return left + (value - x_min) * plot_width / (x_max - x_min)

    def y_coord(value):
        return top + (y_max - value) * plot_height / (y_max - y_min)

    output = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width / 2}" y="30" text-anchor="middle" font-family="sans-serif" font-size="18">{html.escape(title)}</text>',
        f'<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#222"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#222"/>',
        f'<text x="{width / 2}" y="{height - 18}" text-anchor="middle" font-family="sans-serif">measurement duration M</text>',
        f'<text x="18" y="{height / 2}" transform="rotate(-90 18 {height / 2})" text-anchor="middle" font-family="sans-serif">{html.escape(y_label)}</text>',
    ]
    legend_y = 48
    for index, (label, points) in enumerate(series):
        color = colors[index % len(colors)]
        coordinates = " ".join(
            f"{x_coord(x):.2f},{y_coord(y):.2f}" for x, y in points
        )
        output.append(
            f'<polyline points="{coordinates}" fill="none" stroke="{color}" stroke-width="2"/>'
        )
        output.append(
            f'<text x="{left + index * 170}" y="{legend_y}" font-family="sans-serif" font-size="11" fill="{color}">{html.escape(label)}</text>'
        )
    output.append("</svg>")
    path.write_text("\n".join(output) + "\n", encoding="utf-8")
    return path


def _measurement_plot_series(measurement, attribute):
    series = []
    for configuration in measurement.configurations:
        by_class = {}
        for candidate in configuration.candidates:
            for item in candidate.request_classes:
                if item.interval is None:
                    continue
                by_class.setdefault(item.request_class, []).append(
                    (candidate.duration, getattr(item.interval, attribute))
                )
        for request_class, points in by_class.items():
            series.append(
                (
                    f"c={configuration.replica_configuration} {request_class}",
                    tuple(points),
                )
            )
    return tuple(series)


def save_statistical_calibration(
    profile,
    warmup,
    measurement,
    output_directory,
    *,
    diagnostics,
):
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    profile_path = save_calibration_profile(
        profile,
        output_directory / "calibration_profile.yaml",
    )
    warmup_artifacts = save_warmup_calibration(
        warmup,
        output_directory / "warmup",
    )
    json_path = output_directory / "calibration_report.json"
    json_path.write_text(
        json.dumps(
            {
                "calibration_profile": profile.to_dict(),
                "diagnostics": diagnostics,
            },
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    csv_path = output_directory / "measurement_duration_study.csv"
    rows = tuple(_measurement_rows(measurement))
    with csv_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=tuple(rows[0]) if rows else ())
        if rows:
            writer.writeheader()
            writer.writerows(rows)
    plot_paths = ()
    latency_slo_kind = getattr(measurement, "latency_slo_kind", "percentile")
    estimate_series = _measurement_plot_series(measurement, "estimate")
    width_series = _measurement_plot_series(measurement, "half_width")
    if estimate_series and width_series:
        statistic_label = "Mean" if latency_slo_kind == "mean" else "P95"
        filename_prefix = "mean" if latency_slo_kind == "mean" else "p95"
        plot_paths = (
            str(
                _save_series_svg(
                    estimate_series,
                    output_directory
                    / f"{filename_prefix}_vs_measurement_duration.svg",
                    title=f"{statistic_label} stability versus measurement duration",
                    y_label=f"pooled {statistic_label.lower()}",
                )
            ),
            str(
                _save_series_svg(
                    width_series,
                    output_directory / "ci_width_vs_measurement_duration.svg",
                    title=f"{statistic_label} CI precision versus measurement duration",
                    y_label="CI half-width",
                )
            ),
        )
    markdown_path = output_directory / "calibration_report.md"
    interval_method = getattr(
        profile,
        "confidence_interval_method",
        profile.p95_confidence_interval_method,
    )
    markdown_path.write_text(
        "# Statistical simulation calibration\n\n"
        f"- Profile fingerprint: `{profile.profile_fingerprint}`\n"
        f"- Global warm-up W: {profile.warmup_duration:g}\n"
        f"- Global measurement duration M: {profile.measurement_duration:g}\n"
        f"- Optimization replications: {profile.minimum_optimization_replications} to {profile.maximum_optimization_replications}, batch {profile.replication_batch_size}\n"
        f"- Maximum sequential looks: {profile.maximum_sequential_looks}\n"
        f"- Overall alpha: {profile.overall_alpha:g}\n"
        f"- Latency statistic: {getattr(profile, 'latency_slo_kind', 'percentile')}\n"
        f"- Interval: {interval_method}\n"
        f"- Warm-up determining metrics: {', '.join(diagnostics['warmup']['determining_metrics'])}\n"
        f"- Measurement configurations: {len(measurement.configurations)}\n\n"
        "Optimization and final validation must use this frozen profile. "
        "Final validation uses its reserved fresh seed namespace.\n",
        encoding="utf-8",
    )
    return StatisticalCalibrationArtifacts(
        output_directory=str(output_directory),
        profile_path=str(profile_path),
        json_path=str(json_path),
        markdown_path=str(markdown_path),
        measurement_csv_path=str(csv_path),
        plot_paths=plot_paths,
        warmup_output_directory=warmup_artifacts.output_directory,
    )


def save_configuration_evaluation(evaluation, output_directory):
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    path = output_directory / "configuration_evaluation.json"
    payload = _json_value(evaluation)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    series = []
    class_names = tuple(name for name, _ in evaluation.request_class_intervals)
    for request_class in class_names:
        points = tuple(
            (
                look.replication_count,
                dict(look.request_class_intervals)[request_class].half_width,
            )
            for look in evaluation.looks
            if request_class in dict(look.request_class_intervals)
        )
        if points:
            series.append((request_class, points))
    if series:
        first_interval = dict(evaluation.request_class_intervals)[class_names[0]]
        statistic = first_interval.latency_statistic
        _save_series_svg(
            tuple(series),
            output_directory
            / f"replications_vs_{statistic}_ci_half_width.svg",
            title=f"Sequential {statistic} confidence interval precision",
            y_label="CI half-width",
        )
    return path


def save_final_validation(result, output_directory):
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    path = output_directory / "final_validation.json"
    path.write_text(
        json.dumps(_json_value(result), indent=2, sort_keys=True, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    return path


def save_statistical_calibration_failure(
    error,
    output_directory,
    *,
    stage,
    context=None,
):
    """Persist machine- and human-readable evidence for a refused calibration."""
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    record = {
        "status": "failed",
        "stage": str(stage),
        "message": str(error),
        "diagnostics": _json_value(getattr(error, "diagnostics", None)),
        "context": _json_value(context or {}),
    }
    json_path = output_directory / "calibration_failure.json"
    json_path.write_text(
        json.dumps(record, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    markdown_path = output_directory / "calibration_failure.md"
    markdown_path.write_text(
        "# Simulation calibration failed\n\n"
        f"- Stage: {stage}\n"
        f"- Reason: {error}\n\n"
        "No calibration profile was created and optimization must not start.\n",
        encoding="utf-8",
    )
    return json_path, markdown_path

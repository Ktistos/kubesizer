import csv
import html
import json
import math
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path

from .cutoff import MetricWarmupDiagnostic
from .ensemble import WarmupEnsembleSeries, centered_moving_average
from .sensitivity import PostWarmupMetricDiagnostic


@dataclass(frozen=True)
class WarmupCalibrationArtifacts:
    output_directory: str
    json_path: str
    csv_paths: tuple[str, ...]
    plot_paths: tuple[str, ...]


def _json_compatible(value, *, compact_ensembles=False):
    if compact_ensembles and isinstance(value, WarmupEnsembleSeries):
        return {
            "service_names": list(value.service_names),
            "replica_configuration": list(value.replica_configuration),
            "pilot_horizon": value.pilot_horizon,
            "bucket_width": value.bucket_width,
            "bucket_count": value.bucket_count,
            "seeds": list(value.seeds),
            "series_storage": (
                "Exact final and validation series are stored as CSV; "
                "historical ensemble arrays are omitted from JSON."
            ),
        }
    if compact_ensembles and isinstance(
        value,
        (MetricWarmupDiagnostic, PostWarmupMetricDiagnostic),
    ):
        payload = {
            field.name: _json_compatible(
                getattr(value, field.name),
                compact_ensembles=True,
            )
            for field in fields(value)
            if field.name != "window_means"
        }
        start = getattr(
            value,
            "accepted_window_index",
            getattr(value, "first_window_index", 0),
        )
        payload["window_mean_count"] = len(value.window_means)
        payload["decision_window_means"] = list(
            value.window_means[start : start + 3]
        )
        return payload
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_compatible(
                getattr(value, field.name),
                compact_ensembles=compact_ensembles,
            )
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {
            str(key): _json_compatible(
                item,
                compact_ensembles=compact_ensembles,
            )
            for key, item in value.items()
        }
    if isinstance(value, (tuple, list)):
        return [
            _json_compatible(item, compact_ensembles=compact_ensembles)
            for item in value
        ]
    if hasattr(value, "__dict__"):
        return {
            str(key): _json_compatible(
                item,
                compact_ensembles=compact_ensembles,
            )
            for key, item in vars(value).items()
        }
    return value


def _display_points(values, *, maximum_points=2_000):
    """Return block-averaged plot points without changing stored evidence."""
    if maximum_points <= 0:
        raise ValueError("maximum_points must be positive.")
    values = tuple(values)
    if len(values) <= maximum_points:
        return tuple((float(index), value) for index, value in enumerate(values))
    block_size = math.ceil(len(values) / maximum_points)
    points = []
    for start in range(0, len(values), block_size):
        stop = min(start + block_size, len(values))
        points.append(
            (
                (start + stop - 1) / 2.0,
                sum(values[start:stop]) / (stop - start),
            )
        )
    return tuple(points)


def write_ensemble_csv(ensemble, path):
    """Persist one averaged bucket series in a rectangular, inspectable form."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    service_names = ensemble.service_names
    fieldnames = ["bucket_index", "bucket_start", "bucket_end", "active_requests"]
    for service_name in service_names:
        fieldnames.extend(
            (
                f"queue_length[{service_name}]",
                f"busy_replicas[{service_name}]",
                f"number_in_system[{service_name}]",
                f"utilization[{service_name}]",
            )
        )

    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for bucket_index in range(ensemble.bucket_count):
            row = {
                "bucket_index": bucket_index,
                "bucket_start": bucket_index * ensemble.bucket_width,
                "bucket_end": (bucket_index + 1) * ensemble.bucket_width,
                "active_requests": ensemble.active_requests[bucket_index],
            }
            for service_id, service_name in enumerate(service_names):
                row[f"queue_length[{service_name}]"] = (
                    ensemble.queue_length_by_service[service_id][bucket_index]
                )
                row[f"busy_replicas[{service_name}]"] = (
                    ensemble.busy_replicas_by_service[service_id][bucket_index]
                )
                row[f"number_in_system[{service_name}]"] = (
                    ensemble.number_in_system_by_service[service_id][bucket_index]
                )
                row[f"utilization[{service_name}]"] = (
                    ensemble.utilization_by_service[service_id][bucket_index]
                )
            writer.writerow(row)
    return path


def save_ensemble_diagnostic_plot(
    ensemble,
    cutoff,
    path,
    *,
    smoothing_half_window=10,
):
    """Plot Welch-smoothed active, queue/system, and utilization diagnostics."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return _save_ensemble_diagnostic_svg(
            ensemble,
            cutoff,
            Path(path).with_suffix(".svg"),
            smoothing_half_window=smoothing_half_window,
        )

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    figure, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    def plot_series(axis, values, **options):
        points = _display_points(
            centered_moving_average(values, smoothing_half_window)
        )
        axis.plot(
            tuple((index + 0.5) * ensemble.bucket_width for index, _ in points),
            tuple(value for _, value in points),
            **options,
        )

    plot_series(axes[0], ensemble.active_requests, label="active requests")
    for service_id, service_name in enumerate(ensemble.service_names):
        plot_series(
            axes[1],
            ensemble.number_in_system_by_service[service_id],
            label=f"N {service_name}",
        )
        plot_series(
            axes[1],
            ensemble.queue_length_by_service[service_id],
            linestyle="--",
            label=f"Q {service_name}",
        )
        plot_series(
            axes[2],
            ensemble.utilization_by_service[service_id],
            label=service_name,
        )

    reference_start = cutoff.metric_diagnostics[0].reference_start_index * (
        ensemble.bucket_width
    )
    for axis in axes:
        axis.axvline(cutoff.warmup_duration, color="tab:red", label="selected W")
        axis.axvline(
            reference_start,
            color="black",
            linestyle=":",
            label="reference start",
        )
        axis.grid(alpha=0.25)
    axes[0].set_ylabel("requests")
    axes[1].set_ylabel("requests")
    axes[2].set_ylabel("utilization")
    axes[2].set_xlabel(f"simulation time ({ensemble.bucket_width:g}-unit buckets)")
    for axis in axes:
        axis.legend(loc="best", fontsize="small", ncol=2)
    figure.suptitle(
        "Warm-up diagnostics | "
        f"c={ensemble.replica_configuration}, H={ensemble.pilot_horizon:g}, "
        f"delta={ensemble.bucket_width:g}, R={len(ensemble.seeds)}, "
        f"Welch half-window={smoothing_half_window}"
    )
    figure.tight_layout()
    figure.savefig(path, dpi=160)
    plt.close(figure)
    return path


def _save_ensemble_diagnostic_svg(
    ensemble,
    cutoff,
    path,
    *,
    smoothing_half_window,
):
    """Create the same diagnostic panels without an optional plotting package."""
    path.parent.mkdir(parents=True, exist_ok=True)
    canvas_width = 1200
    canvas_height = 920
    left = 85
    right = 30
    plot_width = canvas_width - left - right
    panel_height = 225
    panel_tops = (105, 375, 645)
    colors = (
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#9467bd",
        "#8c564b",
        "#17becf",
        "#e377c2",
        "#7f7f7f",
    )

    panels = [
        (
            "Active end-to-end requests",
            (("active requests", ensemble.active_requests, colors[0], False),),
            None,
        ),
        (
            "Per-service number in system (solid) and queue length (dashed)",
            tuple(
                item
                for service_id, service_name in enumerate(ensemble.service_names)
                for item in (
                    (
                        f"N {service_name}",
                        ensemble.number_in_system_by_service[service_id],
                        colors[service_id % len(colors)],
                        False,
                    ),
                    (
                        f"Q {service_name}",
                        ensemble.queue_length_by_service[service_id],
                        colors[service_id % len(colors)],
                        True,
                    ),
                )
            ),
            None,
        ),
        (
            "Per-service utilization",
            tuple(
                (
                    service_name,
                    ensemble.utilization_by_service[service_id],
                    colors[service_id % len(colors)],
                    False,
                )
                for service_id, service_name in enumerate(ensemble.service_names)
            ),
            1.0,
        ),
    ]
    reference_start = cutoff.metric_diagnostics[0].reference_start_index * (
        ensemble.bucket_width
    )

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{canvas_width}" '
        f'height="{canvas_height}" viewBox="0 0 {canvas_width} {canvas_height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:sans-serif;fill:#222}.axis{stroke:#333;stroke-width:1}'
        '.grid{stroke:#ddd;stroke-width:1}.warmup{stroke:#d62728;stroke-width:2}'
        '.reference{stroke:#111;stroke-width:1;stroke-dasharray:4 4}</style>',
        f'<text x="{canvas_width / 2}" y="28" text-anchor="middle" font-size="18">'
        "Warm-up diagnostics</text>",
        f'<text x="{canvas_width / 2}" y="52" text-anchor="middle" font-size="13">'
        f'c={html.escape(str(ensemble.replica_configuration))}, '
        f'H={ensemble.pilot_horizon:g}, delta={ensemble.bucket_width:g}, '
        f'R={len(ensemble.seeds)}, Welch half-window={smoothing_half_window}</text>',
    ]

    def x_coordinate(simulation_time):
        return left + plot_width * simulation_time / ensemble.pilot_horizon

    for panel_index, (title, panel_series, fixed_maximum) in enumerate(panels):
        top = panel_tops[panel_index]
        smoothed = tuple(
            (
                label,
                centered_moving_average(values, smoothing_half_window),
                color,
                dashed,
            )
            for label, values, color, dashed in panel_series
        )
        data_maximum = max(
            (max(values, default=0.0) for _, values, _, _ in smoothed),
            default=0.0,
        )
        y_maximum = max(fixed_maximum or 0.0, data_maximum, 1e-12)

        svg.append(
            f'<text x="{left}" y="{top - 12}" font-size="14">'
            f'{html.escape(title)}</text>'
        )
        for fraction in (0.0, 0.5, 1.0):
            y = top + panel_height * (1.0 - fraction)
            svg.append(
                f'<line class="grid" x1="{left}" y1="{y:.2f}" '
                f'x2="{left + plot_width}" y2="{y:.2f}"/>'
            )
            svg.append(
                f'<text x="{left - 8}" y="{y + 4:.2f}" text-anchor="end" '
                f'font-size="11">{fraction * y_maximum:.3g}</text>'
            )
        svg.append(
            f'<line class="axis" x1="{left}" y1="{top + panel_height}" '
            f'x2="{left + plot_width}" y2="{top + panel_height}"/>'
        )
        svg.append(
            f'<line class="axis" x1="{left}" y1="{top}" '
            f'x2="{left}" y2="{top + panel_height}"/>'
        )

        for label, values, color, dashed in smoothed:
            display_points = _display_points(values)
            points = " ".join(
                f"{x_coordinate((index + 0.5) * ensemble.bucket_width):.2f},"
                f"{top + panel_height * (1.0 - value / y_maximum):.2f}"
                for index, value in display_points
            )
            dash_attribute = ' stroke-dasharray="6 4"' if dashed else ""
            svg.append(
                f'<polyline points="{points}" fill="none" stroke="{color}" '
                f'stroke-width="1.5"{dash_attribute}/>'
            )

        warmup_x = x_coordinate(cutoff.warmup_duration)
        reference_x = x_coordinate(reference_start)
        svg.append(
            f'<line class="warmup" x1="{warmup_x:.2f}" y1="{top}" '
            f'x2="{warmup_x:.2f}" y2="{top + panel_height}"/>'
        )
        svg.append(
            f'<line class="reference" x1="{reference_x:.2f}" y1="{top}" '
            f'x2="{reference_x:.2f}" y2="{top + panel_height}"/>'
        )
        legend_x = left
        legend_y = top + panel_height + 20
        for label, _, color, dashed in smoothed:
            dash_attribute = ' stroke-dasharray="6 4"' if dashed else ""
            svg.append(
                f'<line x1="{legend_x}" y1="{legend_y}" '
                f'x2="{legend_x + 18}" y2="{legend_y}" stroke="{color}"'
                f'{dash_attribute}/>'
            )
            svg.append(
                f'<text x="{legend_x + 23}" y="{legend_y + 4}" font-size="10">'
                f'{html.escape(label)}</text>'
            )
            legend_x += max(90, 8 * len(label) + 35)

    svg.extend(
        (
            f'<text x="{left}" y="{canvas_height - 8}" font-size="11">'
            '<tspan fill="#d62728">red: selected W</tspan>; '
            "dotted black: late-reference start</text>",
            "</svg>",
        )
    )
    path.write_text("\n".join(svg) + "\n", encoding="utf-8")
    return path


def save_warmup_calibration(
    result,
    output_directory,
    *,
    create_plots=True,
    smoothing_half_windows=(5, 10, 20),
):
    """Persist the numeric result, averaged series, and diagnostic evidence."""
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    json_path = output_directory / "warmup_calibration.json"
    with json_path.open("w", encoding="utf-8") as output:
        json.dump(
            _json_compatible(result, compact_ensembles=True),
            output,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        output.write("\n")

    csv_paths = []
    plot_paths = []
    final_minimum = result.minimum_configuration_calibrations[-1].final_analysis
    for width_analysis in final_minimum.bucket_width_analyses:
        width_label = f"{width_analysis.bucket_width:g}".replace(".", "p")
        csv_path = output_directory / f"minimum_delta_{width_label}.csv"
        write_ensemble_csv(width_analysis.ensemble, csv_path)
        csv_paths.append(str(csv_path))
        if create_plots:
            for half_window in smoothing_half_windows:
                plot_path = output_directory / (
                    f"minimum_delta_{width_label}_welch_{half_window}.png"
                )
                saved_plot_path = save_ensemble_diagnostic_plot(
                    width_analysis.ensemble,
                    width_analysis.cutoff,
                    plot_path,
                    smoothing_half_window=half_window,
                )
                plot_paths.append(str(saved_plot_path))

    final_validation_round = result.validation.rounds[-1]
    for index, configuration_result in enumerate(
        final_validation_round.configurations
    ):
        csv_path = output_directory / f"validation_configuration_{index}.csv"
        write_ensemble_csv(configuration_result.ensemble, csv_path)
        csv_paths.append(str(csv_path))

    return WarmupCalibrationArtifacts(
        output_directory=str(output_directory),
        json_path=str(json_path),
        csv_paths=tuple(csv_paths),
        plot_paths=tuple(plot_paths),
    )


def save_warmup_calibration_failure(error, output_directory, *, parameters=None):
    """Persist partial evidence when the workflow correctly refuses a numeric W."""
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    path = output_directory / "warmup_calibration_failure.json"
    record = {
        "status": "failed",
        "message": str(error),
        "parameters": parameters,
        "diagnostics": getattr(error, "diagnostics", None),
    }
    with path.open("w", encoding="utf-8") as output:
        json.dump(
            _json_compatible(record, compact_ensembles=True),
            output,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        output.write("\n")
    return path

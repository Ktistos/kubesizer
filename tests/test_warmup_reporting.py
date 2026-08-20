import builtins
import csv
import json
from types import SimpleNamespace

from calibration import (
    WarmupEnsembleSeries,
    save_ensemble_diagnostic_plot,
    save_warmup_calibration_failure,
    write_ensemble_csv,
)
from calibration.cutoff import MetricWarmupDiagnostic
from calibration.reporting import _display_points, _json_compatible


def make_ensemble():
    return WarmupEnsembleSeries(
        service_names=("api",),
        replica_configuration=(2,),
        pilot_horizon=2.0,
        bucket_width=1.0,
        seeds=(1, 2),
        active_requests=(1.5, 2.5),
        queue_length_by_service=((0.25, 0.5),),
        busy_replicas_by_service=((1.0, 1.5),),
        number_in_system_by_service=((1.25, 2.0),),
        utilization_by_service=((0.5, 0.75),),
    )


def test_ensemble_csv_contains_bucket_times_and_all_state_metrics(tmp_path):
    path = write_ensemble_csv(make_ensemble(), tmp_path / "nested" / "series.csv")

    with path.open(newline="", encoding="utf-8") as input_file:
        rows = list(csv.DictReader(input_file))

    assert len(rows) == 2
    assert rows[0]["bucket_start"] == "0.0"
    assert rows[0]["bucket_end"] == "1.0"
    assert rows[1]["active_requests"] == "2.5"
    assert rows[1]["queue_length[api]"] == "0.5"
    assert rows[1]["busy_replicas[api]"] == "1.5"
    assert rows[1]["number_in_system[api]"] == "2.0"
    assert rows[1]["utilization[api]"] == "0.75"


def test_plotter_falls_back_to_svg_without_matplotlib(tmp_path, monkeypatch):
    original_import = builtins.__import__

    def import_without_matplotlib(name, *args, **kwargs):
        if name.startswith("matplotlib"):
            raise ImportError("test fallback")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_matplotlib)
    cutoff = SimpleNamespace(
        warmup_duration=0.5,
        metric_diagnostics=(SimpleNamespace(reference_start_index=1),),
    )

    path = save_ensemble_diagnostic_plot(
        make_ensemble(),
        cutoff,
        tmp_path / "diagnostic.png",
        smoothing_half_window=1,
    )

    assert path.suffix == ".svg"
    assert path.exists()
    assert "selected W" in path.read_text(encoding="utf-8")


def test_failed_calibration_writes_partial_diagnostics(tmp_path):
    error = RuntimeError("did not settle")
    error.diagnostics = {"attempts": (SimpleNamespace(),)}

    path = save_warmup_calibration_failure(
        error,
        tmp_path,
        parameters=SimpleNamespace(),
    )
    record = json.loads(path.read_text(encoding="utf-8"))

    assert record["status"] == "failed"
    assert record["message"] == "did not settle"


def test_compact_json_replaces_bucket_arrays_with_ensemble_metadata():
    payload = _json_compatible(make_ensemble(), compact_ensembles=True)

    assert payload["bucket_count"] == 2
    assert "arrays are omitted" in payload["series_storage"]
    assert "active_requests" not in payload


def test_display_points_block_average_and_bound_plot_size():
    points = _display_points(tuple(range(10)), maximum_points=3)

    assert len(points) == 3
    assert points == ((1.5, 1.5), (5.5, 5.5), (8.5, 8.5))


def test_compact_json_retains_only_cutoff_decision_window_means():
    diagnostic = MetricWarmupDiagnostic(
        metric_name="active_requests",
        reference_start_index=8,
        reference_level=2.0,
        tolerance=0.1,
        window_means=(0.0, 1.0, 1.9, 2.0, 2.1, 2.0),
        accepted_window_index=2,
        cutoff_time=20.0,
        accepted_slope=0.01,
        accepted_total_fitted_change=0.03,
    )

    payload = _json_compatible(diagnostic, compact_ensembles=True)

    assert "window_means" not in payload
    assert payload["window_mean_count"] == 6
    assert payload["decision_window_means"] == [1.9, 2.0, 2.1]

from types import SimpleNamespace

from calibration import (
    save_statistical_calibration,
    save_statistical_calibration_failure,
)
from metrics import replication_cluster_bootstrap_p95


def test_statistical_calibration_report_writes_machine_human_csv_and_plots(
    tmp_path,
    monkeypatch,
):
    import calibration.statistical_reporting as reporting

    interval = replication_cluster_bootstrap_p95(
        ((1.0, 1.1), (1.0, 1.2)),
        request_class="checkout",
        slo=2.0,
        resample_count=1_000,
        seed=7,
        lower_tail_probability=0.025,
        upper_tail_probability=0.975,
    )
    diagnostic = SimpleNamespace(
        request_class="checkout",
        duration=10.0,
        doubled_duration=20.0,
        accepted=True,
        interval=interval,
        doubled_interval=interval,
        p95_relative_change=0.0,
        ci_width_relative_change=0.0,
        ci_half_width_absolute_change=0.0,
        sample_requirement_passed=True,
        nonempty_replication_requirement_passed=True,
        precision_requirement_passed=True,
        p95_stability_passed=True,
        ci_width_stability_passed=True,
    )
    candidate = SimpleNamespace(
        duration=10.0,
        doubled_duration=20.0,
        accepted=True,
        request_classes=(diagnostic,),
    )
    measurement = SimpleNamespace(
        configurations=(
            SimpleNamespace(
                replica_configuration=(1,),
                candidates=(candidate,),
            ),
        ),
    )
    profile = SimpleNamespace(
        profile_fingerprint="fingerprint",
        warmup_duration=5.0,
        measurement_duration=10.0,
        minimum_optimization_replications=30,
        maximum_optimization_replications=60,
        replication_batch_size=10,
        maximum_sequential_looks=4,
        overall_alpha=0.05,
        p95_confidence_interval_method="cluster-bootstrap",
        to_dict=lambda: {"profile_fingerprint": "fingerprint"},
    )
    monkeypatch.setattr(
        reporting,
        "save_calibration_profile",
        lambda _profile, path: path,
    )
    monkeypatch.setattr(
        reporting,
        "save_warmup_calibration",
        lambda _warmup, path: SimpleNamespace(output_directory=str(path)),
    )
    diagnostics = {
        "warmup": {"determining_metrics": ["active_requests"]},
    }

    artifacts = save_statistical_calibration(
        profile,
        object(),
        measurement,
        tmp_path,
        diagnostics=diagnostics,
    )

    assert (tmp_path / "calibration_report.json").is_file()
    assert (tmp_path / "calibration_report.md").is_file()
    assert (tmp_path / "measurement_duration_study.csv").is_file()
    assert (tmp_path / "p95_vs_measurement_duration.svg").is_file()
    assert (tmp_path / "ci_width_vs_measurement_duration.svg").is_file()
    assert len(artifacts.plot_paths) == 2
    assert "Global warm-up W: 5" in (tmp_path / "calibration_report.md").read_text(
        encoding="utf-8"
    )


def test_failed_calibration_persists_stage_and_nonfinite_diagnostics(tmp_path):
    error = RuntimeError("rare class is undersampled")
    error.diagnostics = {"accepted_duration": float("nan")}

    json_path, markdown_path = save_statistical_calibration_failure(
        error,
        tmp_path,
        stage="measurement",
    )

    assert '"stage": "measurement"' in json_path.read_text(encoding="utf-8")
    assert '"nan"' in json_path.read_text(encoding="utf-8")
    assert "No calibration profile was created" in markdown_path.read_text(
        encoding="utf-8"
    )


def test_diagnostic_json_normalizes_numpy_scalar_flags_and_counts():
    import json
    import numpy as np
    from calibration.statistical_reporting import _json_value

    value = {
        "rounds": [
            {"passed": np.bool_(True), "count": np.int64(12), "mean": np.float32(0.5)}
        ]
    }
    assert json.loads(json.dumps(_json_value(value), allow_nan=False)) == {
        "rounds": [{"passed": True, "count": 12, "mean": 0.5}]
    }

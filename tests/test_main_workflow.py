import builtins
import io
from pathlib import Path
from types import SimpleNamespace

import pytest

import main


def test_missing_profile_runs_complete_calibration(monkeypatch):
    expected = object()
    captured = {}
    monkeypatch.setattr(
        main, "CALIBRATION_PROFILE_PATH", SimpleNamespace(exists=lambda: False)
    )

    def fake_calibrate(system, **kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(main, "calibrate", fake_calibrate)
    system = SimpleNamespace(get_services=lambda: {"api": object()})

    result = main.load_or_calibrate_profile(system)

    assert result is expected
    assert captured["request_class_slos"] == main.REQUEST_CLASS_SLOS
    assert captured["maximum_replicas"] == main.MAX_REPLICAS_PER_SERVICE
    assert captured["report_directory"] == main.CALIBRATION_OUTPUT_DIRECTORY


def test_existing_profile_is_validated_and_not_recalibrated(monkeypatch):
    profile = object()
    validated = object()
    monkeypatch.setattr(
        main, "CALIBRATION_PROFILE_PATH", SimpleNamespace(exists=lambda: True)
    )
    monkeypatch.setattr(main, "load_calibration_profile", lambda path: profile)
    monkeypatch.setattr(
        main,
        "validate_profile_for_experiment",
        lambda selected, system, **kwargs: validated,
    )
    monkeypatch.setattr(
        main,
        "calibrate_before_optimization",
        lambda *_args, **_kwargs: pytest.fail("must not recalibrate valid profile"),
    )
    system = SimpleNamespace(get_services=lambda: {"api": object(), "db": object()})

    assert main.load_or_calibrate_profile(system) is validated


def test_incompatible_existing_profile_is_rejected_not_silently_replaced(monkeypatch):
    monkeypatch.setattr(
        main, "CALIBRATION_PROFILE_PATH", SimpleNamespace(exists=lambda: True)
    )
    monkeypatch.setattr(main, "load_calibration_profile", lambda path: object())
    monkeypatch.setattr(
        main,
        "validate_profile_for_experiment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            main.CalibrationProfileMismatchError("workload mismatch")
        ),
    )
    monkeypatch.setattr(
        main,
        "calibrate_before_optimization",
        lambda *_args, **_kwargs: pytest.fail("mismatch requires explicit action"),
    )
    system = SimpleNamespace(get_services=lambda: {"api": object()})

    with pytest.raises(main.CalibrationProfileMismatchError, match="workload"):
        main.load_or_calibrate_profile(system)


def test_optimization_and_final_validation_use_public_profile_interface(monkeypatch):
    optimized = object()
    validated = object()
    profile = object()
    monkeypatch.setattr(
        main,
        "optimize_with_calibrated_simulation",
        lambda system, selected, **kwargs: optimized,
    )
    monkeypatch.setattr(
        main,
        "validate_final_configuration",
        lambda system, configuration, selected, **kwargs: validated,
    )

    assert main.optimize_replicas(object(), profile) is optimized
    result = SimpleNamespace(configuration=(2,))
    assert main.validate_optimizer_result(object(), result, profile) is validated


def test_main_does_not_place_when_fresh_validation_fails(monkeypatch):
    system = SimpleNamespace(configure_system_replicas=lambda _value: None)
    profile = SimpleNamespace(
        profile_fingerprint="profile",
        warmup_duration=5.0,
        measurement_duration=10.0,
        minimum_optimization_replications=2,
        maximum_optimization_replications=4,
    )
    optimization = SimpleNamespace(configuration=(1,))
    validation = SimpleNamespace(
        passed=False,
        classification=SimpleNamespace(value="FAILED_SLO_VALIDATION"),
        intervals={},
        seeds=(1, 2),
        failure_reason="upper bound exceeds SLO",
    )
    placement_calls = []
    monkeypatch.setattr(
        builtins,
        "open",
        lambda *_args, **_kwargs: io.StringIO("{}"),
    )
    monkeypatch.setattr(main, "System", lambda *_args, **_kwargs: system)
    monkeypatch.setattr(main, "load_or_calibrate_profile", lambda _system: profile)
    monkeypatch.setattr(main, "optimize_replicas", lambda *_args: optimization)
    monkeypatch.setattr(main, "report_replica_result", lambda *_args: None)
    monkeypatch.setattr(main, "validate_optimizer_result", lambda *_args: validation)
    monkeypatch.setattr(
        main,
        "Cluster",
        lambda *_args, **_kwargs: placement_calls.append(True),
    )

    with pytest.raises(RuntimeError, match="upper bound"):
        main.main()

    assert placement_calls == []

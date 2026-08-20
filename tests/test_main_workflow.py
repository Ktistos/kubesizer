import builtins
import io
from types import SimpleNamespace

import pytest

import main
from optimization import (
    OptimizationMode,
    OptimizationModes,
    ReplicaEvaluationMode,
    WorkflowOptimizationMode,
)


def test_calibrate_before_optimization_always_runs_complete_calibration(monkeypatch):
    expected = object()
    captured = {}

    def fake_calibrate(system, **kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(main, "calibrate", fake_calibrate)
    system = SimpleNamespace(get_services=lambda: {"api": object()})

    result = main.calibrate_before_optimization(system)

    assert result is expected
    assert captured["request_class_slos"] == main.REQUEST_CLASS_SLOS
    assert captured["maximum_replicas"] == main.MAX_REPLICAS_PER_SERVICE
    assert "report_directory" not in captured


def test_optimization_and_final_validation_use_public_profile_interface(monkeypatch):
    optimized = object()
    validated = object()
    profile = object()
    optimization_arguments = {}

    def fake_optimize(system, selected, **kwargs):
        optimization_arguments.update(kwargs)
        return optimized

    monkeypatch.setattr(
        main,
        "optimize_with_calibrated_simulation",
        fake_optimize,
    )
    validation_arguments = {}

    def fake_validate(system, configuration, selected, **kwargs):
        validation_arguments.update(kwargs)
        return validated

    monkeypatch.setattr(main, "validate_final_configuration", fake_validate)

    runtime_limit_calibration = object()
    monkeypatch.setattr(
        main,
        "OPTIMIZATION_MODES",
        OptimizationModes(
            mode=WorkflowOptimizationMode.HYBRID,
            replica=OptimizationMode.EXACT,
            placement=OptimizationMode.SCALABLE,
        ),
    )
    assert (
        main.optimize_replicas(
            object(),
            profile,
            runtime_limit_calibration=runtime_limit_calibration,
        )
        is optimized
    )
    assert optimization_arguments["runtime_limit_calibration"] is (
        runtime_limit_calibration
    )
    assert optimization_arguments["mode"] == OptimizationMode.EXACT
    result = SimpleNamespace(configuration=(2,))
    assert main.validate_optimizer_result(object(), result, profile) is validated
    assert "report_directory" not in validation_arguments


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
    monkeypatch.setattr(
        main,
        "estimate_workflow_runtime",
        lambda *_args, **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(main, "format_runtime_estimate", lambda _value: "estimate")
    monkeypatch.setattr(
        main,
        "calibrate_before_optimization",
        lambda _system, **_kwargs: profile,
    )
    monkeypatch.setattr(
        main,
        "optimize_replicas",
        lambda *_args, **_kwargs: optimization,
    )
    monkeypatch.setattr(main, "report_replica_result", lambda *_args: None)
    monkeypatch.setattr(
        main,
        "validate_optimizer_result",
        lambda *_args, **_kwargs: validation,
    )
    monkeypatch.setattr(
        main,
        "Cluster",
        lambda *_args, **_kwargs: placement_calls.append(True),
    )

    with pytest.raises(RuntimeError, match="upper bound"):
        main.main()

    assert placement_calls == []


def test_main_analytical_mode_skips_every_simulation_stage(monkeypatch):
    system = SimpleNamespace(configure_system_replicas=lambda _value: None)
    optimization = SimpleNamespace(configuration=(2,))
    placement = SimpleNamespace(
        configuration={"api": "node"},
        solution_mode="EXACT",
        complete_states=1,
        pruned_complete_placements=0,
        total_configurations=1,
        optimality_proven=True,
        budget_exhausted=False,
    )
    calls = []

    monkeypatch.setattr(
        builtins,
        "open",
        lambda *_args, **_kwargs: io.StringIO("{}"),
    )
    monkeypatch.setattr(main, "System", lambda *_args, **_kwargs: system)
    monkeypatch.setattr(
        main,
        "REPLICA_EVALUATION_MODE",
        ReplicaEvaluationMode.ANALYTICAL,
    )
    monkeypatch.setattr(
        main,
        "estimate_workflow_runtime",
        lambda *_args, **_kwargs: pytest.fail("runtime preflight must be skipped"),
    )
    monkeypatch.setattr(
        main,
        "calibrate_before_optimization",
        lambda *_args, **_kwargs: pytest.fail("calibration must be skipped"),
    )
    monkeypatch.setattr(
        main,
        "validate_optimizer_result",
        lambda *_args, **_kwargs: pytest.fail("validation must be skipped"),
    )
    monkeypatch.setattr(
        main,
        "optimize_replicas_analytically",
        lambda selected: calls.append(("analytical", selected)) or optimization,
    )
    monkeypatch.setattr(main, "report_replica_result", lambda *_args: None)
    monkeypatch.setattr(main, "Cluster", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        main,
        "optimize_node_placement",
        lambda *_args, **_kwargs: placement,
    )
    monkeypatch.setattr(main, "NodePlacementProblem", lambda value: value)

    main.main()

    assert calls == [("analytical", system)]

import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import kubesizer_cli
from runtime_estimation import StageRuntimeEstimate


class _ManualClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class _Classification:
    value = "ANALYTICALLY_FEASIBLE"


def _fake_system(*, slo_kind="mean"):
    configured = []
    return SimpleNamespace(
        get_services=lambda: {"api": object()},
        get_request_classes=lambda: ("request",),
        get_request_class_latency_slo_kind=lambda: slo_kind,
        get_request_class_mean_latency_limits=lambda: {"request": 1.0},
        get_request_class_mean_latencies=lambda: {"request": 0.5},
        configure_system_replicas=lambda configuration: configured.append(
            tuple(configuration)
        ),
        configured=configured,
    )


def _fake_replica_result():
    metrics = SimpleNamespace(
        classification=_Classification(),
        evaluation_basis="ANALYTICAL_MEAN_LATENCY",
        means={"request": 0.5},
        limits={"request": 1.0},
    )
    return SimpleNamespace(
        configuration=(1,),
        cost=0.0,
        objective_value=1.0,
        metrics=metrics,
        explored_states=1,
        path=((1,),),
        method="UNIFORM_COST_EXACT",
        termination_reason="LOWER_BOUND_MATCHED",
        minimality_proven=True,
        optimality_proven=True,
    )


def test_progress_reporter_emits_only_concise_stage_information():
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(output=output)

    reporter(
        "pilot_batch_started",
        {
            "simulation_horizon": 500.0,
            "replications": 30,
            "workers": 8,
        },
    )
    reporter("pilot_horizon_analyzing", {"pilot_horizon": 250.0})
    reporter("replication_batch_started", {"configuration": (1, 5, 1)})
    reporter("validation_started", {"configurations": 4})
    reporter(
        "measurement_trajectories_started",
        {"simulation_duration": 1000.0, "configuration": (1, 5, 1)},
    )
    reporter("calibration_completed", {"profile_fingerprint": "secret-noise"})

    lines = output.getvalue().splitlines()
    assert [line.split(" | ", 1)[0] for line in lines] == [
        "Calibration: testing warm-up horizon 500",
        "Calibration: validating warm-up",
        "Calibration: testing measurement duration 1000",
        "Calibration complete",
    ]
    assert all("elapsed 00:00" in line for line in lines)
    assert all("secret-noise" not in line for line in lines)


def test_progress_reporter_uses_coarse_search_milestones():
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(output=output)
    reporter("replica_optimization_started", {})
    for completed in (1, 24, 25, 26, 50, 75, 100):
        reporter(
            "replica_configuration_evaluated",
            {
                "completed_configurations": completed,
                "search_space_configurations": 100,
                "configuration": (completed,),
            },
        )
    reporter("replica_optimization_completed", {"completed_configurations": 100})

    lines = output.getvalue().splitlines()
    assert [line.split(" | ", 1)[0] for line in lines] == [
        "Optimizing replicas",
        "Replica search",
        "Replica search",
        "Replica search",
        "Replica optimization complete",
    ]
    assert "[######------] 50%" in lines[2]
    assert all("elapsed 00:00" in line for line in lines)


def test_json_progress_excludes_raw_callback_details():
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(
        json_output=True,
        output=output,
    )

    reporter(
        "replica_optimization_started",
        {
            "total_configurations": 999,
            "internal_value": "not-user-facing",
        },
    )

    progress = json.loads(output.getvalue())["progress"]
    assert progress["stage"] == "replica_sizing"
    assert progress["message"] == "Optimizing replicas"
    assert progress["elapsed_seconds"] >= 0
    assert progress["estimated_remaining_scope"] == "stage"
    assert "internal_value" not in progress


def test_progress_reporter_combines_elapsed_progress_with_planning_eta():
    clock = _ManualClock()
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(
        output=output,
        clock=clock,
    )
    estimate = SimpleNamespace(
        stages=(
            StageRuntimeEstimate(
                "Stage B — replica optimization",
                40.0,
                80.0,
                "ordinary",
                "ceiling",
            ),
            StageRuntimeEstimate(
                "Stage C — final validation",
                20.0,
                30.0,
                "ordinary",
                "ceiling",
            ),
        )
    )
    reporter.configure_workflow(
        ("replica_sizing", "validation"),
        runtime_estimate=estimate,
    )
    reporter(
        "replica_optimization_started",
        {
            "completed_configurations": 0,
            "total_configurations": 100,
        },
    )
    clock.advance(10.0)
    reporter(
        "replica_configuration_evaluated",
        {
            "completed_configurations": 25,
            "total_configurations": 100,
        },
    )

    line = output.getvalue().splitlines()[-1]
    assert "[###---------] 25%" in line
    assert "elapsed 00:10" in line
    assert "estimated overall remaining 00:50" in line


def test_simulation_cli_preflight_estimates_exact_replica_work(monkeypatch):
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(output=output)
    args = SimpleNamespace(
        replica_evaluation="simulation",
        max_replicas=7,
        workers=2,
        quiet=False,
        json=False,
        _progress_reporter=reporter,
    )
    observed = {}
    stages = tuple(
        StageRuntimeEstimate(name, 1.0, 2.0, "ordinary", "ceiling")
        for name in (
            kubesizer_cli._ConciseProgressReporter._STAGE_ESTIMATE_NAMES.values()
        )
    )
    estimate = SimpleNamespace(
        stages=stages,
        total_ordinary_planning_seconds=4.0,
        total_configured_ceiling_seconds=8.0,
    )

    def fake_estimate(system, **kwargs):
        observed.update(kwargs)
        return estimate

    monkeypatch.setattr(kubesizer_cli, "estimate_workflow_runtime", fake_estimate)

    kubesizer_cli._prepare_runtime_reporting(
        object(),
        args,
    )

    assert observed["maximum_replicas"] == 7
    assert observed["workers"] == 2
    assert "replica_optimization_mode" not in observed
    assert reporter._expected_stages == (
        "calibration",
        "replica_sizing",
        "validation",
    )
    assert [line.split(" | ", 1)[0] for line in output.getvalue().splitlines()] == [
        "Estimating runtime",
        "Runtime estimate ready: about 00:04 "
        "(configured stress-path projection 00:08)",
    ]


def test_progress_hides_non_actionable_astronomical_stress_projection():
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(output=output)
    estimate = SimpleNamespace(
        stages=(),
        total_ordinary_planning_seconds=20.0,
        total_configured_ceiling_seconds=1_000_000_000.0,
    )

    reporter.runtime_estimation_completed(estimate)

    line = output.getvalue().split(" | ", 1)[0]
    assert line == "Runtime estimate ready: about 00:20; no finite worst-case guarantee"


def test_recommendation_is_service_centric():
    system = SimpleNamespace(
        get_services=lambda: {"api": object(), "worker": object()},
    )

    recommendation = kubesizer_cli._service_recommendations(
        system,
        (2, 1),
    )

    assert recommendation == {
        "api": {
            "recommended_replicas": 2,
        },
        "worker": {
            "recommended_replicas": 1,
        },
    }


def test_analytical_run_emits_replica_only_recommendation(
    monkeypatch,
    capsys,
):
    system = _fake_system()
    monkeypatch.setattr(
        kubesizer_cli,
        "_load_system",
        lambda path: (system, Path(path)),
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "optimize_with_analytical_model",
        lambda *args, **kwargs: _fake_replica_result(),
    )
    exit_code = kubesizer_cli.main(
        [
            "--workload",
            "ignored.yaml",
            "--replica-evaluation",
            "analytical",
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["result"] == {
        "services": {
            "api": {
                "recommended_replicas": 1,
            }
        }
    }


def test_failed_final_validation_does_not_emit_a_recommendation(
    monkeypatch,
    capsys,
):
    system = _fake_system(slo_kind="percentile")
    validation = SimpleNamespace(
        passed=False,
        failure_reason="p95 upper confidence bound exceeds the SLO",
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "_load_system",
        lambda path: (system, Path(path)),
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "_run_replica_workflow",
        lambda *args: (_fake_replica_result(), object(), validation),
    )

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            "ignored.yaml",
            "--replica-evaluation",
            "simulation",
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert output["ok"] is False
    assert "result" not in output
    assert "failed final validation" in output["error"]["message"]


def test_simulation_workflow_always_runs_internal_final_validation(monkeypatch):
    system = _fake_system(slo_kind="percentile")
    profile = SimpleNamespace(to_dict=lambda: {"profile": "test"})
    validation = SimpleNamespace(passed=True)
    result = _fake_replica_result()
    observed = {}
    arguments = SimpleNamespace(
        replica_evaluation="simulation",
        max_replicas=10,
        workers=1,
        skip_final_validation=False,
    )

    def run_calibration(*args, **kwargs):
        observed["minimum_configuration"] = kwargs["minimum_configuration"]
        return profile

    monkeypatch.setattr(kubesizer_cli, "_run_calibration", run_calibration)

    def optimize(*args, **kwargs):
        return result

    monkeypatch.setattr(kubesizer_cli, "optimize_with_calibrated_simulation", optimize)
    monkeypatch.setattr(
        kubesizer_cli,
        "validate_final_configuration",
        lambda *args, **kwargs: validation,
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "_progress_callback",
        lambda args: None,
    )

    selected, selected_profile, selected_validation = (
        kubesizer_cli._run_replica_workflow(
            system,
            arguments,
            (2,),
        )
    )

    assert selected is result
    assert selected_profile is profile
    assert selected_validation is validation
    assert observed["minimum_configuration"] == (2,)


@pytest.mark.parametrize(
    "obsolete_arguments",
    (
        ("--evaluation", "analytical"),
        ("--search-mode", "hybrid"),
        ("--placement-evaluation", "simulation"),
        ("--placement-search", "exact"),
        ("--resources", "resources.yaml"),
        ("--skip-placement",),
        ("--replica-search", "exact"),
        ("--skip-final-validation",),
    ),
)
def test_ambiguous_or_unsupported_mode_flags_are_rejected(obsolete_arguments):
    with pytest.raises(SystemExit):
        kubesizer_cli.build_parser().parse_args(list(obsolete_arguments))


def test_run_creates_no_files_without_out(tmp_path, capsys, monkeypatch):
    workload_path = Path(__file__).parents[1] / "workload.minimum.yaml"
    monkeypatch.chdir(tmp_path)

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            str(workload_path),
            "--replica-evaluation",
            "analytical",
            "--json",
            "--quiet",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert tuple(tmp_path.iterdir()) == ()


def test_portable_measurements_run_end_to_end_to_service_recommendations(
    tmp_path,
    capsys,
    monkeypatch,
):
    evidence = Path(__file__).parents[1] / "examples" / "portable_evidence"
    monkeypatch.chdir(tmp_path)

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            str(evidence / "workload.yaml"),
            "--measurements",
            str(evidence / "measurements.with-capacity.yaml"),
            "--replica-evaluation",
            "analytical",
            "--max-replicas",
            "4",
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["result"] == {
        "services": {
            "api": {
                "recommended_replicas": 1,
            },
            "database": {
                "recommended_replicas": 1,
            },
        }
    }
    assert tuple(tmp_path.iterdir()) == ()


def test_portable_workflow_refuses_to_claim_stability_without_capacity_runs(
    capsys,
):
    evidence = Path(__file__).parents[1] / "examples" / "portable_evidence"

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            str(evidence / "workload.yaml"),
            "--measurements",
            str(evidence / "measurements.yaml"),
            "--replica-evaluation",
            "analytical",
            "--max-replicas",
            "4",
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert output["ok"] is False
    assert "No statistically controlled stability floor" in output["error"]["message"]


def test_portable_measurements_seed_the_internal_simulation_optimizer(
    tmp_path,
    capsys,
    monkeypatch,
):
    evidence = Path(__file__).parents[1] / "examples" / "portable_evidence"
    observed = {}
    with (evidence / "workload.yaml").open(encoding="utf-8") as stream:
        workload = yaml.safe_load(stream)
    workload["service_blocking_times"] = {
        "api": {"distribution": "deterministic", "mean": 0.001},
        "database": {"distribution": "deterministic", "mean": 0.001},
    }
    workload_path = tmp_path / "workload.yaml"
    workload_path.write_text(yaml.safe_dump(workload), encoding="utf-8")

    def run_replica_workflow(
        system,
        args,
        minimum_configuration,
    ):
        observed["request_classes"] = tuple(system.get_request_classes())
        observed["minimum_configuration"] = minimum_configuration
        return (
            SimpleNamespace(configuration=(1, 1)),
            object(),
            SimpleNamespace(passed=True),
        )

    monkeypatch.setattr(
        kubesizer_cli,
        "_run_replica_workflow",
        run_replica_workflow,
    )

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            str(workload_path),
            "--measurements",
            str(evidence / "measurements.with-capacity.yaml"),
            "--replica-evaluation",
            "simulation",
            "--json",
            "--quiet",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert observed == {
        "request_classes": ("browse",),
        "minimum_configuration": (1, 1),
    }


def test_portable_simulation_rejects_an_incomplete_fitted_model(capsys):
    evidence = Path(__file__).parents[1] / "examples" / "portable_evidence"

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            str(evidence / "workload.yaml"),
            "--measurements",
            str(evidence / "measurements.with-capacity.yaml"),
            "--replica-evaluation",
            "simulation",
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert output["ok"] is False
    assert "incomplete fitted-variability model" in output["error"]["message"]


def test_portable_workflow_uses_statistical_floor_not_legacy_sizing_seed(
    monkeypatch,
):
    evidence = Path(__file__).parents[1] / "examples" / "portable_evidence"
    plan = kubesizer_cli.load_and_prepare_initial_sizing(
        evidence / "workload.yaml",
        evidence / "measurements.with-capacity.yaml",
    )
    legacy_recommendation = SimpleNamespace(
        configuration=lambda service_order: (4,) * len(service_order)
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "load_and_prepare_initial_sizing",
        lambda *args, **kwargs: SimpleNamespace(
            inputs=plan.inputs,
            characterization=plan.characterization,
            recommendation=legacy_recommendation,
        ),
    )
    floor = SimpleNamespace(
        configuration=(1, 1),
        assert_confidence_bounded_stable=lambda: None,
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "estimate_statistical_stability_floor",
        lambda *args, **kwargs: floor,
    )
    arguments = SimpleNamespace(
        workload=str(evidence / "workload.yaml"),
        measurements=str(evidence / "measurements.with-capacity.yaml"),
        max_replicas=4,
        replica_evaluation="analytical",
    )

    _system, _workload_path, minimum = kubesizer_cli._load_workflow_system(arguments)

    assert minimum == (1, 1)


def test_portable_cli_honors_declared_maximum_capacity_utilization(tmp_path):
    evidence = Path(__file__).parents[1] / "examples" / "portable_evidence"
    with (evidence / "workload.yaml").open(encoding="utf-8") as stream:
        workload = yaml.safe_load(stream)
    workload["sizing_policy"] = {"maximum_capacity_utilization": 0.50}
    workload_path = tmp_path / "workload.yaml"
    workload_path.write_text(yaml.safe_dump(workload), encoding="utf-8")
    arguments = SimpleNamespace(
        workload=str(workload_path),
        measurements=str(evidence / "measurements.with-capacity.yaml"),
        max_replicas=4,
        replica_evaluation="analytical",
    )

    _system, _path, minimum = kubesizer_cli._load_workflow_system(arguments)

    assert minimum == (2, 2)


def test_removed_deployment_argument_is_rejected(capsys):
    with pytest.raises(SystemExit) as error:
        kubesizer_cli.main(
            [
                "--workload",
                str(Path(__file__).parents[1] / "workload.minimum.yaml"),
                "--deployment",
                "deployment.yaml",
                "--json",
                "--quiet",
            ]
        )

    assert error.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err


def test_out_is_the_only_explicit_result_write(tmp_path, capsys):
    workload_path = Path(__file__).parents[1] / "workload.minimum.yaml"
    output_path = tmp_path / "nested" / "result.json"

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            str(workload_path),
            "--replica-evaluation",
            "analytical",
            "--json",
            "--quiet",
            "--out",
            str(output_path),
        ]
    )

    assert exit_code == 0
    stdout_value = json.loads(capsys.readouterr().out)
    assert json.loads(output_path.read_text(encoding="utf-8")) == stdout_value


def test_json_usage_error_has_stable_envelope(capsys):
    exit_code = kubesizer_cli.main(
        [
            "--workload",
            "does-not-exist.yaml",
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert output["ok"] is False
    assert output["command"] == "kubesizer"
    assert output["error"]["type"] == "CliUsageError"


@pytest.mark.parametrize(
    "removed_command",
    (
        "doctor",
        "run",
        "inspect",
        "simulate",
        "estimate",
        "calibrate",
        "optimize",
        "validate-final",
    ),
)
def test_cli_exposes_no_verbs(removed_command):
    parser = kubesizer_cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([removed_command])

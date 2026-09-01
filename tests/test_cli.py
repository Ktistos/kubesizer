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
    placements = []
    return SimpleNamespace(
        get_services=lambda: {"api": object()},
        get_request_classes=lambda: ("request",),
        get_request_class_latency_slo_kind=lambda: slo_kind,
        get_request_class_mean_latency_limits=lambda: {"request": 1.0},
        get_request_class_mean_latencies=lambda: {"request": 0.5},
        configure_system_replicas=lambda configuration: configured.append(
            tuple(configuration)
        ),
        configure_simulation_placement=lambda **model: placements.append(model),
        configured=configured,
        placements=placements,
    )


def _fake_replica_result(*, solution_mode="EXACT"):
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
        solution_mode=solution_mode,
        method="UNIFORM_COST_EXACT",
        termination_reason="LOWER_BOUND_MATCHED",
        minimality_proven=True,
        optimality_proven=True,
        locally_minimal=True,
        budget_exhausted=False,
        selection_reason="test",
    )


def _fake_placement_result(*, solution_mode="EXACT"):
    return SimpleNamespace(
        configuration=(("node-a", (("api", 1),)),),
        placement={"node-a": {"api": 1}},
        cost=0.0,
        objective_value=0.0,
        solution_mode=solution_mode,
        method=f"{solution_mode}_PLACEMENT",
        termination_reason="LOCAL_SEARCH_COMPLETED",
        complete_states=1,
        total_configurations=1,
        optimality_proven=solution_mode == "EXACT",
        global_optimal=solution_mode == "EXACT",
        budget_exhausted=False,
        selection_reason="test",
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


def test_placement_recalibration_has_distinct_progress_messages():
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(output=output)

    reporter("placement_calibration_started", {})
    reporter(
        "placement_calibration_pilot_started",
        {"simulation_horizon": 750.0},
    )
    reporter(
        "placement_calibration_measurement_started",
        {"simulation_duration": 1250.0},
    )
    reporter("placement_calibration_completed", {})

    assert [line.split(" | ", 1)[0] for line in output.getvalue().splitlines()] == [
        "Calibrating the selected placement",
        "Placement calibration: testing warm-up horizon 750",
        "Placement calibration: testing measurement duration 1250",
        "Placement-aware calibration complete",
    ]


def test_placement_calibration_callback_translates_only_actionable_events():
    observed = []
    args = SimpleNamespace(quiet=False)
    args._progress_reporter = lambda event, details: observed.append(
        (event, details)
    )
    callback = kubesizer_cli._placement_calibration_progress_callback(args)

    callback("pilot_batch_started", {"simulation_horizon": 500.0})
    callback("pilot_horizon_analyzing", {"pilot_horizon": 250.0})
    callback("measurement_trajectories_started", {"simulation_duration": 900.0})

    assert observed == [
        (
            "placement_calibration_pilot_started",
            {"simulation_horizon": 500.0},
        ),
        (
            "placement_calibration_measurement_started",
            {"simulation_duration": 900.0},
        ),
    ]


def test_progress_reporter_uses_coarse_search_milestones():
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(output=output)
    reporter("replica_optimization_started", {"selected_mode": "SCALABLE"})
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
        "Optimizing replicas (scalable)",
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
        "placement_search_started",
        {
            "selected_mode": "EXACT",
            "total_configurations": 999,
            "internal_value": "not-user-facing",
        },
    )

    progress = json.loads(output.getvalue())["progress"]
    assert progress["stage"] == "placement"
    assert progress["message"] == "Optimizing node placement (exact)"
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
            "selected_mode": "EXACT",
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


def test_simulation_cli_preflight_estimates_selected_modes_and_skipped_placement(
    monkeypatch,
):
    output = io.StringIO()
    reporter = kubesizer_cli._ConciseProgressReporter(output=output)
    args = SimpleNamespace(
        replica_evaluation="simulation",
        replica_search="exact",
        placement_search="scalable",
        skip_final_validation=False,
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

    def fake_estimate(system, resources, **kwargs):
        observed.update(kwargs)
        assert resources is None
        return estimate

    monkeypatch.setattr(kubesizer_cli, "estimate_workflow_runtime", fake_estimate)

    kubesizer_cli._prepare_runtime_reporting(
        object(),
        None,
        args,
        replica_mode=kubesizer_cli.OptimizationMode.EXACT,
        placement_mode=kubesizer_cli.OptimizationMode.SCALABLE,
    )

    assert observed["maximum_replicas"] == 7
    assert observed["workers"] == 2
    assert observed["replica_optimization_mode"] == kubesizer_cli.OptimizationMode.EXACT
    assert (
        observed["placement_optimization_mode"]
        == kubesizer_cli.OptimizationMode.SCALABLE
    )
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


def test_simulation_cli_plans_placement_recalibration_stage(monkeypatch):
    reporter = kubesizer_cli._ConciseProgressReporter(output=io.StringIO())
    args = SimpleNamespace(
        replica_evaluation="simulation",
        replica_search="exact",
        placement_search="exact",
        max_replicas=3,
        workers=1,
        quiet=False,
        json=False,
        _progress_reporter=reporter,
    )
    stage_names = tuple(
        dict.fromkeys(
            kubesizer_cli._ConciseProgressReporter._STAGE_ESTIMATE_NAMES.values()
        )
    )
    estimate = SimpleNamespace(
        stages=tuple(
            StageRuntimeEstimate(name, 1.0, 2.0, "ordinary", "ceiling")
            for name in stage_names
        ),
        total_ordinary_planning_seconds=float(len(stage_names)),
        total_configured_ceiling_seconds=float(2 * len(stage_names)),
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "estimate_workflow_runtime",
        lambda *args, **kwargs: estimate,
    )

    kubesizer_cli._prepare_runtime_reporting(
        object(),
        {"nodes": []},
        args,
        replica_mode=kubesizer_cli.OptimizationMode.EXACT,
        placement_mode=kubesizer_cli.OptimizationMode.EXACT,
    )

    assert reporter._expected_stages == (
        "calibration",
        "replica_sizing",
        "placement",
        "placement_calibration",
        "validation",
    )


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


def test_resources_are_skipped_when_not_supplied():
    resources, status = kubesizer_cli._resources_or_skip(
        None,
        skip_placement=False,
    )

    assert resources is None
    assert status["status"] == "SKIPPED"
    assert "No resources file" in status["reason"]


def test_missing_resources_file_skips_placement(tmp_path):
    resources, status = kubesizer_cli._resources_or_skip(
        str(tmp_path / "missing.yaml"),
        skip_placement=False,
    )

    assert resources is None
    assert status["status"] == "SKIPPED"
    assert "not available" in status["reason"]


def test_explicit_skip_takes_precedence_over_resources_file(tmp_path):
    resources_path = tmp_path / "resources.yaml"
    resources_path.write_text("not: valid: yaml", encoding="utf-8")

    resources, status = kubesizer_cli._resources_or_skip(
        str(resources_path),
        skip_placement=True,
    )

    assert resources is None
    assert status == {
        "status": "SKIPPED",
        "reason": "Node placement was disabled with --skip-placement.",
    }


def test_recommendation_is_service_centric():
    system = SimpleNamespace(
        get_services=lambda: {"api": object(), "worker": object()},
    )

    recommendation = kubesizer_cli._service_recommendations(
        system,
        (2, 1),
        {
            "node-a": {"api": 1, "worker": 1},
            "node-b": {"api": 1},
        },
    )

    assert recommendation == {
        "api": {
            "recommended_replicas": 2,
            "node_placement": {"node-a": 1, "node-b": 1},
        },
        "worker": {
            "recommended_replicas": 1,
            "node_placement": {"node-a": 1},
        },
    }


def test_recommendation_rejects_placement_replica_mismatch():
    system = SimpleNamespace(get_services=lambda: {"api": object()})

    with pytest.raises(RuntimeError, match="sizing recommended 2"):
        kubesizer_cli._service_recommendations(
            system,
            (2,),
            {"node-a": {"api": 1}},
        )


def test_analytical_placement_rejects_non_unit_execution_multipliers():
    system = _fake_system()

    with pytest.raises(
        kubesizer_cli.CliUsageError,
        match="execution-time multipliers",
    ):
        kubesizer_cli._validate_analytical_placement(
            system,
            {
                "placement": {"node-a": {"api": 1}},
                "communication_latency_seconds": {"node-a": {"node-a": 0.0}},
                "execution_time_multipliers": {"node-a": 1.1},
            },
        )

    assert system.placements == []


def test_analytical_placement_rechecks_mean_latency_slos():
    system = _fake_system()
    system.get_request_class_mean_latencies = lambda: {"request": 1.1}

    with pytest.raises(RuntimeError, match="invalidates analytical mean-latency"):
        kubesizer_cli._validate_analytical_placement(
            system,
            {
                "placement": {"node-a": {"api": 1}},
                "communication_latency_seconds": {"node-a": {"node-a": 0.0}},
                "execution_time_multipliers": {"node-a": 1.0},
            },
        )

    assert len(system.placements) == 1


def test_analytical_run_skips_placement_without_resources(
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
    placement_called = False

    def fail_placement(*args, **kwargs):
        nonlocal placement_called
        placement_called = True
        raise AssertionError("placement must not run")

    monkeypatch.setattr(kubesizer_cli, "optimize_node_placement", fail_placement)

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
                "node_placement": None,
            }
        }
    }
    assert placement_called is False


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
        observed["initial_configuration"] = kwargs["initial_configuration"]
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
            kubesizer_cli.OptimizationMode.SCALABLE,
            (2,),
            (2,),
        )
    )

    assert selected is result
    assert selected_profile is profile
    assert selected_validation is validation
    assert observed["initial_configuration"] == (2,)
    assert observed["minimum_configuration"] == (2,)


def test_simulation_final_validation_runs_after_placement_is_attached(
    monkeypatch,
    tmp_path,
    capsys,
):
    system = _fake_system(slo_kind="percentile")
    attached = []
    system.configure_simulation_placement = lambda **model: attached.append(model)
    profile = object()
    placed_profile = object()
    resources_path = tmp_path / "resources.yaml"
    resources_path.write_text(
        "nodes: []\n"
        "service_resource_requests:\n"
        "  api:\n"
        "    cpu_request_millicores: 250\n"
        "    memory_request_bytes: 134217728\n",
        encoding="utf-8",
    )
    placement = {"node-a": {"api": 1}}

    class PlacementCluster:
        def simulation_placement_model(self, selected):
            assert selected == placement
            return {
                "placement": placement,
                "communication_latency_seconds": {"node-a": {}},
                "execution_time_multipliers": {"node-a": 1.0},
            }

    monkeypatch.setattr(
        kubesizer_cli,
        "_load_system",
        lambda path: (system, Path(path)),
    )

    def run_replica_workflow(*args):
        assert args[1]._defer_final_validation is True
        return _fake_replica_result(), profile, None

    monkeypatch.setattr(
        kubesizer_cli,
        "_run_replica_workflow",
        run_replica_workflow,
    )
    monkeypatch.setattr(kubesizer_cli, "Cluster", lambda *args: PlacementCluster())
    def calibrate_placement(selected_system, configuration, **kwargs):
        assert selected_system is system
        assert configuration == (1,)
        assert attached
        return placed_profile

    monkeypatch.setattr(
        kubesizer_cli,
        "calibrate_fixed_placement",
        calibrate_placement,
    )
    def placement_problem(cluster, **kwargs):
        assert kwargs == {}
        return object()

    monkeypatch.setattr(kubesizer_cli, "NodePlacementProblem", placement_problem)
    monkeypatch.setattr(
        kubesizer_cli,
        "optimize_node_placement",
        lambda *args, **kwargs: SimpleNamespace(
            placement=placement,
            configuration=placement,
        ),
    )

    def validate(*args, **kwargs):
        assert attached
        assert args[2] is placed_profile
        return SimpleNamespace(passed=True)

    monkeypatch.setattr(kubesizer_cli, "validate_final_configuration", validate)

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            "ignored.yaml",
            "--resources",
            str(resources_path),
            "--replica-evaluation",
            "simulation",
            "--json",
            "--quiet",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


@pytest.mark.parametrize(
    ("replica_search", "placement_search"),
    (("exact", "scalable"), ("scalable", "exact")),
)
def test_replica_and_placement_search_are_selected_independently(
    replica_search,
    placement_search,
    tmp_path,
    monkeypatch,
    capsys,
):
    system = _fake_system()
    resources_path = tmp_path / "resources.yaml"
    resources_path.write_text(
        "nodes: []\n"
        "service_resource_requests:\n"
        "  api:\n"
        "    cpu_request_millicores: 250\n"
        "    memory_request_bytes: 134217728\n",
        encoding="utf-8",
    )
    observed = {}
    monkeypatch.setattr(
        kubesizer_cli,
        "_load_system",
        lambda path: (system, Path(path)),
    )

    def optimize_replicas(*args, **kwargs):
        observed["replica_mode"] = kwargs["mode"]
        return _fake_replica_result(solution_mode=replica_search.upper())

    def optimize_placement(*args, **kwargs):
        observed["placement_mode"] = kwargs["mode"]
        return _fake_placement_result(solution_mode=placement_search.upper())

    monkeypatch.setattr(
        kubesizer_cli,
        "optimize_with_analytical_model",
        optimize_replicas,
    )
    class PlacementCluster:
        def simulation_placement_model(self, selected):
            assert selected == {"node-a": {"api": 1}}
            return {
                "placement": selected,
                "communication_latency_seconds": {"node-a": {"node-a": 0.0}},
                "execution_time_multipliers": {"node-a": 1.0},
            }

    monkeypatch.setattr(
        kubesizer_cli,
        "Cluster",
        lambda *args: PlacementCluster(),
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "NodePlacementProblem",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "optimize_node_placement",
        optimize_placement,
    )

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            "ignored.yaml",
            "--resources",
            str(resources_path),
            "--replica-evaluation",
            "analytical",
            "--replica-search",
            replica_search,
            "--placement-search",
            placement_search,
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert observed == {
        "replica_mode": kubesizer_cli.OptimizationMode(replica_search.upper()),
        "placement_mode": kubesizer_cli.OptimizationMode(placement_search.upper()),
    }
    assert output["result"] == {
        "services": {
            "api": {
                "recommended_replicas": 1,
                "node_placement": {"node-a": 1},
            }
        }
    }


def test_native_placement_does_not_require_per_service_resource_requests(
    tmp_path,
    monkeypatch,
    capsys,
):
    system = _fake_system()
    resources_path = tmp_path / "resources.yaml"
    resources_path.write_text("nodes: []\n", encoding="utf-8")
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

    class PlacementCluster:
        def simulation_placement_model(self, selected):
            return {
                "placement": selected,
                "communication_latency_seconds": {"node-a": {"node-a": 0.0}},
                "execution_time_multipliers": {"node-a": 1.0},
            }

    monkeypatch.setattr(
        kubesizer_cli,
        "Cluster",
        lambda *args: PlacementCluster(),
    )

    def placement_problem(cluster, **kwargs):
        assert kwargs == {}
        return object()

    monkeypatch.setattr(
        kubesizer_cli,
        "NodePlacementProblem",
        placement_problem,
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "optimize_node_placement",
        lambda *args, **kwargs: _fake_placement_result(),
    )

    exit_code = kubesizer_cli.main(
        [
            "--workload",
            "ignored.yaml",
            "--resources",
            str(resources_path),
            "--replica-evaluation",
            "analytical",
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["result"]["services"]["api"] == {
        "recommended_replicas": 1,
        "node_placement": {"node-a": 1},
    }


@pytest.mark.parametrize("mode", ("auto", "exact", "scalable"))
def test_each_search_flag_accepts_all_supported_modes(mode):
    args = kubesizer_cli.build_parser().parse_args(
        ["--replica-search", mode, "--placement-search", mode]
    )

    assert args.replica_search == mode
    assert args.placement_search == mode


@pytest.mark.parametrize(
    "obsolete_arguments",
    (
        ("--evaluation", "analytical"),
        ("--search-mode", "hybrid"),
        ("--placement-evaluation", "simulation"),
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
            "--replica-search",
            "exact",
            "--skip-placement",
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
            "--replica-search",
            "exact",
            "--max-replicas",
            "4",
            "--skip-placement",
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
                "node_placement": None,
            },
            "database": {
                "recommended_replicas": 1,
                "node_placement": None,
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
            "--replica-search",
            "exact",
            "--max-replicas",
            "4",
            "--skip-placement",
            "--json",
            "--quiet",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert output["ok"] is False
    assert (
        "No statistically controlled stability floor"
        in output["error"]["message"]
    )


def test_portable_measurements_seed_the_internal_simulation_optimizer(
    capsys,
    monkeypatch,
):
    evidence = Path(__file__).parents[1] / "examples" / "portable_evidence"
    observed = {}

    def run_replica_workflow(
        system,
        args,
        mode,
        initial_configuration,
        minimum_configuration,
    ):
        observed["request_classes"] = tuple(system.get_request_classes())
        observed["initial_configuration"] = initial_configuration
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
            str(evidence / "workload.yaml"),
            "--measurements",
            str(evidence / "measurements.with-capacity.yaml"),
            "--replica-evaluation",
            "simulation",
            "--skip-placement",
            "--json",
            "--quiet",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert observed == {
        "request_classes": ("browse",),
        "initial_configuration": (1, 1),
        "minimum_configuration": (1, 1),
    }


def test_portable_workflow_starts_at_statistical_floor_not_legacy_sizing_seed(
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
    )

    _system, _workload_path, initial, minimum = (
        kubesizer_cli._load_workflow_system(arguments)
    )

    assert initial == (1, 1)
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
    )

    _system, _path, initial, minimum = kubesizer_cli._load_workflow_system(
        arguments
    )

    assert initial == (2, 2)
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
            "--replica-search",
            "exact",
            "--skip-placement",
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

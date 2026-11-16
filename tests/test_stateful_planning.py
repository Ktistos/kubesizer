from types import SimpleNamespace
from pathlib import Path

import pytest
import yaml

import kubesizer_cli
from stateful_planning import compare_stateful_candidates, load_stateful_candidates


def candidate_set(tmp_path):
    value = {
        "request_classes": {
            "read": {
                "concurrency_weight": 1.0,
                "routing": {"api": {"database": 1.0}},
                "end_to_end_latency_slos": {"mean_seconds": 0.5},
            }
        },
        "traffic_model": {
            "entry_services": {
                "read": {"api": {"distribution": "exponential", "rate": 1.0}}
            },
            "service_times": {
                "api": {"distribution": "exponential", "rate": 100.0},
                "database": {"distribution": "exponential", "rate": 100.0},
            },
        },
        "replica_constraints": {"database": {"fixed": 1}},
    }
    (tmp_path / "evidence.txt").write_text(
        "Synthetic test fixture; not deployment measurements."
    )
    rows = []
    for name, base_cost in (("smaller", 1.0), ("larger", 2.0)):
        (tmp_path / f"{name}.yaml").write_text(yaml.safe_dump(value))
        rows.append(
            {
                "name": name,
                "mechanism": "fixed storage topology",
                "workload": f"{name}.yaml",
                "base_cost": base_cost,
                "elastic_replica_costs": {"api": 0.25},
                "evidence_sources": ["evidence.txt"],
            }
        )
    path = tmp_path / "candidates.yaml"
    path.write_text(yaml.safe_dump({"version": 1, "candidates": rows}))
    return path


def accepted(candidate):
    return SimpleNamespace(
        configuration=tuple(candidate.system.get_system_replicas()),
        metrics=SimpleNamespace(evaluation_basis="TEST_ONLY"),
        minimality_proven=True,
    )


def test_all_topologies_are_evaluated_before_selecting_physical_cost(tmp_path):
    candidates = load_stateful_candidates(candidate_set(tmp_path))
    seen = []

    def evaluate(candidate):
        seen.append(candidate.name)
        return accepted(candidate)

    result = compare_stateful_candidates(candidates, evaluate)
    assert seen == ["smaller", "larger"]
    assert result["selected"] == "smaller"
    assert result["total_cost"] == 1.25
    assert result["minimum_cost_proven"]
    assert len(result["candidates"][0]["sources"]) == 2


def test_unresolved_alternative_prevents_minimum_cost_claim(tmp_path):
    candidates = load_stateful_candidates(candidate_set(tmp_path))

    def evaluate(candidate):
        if candidate.name == "smaller":
            raise RuntimeError("Missing internal demand evidence")
        return accepted(candidate)

    result = compare_stateful_candidates(candidates, evaluate)
    assert result["selected"] == "larger"
    assert not result["minimum_cost_proven"]
    assert result["candidates"][0]["status"] == "UNRESOLVED"


def test_all_candidates_unresolved_abstains(tmp_path):
    candidates = load_stateful_candidates(candidate_set(tmp_path))

    def evaluate(candidate):
        raise ValueError("Unsupported acknowledgement semantics")

    result = compare_stateful_candidates(candidates, evaluate)
    assert result["selected"] is None
    assert result["status"] == "ABSTAINED"


def test_candidates_cannot_change_the_external_workload_to_look_better(tmp_path):
    path = candidate_set(tmp_path)
    value = yaml.safe_load((tmp_path / "larger.yaml").read_text())
    value["traffic_model"]["entry_services"]["read"]["api"]["rate"] = 0.1
    (tmp_path / "larger.yaml").write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError, match="identical external"):
        load_stateful_candidates(path)


def test_nonelastic_members_must_be_fixed(tmp_path):
    path = candidate_set(tmp_path)
    value = yaml.safe_load((tmp_path / "smaller.yaml").read_text())
    del value["replica_constraints"]
    (tmp_path / "smaller.yaml").write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError, match="must have fixed"):
        load_stateful_candidates(path)


def test_candidate_cli_uses_analytical_method_and_returns_selection(tmp_path, capsys):
    path = candidate_set(tmp_path)
    assert (
        kubesizer_cli.main(
            [
                "--stateful-candidates",
                str(path),
                "--replica-evaluation",
                "analytical",
                "--quiet",
                "--json",
            ]
        )
        == 0
    )
    import json

    result = json.loads(capsys.readouterr().out)["result"]
    assert result["stateful_planning"]["selected"] == "smaller"
    assert result["services"]["database"]["recommended_replicas"] == 1


def test_candidate_simulation_final_validation_failure_cannot_emit_recommendation(
    tmp_path, monkeypatch, capsys
):
    path = candidate_set(tmp_path)
    monkeypatch.setattr(
        kubesizer_cli, "_run_calibration", lambda *args, **kwargs: object()
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "optimize_with_calibrated_simulation",
        lambda model, *args, **kwargs: SimpleNamespace(
            configuration=model.get_system_replicas()
        ),
    )
    monkeypatch.setattr(
        kubesizer_cli,
        "validate_final_configuration",
        lambda *args, **kwargs: SimpleNamespace(
            passed=False, failure_reason="Mean SLO upper bound failed"
        ),
    )
    assert (
        kubesizer_cli.main(["--stateful-candidates", str(path), "--quiet", "--json"])
        == 1
    )
    import json

    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is False
    assert "final validation" in result["error"]["message"]


def test_candidate_count_allocates_simulation_error_budget(tmp_path):
    args = kubesizer_cli.build_parser().parse_args([])
    original = kubesizer_cli._calibration_options(args).overall_alpha
    args._stateful_candidate_count = 2
    assert kubesizer_cli._calibration_options(args).overall_alpha == original / 2


def test_changed_evidence_cannot_be_scored_as_the_frozen_candidate(tmp_path):
    candidates = load_stateful_candidates(candidate_set(tmp_path))
    (tmp_path / "evidence.txt").write_text("Changed after freeze")
    result = compare_stateful_candidates(candidates, accepted)
    assert result["selected"] is None
    assert all("source changed" in row["reason"] for row in result["candidates"])


def test_follower_scaling_changes_read_routing_and_keeps_all_write_propagation():
    path = Path(__file__).parents[1] / "examples" / "stateful" / "candidates.yaml"
    one, two = load_stateful_candidates(path)

    def rates(candidate, request_class):
        return dict(
            zip(
                candidate.system.get_services(),
                candidate.system.get_request_class_effective_arrival_rates()[
                    request_class
                ],
            )
        )

    assert rates(one, "read")["reader_1"] == pytest.approx(8.0)
    assert rates(two, "read")["reader_1"] == pytest.approx(5.6)
    assert rates(two, "read")["reader_2"] == pytest.approx(2.4)
    assert rates(one, "write")["reader_1"] == pytest.approx(2.0)
    assert rates(two, "write")["reader_1"] == pytest.approx(2.0)
    assert rates(two, "write")["reader_2"] == pytest.approx(2.0)
    assert (
        two.system.get_service_time_distribution_specs()["primary"].mean
        > one.system.get_service_time_distribution_specs()["primary"].mean
    )

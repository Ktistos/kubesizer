from pathlib import Path

import pytest
import yaml

from characterization import (
    StabilityFloorStatus,
    StatisticalStabilityError,
    build_characterized_system_variants,
    characterize_deployment,
    estimate_statistical_stability_floor,
    load_and_prepare_initial_sizing,
    parse_portable_sizing_inputs,
)
from optimization import AnalyticalReplicaSizingProblem, UniformCostReplicaOptimizer

EXAMPLE = Path(__file__).parents[1] / "examples" / "portable_evidence"


def _load_yaml(name):
    with (EXAMPLE / name).open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _plan(manifest="measurements.with-capacity.yaml"):
    return load_and_prepare_initial_sizing(
        EXAMPLE / "workload.yaml",
        EXAMPLE / manifest,
    )


def _system_and_order(plan):
    ((_, system),) = build_characterized_system_variants(
        plan.inputs,
        plan.characterization,
        evaluation_mode="analytical",
    )
    order = tuple(
        item.get_name()
        for item in sorted(
            system.get_services().values(),
            key=lambda item: item.get_id(),
        )
    )
    return system, order


def _assessment(plan, maximum=(4, 4)):
    system, order = _system_and_order(plan)
    return estimate_statistical_stability_floor(
        plan.inputs,
        plan.characterization,
        analytical_minimum=system.get_minimum_stable_replicas(),
        maximum_configuration=maximum,
        service_order=order,
    )


def test_example_capacity_evidence_establishes_a_joint_stability_floor():
    result = _assessment(_plan())

    assert result.status == StabilityFloorStatus.CONFIDENCE_BOUNDED_STABLE
    assert result.configuration == (1, 1)
    assert result.overall_confidence_level == pytest.approx(0.95)
    assert result.maximum_capacity_utilization == pytest.approx(0.85)
    # Two arrival bounds and three tested capacity-point bounds.
    assert result.individual_bound_confidence_level == pytest.approx(0.99)
    assert all(
        service.stability_margin_lower_bound_rps > 0.0 for service in result.services
    )
    assert all(
        service.confidence_bounded_capacity_utilization < 0.85
        for service in result.services
    )


def test_declared_lower_utilization_produces_a_no_smaller_stability_floor():
    workload = _load_yaml("workload.yaml")
    workload["sizing_policy"] = {"maximum_capacity_utilization": 0.50}
    inputs = parse_portable_sizing_inputs(
        workload,
        _load_yaml("measurements.with-capacity.yaml"),
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)
    plan = type("Plan", (), {"inputs": inputs, "characterization": characterization})

    result = _assessment(plan)

    assert result.maximum_capacity_utilization == pytest.approx(0.50)
    assert result.configuration == (2, 2)
    assert all(
        service.confidence_bounded_capacity_utilization < 0.50
        for service in result.services
    )


def test_missing_capacity_runs_do_not_fall_back_to_analytical_stability():
    result = _assessment(_plan("measurements.yaml"))

    assert result.status == StabilityFloorStatus.INSUFFICIENT_EVIDENCE
    assert result.minimum_stable_configuration is None
    assert all(
        service.status == StabilityFloorStatus.INSUFFICIENT_EVIDENCE
        for service in result.services
    )
    with pytest.raises(StatisticalStabilityError, match="No statistically controlled"):
        result.assert_confidence_bounded_stable()


def test_floor_uses_strict_capacity_margin_and_labels_untested_count(tmp_path):
    capacity = tmp_path / "capacity.csv"
    capacity.write_text(
        "run_id,service,replicas,offered_rate_rps,completed_rate_rps,"
        "error_rate,healthy,cpu_throttling_ratio\n"
        "api-a,api,1,12,12,0,true,0\n"
        "api-b,api,1,12,12,0,true,0\n"
        "api-c,api,1,12,12,0,true,0\n"
        "db-a,database,1,40,40,0,true,0\n"
        "db-b,database,1,40,40,0,true,0\n"
        "db-c,database,1,40,40,0,true,0\n",
        encoding="utf-8",
    )
    manifest = _load_yaml("measurements.yaml")
    manifest["capacity_observations"] = str(capacity)
    inputs = parse_portable_sizing_inputs(
        _load_yaml("workload.yaml"),
        manifest,
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)
    plan = type("Plan", (), {"inputs": inputs, "characterization": characterization})

    result = _assessment(plan)
    api = {item.service: item for item in result.services}["api"]

    assert result.configuration == (2, 1)
    assert api.required_replicas == 2
    assert api.stability_margin_lower_bound_rps == pytest.approx(0.4)
    assert any("not directly capacity-tested" in warning for warning in api.warnings)


def test_floor_reports_when_confidence_bounded_requirement_exceeds_bounds(tmp_path):
    capacity = tmp_path / "capacity.csv"
    capacity.write_text(
        "run_id,service,replicas,offered_rate_rps,completed_rate_rps,"
        "error_rate,healthy,cpu_throttling_ratio\n"
        "api-a,api,1,12,12,0,true,0\n"
        "api-b,api,1,12,12,0,true,0\n"
        "api-c,api,1,12,12,0,true,0\n"
        "db-a,database,1,40,40,0,true,0\n"
        "db-b,database,1,40,40,0,true,0\n"
        "db-c,database,1,40,40,0,true,0\n",
        encoding="utf-8",
    )
    manifest = _load_yaml("measurements.yaml")
    manifest["capacity_observations"] = str(capacity)
    inputs = parse_portable_sizing_inputs(
        _load_yaml("workload.yaml"),
        manifest,
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)
    plan = type("Plan", (), {"inputs": inputs, "characterization": characterization})

    result = _assessment(plan, maximum=(1, 1))

    assert result.status == StabilityFloorStatus.OUTSIDE_REPLICA_BOUNDS
    assert result.minimum_stable_configuration is None


def test_analytical_search_cannot_delete_below_statistical_floor():
    plan = _plan()
    system, _ = _system_and_order(plan)
    problem = AnalyticalReplicaSizingProblem(
        system,
        {},
        max_replicas=(3, 3),
        minimum_configuration=(2, 1),
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert problem.initial_state() == (2, 1)
    assert problem.stable_configuration() == (2, 1)
    assert result.configuration == (2, 1)

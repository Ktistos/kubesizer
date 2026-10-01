import copy
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from characterization import (
    AlignedInputWindow,
    LatencySummary,
    RecommendationReadiness,
    assess_initial_sizing_risk,
    characterize_deployment,
    load_and_prepare_initial_sizing,
    load_portable_sizing_inputs,
    parse_portable_sizing_inputs,
    recommend_initial_replicas,
)

EXAMPLE = Path(__file__).parents[1] / "examples" / "portable_evidence"


def _evidence():
    inputs = load_portable_sizing_inputs(
        EXAMPLE / "workload.yaml",
        EXAMPLE / "measurements.yaml",
    )
    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(inputs, characterization)
    return inputs, characterization, recommendation


def _yaml(name):
    with (EXAMPLE / name).open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def test_prepared_initial_sizing_exposes_conditional_risk_ranges():
    prepared = load_and_prepare_initial_sizing(
        EXAMPLE / "workload.yaml",
        EXAMPLE / "measurements.yaml",
    )

    risk = prepared.risk_assessment
    assert risk.readiness is RecommendationReadiness.CONDITIONAL
    assert risk.recommendable
    assert {item.service for item in risk.services} == {"api", "database"}
    assert all(
        item.estimated_cpu_utilization_range is not None for item in risk.services
    )
    assert "ARRIVAL_TRACE_UNAVAILABLE" not in {item.code for item in risk.findings}
    assert "COMMUNICATION_LATENCY_UNAVAILABLE" in {item.code for item in risk.findings}


def test_missing_observed_slo_statistic_blocks_a_recommendation():
    inputs, characterization, recommendation = _evidence()
    external = replace(
        characterization.external_request_classes[0],
        latency=LatencySummary(None, None, None, None, None),
    )
    characterization = replace(
        characterization,
        external_request_classes=(external,),
    )

    risk = assess_initial_sizing_risk(
        inputs,
        characterization,
        recommendation,
    )

    assert risk.readiness is RecommendationReadiness.INSUFFICIENT_EVIDENCE
    assert not risk.recommendable
    with pytest.raises(ValueError, match="declared latency statistic"):
        risk.assert_recommendable()


def test_load_dependent_demand_cannot_be_extrapolated_past_observations():
    inputs, characterization, recommendation = _evidence()
    original_windows = characterization.aligned_input_windows
    windows = []
    for index, (rate, demand) in enumerate(
        ((10.0, 0.004), (20.0, 0.008), (30.0, 0.016))
    ):
        original = original_windows[index]
        service_demands = dict(original.service_cpu_demands)
        service_demands["api"] = demand
        windows.append(
            AlignedInputWindow(
                start=original.start,
                end=original.end,
                request_class_arrival_rates=(("browse", rate),),
                service_cpu_demands=tuple(sorted(service_demands.items())),
            )
        )
    characterization = replace(
        characterization,
        aligned_input_windows=tuple(windows),
    )
    services = tuple(
        replace(item, workload_arrival_rate_rps=60.0) if item.service == "api" else item
        for item in recommendation.services
    )
    recommendation = replace(recommendation, services=services)

    risk = assess_initial_sizing_risk(
        inputs,
        characterization,
        recommendation,
    )
    api = next(item for item in risk.services if item.service == "api")

    assert api.load_dependent
    assert api.target_inside_observed_load_range is False
    assert risk.readiness is RecommendationReadiness.INSUFFICIENT_EVIDENCE
    assert "LOAD_DEPENDENT_EXTRAPOLATION" in {item.code for item in risk.findings}


def test_inferred_edges_are_explicit_structural_uncertainty_not_a_hidden_claim():
    workload = copy.deepcopy(_yaml("workload.yaml"))
    workload["request_classes"]["browse"].pop("calls")
    workload["request_classes"]["browse"].pop("call_execution")
    inputs = parse_portable_sizing_inputs(
        workload,
        _yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(inputs, characterization)

    risk = assess_initial_sizing_risk(inputs, characterization, recommendation)

    assert "CALL_ORDER_INFERRED" in {item.code for item in risk.findings}
    assert risk.readiness is RecommendationReadiness.CONDITIONAL

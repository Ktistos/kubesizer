import copy
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from characterization import (
    build_characterized_system_variants,
    characterize_deployment,
    parse_portable_sizing_inputs,
)
from performance_models import QueueingNetworkType, System, parse_workload_spec


def _call_workload(*, cross_probability=1.0):
    return {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "latency_boundary": "external_client",
                "latency_components": {
                    "client_to_ingress": {
                        "distribution": "deterministic",
                        "mean": 0.01,
                    },
                    "ingress_processing": {
                        "distribution": "deterministic",
                        "mean": 0.02,
                    },
                },
                "calls": {"api": {"db": 1.0}},
                "call_execution": "sequential",
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "deterministic", "rate": 2.0}}
            },
            "service_times": {
                service: {"distribution": "deterministic", "mean": 0.1}
                for service in ("api", "db")
            },
            "communication_latency": {
                "same_node_one_way": {
                    "distribution": "deterministic",
                    "mean": 0.001,
                },
                "cross_node_one_way": {
                    "distribution": "deterministic",
                    "mean": 0.004,
                },
                "cross_node_probability": cross_probability,
            },
        },
    }


def test_ingress_boundary_rejects_client_to_ingress_component():
    workload = _call_workload()
    workload["request_classes"]["request"]["latency_boundary"] = "ingress"

    with pytest.raises(ValueError, match="cannot include a client path"):
        parse_workload_spec(workload)


def test_aggregate_client_to_entry_cannot_double_count_finer_components():
    workload = _call_workload()
    workload["request_classes"]["request"]["latency_components"]["client_to_entry"] = {
        "distribution": "deterministic",
        "mean": 0.03,
    }

    with pytest.raises(ValueError, match="cannot combine client_to_entry"):
        parse_workload_spec(workload)


def _portable_external_client_inputs(*, percentile=False):
    base_path = Path(__file__).parents[1] / "examples" / "portable_evidence"
    with (base_path / "workload.yaml").open(encoding="utf-8") as stream:
        workload = yaml.safe_load(stream)
    with (base_path / "measurements.yaml").open(encoding="utf-8") as stream:
        measurements = yaml.safe_load(stream)
    request_class = workload["request_classes"]["browse"]
    request_class["latency_boundary"] = "external_client"
    if percentile:
        request_class["end_to_end_latency_slos"] = {
            "percentile": {"quantile": 0.95, "maximum_seconds": 0.25}
        }
    return parse_portable_sizing_inputs(
        workload,
        measurements,
        base_path=base_path,
    )


def test_boundary_fit_is_not_applied_without_a_latency_slo():
    base_path = Path(__file__).parents[1] / "examples" / "portable_evidence"
    with (base_path / "workload.yaml").open(encoding="utf-8") as stream:
        workload = yaml.safe_load(stream)
    with (base_path / "measurements.yaml").open(encoding="utf-8") as stream:
        measurements = yaml.safe_load(stream)
    request_class = workload["request_classes"]["browse"]
    request_class["latency_boundary"] = "external_client"
    request_class.pop("end_to_end_latency_slos")
    inputs = parse_portable_sizing_inputs(
        workload,
        measurements,
        base_path=base_path,
    )

    characterization = characterize_deployment(inputs)
    boundary = characterization.external_request_classes[0].boundary_latency

    assert boundary.status == "not_applicable"
    assert "mean latency SLO" in boundary.reason


def test_boundary_fit_excludes_a_small_number_of_count_misaligned_windows():
    inputs = _portable_external_client_inputs()
    measurements = inputs.measurements
    latest_end = max(row.end for row in measurements.external_intervals)

    def extend(rows):
        templates = tuple(row for row in rows if row.end == latest_end)
        return rows + tuple(
            replace(row, start=row.start + offset, end=row.end + offset)
            for offset in (3_600.0, 7_200.0)
            for row in templates
        )

    external = list(extend(measurements.external_intervals))
    external[0] = replace(
        external[0],
        request_count=external[0].request_count * 2.0,
    )
    inputs = replace(
        inputs,
        measurements=replace(
            measurements,
            service_intervals=extend(measurements.service_intervals),
            service_class_intervals=extend(measurements.service_class_intervals),
            external_intervals=tuple(external),
        ),
    )

    characterization = characterize_deployment(inputs)
    boundary = characterization.external_request_classes[0].boundary_latency

    assert boundary.status == "inferred"
    assert boundary.mean_seconds == pytest.approx(0.008375)
    assert boundary.candidate_window_count == 5
    assert boundary.excluded_window_count == 1
    assert len(boundary.window_mean_seconds) == 4


def test_boundary_fit_rejects_too_many_count_misaligned_windows():
    inputs = _portable_external_client_inputs()
    external = list(inputs.measurements.external_intervals)
    external[0] = replace(
        external[0],
        request_count=external[0].request_count * 2.0,
    )
    inputs = replace(
        inputs,
        measurements=replace(
            inputs.measurements,
            external_intervals=tuple(external),
        ),
    )

    characterization = characterize_deployment(inputs)
    boundary = characterization.external_request_classes[0].boundary_latency

    assert boundary.status == "unavailable"
    assert boundary.candidate_window_count == 3
    assert boundary.excluded_window_count == 1
    assert "too few" in boundary.reason

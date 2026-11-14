import copy
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from calibration.profile import experiment_fingerprints
from characterization import (
    build_analytical_workload_variants,
    build_characterized_system_variants,
    build_simulation_workload_variants,
    characterize_deployment,
    parse_portable_sizing_inputs,
)
from native_backend import get_native_backend
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
                "request": {
                    "api": {"distribution": "deterministic", "rate": 2.0}
                }
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


def test_named_latency_components_are_parsed_and_summed_for_analytics():
    workload = _call_workload()
    parsed = parse_workload_spec(workload)
    system = System("components", workload, QueueingNetworkType.OPEN)
    serialized = get_native_backend().serialize_system(system)

    assert parsed.request_class_latency_boundaries == {
        "request": "external_client"
    }
    assert tuple(parsed.request_class_latency_component_specs["request"]) == (
        "client_to_ingress",
        "ingress_processing",
    )
    assert parsed.request_class_fixed_latency_overheads["request"] == pytest.approx(
        0.03
    )
    assert system.get_request_class_mean_latencies()["request"] == pytest.approx(
        system.solve().get_mean_latencies()[0]
        + system.solve().get_mean_latencies()[1]
        + 0.03
        + 0.008
    )
    assert serialized["request_classes"][0]["latency_boundary"] == (
        "external_client"
    )
    assert [
        component["name"]
        for component in serialized["request_classes"][0]["latency_components"]
    ] == ["client_to_ingress", "ingress_processing"]


def test_ingress_boundary_rejects_client_to_ingress_component():
    workload = _call_workload()
    workload["request_classes"]["request"]["latency_boundary"] = "ingress"

    with pytest.raises(ValueError, match="cannot include a client path"):
        parse_workload_spec(workload)


def test_aggregate_client_to_entry_cannot_double_count_finer_components():
    workload = _call_workload()
    workload["request_classes"]["request"]["latency_components"][
        "client_to_entry"
    ] = {"distribution": "deterministic", "mean": 0.03}

    with pytest.raises(ValueError, match="cannot combine client_to_entry"):
        parse_workload_spec(workload)


def test_latency_component_and_network_evidence_change_workload_fingerprint():
    first = _call_workload()
    changed_component = copy.deepcopy(first)
    changed_component["request_classes"]["request"]["latency_components"][
        "client_to_ingress"
    ]["mean"] = 0.011
    changed_network = copy.deepcopy(first)
    changed_network["traffic_model"]["communication_latency"][
        "cross_node_one_way"
    ]["mean"] = 0.005

    fingerprints = [
        experiment_fingerprints(
            System(name, workload, QueueingNetworkType.OPEN)
        )
        for name, workload in (
            ("first", first),
            ("component", changed_component),
            ("network", changed_network),
        )
    ]

    assert fingerprints[0][0] == fingerprints[1][0] == fingerprints[2][0]
    assert len({value[1] for value in fingerprints}) == 3


def test_native_simulator_samples_components_and_network_latency_mixture():
    cross = System("cross", _call_workload(), QueueingNetworkType.OPEN)
    same_workload = _call_workload(cross_probability=0.0)
    same = System("same", same_workload, QueueingNetworkType.OPEN)

    cross_result = cross.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(8001,),
    )[0]
    same_result = same.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(8001,),
    )[0]

    assert cross_result.request_class_latencies["request"] == pytest.approx((0.238,))
    assert same_result.request_class_latencies["request"] == pytest.approx((0.232,))


def test_synchronous_request_and_response_reuse_one_empirical_probe_path():
    workload = _call_workload()
    workload["traffic_model"]["communication_latency"]["cross_node_one_way"] = {
        "distribution": "empirical",
        "samples": [0.001, 0.005],
        "sampling": "iid_with_replacement",
    }
    system = System("paired-round-trip", workload, QueueingNetworkType.OPEN)

    result = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=20.0,
        seeds=(8004,),
    )[0]

    network_round_trips = {
        round(latency - 0.23, 9)
        for latency in result.request_class_latencies["request"]
    }
    assert network_round_trips == {0.002, 0.01}


def test_routing_transition_uses_one_way_communication_delay():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "routing": {"api": {"db": 1.0}},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "api": {"distribution": "deterministic", "rate": 0.5}
                }
            },
            "service_times": {
                service: {"distribution": "deterministic", "mean": 0.1}
                for service in ("api", "db")
            },
            "communication_latency": {
                "same_node_one_way": {
                    "distribution": "deterministic",
                    "mean": 0.02,
                },
                "cross_node_one_way": {
                    "distribution": "deterministic",
                    "mean": 0.02,
                },
                "cross_node_probability": 1.0,
            },
        },
    }
    system = System("routing-delay", workload, QueueingNetworkType.OPEN)

    replication = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=2.5,
        seeds=(8005,),
    )[0]

    assert replication.request_class_latencies["request"] == pytest.approx((0.22,))
    assert system.get_request_class_mean_latencies()["request"] == pytest.approx(
        system.solve().get_mean_latencies()[0]
        + system.solve().get_mean_latencies()[1]
        + 0.02
    )


def test_portable_workload_preserves_measured_components_and_network_model():
    base_path = Path(__file__).parents[1] / "examples" / "portable_evidence"
    with (base_path / "workload.yaml").open(encoding="utf-8") as stream:
        workload = yaml.safe_load(stream)
    with (base_path / "measurements.yaml").open(encoding="utf-8") as stream:
        measurements = yaml.safe_load(stream)
    request_class = workload["request_classes"]["browse"]
    request_class["latency_boundary"] = "external_client"
    request_class["latency_components"] = {
        "client_to_ingress": {
            "distribution": "deterministic",
            "mean": 0.01,
        },
        "ingress_processing": {
            "distribution": "deterministic",
            "mean": 0.002,
        },
    }
    workload["communication_latency"] = copy.deepcopy(
        _call_workload()["traffic_model"]["communication_latency"]
    )
    workload["service_blocking_times"] = {
        "api": {"distribution": "deterministic", "mean": 0.006}
    }
    inputs = parse_portable_sizing_inputs(
        workload,
        measurements,
        base_path=base_path,
    )
    characterization = characterize_deployment(inputs)

    (variant,) = build_simulation_workload_variants(inputs, characterization)

    boundary = characterization.external_request_classes[0].boundary_latency
    assert boundary.status == "explicit"
    assert variant.workload["request_classes"]["browse"]["latency_components"] == {
        "client_to_ingress": {"distribution": "deterministic", "mean": 0.01},
        "ingress_processing": {"distribution": "deterministic", "mean": 0.002},
    }
    assert variant.workload["traffic_model"]["communication_latency"] == (
        workload["communication_latency"]
    )
    assert variant.workload["traffic_model"]["service_times"]["api"][
        "non_cpu_blocking_time"
    ] == {"distribution": "deterministic", "mean": 0.006}
    assert all(
        item.name != "browse.client_to_entry_mean"
        for item in variant.uncertainty_dimensions
    )


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


def test_portable_workflow_infers_stable_client_to_entry_mean_component():
    inputs = _portable_external_client_inputs()
    characterization = characterize_deployment(inputs)

    boundary = characterization.external_request_classes[0].boundary_latency
    assert boundary.status == "inferred"
    assert boundary.component_name == "client_to_entry"
    assert boundary.mean_seconds == pytest.approx(0.008)
    assert boundary.window_mean_seconds == pytest.approx((0.0075, 0.008, 0.0085))
    assert boundary.candidate_window_count == 3
    assert boundary.excluded_window_count == 0
    assert boundary.coefficient_of_variation < 0.06
    assert dict(characterization.capabilities)[
        "inferred_client_to_entry_mean"
    ] is True

    (variant,) = build_simulation_workload_variants(inputs, characterization)

    assert variant.workload["request_classes"]["browse"][
        "latency_components"
    ] == {
        "client_to_entry": {
            "distribution": "deterministic",
            "mean": pytest.approx(0.008),
        }
    }
    assert any(
        item.name == "browse.client_to_entry_mean"
        and item.treatment == "aligned_window_stability_gated_mean_fit"
        for item in variant.uncertainty_dimensions
    )
    # Inspect boundary propagation independently of this sample's incomplete
    # local-blocking evidence; that evidence must not pass production sizing.
    (analytical_variant,) = build_analytical_workload_variants(
        inputs,
        characterization,
    )
    analytical_system = System(
        "boundary-inspection", analytical_variant.workload, QueueingNetworkType.OPEN,
    )
    analytical_components = (
        analytical_system.get_request_class_latency_component_distribution_specs()
    )
    assert analytical_components["browse"]["client_to_entry"].mean == pytest.approx(
        0.008
    )
    with pytest.raises(ValueError, match="incomplete"):
        build_characterized_system_variants(
            inputs, characterization, evaluation_mode="analytical"
        )


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


def test_unstable_boundary_means_make_the_simulation_model_incomplete():
    inputs = _portable_external_client_inputs()
    changed_sums = (300.0, 900.0, 312.0)
    external = tuple(
        replace(row, response_duration_sum_seconds=changed_sums[index])
        for index, row in enumerate(inputs.measurements.external_intervals)
    )
    inputs = replace(
        inputs,
        measurements=replace(inputs.measurements, external_intervals=external),
    )

    characterization = characterize_deployment(inputs)
    boundary = characterization.external_request_classes[0].boundary_latency
    (variant,) = build_simulation_workload_variants(inputs, characterization)

    assert boundary.status == "unavailable"
    assert "not stable enough" in boundary.reason
    assert variant.model_complete is False
    assert any("external-client boundary latency" in item for item in variant.model_issues)
    with pytest.raises(ValueError, match="incomplete external-client boundary"):
        build_characterized_system_variants(
            inputs,
            characterization,
            evaluation_mode="analytical",
        )


def test_aggregate_boundary_means_do_not_claim_a_percentile_distribution():
    inputs = _portable_external_client_inputs(percentile=True)
    characterization = characterize_deployment(inputs)
    boundary = characterization.external_request_classes[0].boundary_latency
    (variant,) = build_simulation_workload_variants(inputs, characterization)

    assert boundary.status == "unavailable"
    assert "percentile SLO" in boundary.reason
    assert variant.model_complete is False

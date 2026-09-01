import copy
from pathlib import Path

import pytest
import yaml

from characterization import (
    build_simulation_workload_variants,
    characterize_deployment,
    parse_portable_sizing_inputs,
)
from calibration.profile import experiment_fingerprints
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
                "unplaced_cross_node_probability": cross_probability,
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


def test_native_simulator_samples_components_and_unplaced_network_latency():
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


def test_attached_placement_matrix_overrides_unplaced_network_mixture():
    system = System("placed", _call_workload(), QueueingNetworkType.OPEN)
    system.configure_simulation_placement(
        placement={
            "node-a": {"api": 1, "db": 0},
            "node-b": {"api": 0, "db": 1},
        },
        communication_latency_seconds={
            "node-a": {"node-b": 0.02},
            "node-b": {"node-a": 0.03},
        },
    )

    result = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(8002,),
    )[0]

    assert result.request_class_latencies["request"] == pytest.approx((0.28,))
    assert system.get_request_class_mean_latencies()["request"] == pytest.approx(
        system.solve().get_mean_latencies()[0]
        + system.solve().get_mean_latencies()[1]
        + 0.03
        + 0.05
    )


def test_routing_transition_uses_one_way_analytical_and_simulated_delay():
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
        },
    }
    system = System("placed-routing", workload, QueueingNetworkType.OPEN)
    system.configure_simulation_placement(
        placement={
            "node-a": {"api": 1, "db": 0},
            "node-b": {"api": 0, "db": 1},
        },
        communication_latency_seconds={
            "node-a": {"node-b": 0.02},
            "node-b": {"node-a": 0.03},
        },
    )

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


def test_attached_placement_can_preserve_measured_same_node_delay():
    system = System("same-node-placed", _call_workload(), QueueingNetworkType.OPEN)
    system.configure_simulation_placement(
        placement={"node-a": {"api": 1, "db": 1}},
        communication_latency_seconds={"node-a": {"node-a": 0.007}},
    )

    result = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(8003,),
    )[0]

    assert result.request_class_latencies["request"] == pytest.approx((0.244,))


def test_portable_workload_preserves_measured_components_and_network_model():
    base_path = Path(__file__).parents[1] / "examples" / "portable_evidence"
    with (base_path / "deployment.yaml").open(encoding="utf-8") as stream:
        deployment = yaml.safe_load(stream)
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
        deployment,
        workload,
        measurements,
        base_path=base_path,
    )
    characterization = characterize_deployment(inputs)

    (variant,) = build_simulation_workload_variants(inputs, characterization)

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

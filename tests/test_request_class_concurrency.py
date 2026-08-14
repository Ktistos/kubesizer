import json
import subprocess
from pathlib import Path

import pytest
import yaml

from calibration.profile import experiment_fingerprints
from native_backend import PROTOCOL_VERSION, get_native_backend
from performance_models import QueueingNetworkType, System


BINARY = (
    Path(__file__).resolve().parents[1]
    / "native_backend"
    / "cpp"
    / "build-release"
    / "kubesizer-simulator"
)


def weighted_single_service(*, read_weight=0.5, write_weight=0.5):
    return System(
        "weighted",
        {
            "read": {
                "concurrency_weight": read_weight,
                "services": {"api": {"gamma": 8.0, "mu": 100.0}},
            },
            "write": {
                "concurrency_weight": write_weight,
                "services": {"api": {"gamma": 2.0, "mu": 100.0}},
            },
        },
        QueueingNetworkType.OPEN,
    )


def test_weighted_schema_preserves_total_rate_and_applies_class_mix():
    system = weighted_single_service()

    assert system.get_request_class_concurrency_weights() == {
        "read": 0.5,
        "write": 0.5,
    }
    assert system.get_configured_external_arrival_rates() == {
        "read": (8.0,),
        "write": (2.0,),
    }
    assert system.get_service("api").get_gamma("read") == pytest.approx(5.0)
    assert system.get_service("api").get_gamma("write") == pytest.approx(5.0)
    assert sum(system.get_effective_arrival_rates()) == pytest.approx(10.0)


def test_weighting_preserves_entry_stream_proportions_inside_each_class():
    system = System(
        "multiple-entry-streams",
        {
            "read": {
                "concurrency_weight": 0.5,
                "services": {
                    "api": {"gamma": 6.0, "mu": 100.0},
                    "worker": {"gamma": 2.0, "mu": 100.0},
                },
            },
            "write": {
                "concurrency_weight": 0.5,
                "services": {"api": {"gamma": 2.0, "mu": 100.0}},
            },
        },
        QueueingNetworkType.OPEN,
    )

    assert system.get_configured_external_arrival_rates() == {
        "read": (6.0, 2.0),
        "write": (2.0, 0.0),
    }
    assert system.get_service("api").get_gamma("read") == pytest.approx(3.75)
    assert system.get_service("worker").get_gamma("read") == pytest.approx(1.25)
    assert system.get_service("api").get_gamma("write") == pytest.approx(5.0)


def test_legacy_schema_derives_weights_without_changing_arrival_rates():
    system = System(
        "legacy",
        {
            "read": {"api": {"gamma": 8.0, "mu": 100.0}},
            "write": {"api": {"gamma": 2.0, "mu": 100.0}},
        },
        QueueingNetworkType.OPEN,
    )

    assert system.get_request_class_concurrency_weights() == pytest.approx(
        {"read": 0.8, "write": 0.2}
    )
    assert system.get_service("api").get_gamma("read") == pytest.approx(8.0)
    assert system.get_service("api").get_gamma("write") == pytest.approx(2.0)


@pytest.mark.parametrize(
    ("workload", "message"),
    (
        (
            {
                "read": {
                    "concurrency_weight": 0.5,
                    "services": {"api": {"gamma": 1.0, "mu": 10.0}},
                },
                "write": {"api": {"gamma": 1.0, "mu": 10.0}},
            },
            "Do not mix",
        ),
        (
            {
                "read": {
                    "concurrency_weight": 0.6,
                    "services": {"api": {"gamma": 1.0, "mu": 10.0}},
                },
                "write": {
                    "concurrency_weight": 0.3,
                    "services": {"api": {"gamma": 1.0, "mu": 10.0}},
                },
            },
            "sum to one",
        ),
        (
            {
                "read": {
                    "concurrency_weight": 0.5,
                    "services": {"api": {"gamma": 1.0, "mu": 10.0}},
                },
                "write": {
                    "concurrency_weight": 0.5,
                    "services": {"api": {"gamma": 0.0, "mu": 10.0}},
                },
            },
            "positive external gamma",
        ),
    ),
)
def test_invalid_weighted_schemas_fail_explicitly(workload, message):
    with pytest.raises(ValueError, match=message):
        System("invalid", workload, QueueingNetworkType.OPEN)


def test_native_protocol_receives_effective_distributions_and_class_weights():
    payload = get_native_backend().serialize_system(weighted_single_service())

    assert [item["concurrency_weight"] for item in payload["request_classes"]] == [
        0.5,
        0.5,
    ]
    assert [
        item["interarrival_distribution"]["distribution"]
        for item in payload["external_streams"]
    ] == ["exponential", "exponential"]
    assert [
        1.0 / item["interarrival_distribution"]["mean"]
        for item in payload["external_streams"]
    ] == pytest.approx([5.0, 5.0])


def test_native_measurement_counts_follow_weighted_concurrent_mix():
    system = weighted_single_service(read_weight=0.25, write_weight=0.75)
    duration = 100.0
    seeds = tuple(range(130_000, 130_030))
    replications = system.simulate_measurement_replications(
        warmup_duration=10.0,
        measurement_duration=duration,
        seeds=seeds,
        workers=6,
    )

    read_count = sum(item.measured_request_counts["read"] for item in replications)
    write_count = sum(item.measured_request_counts["write"] for item in replications)
    total_count = read_count + write_count
    total_simulated_measurement_time = len(seeds) * duration

    assert read_count / total_count == pytest.approx(0.25, abs=0.02)
    assert write_count / total_count == pytest.approx(0.75, abs=0.02)
    assert total_count / total_simulated_measurement_time == pytest.approx(
        10.0,
        rel=0.03,
    )


def test_native_protocol_rejects_invalid_weight_total():
    payload = get_native_backend().serialize_system(weighted_single_service())
    payload["request_classes"][0]["concurrency_weight"] = 0.8
    request = {
        "protocol_version": PROTOCOL_VERSION,
        "operation": "simulate",
        "workload": payload,
        "seed": 7,
        "horizon": 1.0,
        "warmup_duration": 0.0,
        "capture_state": False,
    }

    completed = subprocess.run(
        [str(BINARY)],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "weights must sum to one" in completed.stdout


def test_repository_workload_uses_explicit_streams_and_derives_class_mix():
    workload_path = Path(__file__).resolve().parents[1] / "workload.yaml"
    workload = yaml.safe_load(workload_path.read_text(encoding="utf-8"))

    assert set(workload) == {
        "schema_version",
        "time_unit",
        "request_classes",
        "scenarios",
    }
    assert set(workload["scenarios"]) == {"normal"}
    assert set(workload["scenarios"]["normal"]) == {
        "entry_services",
        "service_times",
    }
    assert {
        name: streams["api"]["rate"]
        for name, streams in workload["scenarios"]["normal"]["entry_services"].items()
    } == {
        "read_heavy": 23.1,
        "write_heavy": 6.6,
        "compute_heavy": 3.3,
    }
    assert workload["schema_version"] == 2

    system = System("repository-workload", workload, QueueingNetworkType.OPEN)
    class_rates = {
        request_class: sum(
            service.get_gamma(request_class)
            for service in system.get_services().values()
        )
        for request_class in system.get_request_classes()
    }
    assert class_rates == pytest.approx(
        {
            "read_heavy": 23.1,
            "write_heavy": 6.6,
            "compute_heavy": 3.3,
        }
    )
    assert sum(class_rates.values()) == pytest.approx(33.0)
    assert system.get_request_class_concurrency_weights() == pytest.approx(
        {
            "read_heavy": 0.7,
            "write_heavy": 0.2,
            "compute_heavy": 0.1,
        }
    )


def test_versioned_workload_rejects_an_incomplete_empirical_distribution():
    workload = {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {"read": {"routing": {}}},
        "scenarios": {
            "observed": {
                "entry_services": {
                    "read": {
                        "api": {
                            "distribution": "empirical_interarrival",
                            "rate": 10.0,
                        }
                    }
                },
                "service_times": {
                    "api": {"distribution": "exponential", "rate": 100.0}
                },
            }
        },
    }

    with pytest.raises(ValueError, match="source, column, and sampling"):
        System("unsupported", workload, QueueingNetworkType.OPEN)


def test_system_requires_an_explicit_selection_for_multiple_scenarios():
    workload_path = Path(__file__).resolve().parents[1] / "workload.yaml"
    workload = yaml.safe_load(workload_path.read_text(encoding="utf-8"))
    workload["scenarios"]["peak"] = workload["scenarios"]["normal"]

    with pytest.raises(ValueError, match="scenario_name is required"):
        System("multiple", workload, QueueingNetworkType.OPEN)

    selected = System(
        "multiple",
        workload,
        QueueingNetworkType.OPEN,
        scenario_name="peak",
    )

    assert selected.get_scenario_name() == "peak"


def test_concurrency_weight_change_invalidates_workload_fingerprint():
    first = weighted_single_service(read_weight=0.5, write_weight=0.5)
    second = weighted_single_service(read_weight=0.6, write_weight=0.4)

    assert experiment_fingerprints(first)[1] != experiment_fingerprints(second)[1]

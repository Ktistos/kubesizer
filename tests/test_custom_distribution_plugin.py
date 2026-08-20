import shutil
from pathlib import Path

import pytest
import yaml

from native_backend import NativeBackendError, get_native_backend
from performance_models import (
    AnalyticalQueueingModel,
    QueueingNetworkType,
    System,
    parse_workload_spec,
)


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "workloads" / "custom-distribution.example.yaml"
CUSTOM_LIBRARY = (
    ROOT
    / "native_backend"
    / "cpp"
    / "build-release"
    / "libkubesizer-example-distributions.so"
)


def _custom_spec(*, quantity, scale, symbol, parameters=(), scv=0.0):
    return {
        "distribution": "custom",
        quantity: scale,
        "scv": scv,
        "library": str(CUSTOM_LIBRARY),
        "symbol": symbol,
        "abi_version": 1,
        "parameters": list(parameters),
    }


def _single_service_workload(arrival, service):
    return {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {"request": {"routing": {}}},
        "scenarios": {
            "test": {
                "entry_services": {"request": {"service": arrival}},
                "service_times": {"service": service},
            }
        },
    }


def test_custom_distribution_example_parses_and_serializes_plugin_metadata():
    workload = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))

    parsed = parse_workload_spec(workload, base_path=EXAMPLE.parent)
    system = System(
        "custom-example",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=EXAMPLE.parent,
    )
    payload = get_native_backend().serialize_system(system)

    assert parsed.concurrency_weights == {"interactive": 0.8, "batch": 0.2}
    custom_distributions = [
        stream["interarrival_distribution"]
        for stream in payload["external_streams"]
        if stream["interarrival_distribution"]["distribution"] == "custom"
    ] + [
        service["service_time_distribution"]
        for service in payload["services"]
        if service["service_time_distribution"]["distribution"] == "custom"
    ]
    assert len(custom_distributions) == 3
    assert all(item["abi_version"] == 1 for item in custom_distributions)
    assert all(Path(item["library"]).is_absolute() for item in custom_distributions)
    assert all(len(item["library_sha256"]) == 64 for item in custom_distributions)


def test_unit_multiplier_produces_exact_custom_arrivals_and_service_times():
    unit_symbol = "kubesizer_unit_multiplier"
    system = System(
        "custom-unit",
        _single_service_workload(
            _custom_spec(quantity="rate", scale=4.0, symbol=unit_symbol),
            _custom_spec(quantity="mean", scale=0.1, symbol=unit_symbol),
        ),
        QueueingNetworkType.OPEN,
    )

    assert system.get_analytical_queueing_models() == {
        "service": AnalyticalQueueingModel.GGC
    }

    (replication,) = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(941,),
    )

    assert replication.request_class_arrival_times["request"] == pytest.approx(
        (0.25, 0.5, 0.75)
    )
    assert replication.request_class_latencies["request"] == pytest.approx(
        (0.1, 0.1, 0.1)
    )


def test_random_custom_sampler_is_reproducible():
    workload = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
    system = System(
        "custom-reproducibility",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=EXAMPLE.parent,
    )
    arguments = {
        "warmup_duration": 0.5,
        "measurement_duration": 3.0,
        "seeds": (87_001,),
        "maximum_drain_time": 100.0,
    }

    first = system.simulate_measurement_replications(**arguments)
    second = system.simulate_measurement_replications(**arguments)

    assert first == second
    assert first[0].tagged_requests_in_flight_at_end == 0
    assert sum(first[0].measured_request_counts.values()) > 0


def test_two_point_plugin_matches_its_declared_mean_and_scv():
    system = System(
        "custom-two-point-moments",
        _single_service_workload(
            _custom_spec(
                quantity="rate",
                scale=8.0,
                symbol="kubesizer_two_point_multiplier",
                parameters=(0.5, 2.0, 2.0 / 3.0),
                scv=0.5,
            ),
            {"distribution": "deterministic", "mean": 0.001},
        ),
        QueueingNetworkType.OPEN,
    )

    (replication,) = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=2_500.0,
        seeds=(87_002,),
    )
    arrivals = replication.request_class_arrival_times["request"]
    interarrivals = (arrivals[0],) + tuple(
        right - left for left, right in zip(arrivals, arrivals[1:])
    )
    mean = sum(interarrivals) / len(interarrivals)
    variance = sum((value - mean) ** 2 for value in interarrivals) / len(
        interarrivals
    )

    assert len(interarrivals) > 10_000
    assert mean == pytest.approx(1.0 / 8.0, rel=0.02)
    assert variance / (mean * mean) == pytest.approx(0.5, rel=0.08)


def test_concurrency_weights_rescale_custom_interarrival_means():
    unit_symbol = "kubesizer_unit_multiplier"
    workload = {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {
            "read": {"concurrency_weight": 0.5, "routing": {}},
            "write": {"concurrency_weight": 0.5, "routing": {}},
        },
        "scenarios": {
            "test": {
                "entry_services": {
                    "read": {
                        "service": _custom_spec(
                            quantity="rate",
                            scale=8.0,
                            symbol=unit_symbol,
                        )
                    },
                    "write": {
                        "service": _custom_spec(
                            quantity="rate",
                            scale=2.0,
                            symbol=unit_symbol,
                        )
                    },
                },
                "service_times": {
                    "service": {"distribution": "deterministic", "mean": 0.001}
                },
            }
        },
    }
    system = System("weighted-custom", workload, QueueingNetworkType.OPEN)

    specs = system.get_external_arrival_distribution_specs()
    assert specs["read"]["service"].rate == pytest.approx(5.0)
    assert specs["write"]["service"].rate == pytest.approx(5.0)
    (replication,) = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(87_003,),
    )
    assert replication.request_class_arrival_times["read"] == pytest.approx(
        (0.2, 0.4, 0.6, 0.8)
    )
    assert replication.request_class_arrival_times["write"] == pytest.approx(
        (0.2, 0.4, 0.6, 0.8)
    )


def test_custom_distribution_rejects_a_missing_library(tmp_path):
    missing = tmp_path / "missing-plugin.so"
    workload = _single_service_workload(
        {
            **_custom_spec(
                quantity="rate",
                scale=1.0,
                symbol="kubesizer_unit_multiplier",
            ),
            "library": str(missing),
        },
        {"distribution": "exponential", "rate": 10.0},
    )

    with pytest.raises(ValueError, match="does not identify a file"):
        System("missing-custom-library", workload, QueueingNetworkType.OPEN)


def test_custom_distribution_rejects_a_missing_sampler_symbol():
    system = System(
        "missing-custom-symbol",
        _single_service_workload(
            _custom_spec(
                quantity="rate",
                scale=1.0,
                symbol="symbol_that_does_not_exist",
            ),
            {"distribution": "exponential", "rate": 10.0},
        ),
        QueueingNetworkType.OPEN,
    )

    with pytest.raises(NativeBackendError, match="cannot load custom distribution symbol"):
        system.simulate_measurement_replications(
            warmup_duration=0.0,
            measurement_duration=1.0,
            seeds=(1,),
        )


def test_custom_library_change_after_parsing_is_rejected(tmp_path):
    copied_library = tmp_path / CUSTOM_LIBRARY.name
    shutil.copyfile(CUSTOM_LIBRARY, copied_library)
    arrival = _custom_spec(
        quantity="rate",
        scale=1.0,
        symbol="kubesizer_unit_multiplier",
    )
    arrival["library"] = str(copied_library)
    system = System(
        "changed-custom-library",
        _single_service_workload(
            arrival,
            {"distribution": "exponential", "rate": 10.0},
        ),
        QueueingNetworkType.OPEN,
    )
    with copied_library.open("ab") as stream:
        stream.write(b"changed-after-parse")

    with pytest.raises(NativeBackendError, match="changed after workload parsing"):
        get_native_backend().serialize_system(system)

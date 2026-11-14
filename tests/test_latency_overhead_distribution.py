import copy
import statistics

import pytest

from calibration.profile import experiment_fingerprints
from native_backend import get_native_backend
from performance_models import QueueingNetworkType, System, parse_workload_spec


def _workload(overhead, *, timeout=None):
    request_class = {
        "concurrency_weight": 1.0,
        "end_to_end_latency_slos": {
            "percentile": {"quantile": 0.95, "maximum_seconds": 2.0}
        },
        "routing": {},
        "latency_overhead": overhead,
    }
    if timeout is not None:
        request_class["end_to_end_timeout_seconds"] = timeout
        request_class["maximum_terminal_failure_rate"] = 0.5
    return {
        "request_classes": {"request": request_class},
        "traffic_model": {
            "entry_services": {
                "request": {
                    "service": {"distribution": "deterministic", "rate": 2.0}
                }
            },
            "service_times": {
                "service": {"distribution": "deterministic", "mean": 0.1}
            },
        },
    }


def test_overhead_distribution_is_parsed_serialized_and_used_by_analytics():
    workload = _workload(
        {"distribution": "lognormal", "mean": 0.2, "scv": 3.0}
    )
    parsed = parse_workload_spec(workload)
    system = System("overhead", workload, QueueingNetworkType.OPEN)

    specification = parsed.request_class_latency_overhead_specs["request"]
    serialized = get_native_backend().serialize_system(system)["request_classes"][0]

    assert specification.distribution == "lognormal"
    assert specification.mean == pytest.approx(0.2)
    assert specification.scv == pytest.approx(3.0)
    assert parsed.request_class_fixed_latency_overheads == {"request": 0.2}
    assert serialized["latency_overhead_distribution"] == {
        "distribution": "lognormal",
        "mean": 0.2,
        "scv": 3.0,
    }
    assert system.get_request_class_mean_latencies()["request"] == pytest.approx(
        system.solve().get_mean_latencies()[0] + 0.2
    )


def test_fixed_and_distributional_overhead_are_mutually_exclusive():
    workload = _workload(
        {"distribution": "deterministic", "mean": 0.2}
    )
    workload["request_classes"]["request"][
        "fixed_latency_overhead_seconds"
    ] = 0.2

    with pytest.raises(ValueError, match="cannot define both"):
        parse_workload_spec(workload)


def test_empirical_overhead_samples_are_embedded_and_reproducible(tmp_path):
    sample_file = tmp_path / "overhead.csv"
    sample_file.write_text(
        "overhead_seconds\n0.05\n0.10\n0.40\n",
        encoding="utf-8",
    )
    workload = _workload(
        {
            "distribution": "empirical",
            "source": sample_file.name,
            "column": "overhead_seconds",
            "sampling": "iid_with_replacement",
        }
    )
    system = System(
        "empirical-overhead",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=tmp_path,
    )

    serialized = get_native_backend().serialize_system(system)["request_classes"][0][
        "latency_overhead_distribution"
    ]
    arguments = {
        "warmup_duration": 0.0,
        "measurement_duration": 10.0,
        "seeds": (711,),
    }
    first = system.simulate_measurement_replications(**arguments)
    second = system.simulate_measurement_replications(**arguments)

    assert serialized["samples"] == [0.05, 0.1, 0.4]
    assert "source" not in serialized
    assert first == second
    assert {
        round(value - 0.1, 10)
        for value in first[0].request_class_latencies["request"]
    } <= {0.05, 0.1, 0.4}


def test_overhead_variance_changes_fingerprint_even_when_mean_is_unchanged():
    first = _workload(
        {"distribution": "lognormal", "mean": 0.2, "scv": 1.0}
    )
    second = copy.deepcopy(first)
    second["request_classes"]["request"]["latency_overhead"]["scv"] = 4.0

    first_fingerprints = experiment_fingerprints(
        System("first", first, QueueingNetworkType.OPEN)
    )
    second_fingerprints = experiment_fingerprints(
        System("second", second, QueueingNetworkType.OPEN)
    )

    assert first_fingerprints[0] == second_fingerprints[0]
    assert first_fingerprints[1] != second_fingerprints[1]


def test_sampled_overhead_respects_end_to_end_timeout():
    system = System(
        "overhead-timeout",
        _workload(
            {"distribution": "deterministic", "mean": 0.5},
            timeout=0.3,
        ),
        QueueingNetworkType.OPEN,
    )

    (result,) = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=5.0,
        seeds=(33,),
    )

    assert result.request_class_latencies["request"] == pytest.approx(
        (0.3,) * result.measured_request_counts["request"]
    )
    assert result.failed_measured_request_counts == {
        "request": result.measured_request_counts["request"]
    }


def test_bayesian_bootstrap_propagates_empirical_sample_uncertainty():
    workload = _workload(
        {
            "distribution": "empirical",
            "samples": [0.01, 1.0],
            "sampling": "bayesian_bootstrap",
        }
    )
    system = System("uncertain-overhead", workload, QueueingNetworkType.OPEN)
    arguments = {
        "warmup_duration": 0.0,
        "measurement_duration": 100.0,
        "seeds": tuple(range(500, 540)),
        "workers": 8,
    }

    first = system.simulate_measurement_replications(**arguments)
    second = system.simulate_measurement_replications(**arguments)
    replication_overhead_means = tuple(
        statistics.fmean(value - 0.1 for value in item.request_class_latencies["request"])
        for item in first
    )

    assert first == second
    # With about 200 requests per replication, ordinary IID resampling would
    # have an SD near 0.035. The larger between-replication spread is the
    # posterior uncertainty over the finite two-point empirical distribution.
    assert statistics.stdev(replication_overhead_means) > 0.15


def test_cluster_bootstrap_resamples_complete_measurement_units():
    workload = _workload(
        {
            "distribution": "empirical",
            "clusters": [[0.01] * 20, [1.0] * 20],
            "sampling": "cluster_bootstrap",
        }
    )
    system = System("clustered-overhead", workload, QueueingNetworkType.OPEN)
    serialized = get_native_backend().serialize_system(system)["request_classes"][0][
        "latency_overhead_distribution"
    ]

    replications = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=100.0,
        seeds=tuple(range(700, 740)),
        workers=8,
    )
    replication_overhead_means = tuple(
        statistics.fmean(value - 0.1 for value in item.request_class_latencies["request"])
        for item in replications
    )

    assert len(serialized["clusters"]) == 2
    assert statistics.stdev(replication_overhead_means) > 0.20

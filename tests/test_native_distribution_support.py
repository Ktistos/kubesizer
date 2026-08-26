from pathlib import Path

import pytest
import yaml

from native_backend import get_native_backend
from metrics import SimulationMetricsCollector
from performance_models import QueueingNetworkType, System


ROOT = Path(__file__).resolve().parents[1]
MAXIMUM_WORKLOAD = ROOT / "workload.maximum.yaml"


def _single_service_workload(arrival, service):
    return {
        "request_classes": {"request": {"concurrency_weight": 1.0, "routing": {}}},
        "traffic_model": {
            "entry_services": {"request": {"service": arrival}},
            "service_times": {"service": service},
        },
    }


def test_deterministic_arrival_and_service_samples_are_exact():
    system = System(
        "deterministic",
        _single_service_workload(
            {"distribution": "deterministic", "rate": 4.0},
            {"distribution": "deterministic", "mean": 0.1},
        ),
        QueueingNetworkType.OPEN,
    )

    (replication,) = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(41,),
    )

    assert replication.request_class_arrival_times["request"] == pytest.approx(
        (0.25, 0.5, 0.75)
    )
    assert replication.request_class_latencies["request"] == pytest.approx(
        (0.1, 0.1, 0.1)
    )


def test_serializer_embeds_empirical_samples_instead_of_csv_paths(tmp_path):
    arrivals = tmp_path / "arrivals.csv"
    services = tmp_path / "services.csv"
    arrivals.write_text("seconds\n0.5\n1.0\n", encoding="utf-8")
    services.write_text("seconds\n0.1\n0.2\n", encoding="utf-8")
    workload = _single_service_workload(
        {
            "distribution": "empirical_interarrival",
            "source": arrivals.name,
            "column": "seconds",
            "sampling": "iid_with_replacement",
        },
        {
            "distribution": "empirical",
            "source": services.name,
            "column": "seconds",
            "sampling": "iid_with_replacement",
        },
    )
    system = System(
        "empirical",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=tmp_path,
    )

    serialized = get_native_backend().serialize_system(system)

    service_distribution = serialized["services"][0]["service_time_distributions"][0]
    arrival_distribution = serialized["external_streams"][0][
        "interarrival_distribution"
    ]
    assert service_distribution["samples"] == [0.1, 0.2]
    assert arrival_distribution["samples"] == [0.5, 1.0]
    assert "source" not in service_distribution
    assert "source" not in arrival_distribution


def test_declared_distribution_model_runs_reproducibly():
    workload = yaml.safe_load(MAXIMUM_WORKLOAD.read_text(encoding="utf-8"))
    system = System(
        "distribution-smoke",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=MAXIMUM_WORKLOAD.parent,
    )
    system.configure_system_replicas(
        tuple(value + 1 for value in system.get_minimum_stable_replicas())
    )
    arguments = {
        "warmup_duration": 0.5,
        "measurement_duration": 2.0,
        "seeds": (8123,),
        "maximum_drain_time": 100.0,
    }

    first = system.simulate_measurement_replications(**arguments)
    second = system.simulate_measurement_replications(**arguments)

    assert first == second
    assert first[0].tagged_requests_in_flight_at_end == 0


def _two_class_cpu_workload(execution_model):
    return {
        "request_classes": {
            name: {"concurrency_weight": 0.5, "routing": {}}
            for name in ("left", "right")
        },
        "traffic_model": {
            "entry_services": {
                name: {"service": {"distribution": "deterministic", "rate": 0.4}}
                for name in ("left", "right")
            },
            "service_times": {
                "service": {
                    "execution_model": execution_model,
                    "distribution": "deterministic",
                    "mean": 1.0,
                }
            },
        },
    }


def test_processor_sharing_recalculates_progress_when_concurrency_changes():
    system = System(
        "cpu-sharing",
        _two_class_cpu_workload("processor_sharing"),
        QueueingNetworkType.OPEN,
    )

    result = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=3.0,
        seeds=(91,),
    )[0]

    assert result.measured_request_counts == {"left": 1, "right": 1}
    assert result.request_class_latencies["left"] == pytest.approx((2.0,))
    assert result.request_class_latencies["right"] == pytest.approx((2.0,))
    assert result.queue_lengths_at_end == (0,)


def test_processor_sharing_scales_with_replica_cpu_capacity():
    system = System(
        "cpu-sharing",
        _two_class_cpu_workload("processor_sharing"),
        QueueingNetworkType.OPEN,
    )
    system.configure_system_replicas((2,))

    result = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=3.0,
        seeds=(91,),
    )[0]

    assert result.request_class_latencies["left"] == pytest.approx((1.0,))
    assert result.request_class_latencies["right"] == pytest.approx((1.0,))


def test_processor_sharing_state_conserves_service_population():
    system = System(
        "cpu-sharing-state",
        _two_class_cpu_workload("processor_sharing"),
        QueueingNetworkType.OPEN,
    )
    collector = SimulationMetricsCollector(1)

    system.simulate(4.75, metrics_collector=collector, seed=91)

    assert collector.get_queue_history()[0] == (
        (0.0, 0),
        (2.5, 1),
        (4.5, 0),
    )
    assert collector.get_busy_history()[0] == (
        (0.0, 0),
        (2.5, 1),
        (4.5, 0),
    )
    assert collector.get_active_history() == (
        (0.0, 0),
        (2.5, 2),
        (4.5, 0),
    )

    mid_run = SimulationMetricsCollector(1)
    system.simulate(3.0, metrics_collector=mid_run, seed=91)
    assert mid_run.get_queue_length(0) == 1
    assert mid_run.get_busy_replicas(0) == 1
    assert mid_run.get_number_in_system(0) == 2

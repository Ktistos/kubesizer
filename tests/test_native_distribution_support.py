from pathlib import Path

import pytest
import yaml

from native_backend import get_native_backend
from performance_models import QueueingNetworkType, System


ROOT = Path(__file__).resolve().parents[1]
MAXIMUM_WORKLOAD = ROOT / "workload.maximum.yaml"


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

    service_distribution = serialized["services"][0][
        "service_time_distribution"
    ]
    arrival_distribution = serialized["external_streams"][0][
        "interarrival_distribution"
    ]
    assert service_distribution["samples"] == [0.1, 0.2]
    assert arrival_distribution["samples"] == [0.5, 1.0]
    assert "source" not in service_distribution
    assert "source" not in arrival_distribution


@pytest.mark.parametrize(
    "scenario_name",
    (
        "baseline_exponential",
        "variable_parametric",
        "empirical_iid",
        "correlated_arrival_trace",
    ),
)
def test_every_declared_distribution_scenario_runs_reproducibly(scenario_name):
    workload = yaml.safe_load(MAXIMUM_WORKLOAD.read_text(encoding="utf-8"))
    system = System(
        "distribution-smoke",
        workload,
        QueueingNetworkType.OPEN,
        scenario_name=scenario_name,
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

import math
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from calibration import (
    average_warmup_replications,
    run_warmup_pilot_replications,
    select_warmup_pilot_parameters,
)
from metrics import SimulationMetricsCollector
from native_backend import (
    PROTOCOL_VERSION,
    NativeBackendError,
    get_native_backend,
    slice_measurement_replication,
)
from performance_models import QueueingNetworkType, System


BINARY = (
    Path(__file__).resolve().parents[1]
    / "cpp"
    / "build-release"
    / "kubesizer-simulator"
)


@pytest.fixture(scope="module", autouse=True)
def require_native_binary():
    assert BINARY.is_file(), (
        "Build the mandatory native backend before testing: "
        "native_backend/cpp/build.sh"
    )


def make_single_service_system(gamma=1.0, mu=4.0):
    return System(
        "single-service",
        {"read": {"api": {"gamma": gamma, "mu": mu}}},
        QueueingNetworkType.OPEN,
    )


def assert_valid_history(history):
    assert history[0] == (0.0, 0)
    assert all(left[0] < right[0] for left, right in zip(history, history[1:]))
    assert all(left[1] != right[1] for left, right in zip(history, history[1:]))


def test_system_simulate_uses_native_backend_and_is_seed_reproducible(monkeypatch):
    system = make_single_service_system(gamma=2.0, mu=5.0)
    monkeypatch.setattr(
        "performance_models.service.Service.generate_external_traffic",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("SimPy execution path was used")
        ),
    )

    first_collector = SimulationMetricsCollector(1)
    second_collector = SimulationMetricsCollector(1)
    first = system.simulate(20.0, metrics_collector=first_collector, seed=1234)
    second = system.simulate(20.0, metrics_collector=second_collector, seed=1234)

    assert first.get_mean_utilizations() == second.get_mean_utilizations()
    assert first.get_mean_latencies() == second.get_mean_latencies()
    assert first.get_request_class_p95_latencies() == (
        second.get_request_class_p95_latencies()
    )
    assert first_collector.get_active_history() == second_collector.get_active_history()
    assert first_collector.get_queue_history() == second_collector.get_queue_history()
    assert first_collector.get_busy_history() == second_collector.get_busy_history()


def test_native_state_histories_and_derived_state_satisfy_invariants():
    system = make_single_service_system(gamma=4.0, mu=2.0)
    system.configure_system_replicas((3,))
    collector = SimulationMetricsCollector(1)
    system.simulate(30.0, metrics_collector=collector, seed=91)

    active = collector.get_active_history()
    queue = collector.get_queue_history()[0]
    busy = collector.get_busy_history()[0]
    assert_valid_history(active)
    assert_valid_history(queue)
    assert_valid_history(busy)
    assert all(value >= 0 for _, value in active)
    assert all(value >= 0 for _, value in queue)
    assert all(0 <= value <= 3 for _, value in busy)
    assert active[-1][1] == collector.get_active_requests()
    assert collector.get_number_in_system(0) == (
        collector.get_queue_length(0) + collector.get_busy_replicas(0)
    )
    assert collector.get_utilization(0) == collector.get_busy_replicas(0) / 3


def test_native_online_ensemble_matches_individual_history_bucketization():
    system = make_single_service_system(gamma=1.5, mu=4.0)
    parameters = select_warmup_pilot_parameters(
        system,
        replication_count=4,
        seed_start=70_000,
    )
    parameters = replace(
        parameters,
        pilot_horizon=20.0,
        bucket_counts=(40, 20, 10),
    )

    individual = run_warmup_pilot_replications(
        system,
        parameters,
        bucket_width=0.5,
        workers=1,
    )
    expected = average_warmup_replications(individual)
    actual = run_warmup_pilot_replications(
        system,
        parameters,
        bucket_width=0.5,
        workers=3,
        aggregate=True,
    )

    assert actual.seeds == expected.seeds
    assert actual.active_requests == pytest.approx(expected.active_requests, abs=1e-12)
    for actual_row, expected_row in zip(
        actual.queue_length_by_service,
        expected.queue_length_by_service,
    ):
        assert actual_row == pytest.approx(expected_row, abs=1e-12)
    for actual_row, expected_row in zip(
        actual.busy_replicas_by_service,
        expected.busy_replicas_by_service,
    ):
        assert actual_row == pytest.approx(expected_row, abs=1e-12)


def test_measurement_window_tags_only_arrivals_in_window_and_drains_them():
    system = System(
        "measurement-gates",
        {
            "read": {"api": {"gamma": 5.0, "mu": 30.0}},
            "write": {"api": {"gamma": 5.0, "mu": 30.0}},
        },
        QueueingNetworkType.OPEN,
    )
    results = system.simulate_measurement_replications(
        warmup_duration=10.0,
        measurement_duration=1.0,
        seeds=(100, 101, 102, 103),
        workers=3,
    )

    assert tuple(result.seed for result in results) == (100, 101, 102, 103)
    for result in results:
        assert result.stop_time >= 11.0
        assert result.measured_request_counts == result.completed_measured_request_counts
        assert 0 < sum(result.measured_request_counts.values()) < 40
        assert set(result.request_class_p95_latencies) == {"read", "write"}
        assert all(value > 0 for value in result.request_class_p95_latencies.values())
        assert result.tagged_requests_in_flight_at_end == 0
        for request_class, count in result.measured_request_counts.items():
            assert len(result.request_class_latencies[request_class]) == count
            assert len(result.request_class_arrival_times[request_class]) == count
            assert all(
                10.0 <= arrival < 11.0
                for arrival in result.request_class_arrival_times[request_class]
            )

    shorter_direct = system.simulate_measurement_replications(
        warmup_duration=10.0,
        measurement_duration=0.5,
        seeds=(100,),
    )[0]
    shorter_prefix = slice_measurement_replication(results[0], 0.5)
    assert shorter_prefix.measurement_duration == 0.5
    assert shorter_prefix.request_class_arrival_times == (
        shorter_direct.request_class_arrival_times
    )
    assert shorter_prefix.request_class_latencies == (
        shorter_direct.request_class_latencies
    )


def test_measurement_worker_count_preserves_exact_seed_results():
    system = make_single_service_system(gamma=2.0, mu=5.0)
    seeds = (501, 502, 503, 504)

    sequential = system.simulate_measurement_replications(
        warmup_duration=5.0,
        measurement_duration=10.0,
        seeds=seeds,
        workers=1,
    )
    parallel = system.simulate_measurement_replications(
        warmup_duration=5.0,
        measurement_duration=10.0,
        seeds=seeds,
        workers=4,
    )

    assert parallel == sequential


def test_slow_eligible_request_is_retained_after_measurement_end():
    system = make_single_service_system(gamma=1.0, mu=0.1)
    system.configure_system_replicas((20,))

    replication = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=5.0,
        seeds=(125,),
        maximum_drain_time=10_000.0,
    )[0]

    assert replication.stop_time > 5.0
    assert replication.measured_request_counts["read"] > 0
    assert len(replication.request_class_latencies["read"]) == (
        replication.measured_request_counts["read"]
    )
    assert max(replication.request_class_latencies["read"]) > 5.0


def test_multiple_entry_streams_and_routing_loop_complete_safely():
    system = System(
        "looped-network",
        {
            "interactive": {
                "front": {
                    "gamma": 1.0,
                    "mu": 8.0,
                    "routes": {"back": 0.6},
                },
                "back": {
                    "gamma": 0.5,
                    "mu": 8.0,
                    "routes": {"front": 0.5},
                },
            }
        },
        QueueingNetworkType.OPEN,
    )
    system.configure_system_replicas(system.get_minimum_stable_replicas())

    result = system.simulate_measurement_replications(
        warmup_duration=5.0,
        measurement_duration=20.0,
        seeds=(8, 9),
        workers=2,
    )

    assert all(item.measured_request_counts["interactive"] > 0 for item in result)
    assert all(
        item.measured_request_counts == item.completed_measured_request_counts
        for item in result
    )
    assert all(item.request_class_p95_latencies["interactive"] > 0 for item in result)


def test_mm1_native_outputs_are_statistically_consistent_with_known_limits():
    gamma = 1.0
    mu = 4.0
    system = make_single_service_system(gamma=gamma, mu=mu)
    ensemble = system.run_warmup_ensemble(
        seeds=tuple(range(20_000, 20_030)),
        horizon=400.0,
        bucket_width=10.0,
        workers=6,
    )
    late_busy = [row[-20:] for row in ensemble.busy_replicas_by_service][0]
    estimated_utilization = sum(late_busy) / len(late_busy)
    assert estimated_utilization == pytest.approx(gamma / mu, abs=0.04)

    replications = system.simulate_measurement_replications(
        warmup_duration=100.0,
        measurement_duration=400.0,
        seeds=tuple(range(30_000, 30_030)),
        workers=6,
    )
    estimated_p95 = sum(
        item.request_class_p95_latencies["read"] for item in replications
    ) / len(replications)
    theoretical_p95 = -math.log(0.05) / (mu - gamma)
    assert estimated_p95 == pytest.approx(theoretical_p95, rel=0.15)



def test_zero_arrival_workload_remains_empty_without_special_casing():
    system = make_single_service_system(gamma=0.0, mu=4.0)
    collector = SimulationMetricsCollector(1)
    metrics = system.simulate(10.0, metrics_collector=collector, seed=5)

    assert metrics.get_mean_utilizations() == [0.0]
    assert metrics.get_request_class_p95_latencies() == {}
    assert collector.get_active_history() == ((0.0, 0),)
    assert collector.get_queue_history()[0] == ((0.0, 0),)
    assert collector.get_busy_history()[0] == ((0.0, 0),)

    replication = system.simulate_measurement_replications(
        warmup_duration=2.0,
        measurement_duration=3.0,
        seeds=(7,),
    )[0]
    assert replication.stop_time == 5.0
    assert replication.measured_request_counts == {"read": 0}
    assert replication.completed_measured_request_counts == {"read": 0}
    assert replication.request_class_p95_latencies == {}
    assert replication.request_class_latencies == {"read": ()}
    assert replication.request_class_arrival_times == {"read": ()}


def test_missing_mandatory_binary_fails_without_simpy_fallback(monkeypatch, tmp_path):
    backend = get_native_backend()
    monkeypatch.setattr(backend, "binary_path", tmp_path / "missing-simulator")
    system = make_single_service_system()

    with pytest.raises(NativeBackendError, match="mandatory C\\+\\+ simulator"):
        system.simulate(2.0, seed=1)



def test_adapter_rejects_a_success_response_with_an_invalid_schema(monkeypatch):
    response = (
        f"{{\"protocol_version\":{PROTOCOL_VERSION},"
        "\"operation\":\"simulate\",\"ok\":true,\"result\":{}}"
    )
    monkeypatch.setattr(
        "native_backend.backend.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=response,
            stderr="",
        ),
    )

    with pytest.raises(NativeBackendError, match="violates protocol schema"):
        make_single_service_system().simulate(2.0, seed=1)

def test_native_protocol_returns_a_structured_error():
    completed = subprocess.run(
        [str(BINARY)],
        input="{}",
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode != 0
    assert f'"protocol_version":{PROTOCOL_VERSION}' in completed.stdout
    assert '"ok":false' in completed.stdout
    assert '"code":"NATIVE_SIMULATION_ERROR"' in completed.stdout

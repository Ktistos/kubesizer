"""Statistical validation of the native kernel against queueing theory.

These tests deliberately compare independent implementations: ``System.solve``
provides the M/M/c analytical results, while the C++ kernel produces stochastic
replications.  Fixed random seeds make failures reproducible, but acceptance is
based on independent-replication uncertainty plus a small practical-error floor
so the suite does not rely on one lucky trajectory.
"""

import math
import statistics
from pathlib import Path

import pytest

from performance_models import QueueingNetworkType, System


BINARY = (
    Path(__file__).resolve().parents[1]
    / "native_backend"
    / "cpp"
    / "build-release"
    / "kubesizer-simulator"
)
REPLICATION_COUNT = 30


@pytest.fixture(scope="module", autouse=True)
def require_native_binary():
    assert BINARY.is_file(), (
        "Build the mandatory native backend before testing: "
        "native_backend/cpp/build.sh"
    )


def _replication_means(replications, request_class):
    means = []
    for replication in replications:
        observations = replication.request_class_latencies[request_class]
        assert observations, "Every validation replication needs observations."
        means.append(statistics.fmean(observations))
    return tuple(means)


def _assert_independent_mean_matches(
    replication_estimates,
    expected,
    *,
    label,
    relative_error_floor=0.03,
    standard_error_multiplier=4.0,
):
    """Require agreement within four SEs or a 3% practical-error floor.

    Complete replications are the independent units. The floor prevents a
    numerically tiny estimated SE from turning harmless Monte Carlo variation
    or floating-point effects into a flaky test.
    """
    observed = statistics.fmean(replication_estimates)
    standard_error = statistics.stdev(replication_estimates) / math.sqrt(
        len(replication_estimates)
    )
    tolerance = max(
        relative_error_floor * abs(expected),
        standard_error_multiplier * standard_error,
    )
    assert abs(observed - expected) <= tolerance, (
        f"{label}: observed={observed:.8g}, expected={expected:.8g}, "
        f"standard_error={standard_error:.8g}, tolerance={tolerance:.8g}"
    )


def _assert_late_state_matches(
    system,
    *,
    seeds,
    horizon,
    bucket_width=10.0,
):
    analytical = system.solve()
    expected_utilizations = analytical.get_mean_utilizations()
    expected_response_times = analytical.get_mean_latencies()
    effective_arrival_rates = system.get_effective_arrival_rates()
    ensemble = system.run_warmup_ensemble(
        seeds=seeds,
        horizon=horizon,
        bucket_width=bucket_width,
        workers=6,
    )
    services = tuple(
        sorted(system.get_services().values(), key=lambda item: item.get_id())
    )
    external_rate = sum(
        service.get_gamma(request_class)
        for service in services
        for request_class in system.get_request_classes()
    )
    simulated_time = len(seeds) * horizon
    arrival_throughput = ensemble.external_arrival_count / simulated_time
    completion_throughput = ensemble.completed_request_count / simulated_time
    throughput_standard_error = math.sqrt(external_rate / simulated_time)
    arrival_tolerance = max(0.02 * external_rate, 4.0 * throughput_standard_error)
    completion_tolerance = max(
        0.03 * external_rate,
        4.0 * throughput_standard_error,
    )
    assert abs(arrival_throughput - external_rate) <= arrival_tolerance
    assert abs(completion_throughput - external_rate) <= completion_tolerance

    capacities = tuple(system.get_system_replicas())
    # Analyze only the latter half so the empty initial state does not dilute
    # the utilization estimate. There are at least 30 late buckets per case.
    bucket_count = len(ensemble.active_requests)
    late_start = bucket_count // 2
    assert bucket_count - late_start >= 30
    for service_id, theoretical in enumerate(expected_utilizations):
        observed_busy = statistics.fmean(
            ensemble.busy_replicas_by_service[service_id][late_start:]
        )
        observed_queue = statistics.fmean(
            ensemble.queue_length_by_service[service_id][late_start:]
        )
        observed_utilization = observed_busy / capacities[service_id]
        tolerance = max(0.025, 0.05 * theoretical)
        assert abs(observed_utilization - theoretical) <= tolerance, (
            f"service {service_id} utilization: "
            f"observed={observed_utilization:.8g}, "
            f"expected={theoretical:.8g}, tolerance={tolerance:.8g}"
        )

        # M/M/c throughput conservation gives E[B]=lambda/mu. Little's law
        # gives E[N]=lambda*R, and therefore E[Q]=E[N]-E[B].
        arrival_rate = effective_arrival_rates[service_id]
        expected_busy = arrival_rate / services[service_id].get_mu()
        expected_number = arrival_rate * expected_response_times[service_id]
        expected_queue = expected_number - expected_busy
        observed_number = observed_queue + observed_busy
        number_tolerance = max(0.05, 0.08 * expected_number)
        queue_tolerance = max(0.05, 0.10 * expected_queue)
        assert abs(observed_number - expected_number) <= number_tolerance, (
            f"service {service_id} number-in-system: "
            f"observed={observed_number:.8g}, expected={expected_number:.8g}, "
            f"tolerance={number_tolerance:.8g}"
        )
        assert abs(observed_queue - expected_queue) <= queue_tolerance, (
            f"service {service_id} queue length: observed={observed_queue:.8g}, "
            f"expected={expected_queue:.8g}, tolerance={queue_tolerance:.8g}"
        )


def _analytical_class_mean_latency(system, request_class):
    """Combine visit ratios with analytical per-visit response times."""
    services = tuple(
        sorted(system.get_services().values(), key=lambda item: item.get_id())
    )
    external_rate = sum(
        service.get_gamma(request_class) for service in services
    )
    assert external_rate > 0
    effective_rates = system.get_request_class_effective_arrival_rates()[
        request_class
    ]
    service_response_times = system.solve().get_mean_latencies()
    return sum(
        (effective_rate / external_rate) * response_time
        for effective_rate, response_time in zip(
            effective_rates,
            service_response_times,
        )
    )


@pytest.mark.parametrize(
    (
        "utilization",
        "replicas",
        "warmup_duration",
        "measurement_duration",
        "utilization_horizon",
        "seed_start",
    ),
    (
        (0.25, 1, 50.0, 300.0, 600.0, 100_000),
        (0.50, 1, 75.0, 350.0, 700.0, 101_000),
        (0.75, 1, 150.0, 500.0, 1_000.0, 102_000),
        (0.90, 1, 300.0, 800.0, 1_600.0, 103_000),
        (0.95, 1, 600.0, 1_200.0, 4_800.0, 104_000),
        (0.60, 2, 100.0, 400.0, 800.0, 105_000),
        (0.85, 3, 200.0, 600.0, 1_200.0, 106_000),
    ),
    ids=(
        "mm1-rho-025",
        "mm1-rho-050",
        "mm1-rho-075",
        "mm1-rho-090",
        "mm1-rho-095",
        "mm2-rho-060",
        "mm3-rho-085",
    ),
)
def test_native_single_service_matches_mmc_analytical_model(
    utilization,
    replicas,
    warmup_duration,
    measurement_duration,
    utilization_horizon,
    seed_start,
):
    service_rate = 4.0
    arrival_rate = utilization * replicas * service_rate
    system = System(
        f"mm{replicas}-rho-{utilization}",
        {"request": {"service": {"gamma": arrival_rate, "mu": service_rate}}},
        QueueingNetworkType.OPEN,
    )
    system.configure_system_replicas((replicas,))
    analytical = system.solve()
    assert analytical.get_stability()
    assert analytical.get_mean_utilizations() == pytest.approx((utilization,))

    seeds = tuple(range(seed_start, seed_start + REPLICATION_COUNT))
    _assert_late_state_matches(
        system,
        seeds=seeds,
        horizon=utilization_horizon,
    )
    replications = system.simulate_measurement_replications(
        warmup_duration=warmup_duration,
        measurement_duration=measurement_duration,
        seeds=seeds,
        workers=6,
    )
    _assert_independent_mean_matches(
        _replication_means(replications, "request"),
        analytical.get_mean_latencies()[0],
        label=f"M/M/{replicas} mean response at rho={utilization}",
    )


def test_native_multiclass_feedforward_network_matches_analytical_model():
    system = System(
        "multiclass-feedforward",
        {
            "read": {
                "api": {"gamma": 3.0, "mu": 6.0, "routes": {"worker": 0.7}},
                "worker": {"gamma": 0.0, "mu": 4.0, "routes": {"db": 0.5}},
                "db": {"gamma": 0.0, "mu": 5.0},
            },
            "write": {
                "api": {"gamma": 2.0, "mu": 6.0, "routes": {"db": 0.9}},
                # This creates a second external entry stream for the class.
                "db": {"gamma": 0.5, "mu": 5.0},
            },
        },
        QueueingNetworkType.OPEN,
    )
    system.configure_system_replicas((2, 1, 1))
    analytical = system.solve()
    assert analytical.get_stability()

    seeds = tuple(range(110_000, 110_000 + REPLICATION_COUNT))
    _assert_late_state_matches(
        system,
        seeds=seeds,
        horizon=1_000.0,
    )
    replications = system.simulate_measurement_replications(
        warmup_duration=200.0,
        measurement_duration=600.0,
        seeds=seeds,
        workers=6,
    )
    for request_class in system.get_request_classes():
        _assert_independent_mean_matches(
            _replication_means(replications, request_class),
            _analytical_class_mean_latency(system, request_class),
            label=f"feed-forward class {request_class} end-to-end mean",
        )


def test_native_feedback_network_matches_analytical_visit_ratios():
    system = System(
        "feedback-network",
        {
            "interactive": {
                "front": {
                    "gamma": 2.0,
                    "mu": 5.0,
                    "routes": {"worker": 0.8},
                },
                "worker": {
                    "gamma": 0.0,
                    "mu": 4.0,
                    "routes": {"front": 0.25},
                },
            }
        },
        QueueingNetworkType.OPEN,
    )
    analytical = system.solve()
    assert analytical.get_stability()
    # Traffic equations give lambda_front=2.5 and lambda_worker=2.0.
    assert system.get_effective_arrival_rates() == pytest.approx((2.5, 2.0))

    seeds = tuple(range(120_000, 120_000 + REPLICATION_COUNT))
    _assert_late_state_matches(
        system,
        seeds=seeds,
        horizon=1_200.0,
    )
    replications = system.simulate_measurement_replications(
        warmup_duration=250.0,
        measurement_duration=800.0,
        seeds=seeds,
        workers=6,
    )
    _assert_independent_mean_matches(
        _replication_means(replications, "interactive"),
        _analytical_class_mean_latency(system, "interactive"),
        label="feedback-network end-to-end mean",
    )


def test_native_gamma_service_matches_exact_mg1_mean():
    workload = {
        "request_classes": {
            "request": {"concurrency_weight": 1.0, "routing": {}}
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "service": {
                        "distribution": "exponential",
                        "rate": 4.0,
                    }
                }
            },
            "service_times": {
                "service": {
                    "distribution": "gamma",
                    "mean": 0.1,
                    "scv": 2.0,
                }
            },
        },
    }
    system = System("mg1", workload, QueueingNetworkType.OPEN)
    analytical = system.solve()
    seeds = tuple(range(140_000, 140_000 + REPLICATION_COUNT))

    replications = system.simulate_measurement_replications(
        warmup_duration=100.0,
        measurement_duration=500.0,
        seeds=seeds,
        workers=6,
    )

    _assert_independent_mean_matches(
        _replication_means(replications, "request"),
        analytical.get_mean_latencies()[0],
        label="M/G/1 gamma-service mean response",
    )

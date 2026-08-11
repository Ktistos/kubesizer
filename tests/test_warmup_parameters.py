import math

import pytest

from calibration import select_warmup_pilot_parameters
from performance_models import QueueingNetworkType, System


def test_selects_documented_bucket_widths_and_horizon_without_mutating_system():
    workload = {
        "read": {
            "api": {
                "gamma": 9.0,
                "mu": 10.0,
                "routes": {"worker": 0.5},
            },
            "worker": {"gamma": 0.0, "mu": 5.0},
        },
        "write": {
            "api": {"gamma": 1.0, "mu": 10.0},
        },
    }
    system = System("test", workload, QueueingNetworkType.OPEN)
    system.configure_system_replicas([4, 3])
    original_configuration = system.get_system_replicas()

    parameters = select_warmup_pilot_parameters(system)

    assert parameters.time_unit == "seconds"
    assert parameters.service_names == ("api", "worker")
    assert parameters.request_class_names == ("read", "write")
    assert parameters.external_arrival_rates_by_class == (
        (9.0, 0.0),
        (1.0, 0.0),
    )
    assert parameters.routing_matrices_by_class == (
        ((0.0, 0.5), (0.0, 0.0)),
        ((0.0, 0.0), (0.0, 0.0)),
    )
    assert parameters.effective_arrival_rates_by_class == (
        (9.0, 4.5),
        (1.0, 0.0),
    )
    assert parameters.routing_spectral_radii == pytest.approx((0.0, 0.0))
    assert parameters.external_arrival_rates == pytest.approx((10.0, 0.0))
    assert parameters.effective_arrival_rates == pytest.approx((10.0, 4.5))
    assert parameters.service_rates == pytest.approx((10.0, 5.0))
    assert parameters.minimum_replica_configuration == (2, 1)
    assert parameters.minimum_configuration_utilizations == pytest.approx((0.5, 0.9))
    assert parameters.capacity_slacks == pytest.approx((10.0, 0.5))
    assert parameters.slowest_mean_service_time == pytest.approx(0.2)
    assert parameters.initial_bucket_width == pytest.approx(1.0)
    assert parameters.bucket_width_candidates == pytest.approx((0.5, 1.0, 2.0))
    assert parameters.slowest_congestion_scale == pytest.approx(2.0)
    assert parameters.total_external_arrival_rate == pytest.approx(10.0)
    assert parameters.raw_pilot_horizon == pytest.approx(500.0)
    assert parameters.pilot_horizon == pytest.approx(500.0)
    assert parameters.bucket_counts == (1000, 500, 250)
    assert parameters.replication_count == 30
    assert parameters.calibration_seeds == tuple(range(100_000, 100_030))
    assert system.get_system_replicas() == original_configuration


def test_capacity_slack_term_can_determine_and_round_pilot_horizon():
    system = System(
        "test",
        {"read": {"api": {"gamma": 9.99, "mu": 10.0}}},
        QueueingNetworkType.OPEN,
    )

    parameters = select_warmup_pilot_parameters(system)

    expected_raw_horizon = 100.0 / (10.0 - 9.99)
    assert parameters.slowest_congestion_scale == pytest.approx(100.0)
    assert parameters.raw_pilot_horizon == pytest.approx(expected_raw_horizon)
    assert parameters.pilot_horizon >= parameters.raw_pilot_horizon
    assert parameters.pilot_horizon % max(parameters.bucket_width_candidates) == pytest.approx(0.0)


def test_external_arrival_term_can_determine_pilot_horizon():
    system = System(
        "test",
        {"read": {"api": {"gamma": 0.01, "mu": 1.0}}},
        QueueingNetworkType.OPEN,
    )

    parameters = select_warmup_pilot_parameters(system)

    assert parameters.raw_pilot_horizon == pytest.approx(100_000.0)
    assert parameters.pilot_horizon == pytest.approx(100_000.0)


def test_custom_replication_metadata_is_recorded_reproducibly():
    system = System(
        "test",
        {"read": {"api": {"gamma": 1.0, "mu": 10.0}}},
        QueueingNetworkType.OPEN,
    )

    parameters = select_warmup_pilot_parameters(
        system,
        time_unit=" requests-time ",
        replication_count=3,
        seed_start=700,
    )

    assert parameters.time_unit == "requests-time"
    assert parameters.replication_count == 3
    assert parameters.calibration_seeds == (700, 701, 702)


@pytest.mark.parametrize(
    ("workload", "message"),
    [
        ({"read": {"api": {"gamma": 0.0, "mu": 10.0}}}, "positive external traffic"),
        ({"read": {"api": {"gamma": 1.0, "mu": 0.0}}}, "service rates"),
    ],
)
def test_invalid_workload_rates_are_rejected(workload, message):
    system = System("test", workload, QueueingNetworkType.OPEN)

    with pytest.raises(ValueError, match=message):
        select_warmup_pilot_parameters(system)


def test_closed_routing_class_without_exit_is_rejected():
    system = System(
        "test",
        {
            "read": {
                "front": {
                    "gamma": 1.0,
                    "mu": 10.0,
                    "routes": {"back": 1.0},
                },
                "back": {
                    "gamma": 0.0,
                    "mu": 10.0,
                    "routes": {"front": 1.0},
                },
            }
        },
        QueueingNetworkType.OPEN,
    )

    with pytest.raises(ValueError, match="traffic equations"):
        select_warmup_pilot_parameters(system)


def test_non_open_network_and_invalid_metadata_are_rejected():
    workload = {"read": {"api": {"gamma": 1.0, "mu": 10.0}}}
    closed_system = System("test", workload, QueueingNetworkType.CLOSED)
    open_system = System("test", workload, QueueingNetworkType.OPEN)

    with pytest.raises(ValueError, match="open queueing network"):
        select_warmup_pilot_parameters(closed_system)
    with pytest.raises(ValueError, match="time_unit"):
        select_warmup_pilot_parameters(open_system, time_unit=" ")
    with pytest.raises(ValueError, match="replication_count"):
        select_warmup_pilot_parameters(open_system, replication_count=0)
    with pytest.raises(ValueError, match="seed_start"):
        select_warmup_pilot_parameters(open_system, seed_start=-1)


def test_selected_bucket_counts_are_exact_for_all_candidates():
    system = System(
        "test",
        {"read": {"api": {"gamma": math.pi, "mu": 7.0}}},
        QueueingNetworkType.OPEN,
    )

    parameters = select_warmup_pilot_parameters(system)

    for bucket_width, bucket_count in zip(
        parameters.bucket_width_candidates,
        parameters.bucket_counts,
    ):
        assert bucket_count * bucket_width == pytest.approx(
            parameters.pilot_horizon
        )

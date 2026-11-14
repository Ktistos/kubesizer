import random
from dataclasses import replace

import pytest

from calibration import (
    average_warmup_replications,
    centered_moving_average,
    rebucket_warmup_replication,
    run_warmup_pilot_replication,
    run_warmup_pilot_replications,
    run_warmup_replications_for_configuration,
    select_warmup_pilot_parameters,
    slice_warmup_replication,
)
from metrics import SimulationMetricsCollector
from performance_models import QueueingNetworkType, System


def make_system_and_parameters(replication_count=3):
    system = System(
        "test",
        {"read": {"api": {"gamma": 2.0, "mu": 5.0}}},
        QueueingNetworkType.OPEN,
    )
    parameters = select_warmup_pilot_parameters(
        system,
        replication_count=replication_count,
        seed_start=900,
    )
    parameters = replace(
        parameters,
        raw_pilot_horizon=10.0,
        pilot_horizon=10.0,
        bucket_counts=(20, 10, 5),
    )
    return system, parameters


def test_system_accepts_caller_owned_collector():
    system, _ = make_system_and_parameters()
    collector = SimulationMetricsCollector(1)
    random.seed(42)

    system.simulate(5.0, metrics_collector=collector)

    assert len(collector.get_active_history()) > 1
    assert len(collector.get_busy_history()[0]) > 1


def test_single_replication_is_reproducible_and_restores_external_state():
    system, parameters = make_system_and_parameters()
    system.configure_system_replicas([3])
    original_random_state = random.getstate()

    first = run_warmup_pilot_replication(system, parameters, seed=1234)
    second = run_warmup_pilot_replication(system, parameters, seed=1234)

    assert first == second
    assert first.replica_configuration == (1,)
    assert first.pilot_horizon == 10.0
    assert first.bucket_width == 1.0
    assert first.bucket_count == 10
    assert system.get_system_replicas() == [3]
    assert random.getstate() == original_random_state


def test_different_seeds_produce_different_state_paths():
    system, parameters = make_system_and_parameters()

    first = run_warmup_pilot_replication(system, parameters, seed=100)
    second = run_warmup_pilot_replication(system, parameters, seed=101)

    assert first.active_history != second.active_history


def test_rebucketing_uses_histories_and_derives_number_and_utilization():
    system, parameters = make_system_and_parameters()
    replication = run_warmup_pilot_replication(system, parameters, seed=222)

    rebucketed = rebucket_warmup_replication(replication, 0.5)

    assert rebucketed.bucket_count == 20
    for queue, busy, number, utilization in zip(
        rebucketed.bucketed_queue_length_by_service[0],
        rebucketed.bucketed_busy_replicas_by_service[0],
        rebucketed.bucketed_number_in_system_by_service[0],
        rebucketed.bucketed_utilization_by_service[0],
    ):
        assert number == pytest.approx(queue + busy)
        assert utilization == pytest.approx(busy)


def test_long_replication_prefix_matches_a_direct_short_run():
    system, short_parameters = make_system_and_parameters()
    long_parameters = replace(
        short_parameters,
        raw_pilot_horizon=20.0,
        pilot_horizon=20.0,
        bucket_counts=(40, 20, 10),
    )

    direct = run_warmup_pilot_replication(
        system,
        short_parameters,
        seed=333,
    )
    long = run_warmup_pilot_replication(
        system,
        long_parameters,
        seed=333,
    )
    prefix = slice_warmup_replication(long, 10.0)

    assert prefix.pilot_horizon == 10.0
    assert prefix.active_history == direct.active_history
    assert prefix.queue_history_by_service == direct.queue_history_by_service
    assert prefix.busy_history_by_service == direct.busy_history_by_service
    assert prefix.bucketed_active_requests == direct.bucketed_active_requests
    assert (
        prefix.bucketed_queue_length_by_service
        == direct.bucketed_queue_length_by_service
    )
    assert (
        prefix.bucketed_busy_replicas_by_service
        == direct.bucketed_busy_replicas_by_service
    )


def test_replication_prefix_cannot_exceed_simulated_horizon():
    system, parameters = make_system_and_parameters()
    replication = run_warmup_pilot_replication(system, parameters, seed=334)

    with pytest.raises(ValueError, match="cannot exceed"):
        slice_warmup_replication(replication, 11.0)


def test_replication_batch_and_ensemble_have_reproducible_equal_shapes():
    system, parameters = make_system_and_parameters(replication_count=3)

    replications = run_warmup_pilot_replications(system, parameters)
    ensemble = average_warmup_replications(replications)

    assert tuple(replication.seed for replication in replications) == (900, 901, 902)
    assert all(replication.bucket_count == 10 for replication in replications)
    assert ensemble.seeds == (900, 901, 902)
    assert ensemble.bucket_count == 10
    assert ensemble.active_requests[0] == pytest.approx(
        sum(replication.bucketed_active_requests[0] for replication in replications)
        / 3
    )
    assert set(ensemble.important_metric_series()) == {
        "active_requests",
        "queue_length[api]",
        "number_in_system[api]",
        "utilization[api]",
    }


def test_explicit_configuration_batch_uses_its_seeds_and_restores_system():
    system, parameters = make_system_and_parameters(replication_count=2)
    system.configure_system_replicas([4])

    replications = run_warmup_replications_for_configuration(
        system,
        parameters,
        (2,),
        (7000, 7001),
        replication_id_start=10,
    )

    assert tuple(replication.replication_id for replication in replications) == (10, 11)
    assert tuple(replication.seed for replication in replications) == (7000, 7001)
    assert all(replication.replica_configuration == (2,) for replication in replications)
    assert system.get_system_replicas() == [4]


def test_process_workers_match_sequential_results_and_preserve_order():
    system, parameters = make_system_and_parameters(replication_count=3)
    system.configure_system_replicas([4])

    sequential = run_warmup_pilot_replications(system, parameters, workers=1)
    parallel = run_warmup_pilot_replications(system, parameters, workers=2)

    assert parallel == sequential
    assert tuple(replication.seed for replication in parallel) == (900, 901, 902)
    assert system.get_system_replicas() == [4]


def test_replication_workers_must_be_positive():
    system, parameters = make_system_and_parameters()

    with pytest.raises(ValueError, match="workers"):
        run_warmup_pilot_replications(system, parameters, workers=0)


def test_centered_welch_smoothing_handles_edges_and_validates_width():
    assert centered_moving_average((1.0, 2.0, 9.0, 4.0), 1) == pytest.approx(
        (1.5, 4.0, 5.0, 6.5)
    )
    assert centered_moving_average((), 2) == ()
    with pytest.raises(ValueError, match="half_window"):
        centered_moving_average((1.0,), -1)

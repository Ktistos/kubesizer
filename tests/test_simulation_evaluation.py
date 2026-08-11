from types import SimpleNamespace

import pytest

from calibration import CalibrationOptions, build_calibration_profile
from optimization import (
    AStarOptimizer,
    CalibratedConfigurationEvaluator,
    CalibratedReplicaSizingProblem,
    SimulationClassification,
    StatisticalEvaluationError,
)
from performance_models import QueueingNetworkType, System


SLOS = {"checkout": 1.0}


def make_system(gamma=1.0, mu=10.0):
    return System(
        "sequential-evaluation",
        {"checkout": {"api": {"gamma": gamma, "mu": mu}}},
        QueueingNetworkType.OPEN,
    )


def make_options():
    return CalibrationOptions(
        warmup_replications=2,
        warmup_validation_replications=2,
        measurement_calibration_replications=2,
        target_samples_per_class=2,
        minimum_eligible_samples_per_class=4,
        minimum_nonempty_replications_per_class=2,
        minimum_optimization_replications=2,
        maximum_optimization_replications=4,
        replication_batch_size=2,
        final_validation_replications=2,
        bootstrap_resample_count=1_000,
        minimum_bootstrap_tail_draws=5,
    )


def make_profile(system, maximum=None):
    minimum = tuple(system.get_minimum_stable_replicas())
    maximum = maximum or tuple(value + 1 for value in minimum)
    return build_calibration_profile(
        system,
        maximum_configuration=maximum,
        request_class_slos=SLOS,
        warmup_duration=5.0,
        measurement_duration=10.0,
        measurement_duration_candidates=(10.0, 20.0),
        warmup_bucket_width=1.0,
        representative_configurations=(minimum, maximum),
        calibration_diagnostics={"test": True},
        options=make_options(),
        creation_timestamp="2026-08-11T00:00:00+00:00",
    )


class FakeSimulationSystem:
    def __init__(self, system, latency_for_seed):
        self.system = system
        self.latency_for_seed = latency_for_seed
        self.calls = []

    def __getattr__(self, name):
        return getattr(self.system, name)

    def simulate_measurement_replications(
        self,
        *,
        warmup_duration,
        measurement_duration,
        seeds,
        workers,
        maximum_drain_time,
    ):
        configuration = tuple(self.get_system_replicas())
        seeds = tuple(seeds)
        self.calls.append((configuration, seeds))
        return tuple(
            SimpleNamespace(
                seed=seed,
                request_class_latencies={
                    "checkout": tuple(
                        self.latency_for_seed(seed, configuration)
                    )
                },
                processed_event_count=20,
                external_arrival_count=10,
                completed_request_count=9,
                stop_time=warmup_duration + measurement_duration + 1.0,
            )
            for seed in seeds
        )


def evaluator(latency_for_seed):
    system = make_system()
    profile = make_profile(system)
    wrapped = FakeSimulationSystem(system, latency_for_seed)
    return wrapped, profile, CalibratedConfigurationEvaluator(wrapped, profile)


def test_all_upper_bounds_below_slo_is_feasible_at_rmin():
    system, profile, subject = evaluator(lambda seed, configuration: (0.5, 0.5))

    result = subject.evaluate((1,))

    assert result.classification == SimulationClassification.STATISTICALLY_FEASIBLE
    assert result.feasible
    assert result.replications_used == 2
    assert result.look_count == 1
    assert result.intervals["checkout"].upper_bound == pytest.approx(0.5)
    assert result.sequential_tail_alpha == pytest.approx(0.05 / (2 * 2 * 1))
    assert result.seeds_used == profile.optimization_seeds(2)
    assert len(system.calls) == 1


def test_one_lower_bound_above_slo_is_infeasible():
    _, _, subject = evaluator(lambda seed, configuration: (1.5, 1.5))

    result = subject.evaluate((1,))

    assert result.classification == SimulationClassification.STATISTICALLY_INFEASIBLE
    assert result.intervals["checkout"].lower_bound == pytest.approx(1.5)


def test_overlapping_interval_adds_batches_then_is_inconclusive_at_rmax():
    def mixed(seed, configuration):
        return (0.5, 0.5) if seed % 2 == 0 else (1.5, 1.5)

    system, profile, subject = evaluator(mixed)

    result = subject.evaluate((1,))

    assert result.classification == SimulationClassification.INCONCLUSIVE
    assert result.replications_used == 4
    assert result.look_count == 2
    assert tuple(look.classification for look in result.looks) == (
        SimulationClassification.CONTINUE_SAMPLING,
        SimulationClassification.INCONCLUSIVE,
    )
    assert tuple(len(call[1]) for call in system.calls) == (2, 2)
    assert result.seeds_used == profile.optimization_seeds(4)


def test_insufficient_request_class_data_fails_at_rmax():
    _, _, subject = evaluator(lambda seed, configuration: ())

    result = subject.evaluate((1,))

    assert result.classification == SimulationClassification.SIMULATION_FAILURE
    assert result.replications_used == 4
    assert "Minimum eligible" in result.failure_reason


def test_analytically_unstable_configuration_is_not_simulated():
    system = make_system(gamma=10.0, mu=5.0)
    profile = make_profile(system, maximum=(3,))
    wrapped = FakeSimulationSystem(system, lambda seed, configuration: (0.5,))

    result = CalibratedConfigurationEvaluator(wrapped, profile).evaluate((1,))

    assert result.classification == SimulationClassification.ANALYTICALLY_UNSTABLE
    assert wrapped.calls == []


def test_common_random_numbers_reuse_same_replication_seeds_across_configurations():
    system, profile, subject = evaluator(lambda seed, configuration: (0.5, 0.5))

    subject.evaluate((1,))
    subject.evaluate((2,))

    assert system.calls[0][1] == system.calls[1][1] == profile.optimization_seeds(2)


def test_changed_workload_returns_calibration_failure():
    source = make_system()
    profile = make_profile(source)
    changed = FakeSimulationSystem(
        make_system(gamma=2.0),
        lambda seed, configuration: (0.5,),
    )

    result = CalibratedConfigurationEvaluator(changed, profile).evaluate((1,))

    assert result.classification == SimulationClassification.CALIBRATION_FAILURE
    assert "workload" in result.failure_reason


def test_optimizer_accepts_only_statistically_feasible_goal_and_retains_inconclusive():
    system = make_system()
    profile = make_profile(system)

    def by_configuration(seed, configuration):
        if configuration == (1,):
            return (0.5, 0.5) if seed % 2 == 0 else (1.5, 1.5)
        return (0.5, 0.5)

    wrapped = FakeSimulationSystem(system, by_configuration)
    problem = CalibratedReplicaSizingProblem(
        wrapped,
        profile,
        heuristic=lambda _configuration: 0,
    )

    result = AStarOptimizer().optimize(problem)

    assert result.configuration == (2,)
    assert result.metrics.classification == (
        SimulationClassification.STATISTICALLY_FEASIBLE
    )
    assert problem.configuration_evaluations[(1,)].classification == (
        SimulationClassification.INCONCLUSIVE
    )
    assert not problem.configuration_evaluations[(1,)].feasible


def test_optimizer_aborts_instead_of_treating_simulation_failure_as_infeasible():
    system = make_system()
    profile = make_profile(system)

    class FailedEvaluator:
        def evaluate(self, configuration):
            return SimpleNamespace(
                classification=SimulationClassification.SIMULATION_FAILURE,
                intervals={},
                failure_reason="cohort could not be drained",
            )

    problem = CalibratedReplicaSizingProblem(
        system,
        profile,
        heuristic=lambda _configuration: 0,
        evaluator=FailedEvaluator(),
    )

    with pytest.raises(StatisticalEvaluationError, match="could not be drained") as error:
        AStarOptimizer().optimize(problem)

    assert error.value.evaluation.classification == (
        SimulationClassification.SIMULATION_FAILURE
    )

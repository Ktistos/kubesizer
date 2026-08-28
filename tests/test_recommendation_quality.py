import pytest

from characterization import (
    ClosedLoopExperimentOptions,
    ObservedConfigurationStatus,
    ObservedExperimentReplication,
    RecommendationQualityStatus,
    evaluate_closed_loop_recommendation,
    run_closed_loop_experiments,
)


def _replications(configuration, latency, *, count=5, failures=None):
    return tuple(
        ObservedExperimentReplication.from_mappings(
            configuration,
            seed=1000 + index,
            run_id=f"{configuration}-{index}",
            request_class_latencies={"request": (latency,) * 100},
            request_class_failures=(
                None if failures is None else {"request": (failures,) * 100}
            ),
        )
        for index in range(count)
    )


def _options(**overrides):
    values = {
        "bootstrap_resample_count": 2_000,
        "minimum_replications": 5,
        "minimum_nonempty_replications_per_class": 5,
        "minimum_eligible_samples_per_class": 500,
    }
    values.update(overrides)
    return ClosedLoopExperimentOptions(**values)


def test_closed_loop_finds_and_certifies_minimum_in_declared_test_space():
    observations = (
        *_replications((1,), 0.20),
        *_replications((2,), 0.10),
        *_replications((3,), 0.08),
    )

    result = evaluate_closed_loop_recommendation(
        (2,),
        observations,
        {"request": 0.15},
        declared_configuration_space=((1,), (2,), (3,)),
        predicted_request_class_statistics={(2,): {"request": 0.10}},
        options=_options(),
    )

    assert result.status is RecommendationQualityStatus.MATCHES_EMPIRICAL_MINIMUM
    assert result.safe_in_observed_experiment
    assert result.empirical_minimum_configuration == (2,)
    assert result.minimum_within_declared_space_certified
    assert result.absolute_cost_gap == 0.0
    assert result.configuration_evidence[0].status is (
        ObservedConfigurationStatus.INFEASIBLE
    )
    assert result.prediction_agreement[0].prediction_inside_observed_interval


def test_safe_but_larger_recommendation_is_reported_as_conservative():
    observations = (
        *_replications((1,), 0.20),
        *_replications((2,), 0.10),
        *_replications((3,), 0.08),
    )

    result = evaluate_closed_loop_recommendation(
        (3,),
        observations,
        {"request": 0.15},
        replica_costs=(2.0,),
        options=_options(),
    )

    assert result.status is RecommendationQualityStatus.CONSERVATIVE
    assert result.absolute_cost_gap == pytest.approx(2.0)
    assert result.relative_cost_gap == pytest.approx(0.5)
    assert not result.minimum_within_declared_space_certified


def test_observed_slo_failure_reports_underprovisioning():
    result = evaluate_closed_loop_recommendation(
        (1,),
        _replications((1,), 0.20),
        {"request": 0.15},
        options=_options(),
    )

    assert result.status is RecommendationQualityStatus.UNDERPROVISIONED
    assert not result.safe_in_observed_experiment
    assert result.absolute_cost_gap is None
    assert result.relative_cost_gap is None


def test_missing_independent_runs_stays_insufficient_not_feasible():
    result = evaluate_closed_loop_recommendation(
        (1,),
        _replications((1,), 0.10, count=2),
        {"request": 0.15},
        options=_options(),
    )

    assert result.status is RecommendationQualityStatus.UNRESOLVED
    assert result.recommendation_evidence.status is (
        ObservedConfigurationStatus.INSUFFICIENT_EVIDENCE
    )


def test_censored_lower_bounds_can_prove_infeasibility_but_not_feasibility():
    high = tuple(
        ObservedExperimentReplication.from_mappings(
            (1,),
            seed=2000 + index,
            run_id=f"high-{index}",
            request_class_latencies={"request": (0.30,) * 100},
            request_class_censored={"request": (True,) * 100},
        )
        for index in range(5)
    )
    low = tuple(
        ObservedExperimentReplication.from_mappings(
            (2,),
            seed=3000 + index,
            run_id=f"low-{index}",
            request_class_latencies={"request": (0.05,) * 100},
            request_class_censored={"request": (True,) * 100},
        )
        for index in range(5)
    )

    result = evaluate_closed_loop_recommendation(
        (2,),
        (*high, *low),
        {"request": 0.15},
        options=_options(),
    )

    by_configuration = {
        item.configuration: item for item in result.configuration_evidence
    }
    assert by_configuration[(1,)].status is ObservedConfigurationStatus.INFEASIBLE
    assert by_configuration[(2,)].status is ObservedConfigurationStatus.INCONCLUSIVE
    assert by_configuration[(1,)].censored_observation_counts == (("request", 500),)


def test_declared_space_cannot_hide_an_observed_configuration():
    observations = (
        *_replications((1,), 0.20),
        *_replications((2,), 0.10),
    )

    with pytest.raises(ValueError, match="outside declared_configuration_space"):
        evaluate_closed_loop_recommendation(
            (1,),
            observations,
            {"request": 0.15},
            declared_configuration_space=((1,),),
            options=_options(),
        )


def test_unseen_failures_are_not_certified_by_an_all_zero_bootstrap():
    passing = _replications((1,), 0.10, failures=False)
    result = evaluate_closed_loop_recommendation(
        (1,),
        passing,
        {"request": 0.15},
        failure_rate_slos={"request": 0.01},
        options=_options(bootstrap_resample_count=4_000),
    )

    assert result.status is RecommendationQualityStatus.UNRESOLVED
    assert result.recommendation_evidence.failure_rate_intervals[0][1].upper_bound > 0.0


class _Adapter:
    def __init__(self, *, fail=False):
        self.configuration = (9,)
        self.restored = None
        self.applied = []
        self.fail = fail

    def snapshot_configuration(self):
        return self.configuration

    def apply_configuration(self, configuration):
        self.configuration = configuration
        self.applied.append(configuration)

    def run_replication(self, configuration, seed):
        if self.fail:
            raise RuntimeError("load generator failed")
        return ObservedExperimentReplication.from_mappings(
            configuration,
            seed,
            f"{configuration}-{seed}",
            {"request": (0.1,)},
        )

    def restore_configuration(self, snapshot):
        self.configuration = snapshot
        self.restored = snapshot


def test_experiment_runner_applies_sequentially_and_restores_original_state():
    adapter = _Adapter()
    results = run_closed_loop_experiments(((1,), (2,)), (10, 11), adapter)

    assert len(results) == 4
    assert adapter.applied == [(1,), (2,)]
    assert adapter.configuration == (9,)
    assert adapter.restored == (9,)


def test_experiment_runner_restores_state_after_a_failed_load_test():
    adapter = _Adapter(fail=True)

    with pytest.raises(RuntimeError, match="load generator failed"):
        run_closed_loop_experiments(((1,),), (10,), adapter)

    assert adapter.configuration == (9,)
    assert adapter.restored == (9,)

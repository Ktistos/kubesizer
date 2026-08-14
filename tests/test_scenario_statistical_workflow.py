from calibration import CalibrationOptions, build_calibration_profile
from optimization import FinalValidationClassification, SimulationClassification
from performance_models import QueueingNetworkType, ScenarioSystem
from statistical_workflow import evaluate_configuration, validate_final_configuration
import statistical_workflow


def _two_scenario_workload(slow_mean=1.0):
    return {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {"request": {"routing": {}}},
        "scenarios": {
            "fast": {
                "entry_services": {
                    "request": {
                        "service": {
                            "distribution": "deterministic",
                            "rate": 0.2,
                        }
                    }
                },
                "service_times": {
                    "service": {
                        "distribution": "deterministic",
                        "mean": 0.1,
                    }
                },
            },
            "slow": {
                "entry_services": {
                    "request": {
                        "service": {
                            "distribution": "deterministic",
                            "rate": 0.2,
                        }
                    }
                },
                "service_times": {
                    "service": {
                        "distribution": "deterministic",
                        "mean": slow_mean,
                    }
                },
            },
        },
    }


def _profile(system, slo):
    options = CalibrationOptions(
        warmup_replications=2,
        warmup_validation_replications=2,
        measurement_calibration_replications=2,
        target_samples_per_class=2,
        minimum_eligible_samples_per_class=2,
        minimum_nonempty_replications_per_class=2,
        minimum_optimization_replications=2,
        maximum_optimization_replications=2,
        replication_batch_size=1,
        final_validation_replications=2,
        bootstrap_resample_count=1_000,
    )
    configuration = tuple(system.get_minimum_stable_replicas())
    return build_calibration_profile(
        system,
        maximum_configuration=configuration,
        request_class_slos={"request": slo},
        warmup_duration=0.0,
        measurement_duration=20.0,
        measurement_duration_candidates=(20.0, 40.0),
        warmup_bucket_width=0.5,
        representative_configurations=(configuration,),
        calibration_diagnostics={"test": True},
        options=options,
        creation_timestamp="2026-08-14T00:00:00+00:00",
    )


def test_configuration_requires_every_scenario_to_pass():
    system = ScenarioSystem(
        "robust",
        _two_scenario_workload(),
        QueueingNetworkType.OPEN,
    )
    configuration = tuple(system.get_minimum_stable_replicas())
    profile = _profile(system, slo=0.5)

    result = evaluate_configuration(system, configuration, profile)

    assert result.classification == SimulationClassification.STATISTICALLY_INFEASIBLE
    assert set(result.intervals) == {"fast/request", "slow/request"}
    assert result.intervals["fast/request"].upper_bound <= 0.5
    assert result.intervals["slow/request"].lower_bound > 0.5
    assert result.sequential_tail_alpha == 0.05 / (2 * 1 * 1 * 2)


def test_final_validation_fails_when_any_scenario_fails():
    system = ScenarioSystem(
        "robust",
        _two_scenario_workload(),
        QueueingNetworkType.OPEN,
    )
    configuration = tuple(system.get_minimum_stable_replicas())
    profile = _profile(system, slo=0.5)

    result = validate_final_configuration(system, configuration, profile)

    assert result.classification == (
        FinalValidationClassification.FAILED_SLO_VALIDATION
    )
    assert not result.passed
    assert set(result.intervals) == {"fast/request", "slow/request"}


def test_distribution_change_invalidates_multi_scenario_profile():
    original = ScenarioSystem(
        "robust",
        _two_scenario_workload(),
        QueueingNetworkType.OPEN,
    )
    changed = ScenarioSystem(
        "robust",
        _two_scenario_workload(slow_mean=0.9),
        QueueingNetworkType.OPEN,
    )
    profile = _profile(original, slo=2.0)
    configuration = tuple(changed.get_minimum_stable_replicas())

    result = evaluate_configuration(changed, configuration, profile)

    assert result.classification == SimulationClassification.CALIBRATION_FAILURE
    assert "workload" in result.failure_reason


def test_multi_scenario_calibration_uses_conservative_global_values(
    monkeypatch,
):
    system = ScenarioSystem(
        "robust",
        _two_scenario_workload(),
        QueueingNetworkType.OPEN,
    )
    options = CalibrationOptions(
        warmup_replications=2,
        warmup_validation_replications=2,
        measurement_calibration_replications=2,
        target_samples_per_class=2,
        minimum_eligible_samples_per_class=2,
        minimum_nonempty_replications_per_class=2,
        minimum_optimization_replications=2,
        maximum_optimization_replications=2,
        replication_batch_size=1,
        final_validation_replications=2,
        bootstrap_resample_count=1_000,
    )
    observed_seed_sets = []

    def fake_calibrate_one(scenario_system, **arguments):
        scenario_options = arguments["options"]
        scenario_name = scenario_system.get_scenario_name()
        seeds = scenario_options.seed_policy.seeds("warmup_calibration", 2)
        observed_seed_sets.append(set(seeds))
        warmup = 1.0 if scenario_name == "fast" else 2.0
        measurement = 10.0 if scenario_name == "fast" else 20.0
        return build_calibration_profile(
            scenario_system,
            maximum_configuration=(1,),
            request_class_slos={"request": 2.0},
            warmup_duration=warmup,
            measurement_duration=measurement,
            measurement_duration_candidates=(10.0, 20.0),
            warmup_bucket_width=0.5,
            representative_configurations=((1,),),
            calibration_diagnostics={"scenario": scenario_name},
            options=scenario_options,
            calibration_seeds=seeds,
            creation_timestamp="2026-08-14T00:00:00+00:00",
        )

    monkeypatch.setattr(statistical_workflow, "_calibrate_one", fake_calibrate_one)

    profile = statistical_workflow.calibrate(
        system,
        request_class_slos={"request": 2.0},
        maximum_replicas=(1,),
        options=options,
    )

    assert profile.required_scenarios == ("fast", "slow")
    assert profile.warmup_duration == 2.0
    assert profile.measurement_duration == 20.0
    assert observed_seed_sets[0].isdisjoint(observed_seed_sets[1])
    assert set(profile.calibration_seeds) == set.union(*observed_seed_sets)

import pytest

from optimization import (
    AnalyticalClassification,
    AnalyticalReplicaSizingProblem,
    OptimizationMode,
    ReplicaEvaluationMode,
    ScalableReplicaSearchOptions,
    normalize_optimization_mode,
)
from performance_models import QueueingNetworkType, ScenarioSystem, System
from statistical_workflow import (
    optimize_replica_sizing,
    optimize_with_analytical_model,
)


def _single_service_system():
    return System(
        "analytical",
        {"request": {"api": {"gamma": 4.0, "mu": 10.0}}},
        QueueingNetworkType.OPEN,
    )


def _scenario_workload():
    return {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {"request": {"routing": {}}},
        "scenarios": {
            "light": {
                "entry_services": {
                    "request": {
                        "api": {"distribution": "exponential", "rate": 2.0}
                    }
                },
                "service_times": {
                    "api": {"distribution": "exponential", "rate": 10.0}
                },
            },
            "heavy": {
                "entry_services": {
                    "request": {
                        "api": {"distribution": "exponential", "rate": 8.0}
                    }
                },
                "service_times": {
                    "api": {"distribution": "exponential", "rate": 10.0}
                },
            },
        },
    }


def test_request_class_mean_latency_accounts_for_internal_visits():
    system = System(
        "visits",
        {
            "read": {
                "api": {
                    "gamma": 2.0,
                    "mu": 10.0,
                    "routes": {"worker": 0.5},
                },
                "worker": {"gamma": 0.0, "mu": 5.0},
            },
            "write": {
                "api": {
                    "gamma": 3.0,
                    "mu": 10.0,
                    "routes": {"worker": 0.2},
                }
            },
        },
        QueueingNetworkType.OPEN,
    )

    means = system.get_request_class_mean_latencies()

    assert means["read"] == pytest.approx(0.2 + 0.5 / 3.4)
    assert means["write"] == pytest.approx(0.2 + 0.2 / 3.4)


def test_exact_analytical_search_finds_minimum_model_feasible_cost(monkeypatch):
    system = _single_service_system()

    def fail_if_simulated(*_args, **_kwargs):
        raise AssertionError("analytical optimization must not simulate")

    monkeypatch.setattr(
        system,
        "simulate_measurement_replications",
        fail_if_simulated,
    )

    result = optimize_with_analytical_model(
        system,
        request_class_mean_latency_limits={"request": 0.12},
        maximum_replicas=3,
        mode=OptimizationMode.EXACT,
    )

    assert result.configuration == (2,)
    assert result.objective_value == 2.0
    assert result.cost == 1.0
    assert result.global_optimal
    assert result.simulation_replications == 0
    assert (
        result.metrics.classification
        == AnalyticalClassification.ANALYTICALLY_FEASIBLE
    )
    assert result.metrics.evaluation_basis == "ANALYTICAL_MEAN_LATENCY"


def test_scalable_analytical_search_is_reproducible_and_not_globally_certified():
    options = ScalableReplicaSearchOptions(
        maximum_configuration_evaluations=8,
        maximum_restarts=2,
        maximum_no_improvement=1,
        random_seed=42,
    )

    first = optimize_with_analytical_model(
        _single_service_system(),
        request_class_mean_latency_limits={"request": 0.12},
        maximum_replicas=3,
        mode="approximate",
        scalable_options=options,
    )
    second = optimize_with_analytical_model(
        _single_service_system(),
        request_class_mean_latency_limits={"request": 0.12},
        maximum_replicas=3,
        mode=OptimizationMode.SCALABLE,
        scalable_options=options,
    )

    assert first.configuration == second.configuration == (2,)
    assert first.random_seed == second.random_seed == 42
    assert first.simulation_replications == second.simulation_replications == 0
    assert not first.global_optimal
    assert first.solution_mode == "SCALABLE"
    assert normalize_optimization_mode("approximate") == OptimizationMode.SCALABLE


def test_analytical_evaluation_requires_every_scenario_to_pass():
    system = ScenarioSystem(
        "scenarios",
        _scenario_workload(),
        QueueingNetworkType.OPEN,
    )
    configuration = tuple(system.get_minimum_stable_replicas())
    system.configure_system_replicas(configuration)
    scenario_means = system.get_request_class_mean_latencies_by_scenario()
    light = scenario_means["light"]["request"]
    heavy = scenario_means["heavy"]["request"]
    assert heavy > light

    problem = AnalyticalReplicaSizingProblem(
        system,
        {"request": (light + heavy) / 2.0},
        configuration,
    )
    evaluation = problem.evaluate(configuration)

    assert not evaluation.feasible
    assert (
        evaluation.classification
        == AnalyticalClassification.ANALYTICALLY_INFEASIBLE
    )
    assert set(evaluation.scenario_means) == {"light", "heavy"}


def test_public_dispatcher_keeps_evaluation_and_search_choices_independent():
    result = optimize_replica_sizing(
        _single_service_system(),
        evaluation_mode=ReplicaEvaluationMode.ANALYTICAL,
        mode=OptimizationMode.EXACT,
        request_class_mean_latency_limits={"request": 0.12},
        maximum_replicas=3,
    )

    assert result.configuration == (2,)
    assert result.solution_mode == "EXACT"
    assert result.simulation_replications == 0

    with pytest.raises(ValueError, match="calibration profile"):
        optimize_replica_sizing(
            _single_service_system(),
            evaluation_mode=ReplicaEvaluationMode.SIMULATION,
            mode=OptimizationMode.EXACT,
        )

    with pytest.raises(ValueError, match="does not consume"):
        optimize_replica_sizing(
            _single_service_system(),
            evaluation_mode=ReplicaEvaluationMode.ANALYTICAL,
            mode=OptimizationMode.EXACT,
            calibration_profile=object(),
            request_class_mean_latency_limits={"request": 0.12},
            maximum_replicas=3,
        )

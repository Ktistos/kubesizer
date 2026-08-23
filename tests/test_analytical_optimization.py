import pytest

from optimization import (
    AnalyticalClassification,
    OptimizationMode,
    ReplicaEvaluationMode,
    ScalableReplicaSearchOptions,
    normalize_optimization_mode,
)
from performance_models import QueueingNetworkType, System
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


def _structured_single_service_system():
    return System(
        "structured-analytical",
        {
            "request_classes": {
                "request": {
                    "concurrency_weight": 1.0,
                    "end_to_end_latency_slos": {
                        "mean_seconds": 0.12,
                    },
                    "routing": {},
                }
            },
            "traffic_model": {
                "entry_services": {
                    "request": {
                        "api": {
                            "distribution": "exponential",
                            "rate": 4.0,
                        }
                    }
                },
                "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
            },
        },
        QueueingNetworkType.OPEN,
    )


def _structured_percentile_system():
    workload = _structured_single_service_system().get_workload_spec()
    workload["request_classes"]["request"]["end_to_end_latency_slos"] = {
        "percentile": {"quantile": 0.95, "maximum_seconds": 0.5}
    }
    return System(
        "structured-percentile",
        workload,
        QueueingNetworkType.OPEN,
    )


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
        result.metrics.classification == AnalyticalClassification.ANALYTICALLY_FEASIBLE
    )
    assert result.metrics.evaluation_basis == "ANALYTICAL_MEAN_LATENCY"


def test_analytical_search_reports_bounded_progress_without_simulation():
    events = []

    optimize_with_analytical_model(
        _single_service_system(),
        request_class_mean_latency_limits={"request": 0.12},
        maximum_replicas=3,
        mode=OptimizationMode.EXACT,
        progress_callback=lambda event, details: events.append((event, details)),
    )

    assert events[0] == (
        "replica_optimization_started",
        {
            "selected_mode": "EXACT",
            "completed_configurations": 0,
            "total_configurations": 3,
            "search_space_configurations": 3,
        },
    )
    evaluated = [
        details
        for event, details in events
        if event == "replica_configuration_evaluated"
    ]
    assert [item["completed_configurations"] for item in evaluated] == [1, 2]
    assert events[-1][0] == "replica_optimization_completed"


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


def test_analytical_search_defaults_to_workload_mean_slo_not_percentile_slo():
    result = optimize_with_analytical_model(
        _structured_single_service_system(),
        maximum_replicas=3,
        mode=OptimizationMode.EXACT,
    )

    # One replica has mean latency 1 / (10 - 4) = 0.1667, so it fails the
    # declared analytical mean threshold of 0.12.
    assert result.configuration == (2,)
    assert result.metrics.limits == {"request": 0.12}


def test_percentile_workload_cannot_use_strict_analytical_optimization():
    with pytest.raises(ValueError, match="require simulation"):
        optimize_with_analytical_model(
            _structured_percentile_system(),
            request_class_mean_latency_limits={"request": 0.12},
            maximum_replicas=3,
            mode=OptimizationMode.EXACT,
        )


def test_deadline_workload_cannot_silently_use_strict_analytical_optimization():
    workload = _structured_single_service_system().get_workload_spec()
    workload["request_classes"]["request"]["end_to_end_timeout_seconds"] = 1.0
    system = System("deadline", workload, QueueingNetworkType.OPEN)

    with pytest.raises(ValueError, match="deadlines require simulation"):
        optimize_with_analytical_model(
            system,
            maximum_replicas=3,
            mode=OptimizationMode.EXACT,
        )


def test_workload_without_slos_optimizes_to_the_stable_vector():
    system = _structured_single_service_system()
    workload = system.get_workload_spec()
    del workload["request_classes"]["request"]["end_to_end_latency_slos"]
    system = System("no-slo", workload, QueueingNetworkType.OPEN)

    result = optimize_with_analytical_model(
        system,
        maximum_replicas=3,
        mode=OptimizationMode.EXACT,
    )

    assert result.configuration == tuple(system.get_minimum_stable_replicas())
    assert result.metrics.limits == {}
    assert result.metrics.evaluation_basis == "ANALYTICAL_STABILITY"


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

from pathlib import Path

import pytest
import yaml

from optimization import AnalyticalStabilityHeuristic, ReplicaSizingProblem
from performance_models import (
    AnalyticalQueueingModel,
    QueueingNetworkType,
    ScenarioSystem,
    System,
    parse_workload_spec,
)


ROOT = Path(__file__).resolve().parents[1]


def _multi_scenario_workload():
    return {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {
            "request": {"routing": {"api": {"database": 0.5}}}
        },
        "scenarios": {
            "traffic_peak": {
                "entry_services": {
                    "request": {
                        "api": {"distribution": "exponential", "rate": 9.0}
                    }
                },
                "service_times": {
                    "api": {"distribution": "exponential", "rate": 10.0},
                    "database": {
                        "distribution": "exponential",
                        "rate": 10.0,
                    },
                },
            },
            "slow_services": {
                "entry_services": {
                    "request": {
                        "api": {
                            "distribution": "deterministic",
                            "rate": 4.0,
                        }
                    }
                },
                "service_times": {
                    "api": {"distribution": "deterministic", "mean": 0.5},
                    "database": {
                        "distribution": "gamma",
                        "mean": 1.0,
                        "scv": 2.0,
                    },
                },
            },
        },
    }


def test_parser_loads_every_supported_distribution_and_empirical_moments():
    path = ROOT / "examples" / "workloads" / "distribution-scenarios.example.yaml"
    workload = yaml.safe_load(path.read_text(encoding="utf-8"))

    parsed = parse_workload_spec(workload, base_path=path.parent)

    assert tuple(parsed.scenarios) == (
        "baseline_exponential",
        "variable_parametric",
        "empirical_iid",
        "correlated_arrival_trace",
    )
    assert parsed.scenario("variable_parametric").entry_services["interactive"][
        "gateway"
    ].scv == pytest.approx(3.0)
    empirical = parsed.scenario("empirical_iid").service_times["database"]
    assert empirical.distribution == "empirical"
    assert empirical.mean > 0.0
    assert empirical.scv >= 0.0
    trace = parsed.scenario("correlated_arrival_trace").entry_services[
        "interactive"
    ]["gateway"]
    assert trace.distribution == "arrival_trace"
    assert trace.rate > 0.0


def test_selected_system_uses_distribution_means_as_analytical_rates():
    system = System(
        "selected",
        _multi_scenario_workload(),
        QueueingNetworkType.OPEN,
        scenario_name="slow_services",
    )

    assert system.get_scenario_name() == "slow_services"
    assert system.get_service("api").get_mu() == pytest.approx(2.0)
    assert system.get_service("database").get_mu() == pytest.approx(1.0)
    assert system.get_effective_arrival_rates() == pytest.approx((4.0, 2.0))
    assert system.get_analytical_latency_model() == (
        AnalyticalQueueingModel.GGC.value
    )


def test_robust_minimum_stability_is_componentwise_across_scenarios():
    system = ScenarioSystem(
        "robust",
        _multi_scenario_workload(),
        QueueingNetworkType.OPEN,
    )

    assert system.get_minimum_stable_replicas_by_scenario() == {
        "traffic_peak": (1, 1),
        "slow_services": (3, 3),
    }
    assert system.get_minimum_stable_replicas() == [3, 3]

    system.configure_system_replicas((1, 1))
    scenario_metrics = system.solve_scenarios()
    assert scenario_metrics["traffic_peak"].get_stability()
    assert not scenario_metrics["slow_services"].get_stability()
    assert not system.solve().get_stability()

    system.configure_system_replicas((3, 3))
    assert system.solve().get_stability()
    assert all(
        candidate.get_system_replicas() == [3, 3]
        for candidate in system.get_scenario_systems().values()
    )
    assert system.get_analytical_latency_models() == {
        "traffic_peak": "M/M/c",
        "slow_services": AnalyticalQueueingModel.GGC.value,
    }
    assert system.get_analytical_queueing_models_by_scenario() == {
        "traffic_peak": {
            "api": AnalyticalQueueingModel.MMC,
            "database": AnalyticalQueueingModel.MMC,
        },
        "slow_services": {
            "api": AnalyticalQueueingModel.GGC,
            "database": AnalyticalQueueingModel.GGC,
        },
    }


def test_stability_heuristic_uses_the_robust_scenario_lower_bound():
    system = ScenarioSystem(
        "robust",
        _multi_scenario_workload(),
        QueueingNetworkType.OPEN,
    )
    heuristic = AnalyticalStabilityHeuristic.from_system(
        system,
        replica_costs=(2.0, 1.0),
    )

    assert heuristic((1, 1)) == pytest.approx(6.0)
    assert heuristic((3, 2)) == pytest.approx(1.0)
    assert heuristic((3, 3)) == pytest.approx(0.0)


def test_replica_problem_uses_robust_stability_and_scenario_ranking():
    system = ScenarioSystem(
        "robust",
        _multi_scenario_workload(),
        QueueingNetworkType.OPEN,
    )
    problem = ReplicaSizingProblem(
        system,
        simulation_duration=10.0,
        request_class_slos={"request": 1.0},
        max_replicas=(5, 5),
        replication_count=2,
    )

    assert problem.stable_configuration() == (3, 3)
    assert problem.heuristic((1, 1)) == pytest.approx(4.0)
    assert problem.search_tie_breaker((3, 3)) == pytest.approx(2.0 / 3.0)


def test_ambiguous_single_vector_apis_are_rejected_for_scenario_sets():
    system = ScenarioSystem(
        "robust",
        _multi_scenario_workload(),
        QueueingNetworkType.OPEN,
    )

    with pytest.raises(ValueError, match="no single effective-arrival vector"):
        system.get_effective_arrival_rates()
    with pytest.raises(ValueError, match="no single request-class traffic"):
        system.get_request_class_effective_arrival_rates()

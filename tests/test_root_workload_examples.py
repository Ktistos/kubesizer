from pathlib import Path

import yaml

from performance_models import (
    QueueingNetworkType,
    ScenarioSystem,
    System,
    parse_workload_spec,
)


ROOT = Path(__file__).resolve().parents[1]


def _load(name):
    return yaml.safe_load((ROOT / name).read_text(encoding="utf-8"))


def test_minimum_workload_is_valid_and_analytically_stable():
    system = System(
        "minimum",
        _load("workload.minimum.yaml"),
        QueueingNetworkType.OPEN,
    )

    assert system.get_request_classes() == ("request",)
    assert tuple(system.get_services()) == ("service",)
    assert system.get_configured_external_arrival_rates() == {"request": (1.0,)}
    assert system.get_request_class_concurrency_weights() == {"request": 1.0}
    assert system.get_minimum_stable_replicas() == [1]
    assert system.solve().get_stability()


def test_maximum_workload_exercises_all_parser_and_scenario_features():
    workload = _load("workload.maximum.yaml")
    parsed = parse_workload_spec(workload, base_path=ROOT)
    system = ScenarioSystem(
        "maximum",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=ROOT,
    )

    assert system.get_request_classes() == (
        "interactive",
        "batch",
    )
    assert tuple(system.get_services()) == ("gateway", "database", "worker")
    assert system.get_scenario_names() == (
        "baseline_exponential",
        "variable_parametric",
        "empirical_iid",
        "correlated_arrival_trace",
    )
    distributions = {
        distribution.distribution
        for scenario in parsed.scenarios.values()
        for streams in scenario.entry_services.values()
        for distribution in streams.values()
    } | {
        distribution.distribution
        for scenario in parsed.scenarios.values()
        for distribution in scenario.service_times.values()
    }
    assert distributions == {
        "arrival_trace",
        "deterministic",
        "empirical",
        "empirical_interarrival",
        "exponential",
        "gamma",
        "lognormal",
        "weibull",
    }

    system.configure_system_replicas(system.get_minimum_stable_replicas())
    assert system.solve().get_stability()

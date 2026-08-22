from pathlib import Path

import pytest
import yaml

from performance_models import (
    QueueingNetworkType,
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
    assert system.get_request_class_latency_slo_kind() == "mean"
    assert system.get_request_class_mean_latency_limits() == {"request": 1.0}
    assert system.get_minimum_stable_replicas() == [1]
    assert system.solve().get_stability()


def test_maximum_workload_exercises_all_single_model_parser_features():
    workload = _load("workload.maximum.yaml")
    parsed = parse_workload_spec(workload, base_path=ROOT)
    system = System(
        "maximum",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=ROOT,
    )

    assert system.get_request_classes() == (
        "interactive",
        "batch",
    )
    assert tuple(system.get_services()) == (
        "gateway",
        "database",
        "worker",
        "cache",
        "search",
        "mailer",
        "audit",
    )
    assert system.get_traffic_model_name() == "traffic_model"
    assert parsed.traffic_model.name == "traffic_model"
    assert system.get_request_class_latency_slo_kind() == "percentile"
    assert system.get_request_class_percentile_latency_limits() == {
        "interactive": 1.0,
        "batch": 5.0,
    }
    distributions = {
        distribution.distribution
        for streams in parsed.traffic_model.entry_services.values()
        for distribution in streams.values()
    } | {
        distribution.distribution
        for distribution in parsed.traffic_model.service_times.values()
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


def test_visit_ratio_workload_is_valid_for_analytical_optimization():
    workload = _load("workload.visits.yaml")
    parsed = parse_workload_spec(workload, base_path=ROOT)
    system = System(
        "visits",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=ROOT,
    )

    assert dict(parsed.request_class_visit_ratios["metadata_read"]) == {
        "stelarapi": 1.0,
        "ckan": 1.0003,
        "keycloak": 2.996,
        "solr": 0.7446,
    }
    assert system.get_effective_arrival_rates() == pytest.approx(
        [4.0, 4.0012, 11.984, 2.9784]
    )
    system.configure_system_replicas(system.get_minimum_stable_replicas())
    assert system.solve().get_stability()


def test_multiclass_service_demand_example_is_valid_for_analytical_optimization():
    workload_path = "examples/workloads/multiclass-service-demands.example.yaml"
    workload = _load(workload_path)
    parsed = parse_workload_spec(workload, base_path=ROOT)
    system = System(
        "multiclass-service-demands",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=ROOT,
    )

    assert tuple(parsed.request_classes) == ("dataset_search", "organizations")
    assert parsed.traffic_model.service_times_by_request_class["dataset_search"][
        "api"
    ].mean == pytest.approx(0.012)
    assert parsed.traffic_model.service_times_by_request_class["organizations"][
        "api"
    ].mean == pytest.approx(0.008)
    system.configure_system_replicas(system.get_minimum_stable_replicas())
    assert system.solve().get_stability()

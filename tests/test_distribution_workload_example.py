import csv
from pathlib import Path

import yaml

from performance_models import parse_workload_spec


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "workloads" / "distribution-scenarios.example.yaml"


def _read_positive_column(path, column):
    with path.open(newline="", encoding="utf-8") as source:
        rows = tuple(csv.DictReader(source))
    assert len(rows) >= 30
    values = tuple(float(row[column]) for row in rows)
    assert all(value > 0.0 for value in values)
    return values


def test_distribution_scenario_example_is_complete_and_self_contained():
    workload = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))

    request_classes = set(workload["request_classes"])
    scenarios = workload["scenarios"]
    assert set(scenarios) == {
        "baseline_exponential",
        "variable_parametric",
        "empirical_iid",
        "correlated_arrival_trace",
    }
    for scenario in scenarios.values():
        assert set(scenario) == {"entry_services", "service_times"}
        assert set(scenario["entry_services"]) == request_classes
        assert all(scenario["entry_services"].values())
        assert scenario["service_times"]

    declared = {
        specification["distribution"]
        for scenario in scenarios.values()
        for class_streams in scenario["entry_services"].values()
        for specification in class_streams.values()
    } | {
        specification["distribution"]
        for scenario in scenarios.values()
        for specification in scenario["service_times"].values()
    }
    assert declared == {
        "exponential",
        "deterministic",
        "gamma",
        "lognormal",
        "weibull",
        "empirical",
        "empirical_interarrival",
        "arrival_trace",
    }


def test_empirical_example_files_exist_and_contain_valid_samples():
    data_directory = EXAMPLE.parent / "data"
    _read_positive_column(
        data_directory / "interactive-gateway-interarrivals.csv",
        "interarrival_seconds",
    )
    _read_positive_column(
        data_directory / "database-service-times.csv",
        "service_time_seconds",
    )

    trace_path = data_directory / "interactive-gateway-arrival-trace.csv"
    with trace_path.open(newline="", encoding="utf-8") as source:
        rows = tuple(csv.DictReader(source))
    timestamps = tuple(float(row["arrival_offset_seconds"]) for row in rows)
    assert len(timestamps) >= 30
    assert timestamps[0] == 0.0
    assert all(left < right for left, right in zip(timestamps, timestamps[1:]))


def test_every_example_source_resolves_relative_to_the_workload_file():
    workload = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))

    source_paths = []
    for scenario in workload["scenarios"].values():
        for streams in scenario["entry_services"].values():
            source_paths.extend(
                specification["source"]
                for specification in streams.values()
                if "source" in specification
            )
        source_paths.extend(
            specification["source"]
            for specification in scenario["service_times"].values()
            if "source" in specification
        )

    assert source_paths
    assert all((EXAMPLE.parent / path).is_file() for path in source_paths)


def test_distribution_example_is_accepted_by_the_strict_parser():
    workload = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))

    parsed = parse_workload_spec(workload, base_path=EXAMPLE.parent)

    assert tuple(parsed.scenarios) == tuple(workload["scenarios"])

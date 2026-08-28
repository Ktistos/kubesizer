import csv
import json

import pytest
import yaml

from characterization import (
    ExperimentEvidenceError,
    load_observed_experiment_replication,
    load_observed_experiment_replications,
)


def _write_yaml(path, value):
    path.write_text(yaml.safe_dump(value), encoding="utf-8")


def _experiment(tmp_path, *, seed=7, censored=0, saturated=False):
    directory = tmp_path / f"run-{seed}"
    directory.mkdir(parents=True)
    _write_yaml(
        directory / "load_completed.yaml",
        {
            "completed": True,
            "runtime_exceptions": False,
            "locust_exit_status": 0,
        },
    )
    _write_yaml(directory / "config.yaml", {"load": {"random_seed": seed}})
    _write_yaml(
        directory / "measurement.yaml",
        {
            "services": {
                "api": {"replica_count": 1},
                "db": {"replica_count": 2},
            }
        },
    )
    _write_yaml(
        directory / "request_cohort.yaml",
        {
            "semantics": (
                "actual-client-start arrival cohort with post-window completion drain"
            ),
            "measurement_start_epoch": 10.0,
            "measurement_end_epoch": 20.0,
            "started_count": 2,
            "completed_count": 2 - censored,
            "censored_count": censored,
            "failure_count": 0 if censored else 1,
            "load_generator_saturated": saturated,
        },
    )
    (directory / "timeline.json").write_text(
        json.dumps({"test_stop_epoch": 30.0}), encoding="utf-8"
    )
    with (directory / "request_trace.search.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "request_class",
                "started_epoch",
                "completed_epoch",
                "latency_ms",
                "status_code",
                "success",
                "error",
            ),
        )
        writer.writeheader()
        writer.writerows(
            (
                {
                    "request_class": "search",
                    "started_epoch": 9.0,
                    "completed_epoch": 9.1,
                    "latency_ms": 100.0,
                    "status_code": 200,
                    "success": True,
                    "error": "",
                },
                {
                    "request_class": "search",
                    "started_epoch": 10.0,
                    "completed_epoch": 10.2,
                    "latency_ms": 200.0,
                    "status_code": 200,
                    "success": True,
                    "error": "",
                },
                {
                    "request_class": "search",
                    "started_epoch": 19.9,
                    "completed_epoch": "" if censored else 21.1,
                    "latency_ms": "" if censored else 1200.0,
                    "status_code": "" if censored else 500,
                    "success": "" if censored else False,
                    "error": "" if censored else "server error",
                },
                {
                    "request_class": "search",
                    "started_epoch": 20.0,
                    "completed_epoch": 20.1,
                    "latency_ms": 100.0,
                    "status_code": 200,
                    "success": True,
                    "error": "",
                },
                {
                    "request_class": "search",
                    "started_epoch": "",
                    "completed_epoch": "",
                    "latency_ms": "",
                    "status_code": "",
                    "success": "",
                    "error": "",
                },
            )
        )
    return directory


def test_importer_filters_by_arrival_cohort_and_keeps_post_window_completion(
    tmp_path,
):
    result = load_observed_experiment_replication(
        _experiment(tmp_path),
        configuration=(1, 2),
        expected_request_classes=("search",),
    )

    assert result.configuration == (1, 2)
    assert result.seed == 7
    assert result.latencies == {"search": pytest.approx((0.2, 1.2))}
    assert result.failures == {"search": (False, True)}


def test_importer_preserves_censored_latency_as_a_lower_bound(tmp_path):
    result = load_observed_experiment_replication(
        _experiment(tmp_path, censored=1),
        configuration=(1,),
    )

    assert result.latencies == {"search": pytest.approx((0.2, 10.1))}
    assert result.censored == {"search": (False, True)}
    assert result.failures == {"search": (False, False)}


def test_importer_rejects_saturated_load_generator(tmp_path):
    with pytest.raises(ExperimentEvidenceError, match="load generator saturated"):
        load_observed_experiment_replication(
            _experiment(tmp_path, saturated=True),
            configuration=(1,),
        )


def test_importer_verifies_optimizer_service_order_against_measured_replicas(tmp_path):
    directory = _experiment(tmp_path)

    result = load_observed_experiment_replication(
        directory,
        configuration=(1, 2),
        service_order=("api", "db"),
    )
    assert result.configuration == (1, 2)

    with pytest.raises(ExperimentEvidenceError, match="configurations differ"):
        load_observed_experiment_replication(
            directory,
            configuration=(2, 1),
            service_order=("api", "db"),
        )


def test_multi_run_import_rejects_duplicate_seeds(tmp_path):
    first = _experiment(tmp_path / "first", seed=11)
    second = _experiment(tmp_path / "second", seed=11)

    with pytest.raises(ExperimentEvidenceError, match="seeds must be unique"):
        load_observed_experiment_replications(
            (first, second),
            configuration=(1,),
        )

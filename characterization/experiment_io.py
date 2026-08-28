"""Strict import of independent real-cluster load-test evidence.

The importer deliberately understands only the small, machine-readable contract
produced by the STELAR load harness.  It filters complete trace files to the
arrival cohort declared by ``request_cohort.yaml``. Censored requests remain as
explicit latency lower bounds, so slow requests cannot disappear from
recommendation-quality evidence or support a false feasibility claim.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path

import yaml

from .quality import ObservedExperimentReplication


class ExperimentEvidenceError(ValueError):
    """A load-test directory cannot provide statistically valid evidence."""


def _load_mapping(path: Path):
    if not path.is_file():
        raise ExperimentEvidenceError(f"Missing required experiment file: {path.name}.")
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ExperimentEvidenceError(f"Cannot read {path.name}: {error}.") from error
    if not isinstance(value, dict):
        raise ExperimentEvidenceError(f"{path.name} must contain a mapping.")
    return value


def _nonnegative_integer(value, *, name):
    if isinstance(value, bool):
        raise ExperimentEvidenceError(f"{name} must be a non-negative integer.")
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise ExperimentEvidenceError(
            f"{name} must be a non-negative integer."
        ) from error
    if result < 0 or result != value:
        raise ExperimentEvidenceError(f"{name} must be a non-negative integer.")
    return result


def _finite_float(value, *, name):
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ExperimentEvidenceError(f"{name} must be numeric.") from error
    if not math.isfinite(result):
        raise ExperimentEvidenceError(f"{name} must be finite.")
    return result


def _parse_success(value, *, row_description):
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ExperimentEvidenceError(
        f"{row_description} has no completed boolean success outcome."
    )


def _completed_row_succeeded(row, *, row_description):
    success = _parse_success(row.get("success"), row_description=row_description)
    try:
        status_code = int(row.get("status_code") or 0)
    except (TypeError, ValueError) as error:
        raise ExperimentEvidenceError(
            f"{row_description} has an invalid HTTP status code."
        ) from error
    return success and 200 <= status_code < 400 and not row.get("error")


def load_observed_experiment_replication(
    directory,
    *,
    configuration,
    service_order=None,
    expected_request_classes=None,
):
    """Load one complete arrival-cohort replication from ``directory``.

    ``configuration`` is explicit because a trace directory does not define a
    portable service-vector order.  Requiring it prevents silently attaching
    measurements to the wrong optimizer state.
    """

    directory = Path(directory)
    if not directory.is_dir():
        raise ExperimentEvidenceError(
            f"Experiment directory does not exist: {directory}."
        )

    completion = _load_mapping(directory / "load_completed.yaml")
    if completion.get("completed") is not True:
        raise ExperimentEvidenceError("The load test did not complete successfully.")
    if completion.get("runtime_exceptions") is not False:
        raise ExperimentEvidenceError("The load test reported runtime exceptions.")
    if completion.get("locust_exit_status") != 0:
        raise ExperimentEvidenceError("The load generator returned a non-zero status.")

    config = _load_mapping(directory / "config.yaml")
    load = config.get("load")
    if not isinstance(load, dict):
        raise ExperimentEvidenceError("config.yaml is missing the load mapping.")
    seed = _nonnegative_integer(load.get("random_seed"), name="load.random_seed")

    if service_order is not None:
        service_order = tuple(str(value) for value in service_order)
        if (
            not service_order
            or len(set(service_order)) != len(service_order)
            or any(not value for value in service_order)
        ):
            raise ExperimentEvidenceError(
                "service_order must contain unique nonempty service names."
            )
        measurement = _load_mapping(directory / "measurement.yaml")
        services = measurement.get("services")
        if not isinstance(services, dict):
            raise ExperimentEvidenceError(
                "measurement.yaml is missing per-service replica evidence."
            )
        try:
            measured_configuration = tuple(
                _nonnegative_integer(
                    services[name]["replica_count"],
                    name=f"services.{name}.replica_count",
                )
                for name in service_order
            )
        except (KeyError, TypeError) as error:
            raise ExperimentEvidenceError(
                "measurement.yaml does not cover every service in service_order."
            ) from error
        declared_configuration = tuple(configuration)
        if declared_configuration != measured_configuration:
            raise ExperimentEvidenceError(
                "Declared and measured replica configurations differ for service "
                f"order {service_order}: declared={declared_configuration}, "
                f"measured={measured_configuration}."
            )

    cohort = _load_mapping(directory / "request_cohort.yaml")
    semantics = cohort.get("semantics")
    if (
        semantics
        != "actual-client-start arrival cohort with post-window completion drain"
    ):
        raise ExperimentEvidenceError(
            "Unsupported request-cohort semantics; actual-client-start arrival "
            "cohorts with completion drain are required."
        )
    start = _finite_float(
        cohort.get("measurement_start_epoch"),
        name="measurement_start_epoch",
    )
    end = _finite_float(
        cohort.get("measurement_end_epoch"),
        name="measurement_end_epoch",
    )
    if end <= start:
        raise ExperimentEvidenceError("The measurement cohort has an invalid interval.")

    started_count = _nonnegative_integer(
        cohort.get("started_count"), name="started_count"
    )
    completed_count = _nonnegative_integer(
        cohort.get("completed_count"), name="completed_count"
    )
    censored_count = _nonnegative_integer(
        cohort.get("censored_count"), name="censored_count"
    )
    failure_count = _nonnegative_integer(
        cohort.get("failure_count"), name="failure_count"
    )
    if completed_count + censored_count != started_count:
        raise ExperimentEvidenceError(
            "The arrival-cohort counts are inconsistent: "
            f"started={started_count}, completed={completed_count}, "
            f"censored={censored_count}."
        )
    if cohort.get("load_generator_saturated") is True:
        raise ExperimentEvidenceError(
            "The load generator saturated, so the requested workload was not reliable."
        )

    trace_paths = tuple(sorted(directory.glob("request_trace.*.csv")))
    if not trace_paths:
        raise ExperimentEvidenceError("No request_trace.<class>.csv files were found.")

    censor_time = None
    if censored_count:
        timeline_path = directory / "timeline.json"
        if not timeline_path.is_file():
            raise ExperimentEvidenceError(
                "Censored evidence requires timeline.json to calculate latency "
                "lower bounds."
            )
        try:
            timeline = json.loads(timeline_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ExperimentEvidenceError(
                f"Cannot read timeline.json: {error}."
            ) from error
        if not isinstance(timeline, dict):
            raise ExperimentEvidenceError("timeline.json must contain a mapping.")
        censor_time = _finite_float(
            timeline.get("test_stop_epoch"), name="test_stop_epoch"
        )
        if censor_time < end:
            raise ExperimentEvidenceError(
                "test_stop_epoch precedes the measurement-cohort end."
            )

    latency_by_class = {}
    failure_by_class = {}
    censored_by_class = {}
    cohort_rows = 0
    observed_completed = 0
    observed_censored = 0
    observed_failures = 0
    for trace_path in trace_paths:
        try:
            handle = trace_path.open(newline="", encoding="utf-8")
        except OSError as error:
            raise ExperimentEvidenceError(
                f"Cannot read {trace_path.name}: {error}."
            ) from error
        with handle:
            reader = csv.DictReader(handle)
            required = {
                "request_class",
                "started_epoch",
                "completed_epoch",
                "latency_ms",
                "status_code",
                "success",
                "error",
            }
            if reader.fieldnames is None or not required.issubset(reader.fieldnames):
                missing = sorted(required - set(reader.fieldnames or ()))
                raise ExperimentEvidenceError(
                    f"{trace_path.name} is missing columns: {missing}."
                )
            for row_number, row in enumerate(reader, start=2):
                description = f"{trace_path.name} row {row_number}"
                # A scheduler entry that never acquired a client worker is not an
                # external arrival under the declared actual-start semantics.
                if not row.get("started_epoch"):
                    continue
                started = _finite_float(
                    row.get("started_epoch"),
                    name=f"{description} started_epoch",
                )
                if not start <= started < end:
                    continue
                cohort_rows += 1
                request_class = str(row.get("request_class", "")).strip()
                if not request_class:
                    raise ExperimentEvidenceError(
                        f"{description} has no request class."
                    )
                if row.get("completed_epoch"):
                    completed = _finite_float(
                        row.get("completed_epoch"),
                        name=f"{description} completed_epoch",
                    )
                    if completed < started:
                        raise ExperimentEvidenceError(
                            f"{description} completes before it starts."
                        )
                    latency_ms = _finite_float(
                        row.get("latency_ms"),
                        name=f"{description} latency_ms",
                    )
                    if latency_ms < 0.0:
                        raise ExperimentEvidenceError(
                            f"{description} has a negative latency."
                        )
                    success = _completed_row_succeeded(row, row_description=description)
                    is_censored = False
                    observed_completed += 1
                    observed_failures += not success
                else:
                    if censor_time is None:
                        raise ExperimentEvidenceError(
                            f"{description} is incomplete but the cohort declares no "
                            "censoring."
                        )
                    latency_ms = max(0.0, censor_time - started) * 1000.0
                    success = False
                    is_censored = True
                    observed_censored += 1
                latency_by_class.setdefault(request_class, []).append(
                    latency_ms / 1000.0
                )
                # Censoring is not an observed HTTP/client failure.  It is tracked
                # separately and prevents a feasibility claim for either latency
                # or failure-rate constraints.
                failure_by_class.setdefault(request_class, []).append(
                    not success and not is_censored
                )
                censored_by_class.setdefault(request_class, []).append(is_censored)

    if cohort_rows != started_count:
        raise ExperimentEvidenceError(
            "Trace/cohort count mismatch: "
            f"trace={cohort_rows}, declared={started_count}."
        )
    if observed_completed != completed_count or observed_censored != censored_count:
        raise ExperimentEvidenceError(
            "Trace/cohort completion-count mismatch: "
            f"trace completed={observed_completed}, censored={observed_censored}; "
            f"declared completed={completed_count}, censored={censored_count}."
        )
    if observed_failures != failure_count:
        raise ExperimentEvidenceError(
            "Trace/cohort failure-count mismatch: "
            f"trace={observed_failures}, declared={failure_count}."
        )

    observed_classes = set(latency_by_class)
    if expected_request_classes is not None:
        expected_classes = {str(value) for value in expected_request_classes}
        if observed_classes != expected_classes:
            raise ExperimentEvidenceError(
                "Request-class mismatch: "
                f"observed={sorted(observed_classes)}, "
                f"expected={sorted(expected_classes)}."
            )

    return ObservedExperimentReplication.from_mappings(
        configuration,
        seed,
        directory.name,
        latency_by_class,
        request_class_failures=failure_by_class,
        request_class_censored=censored_by_class,
    )


def load_observed_experiment_replications(
    directories,
    *,
    configuration,
    service_order=None,
    expected_request_classes=None,
):
    """Load several independent runs for the same replica configuration."""

    results = tuple(
        load_observed_experiment_replication(
            directory,
            configuration=configuration,
            service_order=service_order,
            expected_request_classes=expected_request_classes,
        )
        for directory in directories
    )
    seeds = tuple(item.seed for item in results)
    run_ids = tuple(item.run_id for item in results)
    if len(set(seeds)) != len(seeds):
        raise ExperimentEvidenceError("Experiment replication seeds must be unique.")
    if len(set(run_ids)) != len(run_ids):
        raise ExperimentEvidenceError("Experiment run identifiers must be unique.")
    return results

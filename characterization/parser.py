"""Strict readers for the vendor-neutral characterization evidence package."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime
from itertools import pairwise
from pathlib import Path

import yaml  # type: ignore[import-untyped]

from performance_models import CommunicationLatencySpec, parse_duration_distribution

from .models import (
    DEFAULT_MAXIMUM_CAPACITY_UTILIZATION,
    MEASUREMENT_METHODOLOGY_VERSION,
    CapacityObservation,
    ExternalArrivalSample,
    ExternalInterval,
    LatencyHistogramBucket,
    MeasurementPackage,
    PortableSizingInputs,
    ServiceClassInterval,
    ServiceDeployment,
    ServiceEdgeInterval,
    ServiceInterval,
    TargetRequestClass,
    WorkloadDefinition,
)


def _finite(value, label, *, positive=False, nonnegative=False):
    if isinstance(value, bool):
        raise ValueError(f"{label} must be numeric.")
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} must be numeric.") from error
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite.")
    if positive and result <= 0.0:
        raise ValueError(f"{label} must be greater than zero.")
    if nonnegative and result < 0.0:
        raise ValueError(f"{label} must be non-negative.")
    return result


def _optional_number(mapping, key, label, *, positive=False, nonnegative=False):
    value = mapping.get(key)
    if value in (None, ""):
        return None
    return _finite(value, label, positive=positive, nonnegative=nonnegative)


def _name(value, label):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string.")
    return value.strip()


def _parse_time(value, label):
    try:
        result = float(value)
    except (TypeError, ValueError):
        if not isinstance(value, str):
            raise ValueError(f"{label} must be numeric or an ISO-8601 timestamp.")
        normalized = value.strip().replace("Z", "+00:00")
        try:
            result = datetime.fromisoformat(normalized).timestamp()
        except ValueError as error:
            raise ValueError(
                f"{label} must be numeric or an ISO-8601 timestamp."
            ) from error
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite.")
    return result


def _interval(row, label):
    start = _parse_time(row.get("start"), f"{label}.start")
    end = _parse_time(row.get("end"), f"{label}.end")
    if end <= start:
        raise ValueError(f"{label}.end must be later than start.")
    return start, end


def _read_yaml(path, label):
    try:
        with path.open("r", encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except OSError as error:
        raise ValueError(f"Cannot read {label} file {path}.") from error
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in {label} file {path}.") from error
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a YAML mapping.")
    return value


def _file_digest(path):
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ValueError(f"Cannot read measurement source {path}.") from error
    return digest.hexdigest()


def _resolve_source(value, label, base_path):
    source = _name(value, label)
    path = Path(source)
    if not path.is_absolute():
        path = Path(base_path) / path
    path = path.resolve()
    if not path.is_file():
        raise ValueError(f"{label} does not identify a file: {path}.")
    return path


def _csv_rows(path, required_columns, label):
    try:
        with path.open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            columns = set(reader.fieldnames or ())
            missing = set(required_columns) - columns
            if missing:
                raise ValueError(
                    f"{label} is missing required columns {sorted(missing)}."
                )
            rows = tuple(reader)
    except OSError as error:
        raise ValueError(f"Cannot read {label} at {path}.") from error
    if not rows:
        raise ValueError(f"{label} contains no data rows.")
    return rows


def _validate_service_interval_grid(rows):
    """Require one comparable, aligned counter-delta grid across services.

    Each row is already the join of destination request counts and application
    container CPU consumption for one Kubernetes ``(namespace, pod)`` identity.
    Repeated identities would double-count CPU or throughput, while unequal
    windows would make an unweighted demand quantile cluster-dependent.
    """
    identities = set()
    pod_owners = {}
    durations = []
    windows_by_service = {}
    for row in rows:
        identity = (row.start, row.end, row.service, row.namespace, row.pod)
        if identity in identities:
            raise ValueError(
                "service_intervals contains duplicate rows for the same "
                "(window, service, namespace, pod)."
            )
        identities.add(identity)
        pod_identity = (row.start, row.end, row.namespace, row.pod)
        previous_service = pod_owners.setdefault(pod_identity, row.service)
        if previous_service != row.service:
            raise ValueError(
                "service_intervals assigns one (namespace, pod) to multiple "
                "services in the same window."
            )
        durations.append(row.duration)
        windows_by_service.setdefault(row.service, set()).add((row.start, row.end))
        if row.errors_4xx + row.errors_5xx > row.request_count + 1e-12:
            raise ValueError(
                "service_intervals errors_4xx + errors_5xx cannot exceed "
                "request_count."
            )

    reference_duration = durations[0]
    if any(
        not math.isclose(
            duration,
            reference_duration,
            rel_tol=1e-9,
            abs_tol=1e-9,
        )
        for duration in durations[1:]
    ):
        raise ValueError(
            "service_intervals must use one common observation-window duration."
        )

    window_sets = tuple(windows_by_service.values())
    if any(windows != window_sets[0] for windows in window_sets[1:]):
        raise ValueError(
            "service_intervals must contain the same observation windows for "
            "every service."
        )
    ordered = sorted(window_sets[0])
    if any(right[0] < left[1] for left, right in pairwise(ordered)):
        raise ValueError("service_intervals observation windows must not overlap.")


def _validate_interval_alignment(measurements):
    service_windows = {(row.start, row.end) for row in measurements.service_intervals}
    interval_sources = (
        ("service_class_intervals", measurements.service_class_intervals),
        ("external_intervals", measurements.external_intervals),
        ("service_edges", measurements.service_edges),
        ("service_latency_histograms", measurements.service_latency_histograms),
        ("external_latency_histograms", measurements.external_latency_histograms),
    )
    for label, rows in interval_sources:
        misaligned = {
            (row.start, row.end)
            for row in rows
            if (row.start, row.end) not in service_windows
        }
        if misaligned:
            raise ValueError(
                f"{label} contains windows not aligned with service_intervals."
            )

    service_totals = {}
    for row in measurements.service_intervals:
        key = (row.start, row.end, row.service)
        service_totals[key] = service_totals.get(key, 0.0) + row.request_count
    class_totals = {}
    for row in measurements.service_class_intervals:
        key = (row.start, row.end, row.service)
        class_totals[key] = class_totals.get(key, 0.0) + row.request_count
    overcounts = {
        key: (count, service_totals.get(key, 0.0))
        for key, count in class_totals.items()
        if count > service_totals.get(key, 0.0) + 1e-12
    }
    if overcounts:
        raise ValueError(
            "service_class_intervals request counts exceed the corresponding "
            f"pod-attributed service total: {overcounts}."
        )

    external_totals = {}
    for row in measurements.external_intervals:
        key = (row.start, row.end, row.request_class)
        external_totals[key] = external_totals.get(key, 0.0) + row.request_count

    histogram_sources = (
        (
            "service_latency_histograms",
            measurements.service_latency_histograms,
            service_totals,
        ),
        (
            "external_latency_histograms",
            measurements.external_latency_histograms,
            external_totals,
        ),
    )
    for label, buckets, totals in histogram_sources:
        for bucket in buckets:
            if not math.isinf(bucket.upper_bound_seconds):
                continue
            key = (bucket.start, bucket.end, bucket.subject)
            expected = totals.get(key)
            if expected is None:
                raise ValueError(
                    f"{label} has no matching completed-request count for {key}."
                )
            if not math.isclose(
                bucket.cumulative_count,
                expected,
                rel_tol=0.01,
                abs_tol=1.0,
            ):
                raise ValueError(
                    f"{label} +Inf count disagrees with completed requests for "
                    f"{key}: histogram={bucket.cumulative_count}, requests={expected}."
                )


def _parse_workload_services(raw_services):
    """Validate service execution facts embedded in the workload."""
    if not isinstance(raw_services, Mapping) or not raw_services:
        raise ValueError("workload.services must be a nonempty mapping.")
    services = []
    allowed = {
        "namespace",
        "cpu_request_cores",
        "cpu_limit_cores",
        "memory_request_bytes",
        "memory_limit_bytes",
        "concurrency_limit_per_replica",
    }
    for service_name, raw in raw_services.items():
        name = _name(service_name, "workload service name")
        if not isinstance(raw, Mapping) or set(raw) - allowed:
            raise ValueError(
                f"workload.services.{name} contains unsupported fields: "
                f"{sorted(set(raw) - allowed) if isinstance(raw, Mapping) else []}."
            )
        required = {"namespace", "cpu_request_cores"}
        missing = required - set(raw)
        if missing:
            raise ValueError(f"workload.services.{name} is missing {sorted(missing)}.")
        cpu_request = _finite(
            raw["cpu_request_cores"],
            f"workload.services.{name}.cpu_request_cores",
            positive=True,
        )
        cpu_limit = _optional_number(
            raw,
            "cpu_limit_cores",
            f"workload.services.{name}.cpu_limit_cores",
            positive=True,
        )
        if cpu_limit is not None and cpu_limit < cpu_request:
            raise ValueError(
                f"workload.services.{name}.cpu_limit_cores cannot be smaller "
                "than cpu_request_cores."
            )
        memory_request = _optional_number(
            raw,
            "memory_request_bytes",
            f"workload.services.{name}.memory_request_bytes",
            positive=True,
        )
        memory_limit = _optional_number(
            raw,
            "memory_limit_bytes",
            f"workload.services.{name}.memory_limit_bytes",
            positive=True,
        )
        if (
            memory_request is not None
            and memory_limit is not None
            and memory_limit < memory_request
        ):
            raise ValueError(
                f"workload.services.{name}.memory_limit_bytes cannot be "
                "smaller than memory_request_bytes."
            )
        concurrency_limit = raw.get("concurrency_limit_per_replica")
        if concurrency_limit is not None and (
            isinstance(concurrency_limit, bool)
            or not isinstance(concurrency_limit, int)
            or concurrency_limit <= 0
        ):
            raise ValueError(
                f"workload.services.{name}.concurrency_limit_per_replica "
                "must be a positive integer when supplied."
            )
        services.append(
            ServiceDeployment(
                name=name,
                namespace=_name(
                    raw["namespace"],
                    f"workload.services.{name}.namespace",
                ),
                cpu_request_cores=cpu_request,
                cpu_limit_cores=cpu_limit,
                memory_request_bytes=memory_request,
                memory_limit_bytes=memory_limit,
                concurrency_limit_per_replica=concurrency_limit,
            )
        )
    return tuple(services)


def _canonical_slo(raw, label):
    if raw is None:
        return None
    if not isinstance(raw, Mapping) or set(raw) not in (
        {"mean_seconds"},
        {"percentile"},
    ):
        raise ValueError(f"{label} must choose mean_seconds or percentile.")
    if "mean_seconds" in raw:
        return (("mean_seconds", _finite(raw["mean_seconds"], label, positive=True)),)
    percentile = raw["percentile"]
    if not isinstance(percentile, Mapping) or set(percentile) != {
        "quantile",
        "maximum_seconds",
    }:
        raise ValueError(
            f"{label}.percentile must contain quantile and maximum_seconds."
        )
    quantile = _finite(percentile["quantile"], f"{label}.percentile.quantile")
    if not 0.0 < quantile < 1.0:
        raise ValueError(f"{label}.percentile.quantile must lie between zero and one.")
    maximum = _finite(
        percentile["maximum_seconds"],
        f"{label}.percentile.maximum_seconds",
        positive=True,
    )
    return (("percentile", (("quantile", quantile), ("maximum_seconds", maximum))),)


def parse_workload_definition(value, *, base_path=None):
    """Validate request semantics and SLOs for the measured workload."""
    allowed_top_level = {
        "services",
        "request_classes",
        "load_forecast",
        "sizing_policy",
        "communication_latency",
        "service_blocking_times",
    }
    if not isinstance(value, Mapping) or set(value) - allowed_top_level:
        raise ValueError(
            "workload must contain services and request_classes and may contain "
            "load_forecast, sizing_policy, communication_latency, and "
            "service_blocking_times; "
            "arrival rates come from the measurement package."
        )
    missing = {"services", "request_classes"} - set(value)
    if missing:
        raise ValueError(f"workload is missing {sorted(missing)}.")
    services = _parse_workload_services(value["services"])
    raw_classes = value["request_classes"]
    if not isinstance(raw_classes, Mapping) or not raw_classes:
        raise ValueError("request_classes must be a nonempty mapping.")
    service_names = {service.name for service in services}
    classes = []
    allowed = {
        "concurrency_weight",
        "entry_services",
        "visits",
        "calls",
        "call_execution",
        "fixed_latency_overhead_seconds",
        "latency_overhead",
        "latency_boundary",
        "latency_components",
        "end_to_end_timeout_seconds",
        "end_to_end_latency_slos",
        "maximum_terminal_failure_rate",
    }
    for class_name, raw in raw_classes.items():
        name = _name(class_name, "request class name")
        if not isinstance(raw, Mapping) or set(raw) - allowed:
            raise ValueError(
                f"request_classes.{name} contains unsupported fields: "
                f"{sorted(set(raw) - allowed) if isinstance(raw, Mapping) else []}."
            )
        missing = {"concurrency_weight", "entry_services"} - set(raw)
        if missing:
            raise ValueError(f"request_classes.{name} is missing {sorted(missing)}.")
        latency_fields = {
            "fixed_latency_overhead_seconds",
            "latency_overhead",
            "latency_components",
        } & set(raw)
        if len(latency_fields) > 1:
            raise ValueError(
                f"request_classes.{name} cannot define both a legacy latency "
                "overhead and latency_components."
            )
        weight = _finite(
            raw["concurrency_weight"],
            f"request_classes.{name}.concurrency_weight",
            positive=True,
        )
        entries = raw["entry_services"]
        if not isinstance(entries, Mapping) or not entries:
            raise ValueError(
                f"request_classes.{name}.entry_services must be a mapping."
            )
        unknown_entries = set(entries) - service_names
        if unknown_entries:
            raise ValueError(
                f"request_classes.{name}.entry_services contains unknown services "
                f"{sorted(unknown_entries)}."
            )
        entry_weights = tuple(
            (
                service,
                _finite(
                    entry_weight,
                    f"request_classes.{name}.entry_services.{service}",
                    positive=True,
                ),
            )
            for service, entry_weight in entries.items()
        )
        if not math.isclose(
            sum(entry_weight for _, entry_weight in entry_weights),
            1.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError(
                f"request_classes.{name}.entry_services weights must sum to one."
            )
        has_visits = "visits" in raw
        has_calls = "calls" in raw
        if has_visits and has_calls:
            raise ValueError(f"request_classes.{name} cannot define visits and calls.")
        visits = ()
        if has_visits:
            if not isinstance(raw["visits"], Mapping) or not raw["visits"]:
                raise ValueError(f"request_classes.{name}.visits must be a mapping.")
            unknown = set(raw["visits"]) - service_names
            if unknown:
                raise ValueError(
                    f"request_classes.{name}.visits contains unknown services "
                    f"{sorted(unknown)}."
                )
            visits = tuple(
                (
                    service,
                    _finite(
                        count,
                        f"request_classes.{name}.visits.{service}",
                        positive=True,
                    ),
                )
                for service, count in raw["visits"].items()
            )
        calls = ()
        call_execution = raw.get("call_execution")
        if has_calls != (call_execution is not None):
            raise ValueError(
                f"request_classes.{name} must define call_execution exactly when "
                "it defines calls."
            )
        if has_calls:
            if call_execution not in {"sequential", "parallel", "uncertain"}:
                raise ValueError(
                    f"request_classes.{name}.call_execution must be sequential, "
                    "parallel, or uncertain."
                )
            raw_calls = raw["calls"]
            if not isinstance(raw_calls, Mapping):
                raise ValueError(f"request_classes.{name}.calls must be a mapping.")
            unknown_sources = set(raw_calls) - service_names
            if unknown_sources:
                raise ValueError(
                    f"request_classes.{name}.calls has unknown sources "
                    f"{sorted(unknown_sources)}."
                )
            parsed_calls = []
            for source, targets in raw_calls.items():
                if not isinstance(targets, Mapping):
                    raise ValueError(
                        f"request_classes.{name}.calls.{source} must be a mapping."
                    )
                unknown_targets = set(targets) - service_names
                if unknown_targets:
                    raise ValueError(
                        f"request_classes.{name}.calls.{source} has unknown targets "
                        f"{sorted(unknown_targets)}."
                    )
                parsed_calls.append(
                    (
                        source,
                        tuple(
                            (
                                target,
                                _finite(
                                    count,
                                    f"request_classes.{name}.calls.{source}.{target}",
                                    positive=True,
                                ),
                            )
                            for target, count in targets.items()
                        ),
                    )
                )
            calls = tuple(parsed_calls)
        overhead = _finite(
            raw.get("fixed_latency_overhead_seconds", 0.0),
            f"request_classes.{name}.fixed_latency_overhead_seconds",
            nonnegative=True,
        )
        overhead_distribution = None
        latency_boundary = raw.get("latency_boundary", "unspecified")
        if latency_boundary not in {"unspecified", "ingress", "external_client"}:
            raise ValueError(
                f"request_classes.{name}.latency_boundary must be ingress or "
                "external_client."
            )
        latency_components = ()
        if "latency_components" in raw:
            if latency_boundary == "unspecified":
                raise ValueError(
                    f"request_classes.{name} must declare latency_boundary with "
                    "latency_components."
                )
            raw_components = raw["latency_components"]
            allowed_components = {
                "client_to_ingress",
                "client_to_entry",
                "ingress_processing",
                "unmodeled_residual",
            }
            if not isinstance(raw_components, Mapping) or not raw_components:
                raise ValueError(
                    f"request_classes.{name}.latency_components must be a "
                    "nonempty mapping."
                )
            unknown = set(raw_components) - allowed_components
            if unknown:
                raise ValueError(
                    f"request_classes.{name}.latency_components contains "
                    f"unsupported components {sorted(unknown)}."
                )
            if latency_boundary == "ingress" and (
                {"client_to_ingress", "client_to_entry"} & set(raw_components)
            ):
                raise ValueError(
                    f"request_classes.{name} cannot include a client path "
                    "component at the ingress latency boundary."
                )
            if "client_to_entry" in raw_components and (
                {"client_to_ingress", "ingress_processing"} & set(raw_components)
            ):
                raise ValueError(
                    f"request_classes.{name} cannot combine client_to_entry "
                    "with finer ingress-boundary components."
                )
            latency_components = tuple(
                (
                    component_name,
                    parse_duration_distribution(
                        component,
                        (
                            f"request_classes.{name}.latency_components."
                            f"{component_name}"
                        ),
                        base_path=base_path,
                    ),
                )
                for component_name, component in raw_components.items()
            )
            overhead = sum(component.mean for _, component in latency_components)
        if "latency_overhead" in raw:
            overhead_distribution = parse_duration_distribution(
                raw["latency_overhead"],
                f"request_classes.{name}.latency_overhead",
                base_path=base_path,
            )
            overhead = overhead_distribution.mean
        timeout = _finite(
            raw.get("end_to_end_timeout_seconds", 0.0),
            f"request_classes.{name}.end_to_end_timeout_seconds",
            nonnegative=True,
        )
        failure_limit = _optional_number(
            raw,
            "maximum_terminal_failure_rate",
            f"request_classes.{name}.maximum_terminal_failure_rate",
            nonnegative=True,
        )
        if failure_limit is not None and not failure_limit < 1.0:
            raise ValueError(
                f"request_classes.{name}.maximum_terminal_failure_rate must be "
                "less than one."
            )
        if failure_limit is not None and timeout <= 0.0:
            raise ValueError(
                f"request_classes.{name} needs a positive timeout when it declares "
                "a terminal failure-rate limit."
            )
        classes.append(
            TargetRequestClass(
                name=name,
                concurrency_weight=weight,
                entry_service_weights=entry_weights,
                visits=visits,
                calls=calls,
                call_execution=call_execution,
                fixed_latency_overhead_seconds=overhead,
                latency_overhead_distribution=overhead_distribution,
                latency_boundary=latency_boundary,
                latency_components=latency_components,
                end_to_end_timeout_seconds=timeout,
                latency_slo=_canonical_slo(
                    raw.get("end_to_end_latency_slos"),
                    f"request_classes.{name}.end_to_end_latency_slos",
                ),
                maximum_terminal_failure_rate=failure_limit,
            )
        )
    if not math.isclose(
        sum(item.concurrency_weight for item in classes),
        1.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ValueError("Request-class concurrency weights must sum to one.")
    raw_forecast = value.get("load_forecast", {"multipliers": [1.0]})
    if not isinstance(raw_forecast, Mapping) or set(raw_forecast) != {"multipliers"}:
        raise ValueError("load_forecast must contain exactly multipliers.")
    raw_multipliers = raw_forecast["multipliers"]
    if not isinstance(raw_multipliers, (list, tuple)) or not raw_multipliers:
        raise ValueError("load_forecast.multipliers must be a nonempty array.")
    multipliers = tuple(
        _finite(
            value,
            f"load_forecast.multipliers[{index}]",
            positive=True,
        )
        for index, value in enumerate(raw_multipliers)
    )
    if tuple(sorted(set(multipliers))) != multipliers:
        raise ValueError(
            "load_forecast.multipliers must be unique and strictly increasing."
        )
    raw_sizing_policy = value.get(
        "sizing_policy",
        {"maximum_capacity_utilization": (DEFAULT_MAXIMUM_CAPACITY_UTILIZATION)},
    )
    if not isinstance(raw_sizing_policy, Mapping) or set(raw_sizing_policy) != {
        "maximum_capacity_utilization"
    }:
        raise ValueError(
            "sizing_policy must contain exactly maximum_capacity_utilization."
        )
    maximum_capacity_utilization = _finite(
        raw_sizing_policy["maximum_capacity_utilization"],
        "sizing_policy.maximum_capacity_utilization",
        positive=True,
    )
    if maximum_capacity_utilization >= 1.0:
        raise ValueError(
            "sizing_policy.maximum_capacity_utilization must be less than one."
        )
    communication_latency = None
    if "communication_latency" in value:
        raw_communication = value["communication_latency"]
        expected = {
            "same_node_one_way",
            "cross_node_one_way",
            "cross_node_probability",
        }
        if (
            not isinstance(raw_communication, Mapping)
            or set(raw_communication) != expected
        ):
            raise ValueError(
                "communication_latency must contain exactly same_node_one_way, "
                "cross_node_one_way, and cross_node_probability."
            )
        probability = _finite(
            raw_communication["cross_node_probability"],
            "communication_latency.cross_node_probability",
            nonnegative=True,
        )
        if probability > 1.0:
            raise ValueError(
                "communication_latency.cross_node_probability cannot " "exceed one."
            )
        communication_latency = CommunicationLatencySpec(
            same_node_one_way=parse_duration_distribution(
                raw_communication["same_node_one_way"],
                "communication_latency.same_node_one_way",
                base_path=base_path,
            ),
            cross_node_one_way=parse_duration_distribution(
                raw_communication["cross_node_one_way"],
                "communication_latency.cross_node_one_way",
                base_path=base_path,
            ),
            cross_node_probability=probability,
        )
    service_blocking_times = ()
    if "service_blocking_times" in value:
        raw_blocking_times = value["service_blocking_times"]
        if not isinstance(raw_blocking_times, Mapping) or not raw_blocking_times:
            raise ValueError("service_blocking_times must be a nonempty mapping.")
        unknown_services = set(raw_blocking_times) - service_names
        if unknown_services:
            raise ValueError(
                "service_blocking_times contains unknown services "
                f"{sorted(unknown_services)}."
            )
        service_blocking_times = tuple(
            (
                service_name,
                parse_duration_distribution(
                    specification,
                    f"service_blocking_times.{service_name}",
                    base_path=base_path,
                ),
            )
            for service_name, specification in raw_blocking_times.items()
        )
    return WorkloadDefinition(
        services=services,
        request_classes=tuple(classes),
        load_forecast_multipliers=multipliers,
        communication_latency=communication_latency,
        service_blocking_times=service_blocking_times,
        maximum_capacity_utilization=maximum_capacity_utilization,
    )


def _parse_service_intervals(path):
    rows = _csv_rows(
        path,
        {
            "start",
            "end",
            "service",
            "namespace",
            "pod",
            "request_count",
            "cpu_seconds",
        },
        "service_intervals",
    )
    result = []
    for index, row in enumerate(rows, 2):
        label = f"service_intervals row {index}"
        start, end = _interval(row, label)
        result.append(
            ServiceInterval(
                start=start,
                end=end,
                service=_name(row.get("service"), f"{label}.service"),
                namespace=_name(row.get("namespace"), f"{label}.namespace"),
                pod=_name(row.get("pod"), f"{label}.pod"),
                request_count=_finite(
                    row.get("request_count"), f"{label}.request_count", nonnegative=True
                ),
                cpu_seconds=_finite(
                    row.get("cpu_seconds"), f"{label}.cpu_seconds", nonnegative=True
                ),
                errors_4xx=_finite(
                    row.get("errors_4xx") or 0.0,
                    f"{label}.errors_4xx",
                    nonnegative=True,
                ),
                errors_5xx=_finite(
                    row.get("errors_5xx") or 0.0,
                    f"{label}.errors_5xx",
                    nonnegative=True,
                ),
                throttled_seconds=_finite(
                    row.get("throttled_seconds") or 0.0,
                    f"{label}.throttled_seconds",
                    nonnegative=True,
                ),
                response_duration_sum_seconds=_optional_number(
                    row,
                    "response_duration_sum_seconds",
                    f"{label}.response_duration_sum_seconds",
                    nonnegative=True,
                ),
            )
        )
    _validate_service_interval_grid(result)
    return tuple(result)


def _parse_service_class_intervals(path):
    rows = _csv_rows(
        path,
        {"start", "end", "service", "request_class", "request_count"},
        "service_class_intervals",
    )
    result = []
    identities = set()
    for index, row in enumerate(rows, 2):
        label = f"service_class_intervals row {index}"
        start, end = _interval(row, label)
        item = ServiceClassInterval(
            start=start,
            end=end,
            service=_name(row.get("service"), f"{label}.service"),
            request_class=_name(row.get("request_class"), f"{label}.request_class"),
            request_count=_finite(
                row.get("request_count"),
                f"{label}.request_count",
                nonnegative=True,
            ),
        )
        identity = (start, end, item.service, item.request_class)
        if identity in identities:
            raise ValueError(
                "service_class_intervals contains duplicate rows for the same "
                "(window, service, request_class)."
            )
        identities.add(identity)
        result.append(item)
    return tuple(result)


def _parse_external_intervals(path):
    rows = _csv_rows(
        path,
        {"start", "end", "request_class", "request_count"},
        "external_intervals",
    )
    result = []
    for index, row in enumerate(rows, 2):
        label = f"external_intervals row {index}"
        start, end = _interval(row, label)
        result.append(
            ExternalInterval(
                start=start,
                end=end,
                request_class=_name(row.get("request_class"), f"{label}.request_class"),
                request_count=_finite(
                    row.get("request_count"), f"{label}.request_count", nonnegative=True
                ),
                errors_4xx=_finite(
                    row.get("errors_4xx") or 0.0,
                    f"{label}.errors_4xx",
                    nonnegative=True,
                ),
                errors_5xx=_finite(
                    row.get("errors_5xx") or 0.0,
                    f"{label}.errors_5xx",
                    nonnegative=True,
                ),
                response_duration_sum_seconds=_optional_number(
                    row,
                    "response_duration_sum_seconds",
                    f"{label}.response_duration_sum_seconds",
                    nonnegative=True,
                ),
            )
        )
    return tuple(result)


def _parse_edges(path):
    rows = _csv_rows(
        path,
        {"start", "end", "source_service", "destination_service", "call_count"},
        "service_edges",
    )
    result = []
    for index, row in enumerate(rows, 2):
        label = f"service_edges row {index}"
        start, end = _interval(row, label)
        result.append(
            ServiceEdgeInterval(
                start=start,
                end=end,
                source_service=_name(
                    row.get("source_service"), f"{label}.source_service"
                ),
                destination_service=_name(
                    row.get("destination_service"),
                    f"{label}.destination_service",
                ),
                call_count=_finite(
                    row.get("call_count"), f"{label}.call_count", nonnegative=True
                ),
                request_class=(row.get("request_class") or "").strip() or None,
            )
        )
    return tuple(result)


def _parse_histograms(path, subject_column, label):
    rows = _csv_rows(
        path,
        {"start", "end", subject_column, "upper_bound_seconds", "cumulative_count"},
        label,
    )
    result = []
    for index, row in enumerate(rows, 2):
        row_label = f"{label} row {index}"
        start, end = _interval(row, row_label)
        raw_bound = str(row.get("upper_bound_seconds", "")).strip().lower()
        bound = (
            math.inf
            if raw_bound in {"+inf", "inf", "infinity", "+infinity"}
            else _finite(raw_bound, f"{row_label}.upper_bound_seconds", positive=True)
        )
        result.append(
            LatencyHistogramBucket(
                start=start,
                end=end,
                subject=_name(row.get(subject_column), f"{row_label}.{subject_column}"),
                upper_bound_seconds=bound,
                cumulative_count=_finite(
                    row.get("cumulative_count"),
                    f"{row_label}.cumulative_count",
                    nonnegative=True,
                ),
            )
        )
    groups = {}
    identities = set()
    for bucket in result:
        group = (bucket.start, bucket.end, bucket.subject)
        identity = (*group, bucket.upper_bound_seconds)
        if identity in identities:
            raise ValueError(f"{label} contains a duplicate bucket for {identity}.")
        identities.add(identity)
        groups.setdefault(group, []).append(bucket)
    boundaries_by_subject = {}
    for key, buckets in groups.items():
        ordered = sorted(buckets, key=lambda item: item.upper_bound_seconds)
        if any(
            right.cumulative_count < left.cumulative_count
            for left, right in pairwise(ordered)
        ):
            raise ValueError(f"{label} cumulative counts decrease for {key}.")
        if not math.isinf(ordered[-1].upper_bound_seconds):
            raise ValueError(f"{label} must contain a +Inf bucket for {key}.")
        boundaries = tuple(bucket.upper_bound_seconds for bucket in ordered)
        previous = boundaries_by_subject.setdefault(key[2], boundaries)
        if boundaries != previous:
            raise ValueError(
                f"{label} uses inconsistent bucket boundaries for subject "
                f"{key[2]!r}."
            )
    return tuple(result)


def _parse_arrival_samples(path):
    rows = _csv_rows(
        path,
        {"timestamp", "request_class"},
        "external_arrival_samples",
    )
    result = tuple(
        ExternalArrivalSample(
            timestamp=_parse_time(
                row.get("timestamp"),
                f"external_arrival_samples row {index}.timestamp",
            ),
            request_class=_name(
                row.get("request_class"),
                f"external_arrival_samples row {index}.request_class",
            ),
        )
        for index, row in enumerate(rows, 2)
    )
    by_class = {}
    for item in result:
        by_class.setdefault(item.request_class, []).append(item.timestamp)
    for request_class, timestamps in by_class.items():
        if any(right <= left for left, right in pairwise(timestamps)):
            raise ValueError(
                "external_arrival_samples timestamps must be strictly increasing "
                f"within request class {request_class!r}."
            )
    return result


def _boolean(value, label):
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"{label} must be true or false.")


def _parse_capacity(path):
    rows = _csv_rows(
        path,
        {
            "run_id",
            "service",
            "replicas",
            "offered_rate_rps",
            "completed_rate_rps",
            "healthy",
        },
        "capacity_observations",
    )
    result = []
    identities = set()
    for index, row in enumerate(rows, 2):
        label = f"capacity_observations row {index}"
        replicas = int(_finite(row.get("replicas"), f"{label}.replicas", positive=True))
        if str(replicas) != str(row.get("replicas", "")).strip() and not math.isclose(
            float(row["replicas"]), replicas
        ):
            raise ValueError(f"{label}.replicas must be an integer.")
        observation = CapacityObservation(
            run_id=_name(row.get("run_id"), f"{label}.run_id"),
            service=_name(row.get("service"), f"{label}.service"),
            replicas=replicas,
            offered_rate_rps=_finite(
                row.get("offered_rate_rps"),
                f"{label}.offered_rate_rps",
                positive=True,
            ),
            completed_rate_rps=_finite(
                row.get("completed_rate_rps"),
                f"{label}.completed_rate_rps",
                nonnegative=True,
            ),
            healthy=_boolean(row.get("healthy"), f"{label}.healthy"),
        )
        identity = (observation.service, observation.replicas, observation.run_id)
        if identity in identities:
            raise ValueError(
                "capacity_observations contains duplicate "
                "(service, replicas, run_id) evidence."
            )
        identities.add(identity)
        result.append(observation)
    return tuple(result)


def parse_measurement_manifest(value, *, base_path):
    """Load the normalized CSV evidence named by a measurement manifest."""
    allowed = {
        "measurement_id",
        "methodology_version",
        "service_intervals",
        "service_class_intervals",
        "external_intervals",
        "service_edges",
        "service_latency_histograms",
        "external_latency_histograms",
        "external_arrival_samples",
        "capacity_observations",
    }
    if not isinstance(value, Mapping) or set(value) - allowed:
        raise ValueError(
            "measurement manifest contains unsupported fields: "
            f"{sorted(set(value) - allowed) if isinstance(value, Mapping) else []}."
        )
    missing = {
        "measurement_id",
        "methodology_version",
        "service_intervals",
    } - set(value)
    if missing:
        raise ValueError(f"measurement manifest is missing {sorted(missing)}.")
    measurement_id = _name(value["measurement_id"], "measurement_id")
    methodology_version = _name(
        value["methodology_version"],
        "methodology_version",
    )
    if methodology_version != MEASUREMENT_METHODOLOGY_VERSION:
        raise ValueError(
            "Unsupported measurement methodology version "
            f"{methodology_version!r}; expected "
            f"{MEASUREMENT_METHODOLOGY_VERSION!r}."
        )
    paths = {
        field: _resolve_source(value[field], field, base_path)
        for field in allowed - {"measurement_id", "methodology_version"}
        if field in value
    }
    fingerprints = tuple(
        sorted((field, _file_digest(path)) for field, path in paths.items())
    )
    measurements = MeasurementPackage(
        measurement_id=measurement_id,
        methodology_version=methodology_version,
        service_intervals=_parse_service_intervals(paths["service_intervals"]),
        service_class_intervals=(
            _parse_service_class_intervals(paths["service_class_intervals"])
            if "service_class_intervals" in paths
            else ()
        ),
        external_intervals=(
            _parse_external_intervals(paths["external_intervals"])
            if "external_intervals" in paths
            else ()
        ),
        service_edges=(
            _parse_edges(paths["service_edges"]) if "service_edges" in paths else ()
        ),
        service_latency_histograms=(
            _parse_histograms(
                paths["service_latency_histograms"],
                "service",
                "service_latency_histograms",
            )
            if "service_latency_histograms" in paths
            else ()
        ),
        external_latency_histograms=(
            _parse_histograms(
                paths["external_latency_histograms"],
                "request_class",
                "external_latency_histograms",
            )
            if "external_latency_histograms" in paths
            else ()
        ),
        external_arrival_samples=(
            _parse_arrival_samples(paths["external_arrival_samples"])
            if "external_arrival_samples" in paths
            else ()
        ),
        capacity_observations=(
            _parse_capacity(paths["capacity_observations"])
            if "capacity_observations" in paths
            else ()
        ),
        source_fingerprints=fingerprints,
    )
    _validate_interval_alignment(measurements)
    return measurements


def _fingerprint(*values):
    def normalize(value):
        if isinstance(value, float) and not math.isfinite(value):
            return "+Infinity" if value > 0.0 else "-Infinity"
        if isinstance(value, dict):
            return {str(key): normalize(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [normalize(item) for item in value]
        return value

    payload = json.dumps(
        [normalize(asdict(value)) for value in values],
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def parse_portable_sizing_inputs(
    workload,
    measurement_manifest,
    *,
    base_path,
    workload_base_path=None,
):
    parsed_workload = parse_workload_definition(
        workload,
        base_path=(base_path if workload_base_path is None else workload_base_path),
    )
    parsed_measurements = parse_measurement_manifest(
        measurement_manifest,
        base_path=base_path,
    )
    return PortableSizingInputs(
        workload=parsed_workload,
        measurements=parsed_measurements,
        fingerprint=_fingerprint(
            parsed_workload,
            parsed_measurements,
        ),
    )


def load_portable_sizing_inputs(workload_path, measurements_path):
    """Load a complete offline evidence package from two YAML files."""
    workload_path = Path(workload_path).expanduser().resolve()
    measurements_path = Path(measurements_path).expanduser().resolve()
    return parse_portable_sizing_inputs(
        _read_yaml(workload_path, "workload"),
        _read_yaml(measurements_path, "measurement manifest"),
        base_path=measurements_path.parent,
        workload_base_path=workload_path.parent,
    )

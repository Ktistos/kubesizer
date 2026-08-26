"""Derive portable, uncertainty-aware deployment characteristics from evidence."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict

import numpy as np

from metrics import student_t_quantile

from .models import (
    CapacityEnvelopePoint,
    DeploymentCharacterization,
    ExternalRequestClassCharacterization,
    LatencySummary,
    ServiceCharacterization,
)


CAPACITY_ENVELOPE_CONFIDENCE_LEVEL = 0.95


def _quantile(values, probability):
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * float(probability)
    lower = math.floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _moments(values):
    values = tuple(float(value) for value in values)
    if not values:
        return None, None
    mean = sum(values) / len(values)
    if mean <= 0.0:
        return mean, None
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return mean, variance / (mean * mean)


def _one_sided_lower_mean_bound(values, confidence_level):
    """Student-t lower bound over independent run-level observations."""
    values = tuple(float(value) for value in values)
    if len(values) < 2:
        return None
    mean = statistics.fmean(values)
    deviation = statistics.stdev(values)
    critical = student_t_quantile(confidence_level, len(values) - 1)
    return max(0.0, mean - critical * deviation / math.sqrt(len(values)))


def _capacity_envelope(observations):
    grouped = defaultdict(list)
    for observation in observations:
        grouped[observation.replicas].append(observation)
    points = []
    for replicas, items in sorted(grouped.items()):
        healthy = tuple(item for item in items if item.healthy)
        unhealthy = tuple(item for item in items if not item.healthy)
        completed = tuple(item.completed_rate_rps for item in healthy)
        points.append(
            CapacityEnvelopePoint(
                replicas=replicas,
                healthy_run_count=len(healthy),
                unhealthy_run_count=len(unhealthy),
                healthy_completed_rates_rps=completed,
                mean_healthy_completed_rate_rps=(
                    statistics.fmean(completed) if completed else None
                ),
                lower_mean_healthy_completed_rate_rps=(
                    _one_sided_lower_mean_bound(
                        completed,
                        CAPACITY_ENVELOPE_CONFIDENCE_LEVEL,
                    )
                ),
                lower_bound_confidence_level=(
                    CAPACITY_ENVELOPE_CONFIDENCE_LEVEL
                ),
                minimum_healthy_completed_rate_rps=(
                    min(completed) if completed else None
                ),
                maximum_healthy_offered_rate_rps=(
                    max(item.offered_rate_rps for item in healthy)
                    if healthy
                    else None
                ),
                minimum_unhealthy_offered_rate_rps=(
                    min(item.offered_rate_rps for item in unhealthy)
                    if unhealthy
                    else None
                ),
            )
        )
    return tuple(points)


def _histogram_quantile(buckets, probability):
    if not buckets:
        return None
    aggregated = defaultdict(float)
    for bucket in buckets:
        aggregated[bucket.upper_bound_seconds] += bucket.cumulative_count
    ordered = sorted(aggregated.items())
    total = ordered[-1][1]
    if total <= 0.0:
        return None
    target = total * probability
    previous_bound = 0.0
    previous_count = 0.0
    for bound, cumulative in ordered:
        if cumulative < target:
            if math.isfinite(bound):
                previous_bound = bound
            previous_count = cumulative
            continue
        if not math.isfinite(bound):
            return previous_bound if previous_bound > 0.0 else None
        bucket_count = cumulative - previous_count
        if bucket_count <= 0.0:
            return bound
        fraction = (target - previous_count) / bucket_count
        return previous_bound + fraction * (bound - previous_bound)
    return None


def _latency_summary(histograms, interval_rows):
    duration_sum = sum(
        row.response_duration_sum_seconds or 0.0 for row in interval_rows
    )
    duration_count = sum(
        row.request_count
        for row in interval_rows
        if row.response_duration_sum_seconds is not None
    )
    mean = duration_sum / duration_count if duration_count > 0.0 else None
    return LatencySummary(
        mean_seconds=mean,
        p50_seconds=_histogram_quantile(histograms, 0.50),
        p90_seconds=_histogram_quantile(histograms, 0.90),
        p95_seconds=_histogram_quantile(histograms, 0.95),
        p99_seconds=_histogram_quantile(histograms, 0.99),
    )


def _windowed_service_statistics(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row.service, row.start, row.end)].append(row)
    result = defaultdict(list)
    for (service, start, end), items in sorted(grouped.items()):
        duration = end - start
        requests = sum(item.request_count for item in items)
        cpu_seconds = sum(item.cpu_seconds for item in items)
        pods = len({item.pod for item in items})
        errors_4xx = sum(item.errors_4xx for item in items)
        errors_5xx = sum(item.errors_5xx for item in items)
        throttled = sum(item.throttled_seconds for item in items)
        active_values = [
            item.mean_active_requests
            for item in items
            if item.mean_active_requests is not None
        ]
        result[service].append(
            {
                "start": start,
                "end": end,
                "duration": duration,
                "requests": requests,
                "arrival_rate": requests / duration,
                "cpu_demand": cpu_seconds / requests if requests > 0.0 else None,
                "cpu_usage_per_replica": (
                    cpu_seconds / duration / pods if pods > 0 else None
                ),
                "throughput_per_replica": (
                    requests / duration / pods if pods > 0 else None
                ),
                "active_per_replica": (
                    sum(active_values) / pods if active_values and pods > 0 else None
                ),
                "errors_4xx": errors_4xx,
                "errors_5xx": errors_5xx,
                "throttling_ratio": (
                    throttled / (duration * pods) if pods > 0 else 0.0
                ),
            }
        )
    return result


def _aggregate_rate(rows, identity):
    grouped = defaultdict(lambda: [0.0, set()])
    for row in rows:
        key = identity(row)
        grouped[key][0] += row.request_count
        grouped[key][1].add((row.start, row.end))
    result = {}
    for key, (count, intervals) in grouped.items():
        duration = sum(end - start for start, end in intervals)
        result[key] = count / duration if duration > 0.0 else 0.0
    return result


def _external_rates(rows):
    return _aggregate_rate(rows, lambda row: row.request_class)


def _aggregate_external_rate(rows):
    if not rows:
        return None
    grouped = defaultdict(float)
    for row in rows:
        grouped[(row.start, row.end)] += row.request_count
    total_duration = sum(end - start for start, end in grouped)
    return sum(grouped.values()) / total_duration if total_duration > 0.0 else None


def _service_class_rates(rows):
    classified = tuple(row for row in rows if row.request_class is not None)
    if not classified:
        return {}
    return _aggregate_rate(classified, lambda row: (row.request_class, row.service))


def _edge_rates(rows, *, request_class=None):
    selected = tuple(
        row
        for row in rows
        if request_class is None or row.request_class == request_class
    )
    grouped = defaultdict(lambda: [0.0, set()])
    for row in selected:
        key = (row.source_service, row.destination_service)
        grouped[key][0] += row.call_count
        grouped[key][1].add((row.start, row.end))
    return {
        key: count / sum(end - start for start, end in intervals)
        for key, (count, intervals) in grouped.items()
        if intervals
    }


def _visits_from_calls(service_names, entries, calls, request_class):
    index = {name: position for position, name in enumerate(service_names)}
    matrix = np.zeros((len(service_names), len(service_names)), dtype=float)
    for source, targets in calls.items():
        for target, count in targets.items():
            matrix[index[source], index[target]] = count
    spectral_radius = max(abs(np.linalg.eigvals(matrix)), default=0.0)
    if spectral_radius >= 1.0 - 1e-12:
        raise ValueError(
            f"Request class {request_class!r} has a non-terminating measured "
            "call graph (spectral radius is not below one)."
        )
    external = np.zeros(len(service_names), dtype=float)
    for service, weight in entries.items():
        external[index[service]] = weight
    visits = np.linalg.solve(np.eye(len(service_names)) - matrix.T, external)
    return {
        service: float(visits[position])
        for service, position in index.items()
        if visits[position] > 1e-12
    }


def _derive_class_flows(inputs, service_rates, external_rates):
    service_names = inputs.deployment.service_names
    class_service_rates = _service_class_rates(
        inputs.measurements.service_class_intervals
    )
    class_names = {item.name for item in inputs.workload.request_classes}
    calls_by_class = {}
    visits_by_class = {}
    warnings = []

    for request_class in inputs.workload.request_classes:
        explicit_visits = request_class.visit_ratios
        explicit_calls = request_class.call_graph
        if explicit_visits:
            visits_by_class[request_class.name] = explicit_visits
        if explicit_calls:
            calls_by_class[request_class.name] = explicit_calls
            visits_by_class.setdefault(
                request_class.name,
                _visits_from_calls(
                    service_names,
                    request_class.entry_services,
                    explicit_calls,
                    request_class.name,
                ),
            )
            continue

        class_edges = _edge_rates(
            inputs.measurements.service_edges,
            request_class=request_class.name,
        )
        if not class_edges and len(class_names) == 1:
            class_edges = _edge_rates(inputs.measurements.service_edges)
        denominators = {
            service: class_service_rates.get((request_class.name, service))
            for service in service_names
        }
        if len(class_names) == 1:
            denominators = {
                service: denominators[service] or service_rates.get(service)
                for service in service_names
            }
        derived_calls = defaultdict(dict)
        for (source, destination), rate in class_edges.items():
            denominator = denominators.get(source)
            if denominator and denominator > 0.0:
                derived_calls[source][destination] = rate / denominator
        if derived_calls:
            calls = {source: dict(targets) for source, targets in derived_calls.items()}
            calls_by_class[request_class.name] = calls
            visits_by_class.setdefault(
                request_class.name,
                _visits_from_calls(
                    service_names,
                    request_class.entry_services,
                    calls,
                    request_class.name,
                ),
            )

        if request_class.name not in visits_by_class:
            external_rate = external_rates.get(request_class.name)
            measured = {
                service: class_service_rates.get((request_class.name, service), 0.0)
                for service in service_names
            }
            if external_rate and all(value >= 0.0 for value in measured.values()):
                visits = {
                    service: value / external_rate
                    for service, value in measured.items()
                    if value > 0.0
                }
                if visits:
                    visits_by_class[request_class.name] = visits

        if request_class.name not in visits_by_class:
            warnings.append(
                f"No request-class service visits could be derived for "
                f"{request_class.name!r}."
            )

    return visits_by_class, calls_by_class, tuple(warnings)


def characterize_deployment(inputs):
    """Produce normalized evidence without fitting facts the data cannot identify."""
    declared_services = set(inputs.deployment.service_names)
    measured_services = {row.service for row in inputs.measurements.service_intervals}
    class_rate_services = {
        row.service for row in inputs.measurements.service_class_intervals
    }
    edge_services = {
        name
        for row in inputs.measurements.service_edges
        for name in (row.source_service, row.destination_service)
    }
    capacity_services = {
        row.service for row in inputs.measurements.capacity_observations
    }
    histogram_services = {
        row.subject for row in inputs.measurements.service_latency_histograms
    }
    unknown = (
        measured_services
        | class_rate_services
        | edge_services
        | capacity_services
        | histogram_services
    ) - declared_services
    if unknown:
        raise ValueError(
            "Measurements reference services absent from deployment: "
            f"{sorted(unknown)}."
        )
    missing = declared_services - measured_services
    if missing:
        raise ValueError(
            f"service_intervals contain no measurements for services {sorted(missing)}."
        )
    deployment_namespaces = {
        service.name: service.namespace for service in inputs.deployment.services
    }
    mismatched_namespaces = sorted(
        {
            (row.service, row.namespace, deployment_namespaces.get(row.service))
            for row in inputs.measurements.service_intervals
            if row.service in deployment_namespaces
            and row.namespace != deployment_namespaces[row.service]
        }
    )
    if mismatched_namespaces:
        raise ValueError(
            "service_intervals namespace does not match the deployment for: "
            f"{mismatched_namespaces}."
        )
    pod_counts = defaultdict(dict)
    for row in inputs.measurements.service_intervals:
        pod_counts[row.service].setdefault((row.start, row.end), set()).add(
            (row.namespace, row.pod)
        )
    observed_replica_counts = {}
    variable_replica_services = []
    for service in inputs.deployment.services:
        counts = {
            len(pods) for pods in pod_counts[service.name].values()
        }
        if len(counts) != 1:
            variable_replica_services.append((service.name, sorted(counts)))
        else:
            observed_replica_counts[service.name] = counts.pop()
    if variable_replica_services:
        raise ValueError(
            "service_intervals must cover a fixed-replica period; observed pod "
            f"counts changed for: {variable_replica_services}."
        )
    declared_classes = {item.name for item in inputs.workload.request_classes}
    measured_classes = {
        value
        for value in (
            [
                row.request_class
                for row in inputs.measurements.service_class_intervals
            ]
            + [row.request_class for row in inputs.measurements.external_intervals]
            + [row.request_class for row in inputs.measurements.service_edges]
            + [
                row.request_class
                for row in inputs.measurements.external_arrival_samples
            ]
            + [row.subject for row in inputs.measurements.external_latency_histograms]
        )
        if value is not None
    }
    unknown_classes = measured_classes - declared_classes
    if unknown_classes:
        raise ValueError(
            "Measurements reference request classes absent from the target: "
            f"{sorted(unknown_classes)}."
        )

    windows = _windowed_service_statistics(inputs.measurements.service_intervals)
    service_rates = {
        service: (
            sum(item["requests"] for item in values)
            / sum(item["duration"] for item in values)
        )
        for service, values in windows.items()
    }
    external_rates = _external_rates(inputs.measurements.external_intervals)
    observed_external_rate = _aggregate_external_rate(
        inputs.measurements.external_intervals
    )
    if observed_external_rate and all(
        request_class.name in external_rates
        for request_class in inputs.workload.request_classes
    ):
        mix_mismatches = []
        for request_class in inputs.workload.request_classes:
            measured_weight = (
                external_rates[request_class.name] / observed_external_rate
            )
            if abs(measured_weight - request_class.concurrency_weight) > 0.05:
                mix_mismatches.append(
                    (
                        request_class.name,
                        request_class.concurrency_weight,
                        measured_weight,
                    )
                )
        if mix_mismatches:
            raise ValueError(
                "Measured request-class shares differ from concurrency_weight "
                f"by more than 0.05: {mix_mismatches}."
            )
    visits_by_class, calls_by_class, flow_warnings = _derive_class_flows(
        inputs,
        service_rates,
        external_rates,
    )
    # The recommendation is for the measured workload. Observed destination
    # rates are therefore the sizing rates; a separate forecast multiplier is
    # intentionally not accepted by this workflow.
    rate_source = "measured_workload"
    workload_rates = dict(service_rates)

    flow_balance_errors = {service: None for service in inputs.deployment.service_names}
    if all(
        request_class.name in external_rates
        for request_class in inputs.workload.request_classes
    ) and all(
        request_class.name in visits_by_class
        for request_class in inputs.workload.request_classes
    ):
        predicted_observed_rates = {
            service: sum(
                external_rates[request_class.name]
                * visits_by_class[request_class.name].get(service, 0.0)
                for request_class in inputs.workload.request_classes
            )
            for service in inputs.deployment.service_names
        }
        flow_balance_errors = {
            service: abs(predicted_observed_rates[service] - service_rates[service])
            / max(service_rates[service], 1e-12)
            for service in inputs.deployment.service_names
        }

    aggregate_edges = _edge_rates(inputs.measurements.service_edges)
    histogram_by_service = defaultdict(list)
    for bucket in inputs.measurements.service_latency_histograms:
        histogram_by_service[bucket.subject].append(bucket)
    interval_by_service = defaultdict(list)
    for row in inputs.measurements.service_intervals:
        interval_by_service[row.service].append(row)
    capacity_by_service = defaultdict(list)
    for observation in inputs.measurements.capacity_observations:
        capacity_by_service[observation.service].append(observation)

    services = []
    global_warnings = list(flow_warnings)
    for deployment_service in inputs.deployment.services:
        name = deployment_service.name
        service_windows = windows[name]
        window_demands = tuple(
            item["cpu_demand"]
            for item in service_windows
            if item["cpu_demand"] is not None and item["cpu_demand"] > 0.0
        )
        # These are observation-window means U/X. Prometheus counters cannot
        # identify the distribution of per-request CPU work, so they are never
        # re-labelled or replaced by synthetic per-request samples.
        demand_samples = window_demands
        mean_demand, demand_scv = _moments(demand_samples)
        total_requests = sum(item["requests"] for item in service_windows)
        total_errors = sum(
            item["errors_4xx"] + item["errors_5xx"] for item in service_windows
        )
        total_errors_4xx = sum(item["errors_4xx"] for item in service_windows)
        total_errors_5xx = sum(item["errors_5xx"] for item in service_windows)
        throttled = sum(
            item["throttling_ratio"] * item["duration"] for item in service_windows
        )
        throttling_ratio = (
            throttled / sum(item["duration"] for item in service_windows)
            if service_windows
            else 0.0
        )
        healthy_capacity = [
            item.completed_rate_rps / item.replicas
            for item in capacity_by_service[name]
            if item.healthy and item.completed_rate_rps > 0.0
        ]
        capacity_envelope = _capacity_envelope(capacity_by_service[name])
        outgoing = {
            destination: rate
            for (source, destination), rate in aggregate_edges.items()
            if source == name
        }
        outgoing_total = sum(outgoing.values())
        routing = {
            destination: rate / outgoing_total
            for destination, rate in outgoing.items()
            if outgoing_total > 0.0
        }
        visits = {
            destination: rate / service_rates[name]
            for destination, rate in outgoing.items()
            if service_rates[name] > 0.0
        }
        warnings = []
        if len(service_windows) < 3:
            warnings.append("fewer than three measurement windows")
        if not demand_samples:
            warnings.append("CPU demand could not be derived")
        if throttling_ratio > 0.05:
            warnings.append("measurement contains material CPU throttling")
        if not healthy_capacity:
            warnings.append("no explicit healthy capacity experiment")
        elif not any(
            point.lower_mean_healthy_completed_rate_rps is not None
            for point in capacity_envelope
        ):
            warnings.append(
                "capacity evidence has fewer than two independent healthy runs "
                "at every observed replica count"
            )
        if deployment_service.cpu_limit_cores is not None:
            warnings.append(
                "CPU sizing uses the Kubernetes CPU request as guaranteed "
                "capacity; the CPU limit is only a ceiling"
            )
        if flow_balance_errors[name] is not None and flow_balance_errors[name] > 0.20:
            warnings.append(
                "request-class flow prediction differs from observed service rate "
                "by more than 20%"
            )
        if outgoing:
            warnings.append(
                "normalized routing probabilities are descriptive only when each "
                "source request selects exactly one next hop"
            )
        memory_values = tuple(
            row.memory_bytes
            for row in interval_by_service[name]
            if row.memory_bytes is not None
        )
        services.append(
            ServiceCharacterization(
                service=name,
                observed_replica_count=observed_replica_counts[name],
                observed_arrival_rate_rps=service_rates[name],
                workload_arrival_rate_rps=workload_rates[name],
                flow_balance_relative_error=flow_balance_errors[name],
                cpu_demand_window_means_seconds=demand_samples,
                cpu_demand_mean_seconds=mean_demand,
                cpu_demand_safe_seconds=_quantile(demand_samples, 0.90),
                cpu_demand_window_scv=demand_scv,
                observed_cpu_usage_cores_per_replica=tuple(
                    item["cpu_usage_per_replica"]
                    for item in service_windows
                    if item["cpu_usage_per_replica"] is not None
                ),
                observed_throughput_per_replica_rps=tuple(
                    item["throughput_per_replica"]
                    for item in service_windows
                    if item["throughput_per_replica"] is not None
                ),
                maximum_healthy_throughput_per_replica_rps=(
                    max(healthy_capacity) if healthy_capacity else None
                ),
                capacity_envelope=capacity_envelope,
                observed_mean_active_requests_per_replica=tuple(
                    item["active_per_replica"]
                    for item in service_windows
                    if item["active_per_replica"] is not None
                ),
                error_rate=(total_errors / total_requests if total_requests else 0.0),
                error_rate_4xx=(
                    total_errors_4xx / total_requests if total_requests else 0.0
                ),
                error_rate_5xx=(
                    total_errors_5xx / total_requests if total_requests else 0.0
                ),
                throttling_ratio=throttling_ratio,
                mean_memory_bytes_per_replica=(
                    sum(memory_values) / len(memory_values) if memory_values else None
                ),
                maximum_memory_bytes_per_replica=(
                    max(memory_values) if memory_values else None
                ),
                latency=_latency_summary(
                    histogram_by_service[name],
                    interval_by_service[name],
                ),
                outgoing_call_rates=tuple(sorted(outgoing.items())),
                routing_probabilities=tuple(sorted(routing.items())),
                visits_per_service_request=tuple(sorted(visits.items())),
                measurement_window_count=len(service_windows),
                warnings=tuple(warnings),
            )
        )

    external_histograms = defaultdict(list)
    for bucket in inputs.measurements.external_latency_histograms:
        external_histograms[bucket.subject].append(bucket)
    external_rows = defaultdict(list)
    for row in inputs.measurements.external_intervals:
        external_rows[row.request_class].append(row)
    external_characterizations = []
    for request_class in inputs.workload.request_classes:
        rows = external_rows[request_class.name]
        total_requests = sum(row.request_count for row in rows)
        external_characterizations.append(
            ExternalRequestClassCharacterization(
                request_class=request_class.name,
                observed_arrival_rate_rps=external_rates.get(request_class.name),
                error_rate_4xx=(
                    sum(row.errors_4xx for row in rows) / total_requests
                    if total_requests > 0.0
                    else None
                ),
                error_rate_5xx=(
                    sum(row.errors_5xx for row in rows) / total_requests
                    if total_requests > 0.0
                    else None
                ),
                latency=_latency_summary(
                    external_histograms[request_class.name],
                    rows,
                ),
            )
        )
    capabilities = (
        (
            "class_specific_service_rates",
            bool(_service_class_rates(inputs.measurements.service_class_intervals)),
        ),
        ("external_request_rates", bool(inputs.measurements.external_intervals)),
        (
            "external_latency_histograms",
            bool(inputs.measurements.external_latency_histograms),
        ),
        ("raw_external_arrivals", bool(inputs.measurements.external_arrival_samples)),
        (
            "aligned_cpu_demand_windows",
            all(service.cpu_demand_window_means_seconds for service in services),
        ),
        ("service_edges", bool(inputs.measurements.service_edges)),
        (
            "service_latency_histograms",
            bool(inputs.measurements.service_latency_histograms),
        ),
        (
            "explicit_capacity_observations",
            bool(inputs.measurements.capacity_observations),
        ),
        (
            "concurrency_measurements",
            any(
                service.observed_mean_active_requests_per_replica
                for service in services
            ),
        ),
    )
    return DeploymentCharacterization(
        input_fingerprint=inputs.fingerprint,
        measurement_methodology_version=(
            inputs.measurements.methodology_version
        ),
        rate_source=rate_source,
        services=tuple(services),
        observed_external_arrival_rate_rps=observed_external_rate,
        external_request_classes=tuple(external_characterizations),
        request_class_visits=tuple(
            sorted(
                (
                    request_class,
                    tuple(sorted(visits.items())),
                )
                for request_class, visits in visits_by_class.items()
            )
        ),
        request_class_calls=tuple(
            sorted(
                (
                    request_class,
                    tuple(
                        sorted(
                            (source, tuple(sorted(targets.items())))
                            for source, targets in calls.items()
                        )
                    ),
                )
                for request_class, calls in calls_by_class.items()
            )
        ),
        capabilities=capabilities,
        warnings=tuple(global_warnings),
    )

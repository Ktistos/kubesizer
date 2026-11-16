"""Joint-confidence replica floor from portable workload evidence."""

from __future__ import annotations

import math
import statistics

from metrics import student_t_quantile

from .models import (
    CapacityPointConfidenceBound,
    ServiceStabilityAssessment,
    StabilityFloorStatus,
    StatisticalStabilityFloor,
)
from .sizing import _validate_policy, resolve_sizing_policy


def _one_sided_mean_bound(values, confidence_level, *, upper):
    values = tuple(float(value) for value in values)
    if len(values) < 2:
        return None
    mean = statistics.fmean(values)
    deviation = statistics.stdev(values)
    critical = student_t_quantile(confidence_level, len(values) - 1)
    offset = critical * deviation / math.sqrt(len(values))
    return max(0.0, mean + offset if upper else mean - offset)


def _normalize_configuration(value, service_count, label):
    values = tuple(value)
    if len(values) != service_count or any(
        isinstance(item, bool) or not isinstance(item, int) or item <= 0
        for item in values
    ):
        raise ValueError(f"{label} must define one positive integer per service.")
    return values


def _service_arrival_samples(inputs, characterization, service):
    visits_by_class = {
        request_class: dict(visits)
        for request_class, visits in characterization.request_class_visits
    }
    declared_classes = tuple(
        request_class.name for request_class in inputs.workload.request_classes
    )
    if any(request_class not in visits_by_class for request_class in declared_classes):
        return ()
    multiplier = inputs.workload.maximum_load_multiplier
    return tuple(
        multiplier
        * sum(
            dict(window.request_class_arrival_rates)[request_class]
            * visits_by_class[request_class].get(service, 0.0)
            for request_class in declared_classes
        )
        for window in characterization.aligned_input_windows
    )


def estimate_statistical_stability_floor(
    inputs,
    characterization,
    *,
    analytical_minimum,
    maximum_configuration,
    service_order=None,
    policy=None,
):
    """Estimate the smallest confidence-bounded stable replica vector.

    Arrival uncertainty is represented by an upper one-sided Student-t bound
    over complete aligned workload windows.  Capacity uncertainty is
    represented by lower one-sided Student-t bounds over independent healthy
    fixed-replica load-test runs.  Bonferroni allocation makes all bounds joint
    at ``SizingPolicy.capacity_confidence_level`` without assuming that service
    estimates are mutually independent.

    Capacity observations are normalized per replica.  Taking the smallest
    lower bound seen at any tested replica count makes observed scaling loss
    part of the floor.  Applying that rate to an untested count remains an
    explicit identical-replica/scaling assumption; it is not an empirical
    guarantee for the untested configuration.
    """
    policy = resolve_sizing_policy(inputs, policy)
    _validate_policy(policy)
    if characterization.input_fingerprint != inputs.fingerprint:
        raise ValueError("Characterization belongs to a different evidence package.")

    service_order = tuple(service_order or inputs.workload.service_names)
    declared_services = tuple(inputs.workload.service_names)
    if len(service_order) != len(declared_services) or set(service_order) != set(
        declared_services
    ):
        raise ValueError("service_order must contain every deployment service once.")
    analytical_minimum = _normalize_configuration(
        analytical_minimum,
        len(service_order),
        "analytical_minimum",
    )
    maximum = _normalize_configuration(
        maximum_configuration,
        len(service_order),
        "maximum_configuration",
    )
    constraints = dict(inputs.workload.replica_constraints)
    maximum = tuple(
        min(high, constraints[name][1]) if name in constraints else high
        for name, high in zip(service_order, maximum)
    )
    analytical_minimum = tuple(
        max(low, constraints[name][0]) if name in constraints else low
        for name, low in zip(service_order, analytical_minimum)
    )
    if any(high < low for low, high in zip(analytical_minimum, maximum)):
        raise ValueError("Maximum bounds cannot be below analytical stability.")

    evidence_by_service = characterization.service_map
    adequate_points = {
        service: tuple(
            point
            for point in evidence_by_service[service].capacity_envelope
            if point.healthy_run_count >= policy.minimum_capacity_runs_per_replica
        )
        for service in service_order
    }
    positive_services = tuple(
        service
        for service in service_order
        if evidence_by_service[service].workload_arrival_rate_rps > 0.0
    )
    bound_count = len(positive_services) + sum(
        len(adequate_points[service]) for service in positive_services
    )
    overall_alpha = 1.0 - policy.capacity_confidence_level
    individual_confidence = (
        1.0 - overall_alpha / bound_count
        if bound_count
        else policy.capacity_confidence_level
    )

    assessments = []
    floor = []
    overall_status = StabilityFloorStatus.CONFIDENCE_BOUNDED_STABLE
    overall_warnings = []
    for index, service in enumerate(service_order):
        evidence = evidence_by_service[service]
        analytical_floor = analytical_minimum[index]
        upper_limit = maximum[index]
        arrivals = _service_arrival_samples(inputs, characterization, service)
        warnings = []
        point_bounds = []
        status = StabilityFloorStatus.CONFIDENCE_BOUNDED_STABLE
        reason = None
        required = None
        arrival_mean = statistics.fmean(arrivals) if arrivals else None
        arrival_upper = None
        service_rate_lower = None
        usable_rate_lower = None
        margin = None

        if evidence.workload_arrival_rate_rps <= 0.0:
            required = analytical_floor
            arrival_mean = 0.0
            arrival_upper = 0.0
            margin = 0.0
        elif len(arrivals) < policy.minimum_measurement_windows:
            status = StabilityFloorStatus.INSUFFICIENT_EVIDENCE
            reason = "too few complete aligned arrival windows for a confidence bound"
        elif not any(value > 0.0 for value in arrivals):
            status = StabilityFloorStatus.INSUFFICIENT_EVIDENCE
            reason = (
                "service traffic is positive but request-class visits produce no "
                "positive aligned arrival samples"
            )
        elif not adequate_points[service]:
            status = StabilityFloorStatus.INSUFFICIENT_EVIDENCE
            reason = "no replica count has enough independent healthy capacity runs"
        else:
            arrival_upper = _one_sided_mean_bound(
                arrivals,
                individual_confidence,
                upper=True,
            )
            for point in adequate_points[service]:
                normalized = tuple(
                    rate / point.replicas for rate in point.healthy_completed_rates_rps
                )
                lower = _one_sided_mean_bound(
                    normalized,
                    individual_confidence,
                    upper=False,
                )
                if lower is None or lower <= 0.0:
                    continue
                point_bounds.append(
                    CapacityPointConfidenceBound(
                        replicas=point.replicas,
                        healthy_run_count=len(normalized),
                        completed_rates_rps_per_replica=normalized,
                        mean_completed_rate_rps_per_replica=statistics.fmean(
                            normalized
                        ),
                        lower_bound_rps_per_replica=lower,
                        confidence_level=individual_confidence,
                    )
                )
            if arrival_upper is None or not point_bounds:
                status = StabilityFloorStatus.INSUFFICIENT_EVIDENCE
                reason = "a finite positive one-sided confidence bound was unavailable"
            else:
                bounds_by_count = {point.replicas: point for point in point_bounds}
                source_points = {
                    point.replicas: point for point in adequate_points[service]
                }
                worst_observed_rate = min(
                    point.lower_bound_rps_per_replica for point in point_bounds
                )
                selected_rate = None
                for candidate in range(analytical_floor, upper_limit + 1):
                    exact_bound = bounds_by_count.get(candidate)
                    candidate_rate = (
                        exact_bound.lower_bound_rps_per_replica
                        if exact_bound is not None
                        else worst_observed_rate
                    )
                    candidate_usable_rate = (
                        candidate_rate * policy.empirical_capacity_utilization
                    )
                    exact_source = source_points.get(candidate)
                    conflicts_with_failure = (
                        exact_source is not None
                        and exact_source.minimum_unhealthy_offered_rate_rps is not None
                        and arrival_upper
                        >= exact_source.minimum_unhealthy_offered_rate_rps
                    )
                    if (
                        not conflicts_with_failure
                        and candidate * candidate_usable_rate > arrival_upper
                    ):
                        required = candidate
                        selected_rate = candidate_rate
                        break
                if required is None:
                    status = StabilityFloorStatus.OUTSIDE_REPLICA_BOUNDS
                    reason = (
                        "no candidate up to the configured maximum "
                        f"{upper_limit} has a positive confidence-bounded "
                        "capacity margin"
                    )
                else:
                    service_rate_lower = selected_rate
                    usable_rate_lower = (
                        service_rate_lower * policy.empirical_capacity_utilization
                    )
                    margin = required * usable_rate_lower - arrival_upper
                    observed_counts = {point.replicas for point in point_bounds}
                    if required not in observed_counts:
                        warnings.append(
                            "required replica count was not directly capacity-tested; "
                            "the result assumes per-replica capacity does not fall "
                            "below "
                            "the worst confidence bound observed at tested counts"
                        )

        if status != StabilityFloorStatus.CONFIDENCE_BOUNDED_STABLE:
            overall_status = (
                StabilityFloorStatus.OUTSIDE_REPLICA_BOUNDS
                if status == StabilityFloorStatus.OUTSIDE_REPLICA_BOUNDS
                else (
                    overall_status
                    if overall_status == StabilityFloorStatus.OUTSIDE_REPLICA_BOUNDS
                    else StabilityFloorStatus.INSUFFICIENT_EVIDENCE
                )
            )
        else:
            floor.append(required)
        assessments.append(
            ServiceStabilityAssessment(
                service=service,
                status=status,
                analytical_minimum_replicas=analytical_floor,
                maximum_replicas=upper_limit,
                required_replicas=required,
                arrival_rate_samples_rps=arrivals,
                arrival_rate_mean_rps=arrival_mean,
                arrival_rate_upper_bound_rps=arrival_upper,
                capacity_bounds=tuple(point_bounds),
                service_rate_lower_bound_rps_per_replica=service_rate_lower,
                usable_service_rate_lower_bound_rps_per_replica=usable_rate_lower,
                stability_margin_lower_bound_rps=margin,
                limiting_reason=reason,
                warnings=tuple(warnings),
            )
        )
        overall_warnings.extend(f"{service}: {warning}" for warning in warnings)

    configuration = (
        tuple(floor)
        if overall_status == StabilityFloorStatus.CONFIDENCE_BOUNDED_STABLE
        else None
    )
    return StatisticalStabilityFloor(
        input_fingerprint=inputs.fingerprint,
        method="BONFERRONI_ONE_SIDED_T_CAPACITY_FLOOR",
        status=overall_status,
        overall_confidence_level=policy.capacity_confidence_level,
        individual_bound_confidence_level=individual_confidence,
        multiplicity_correction="BONFERRONI_OVER_SERVICE_ARRIVAL_AND_CAPACITY_BOUNDS",
        maximum_capacity_utilization=policy.empirical_capacity_utilization,
        analytical_minimum_configuration=analytical_minimum,
        minimum_stable_configuration=configuration,
        maximum_configuration=maximum,
        services=tuple(assessments),
        assumptions=(
            "aligned workload windows are representative and independent enough "
            "for Student-t mean bounds",
            "fixed-replica capacity runs are independent and their healthy labels "
            "use predeclared stationarity and failure gates",
            "future traffic remains inside the measured workload and forecast "
            "envelope",
            "per-replica effective capacity at an untested count is no lower than "
            "the worst bounded per-replica capacity observed",
            "routing, service behavior, shared dependencies, and cluster resource "
            "conditions remain materially unchanged",
        ),
        warnings=tuple(overall_warnings),
    )

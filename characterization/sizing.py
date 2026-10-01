"""Conservative initial replica sizing from portable operational evidence."""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from dataclasses import asdict

from metrics import student_t_quantile

from .models import (
    InitialSizingRecommendation,
    ServiceSizingRecommendation,
    SizingPolicy,
)


def _quantile(values, probability):
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _validate_policy(policy):
    for name in (
        "target_cpu_utilization",
        "empirical_capacity_utilization",
        "cpu_demand_quantile",
        "capacity_confidence_level",
    ):
        value = float(getattr(policy, name))
        if not math.isfinite(value) or not 0.0 < value < 1.0:
            raise ValueError(f"{name} must lie strictly between zero and one.")
    if (
        not math.isfinite(float(policy.demand_uncertainty_multiplier))
        or policy.demand_uncertainty_multiplier < 1.0
    ):
        raise ValueError("demand_uncertainty_multiplier must be at least one.")
    if (
        isinstance(policy.minimum_measurement_windows, bool)
        or not isinstance(policy.minimum_measurement_windows, int)
        or policy.minimum_measurement_windows <= 0
    ):
        raise ValueError("minimum_measurement_windows must be a positive integer.")
    if (
        not math.isfinite(float(policy.maximum_accepted_throttling_ratio))
        or not 0.0 <= policy.maximum_accepted_throttling_ratio <= 1.0
    ):
        raise ValueError(
            "maximum_accepted_throttling_ratio must lie between zero and one."
        )
    if (
        isinstance(policy.minimum_capacity_runs_per_replica, bool)
        or not isinstance(policy.minimum_capacity_runs_per_replica, int)
        or policy.minimum_capacity_runs_per_replica < 2
    ):
        raise ValueError(
            "minimum_capacity_runs_per_replica must be an integer of at least two."
        )


def _one_sided_lower_mean_bound(values, confidence_level):
    values = tuple(float(value) for value in values)
    if len(values) < 2:
        return None
    mean = statistics.fmean(values)
    deviation = statistics.stdev(values)
    critical = student_t_quantile(confidence_level, len(values) - 1)
    return max(0.0, mean - critical * deviation / math.sqrt(len(values)))


def _policy_fingerprint(policy):
    return hashlib.sha256(
        json.dumps(asdict(policy), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def resolve_sizing_policy(inputs, policy=None):
    """Use an explicit API policy or the utilization declared by the workload."""
    if policy is not None:
        return policy
    return SizingPolicy(
        empirical_capacity_utilization=(inputs.workload.maximum_capacity_utilization)
    )


def recommend_initial_replicas(inputs, characterization, *, policy=None):
    """Return a conservative, idempotent starting vector rather than an optimum.

    The independent CPU and demonstrated-capacity calculations are
    intentionally combined with ``max``. Missing evidence never produces an
    optimistic zero-replica constraint.
    """
    policy = resolve_sizing_policy(inputs, policy)
    _validate_policy(policy)
    if characterization.input_fingerprint != inputs.fingerprint:
        raise ValueError("Characterization belongs to a different evidence package.")

    service_characterizations = characterization.service_map
    recommendations = []
    overall_warnings = list(characterization.warnings)
    strong_evidence = True
    detailed_evidence = True
    for deployment_service in inputs.workload.services:
        evidence = service_characterizations[deployment_service.name]
        workload_rate = evidence.workload_arrival_rate_rps
        minimum_replicas = 1
        warnings = list(evidence.warnings)

        safe_demand = _quantile(
            evidence.cpu_demand_window_means_seconds,
            policy.cpu_demand_quantile,
        )
        if safe_demand is not None:
            safe_demand *= policy.demand_uncertainty_multiplier
            cpu_based = max(
                minimum_replicas,
                math.ceil(
                    workload_rate
                    * safe_demand
                    / (
                        deployment_service.cpu_capacity_cores
                        * policy.target_cpu_utilization
                    )
                ),
            )
        elif workload_rate <= 0.0:
            cpu_based = minimum_replicas
        else:
            cpu_based = None

        empirical_based = None
        empirical_evidence_replicas = []
        adequate_capacity_points = []
        for point in evidence.capacity_envelope:
            if point.healthy_run_count < policy.minimum_capacity_runs_per_replica:
                continue
            lower_capacity = _one_sided_lower_mean_bound(
                point.healthy_completed_rates_rps,
                policy.capacity_confidence_level,
            )
            if lower_capacity is None or lower_capacity <= 0.0:
                continue
            empirical_evidence_replicas.append(point.replicas)
            adequate_capacity_points.append((point, lower_capacity))
            conflicting_failure = (
                point.minimum_unhealthy_offered_rate_rps is not None
                and workload_rate >= point.minimum_unhealthy_offered_rate_rps
            )
            if (
                empirical_based is None
                and not conflicting_failure
                and workload_rate
                <= lower_capacity * policy.empirical_capacity_utilization
            ):
                empirical_based = point.replicas

        empirical_extrapolated = bool(
            workload_rate > 0.0 and adequate_capacity_points and empirical_based is None
        )
        if empirical_extrapolated:
            warnings.append(
                "target traffic lies outside the statistically supported "
                "capacity envelope; no linear replica extrapolation was used"
            )
        elif evidence.capacity_envelope and not adequate_capacity_points:
            warnings.append(
                "capacity observations are insufficient for the configured "
                "run-level confidence bound"
            )

        candidates = {
            "one_replica_floor": minimum_replicas,
            "cpu": cpu_based,
            "empirical_capacity": empirical_based,
        }
        available = {
            name: value for name, value in candidates.items() if value is not None
        }
        if workload_rate > 0.0 and len(available) == 1:
            raise ValueError(
                f"Service {deployment_service.name!r} has measured traffic but "
                "no CPU-demand or demonstrated-capacity evidence."
            )
        recommended = max(available.values())
        limiting = tuple(
            name for name, value in available.items() if value == recommended
        )
        estimated_cpu_utilization = (
            workload_rate
            * safe_demand
            / (recommended * deployment_service.cpu_capacity_cores)
            if safe_demand is not None and recommended > 0
            else None
        )

        if evidence.measurement_window_count < policy.minimum_measurement_windows:
            strong_evidence = False
            warnings.append(
                "measurement-window count is below the sizing policy minimum"
            )
        if evidence.throttling_ratio > policy.maximum_accepted_throttling_ratio:
            strong_evidence = False
            warnings.append("CPU throttling exceeds the sizing policy's accepted ratio")
        if cpu_based is None:
            strong_evidence = False
        if empirical_based is None:
            detailed_evidence = False
        recommendations.append(
            ServiceSizingRecommendation(
                service=deployment_service.name,
                recommended_replicas=recommended,
                cpu_based_replicas=cpu_based,
                empirical_capacity_based_replicas=empirical_based,
                empirical_capacity_evidence_replicas=tuple(empirical_evidence_replicas),
                empirical_capacity_extrapolated=empirical_extrapolated,
                cpu_capacity_cores_per_replica=(deployment_service.cpu_capacity_cores),
                cpu_capacity_basis=deployment_service.cpu_capacity_basis,
                workload_arrival_rate_rps=workload_rate,
                estimated_cpu_utilization=estimated_cpu_utilization,
                limiting_factors=limiting,
                warnings=tuple(dict.fromkeys(warnings)),
            )
        )

    if strong_evidence and detailed_evidence:
        confidence = "DETAILED_TELEMETRY"
    elif strong_evidence:
        confidence = "TELEMETRY_INFORMED"
    else:
        confidence = "LIMITED_TELEMETRY"
    return InitialSizingRecommendation(
        input_fingerprint=inputs.fingerprint,
        policy_fingerprint=_policy_fingerprint(policy),
        method="CONSERVATIVE_MULTI_BOUND_INITIAL_SIZING",
        confidence=confidence,
        services=tuple(recommendations),
        warnings=tuple(dict.fromkeys(overall_warnings)),
    )

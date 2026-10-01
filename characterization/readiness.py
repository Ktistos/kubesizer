"""Evidence gates and risk ranges for portable initial sizing.

The CPU sizing equation can always produce a number when it is given a demand
and an arrival rate.  That arithmetic fact is deliberately kept separate from
the scientific question: is the supplied evidence rich enough to call that
number a recommendation for this workload?
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from enum import Enum

from .models import MINIMUM_ARRIVAL_TRACE_INTERVALS, SizingPolicy


class RecommendationReadiness(str, Enum):
    READY = "READY"
    CONDITIONAL = "CONDITIONAL"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


@dataclass(frozen=True)
class EvidenceGatePolicy:
    """Configurable practical limits; none is presented as a universal law."""

    maximum_flow_balance_relative_error: float = 0.10
    maximum_cpu_demand_coefficient_of_variation: float = 0.50
    load_dependence_minimum_relative_change: float = 0.25
    load_dependence_minimum_correlation: float = 0.50
    maximum_accepted_5xx_rate: float = 0.01
    require_observed_latency_for_declared_slo: bool = True


@dataclass(frozen=True)
class EvidenceFinding:
    severity: str
    code: str
    subject: str
    message: str


@dataclass(frozen=True)
class ServiceSizingRisk:
    service: str
    recommended_replicas: int
    target_arrival_rate_rps: float
    observed_arrival_rate_range_rps: tuple[float, float] | None
    cpu_demand_range_seconds: tuple[float, float] | None
    estimated_cpu_utilization_range: tuple[float, float] | None
    cpu_demand_coefficient_of_variation: float | None
    load_demand_correlation: float | None
    load_dependent: bool
    target_inside_observed_load_range: bool | None
    demonstrated_at_recommended_replica_count: bool


@dataclass(frozen=True)
class InitialSizingRiskAssessment:
    readiness: RecommendationReadiness
    services: tuple[ServiceSizingRisk, ...]
    findings: tuple[EvidenceFinding, ...]

    @property
    def recommendable(self):
        return self.readiness is not RecommendationReadiness.INSUFFICIENT_EVIDENCE

    def assert_recommendable(self):
        if self.recommendable:
            return
        blockers = "; ".join(
            f"{item.subject}: {item.message}"
            for item in self.findings
            if item.severity == "BLOCKING"
        )
        raise ValueError(
            "The supplied measurements are insufficient for a defensible "
            f"initial recommendation. {blockers}"
        )


def _validate_gate_policy(policy):
    for name in (
        "maximum_flow_balance_relative_error",
        "maximum_cpu_demand_coefficient_of_variation",
        "load_dependence_minimum_relative_change",
        "load_dependence_minimum_correlation",
        "maximum_accepted_5xx_rate",
    ):
        value = float(getattr(policy, name))
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"{name} must be finite and non-negative.")
    if policy.load_dependence_minimum_correlation > 1.0:
        raise ValueError("load_dependence_minimum_correlation cannot exceed one.")
    if policy.maximum_accepted_5xx_rate > 1.0:
        raise ValueError("maximum_accepted_5xx_rate cannot exceed one.")


def _coefficient_of_variation(values):
    values = tuple(float(value) for value in values)
    if len(values) < 2:
        return None
    mean = statistics.fmean(values)
    if mean <= 0.0:
        return None
    return statistics.stdev(values) / mean


def _correlation(left, right):
    left = tuple(float(value) for value in left)
    right = tuple(float(value) for value in right)
    if len(left) < 3 or len(left) != len(right):
        return None
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    left_ss = sum((value - left_mean) ** 2 for value in left)
    right_ss = sum((value - right_mean) ** 2 for value in right)
    if left_ss <= 0.0 or right_ss <= 0.0:
        return 0.0
    covariance = sum(
        (x - left_mean) * (y - right_mean) for x, y in zip(left, right, strict=True)
    )
    return covariance / math.sqrt(left_ss * right_ss)


def _service_loads(characterization, service):
    visits = {
        request_class: dict(values)
        for request_class, values in characterization.request_class_visits
    }
    loads = []
    for window in characterization.aligned_input_windows:
        arrivals = dict(window.request_class_arrival_rates)
        loads.append(
            sum(
                rate * visits.get(request_class, {}).get(service, 0.0)
                for request_class, rate in arrivals.items()
            )
        )
    return tuple(loads)


def assess_initial_sizing_risk(
    inputs,
    characterization,
    recommendation,
    *,
    sizing_policy=None,
    gate_policy=None,
):
    """Explain the usable range and evidence quality of an initial plan.

    This assessment does not turn a confidence interval into a probability of
    SLO compliance.  It reports measured ranges, validation coverage and
    explicit blockers so callers can decide whether to proceed.
    """

    sizing_policy = sizing_policy or SizingPolicy()
    gate_policy = gate_policy or EvidenceGatePolicy()
    _validate_gate_policy(gate_policy)
    if characterization.input_fingerprint != inputs.fingerprint:
        raise ValueError("Characterization belongs to a different evidence package.")
    if recommendation.input_fingerprint != inputs.fingerprint:
        raise ValueError("Recommendation belongs to a different evidence package.")

    findings = []
    risks = []
    recommendations = {item.service: item for item in recommendation.services}
    external = {
        item.request_class: item for item in characterization.external_request_classes
    }
    arrival_samples_by_class = {}
    for sample in inputs.measurements.external_arrival_samples:
        arrival_samples_by_class.setdefault(sample.request_class, []).append(sample)
    inferred_calls = {
        request_class: {source: dict(targets) for source, targets in sources}
        for request_class, sources in characterization.request_class_calls
    }

    for request_class in inputs.workload.request_classes:
        observed = external.get(request_class.name)
        if observed is None or observed.observed_arrival_rate_rps is None:
            findings.append(
                EvidenceFinding(
                    "BLOCKING",
                    "REQUEST_CLASS_ARRIVAL_UNAVAILABLE",
                    request_class.name,
                    "no class-attributed external arrival rate was measured",
                )
            )
        if (
            gate_policy.require_observed_latency_for_declared_slo
            and request_class.latency_slo is not None
        ):
            slo = dict(request_class.latency_slo)
            latency = None if observed is None else observed.latency
            statistic = (
                None
                if latency is None
                else (
                    latency.mean_seconds
                    if "mean_seconds" in slo
                    else latency.p95_seconds
                )
            )
            if statistic is None:
                findings.append(
                    EvidenceFinding(
                        "BLOCKING",
                        "OBSERVED_SLO_STATISTIC_UNAVAILABLE",
                        request_class.name,
                        "the declared latency statistic was not measured for this class",
                    )
                )
        class_arrival_samples = tuple(
            arrival_samples_by_class.get(request_class.name, ())
        )
        if not class_arrival_samples:
            findings.append(
                EvidenceFinding(
                    (
                        "BLOCKING"
                        if request_class.latency_slo is not None
                        else "CONDITIONAL"
                    ),
                    "REQUEST_CLASS_ARRIVAL_TRACE_UNAVAILABLE",
                    request_class.name,
                    (
                        "burst ordering was not measured; a latency recommendation "
                        "cannot substitute exponential arrivals for missing trace "
                        "evidence"
                    ),
                )
            )
        elif len(request_class.entry_service_weights) == 1 and (
            len(class_arrival_samples) - 1 < MINIMUM_ARRIVAL_TRACE_INTERVALS
        ):
            findings.append(
                EvidenceFinding(
                    (
                        "BLOCKING"
                        if request_class.latency_slo is not None
                        else "CONDITIONAL"
                    ),
                    "REQUEST_CLASS_ARRIVAL_TRACE_INSUFFICIENT",
                    request_class.name,
                    "the arrival trace is too short to characterize burst ordering; "
                    f"at least {MINIMUM_ARRIVAL_TRACE_INTERVALS} interarrival "
                    "gaps are required",
                )
            )
        if len(request_class.entry_service_weights) > 1 and class_arrival_samples:
            attributed_entries = {
                sample.entry_service
                for sample in class_arrival_samples
                if sample.entry_service is not None
            }
            declared_entries = set(request_class.entry_services)
            if (
                any(sample.entry_service is None for sample in class_arrival_samples)
                or attributed_entries != declared_entries
            ):
                findings.append(
                    EvidenceFinding(
                        (
                            "BLOCKING"
                            if request_class.latency_slo is not None
                            else "CONDITIONAL"
                        ),
                        "MULTI_ENTRY_TRACE_ATTRIBUTION_UNAVAILABLE",
                        request_class.name,
                        "arrival samples for a multi-entry class must identify "
                        "every entry service without unattributed rows",
                    )
                )
            else:
                insufficient_entries = sorted(
                    entry
                    for entry in declared_entries
                    if sum(
                        sample.entry_service == entry
                        for sample in class_arrival_samples
                    )
                    - 1
                    < MINIMUM_ARRIVAL_TRACE_INTERVALS
                )
                if insufficient_entries:
                    findings.append(
                        EvidenceFinding(
                            (
                                "BLOCKING"
                                if request_class.latency_slo is not None
                                else "CONDITIONAL"
                            ),
                            "MULTI_ENTRY_ARRIVAL_TRACE_INSUFFICIENT",
                            request_class.name,
                            "arrival traces are too short for entry services "
                            f"{insufficient_entries}; each needs at least "
                            f"{MINIMUM_ARRIVAL_TRACE_INTERVALS} interarrival gaps",
                        )
                    )
        if not request_class.call_graph:
            calls = inferred_calls.get(request_class.name, {})
            nonentry_visits = set(request_class.visit_ratios) - set(
                request_class.entry_services
            )
            if calls:
                findings.append(
                    EvidenceFinding(
                        "CONDITIONAL",
                        "CALL_ORDER_INFERRED",
                        request_class.name,
                        "edge counters identify call multiplicity but not sibling ordering; sensitivity variants are required",
                    )
                )
            elif nonentry_visits:
                findings.append(
                    EvidenceFinding(
                        "BLOCKING",
                        "EXECUTABLE_CALL_GRAPH_UNAVAILABLE",
                        request_class.name,
                        "mean visits include internal services but no executable call graph was measured or declared",
                    )
                )

    if inputs.workload.communication_latency is None:
        findings.append(
            EvidenceFinding(
                "CONDITIONAL",
                "COMMUNICATION_LATENCY_UNAVAILABLE",
                "workload",
                "same-node and cross-node call latency were not supplied",
            )
        )

    for service in inputs.workload.services:
        evidence = characterization.service_map[service.name]
        item = recommendations[service.name]
        aligned_demands = tuple(
            dict(window.service_cpu_demands).get(service.name)
            for window in characterization.aligned_input_windows
        )
        demands = (
            tuple(float(value) for value in aligned_demands)
            if aligned_demands and all(value is not None for value in aligned_demands)
            else tuple(evidence.cpu_demand_window_means_seconds)
        )
        loads = _service_loads(characterization, service.name)
        if not demands:
            findings.append(
                EvidenceFinding(
                    "BLOCKING",
                    "CPU_DEMAND_UNAVAILABLE",
                    service.name,
                    "no aligned application CPU-demand windows were measured",
                )
            )
        if evidence.flow_balance_relative_error is not None and (
            evidence.flow_balance_relative_error
            > gate_policy.maximum_flow_balance_relative_error
        ):
            findings.append(
                EvidenceFinding(
                    "BLOCKING",
                    "FLOW_BALANCE_MISMATCH",
                    service.name,
                    "observed service traffic disagrees materially with class visits",
                )
            )
        if evidence.error_rate_5xx > gate_policy.maximum_accepted_5xx_rate:
            findings.append(
                EvidenceFinding(
                    "BLOCKING",
                    "OBSERVED_5XX_RATE_TOO_HIGH",
                    service.name,
                    "the measurement window contains too many server failures to represent healthy capacity",
                )
            )
        if evidence.throttling_ratio > sizing_policy.maximum_accepted_throttling_ratio:
            findings.append(
                EvidenceFinding(
                    "BLOCKING",
                    "CPU_THROTTLING_TOO_HIGH",
                    service.name,
                    "CPU throttling exceeds the sizing policy's accepted measurement limit",
                )
            )

        demand_cv = _coefficient_of_variation(demands)
        correlation = _correlation(loads, demands)
        relative_change = (
            (max(demands) - min(demands)) / statistics.fmean(demands)
            if demands and statistics.fmean(demands) > 0.0
            else 0.0
        )
        load_dependent = bool(
            correlation is not None
            and abs(correlation) >= gate_policy.load_dependence_minimum_correlation
            and relative_change >= gate_policy.load_dependence_minimum_relative_change
        )
        observed_range = (min(loads), max(loads)) if loads else None
        target_inside = (
            observed_range[0] <= item.workload_arrival_rate_rps <= observed_range[1]
            if observed_range is not None
            else None
        )
        if demand_cv is not None and demand_cv > (
            gate_policy.maximum_cpu_demand_coefficient_of_variation
        ):
            findings.append(
                EvidenceFinding(
                    "CONDITIONAL",
                    "HIGH_CPU_DEMAND_VARIABILITY",
                    service.name,
                    "CPU demand per request varies materially across aligned windows",
                )
            )
        if load_dependent and target_inside is False:
            findings.append(
                EvidenceFinding(
                    "BLOCKING",
                    "LOAD_DEPENDENT_EXTRAPOLATION",
                    service.name,
                    "CPU demand changes with load and the target is outside the observed load range",
                )
            )
        elif target_inside is False:
            findings.append(
                EvidenceFinding(
                    "CONDITIONAL",
                    "LOAD_EXTRAPOLATION",
                    service.name,
                    "the target service load is outside the observed window range",
                )
            )

        if service.concurrency_limit_per_replica is None:
            findings.append(
                EvidenceFinding(
                    "CONDITIONAL",
                    "CONCURRENCY_LIMIT_UNAVAILABLE",
                    service.name,
                    "worker/thread/connection-pool capacity was not supplied",
                )
            )
        demonstrated = item.recommended_replicas in (
            item.empirical_capacity_evidence_replicas
        )
        if not demonstrated:
            findings.append(
                EvidenceFinding(
                    "CONDITIONAL",
                    "RECOMMENDED_COUNT_NOT_LOAD_TESTED",
                    service.name,
                    "the recommended replica count has no independent capacity-run evidence",
                )
            )

        demand_range = (min(demands), max(demands)) if demands else None
        utilization_range = None
        if demand_range is not None and item.recommended_replicas > 0:
            denominator = item.recommended_replicas * service.cpu_capacity_cores
            utilization_range = (
                item.workload_arrival_rate_rps * demand_range[0] / denominator,
                item.workload_arrival_rate_rps
                * demand_range[1]
                * sizing_policy.demand_uncertainty_multiplier
                / denominator,
            )
        risks.append(
            ServiceSizingRisk(
                service=service.name,
                recommended_replicas=item.recommended_replicas,
                target_arrival_rate_rps=item.workload_arrival_rate_rps,
                observed_arrival_rate_range_rps=observed_range,
                cpu_demand_range_seconds=demand_range,
                estimated_cpu_utilization_range=utilization_range,
                cpu_demand_coefficient_of_variation=demand_cv,
                load_demand_correlation=correlation,
                load_dependent=load_dependent,
                target_inside_observed_load_range=target_inside,
                demonstrated_at_recommended_replica_count=demonstrated,
            )
        )

    if recommendation.confidence == "LIMITED_TELEMETRY":
        findings.append(
            EvidenceFinding(
                "BLOCKING",
                "LIMITED_TELEMETRY",
                "workload",
                "the underlying sizing calculation did not meet its evidence minimums",
            )
        )
    severities = {item.severity for item in findings}
    readiness = (
        RecommendationReadiness.INSUFFICIENT_EVIDENCE
        if "BLOCKING" in severities
        else (
            RecommendationReadiness.CONDITIONAL
            if "CONDITIONAL" in severities
            else RecommendationReadiness.READY
        )
    )
    return InitialSizingRiskAssessment(
        readiness=readiness,
        services=tuple(risks),
        findings=tuple(findings),
    )

"""Immutable contracts for portable deployment characterization and sizing."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .readiness import InitialSizingRiskAssessment


MEASUREMENT_METHODOLOGY_VERSION = "prometheus-istio-kubernetes-v1"
DEFAULT_MAXIMUM_CAPACITY_UTILIZATION = 0.85
MINIMUM_ARRIVAL_TRACE_INTERVALS = 30


@dataclass(frozen=True)
class ServiceDeployment:
    name: str
    namespace: str
    cpu_request_cores: float
    cpu_limit_cores: float | None
    memory_request_bytes: float | None
    memory_limit_bytes: float | None
    concurrency_limit_per_replica: int | None = None

    @property
    def cpu_capacity_cores(self):
        """Return the CPU share Kubernetes reserves for one replica.

        A CPU limit is a ceiling, not an entitlement under node contention.
        Portable sizing therefore uses the request as its capacity basis.
        """
        return self.cpu_request_cores

    @property
    def cpu_capacity_basis(self):
        return "kubernetes_cpu_request"


@dataclass(frozen=True)
class TargetRequestClass:
    name: str
    concurrency_weight: float
    entry_service_weights: tuple[tuple[str, float], ...]
    visits: tuple[tuple[str, float], ...]
    calls: tuple[tuple[str, tuple[tuple[str, float], ...]], ...]
    call_execution: str | None
    fixed_latency_overhead_seconds: float
    latency_overhead_distribution: object | None
    latency_boundary: str
    latency_components: tuple[tuple[str, object], ...]
    end_to_end_timeout_seconds: float
    latency_slo: tuple[tuple[str, object], ...] | None
    maximum_terminal_failure_rate: float | None

    @property
    def entry_services(self):
        return dict(self.entry_service_weights)

    @property
    def visit_ratios(self):
        return dict(self.visits)

    @property
    def call_graph(self):
        return {source: dict(targets) for source, targets in self.calls}


@dataclass(frozen=True)
class WorkloadDefinition:
    services: tuple[ServiceDeployment, ...]
    request_classes: tuple[TargetRequestClass, ...]
    load_forecast_multipliers: tuple[float, ...] = (1.0,)
    communication_latency: object | None = None
    service_blocking_times: tuple[tuple[str, object], ...] = ()
    maximum_capacity_utilization: float = DEFAULT_MAXIMUM_CAPACITY_UTILIZATION

    @property
    def service_names(self):
        return tuple(service.name for service in self.services)

    def service(self, name):
        for service in self.services:
            if service.name == name:
                return service
        raise KeyError(name)

    @property
    def maximum_load_multiplier(self):
        return max(self.load_forecast_multipliers)


@dataclass(frozen=True)
class AlignedInputWindow:
    """One joint observation used as an indivisible input-uncertainty unit."""

    start: float
    end: float
    request_class_arrival_rates: tuple[tuple[str, float], ...]
    service_cpu_demands: tuple[tuple[str, float], ...]


@dataclass(frozen=True)
class UncertaintyDimension:
    """One named source of uncertainty and how the workflow treats it."""

    kind: str
    name: str
    treatment: str
    values: tuple[object, ...] = ()


@dataclass(frozen=True)
class ServiceInterval:
    start: float
    end: float
    service: str
    namespace: str
    pod: str
    request_count: float
    cpu_seconds: float
    errors_4xx: float = 0.0
    errors_5xx: float = 0.0
    throttled_seconds: float = 0.0
    memory_bytes: float | None = None
    mean_active_requests: float | None = None
    maximum_active_requests: float | None = None
    response_duration_sum_seconds: float | None = None

    @property
    def duration(self):
        return self.end - self.start


@dataclass(frozen=True)
class ServiceClassInterval:
    start: float
    end: float
    service: str
    request_class: str
    request_count: float

    @property
    def duration(self):
        return self.end - self.start


@dataclass(frozen=True)
class ExternalInterval:
    start: float
    end: float
    request_class: str
    request_count: float
    errors_4xx: float = 0.0
    errors_5xx: float = 0.0
    response_duration_sum_seconds: float | None = None

    @property
    def duration(self):
        return self.end - self.start


@dataclass(frozen=True)
class ServiceEdgeInterval:
    start: float
    end: float
    source_service: str
    destination_service: str
    call_count: float
    request_class: str | None = None

    @property
    def duration(self):
        return self.end - self.start


@dataclass(frozen=True)
class LatencyHistogramBucket:
    start: float
    end: float
    subject: str
    upper_bound_seconds: float
    cumulative_count: float


@dataclass(frozen=True)
class ExternalArrivalSample:
    timestamp: float
    request_class: str
    entry_service: str | None = None


@dataclass(frozen=True)
class CapacityObservation:
    run_id: str
    service: str
    replicas: int
    offered_rate_rps: float
    completed_rate_rps: float
    error_rate: float
    healthy: bool
    cpu_throttling_ratio: float = 0.0


@dataclass(frozen=True)
class CapacityEnvelopePoint:
    """Independent capacity evidence for one observed replica count.

    The lower bound is a one-sided confidence bound for the mean completed
    throughput of healthy runs.  It is deliberately attached to the observed
    replica count; callers must not silently turn it into a linear per-replica
    capacity law.
    """

    replicas: int
    healthy_run_count: int
    unhealthy_run_count: int
    healthy_completed_rates_rps: tuple[float, ...]
    mean_healthy_completed_rate_rps: float | None
    lower_mean_healthy_completed_rate_rps: float | None
    lower_bound_confidence_level: float
    minimum_healthy_completed_rate_rps: float | None
    maximum_healthy_offered_rate_rps: float | None
    minimum_unhealthy_offered_rate_rps: float | None


@dataclass(frozen=True)
class MeasurementPackage:
    measurement_id: str
    methodology_version: str
    service_intervals: tuple[ServiceInterval, ...]
    service_class_intervals: tuple[ServiceClassInterval, ...] = ()
    external_intervals: tuple[ExternalInterval, ...] = ()
    service_edges: tuple[ServiceEdgeInterval, ...] = ()
    service_latency_histograms: tuple[LatencyHistogramBucket, ...] = ()
    external_latency_histograms: tuple[LatencyHistogramBucket, ...] = ()
    external_arrival_samples: tuple[ExternalArrivalSample, ...] = ()
    capacity_observations: tuple[CapacityObservation, ...] = ()
    source_fingerprints: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class PortableSizingInputs:
    workload: WorkloadDefinition
    measurements: MeasurementPackage
    fingerprint: str


@dataclass(frozen=True)
class LatencySummary:
    mean_seconds: float | None
    p50_seconds: float | None
    p90_seconds: float | None
    p95_seconds: float | None
    p99_seconds: float | None


@dataclass(frozen=True)
class LatencyBoundaryCharacterization:
    """Evidence for latency outside the modeled application service graph."""

    status: str
    component_name: str | None
    mean_seconds: float | None
    window_mean_seconds: tuple[float, ...]
    candidate_window_count: int
    excluded_window_count: int
    coefficient_of_variation: float | None
    method: str | None
    reason: str | None


@dataclass(frozen=True)
class ServiceCharacterization:
    service: str
    observed_replica_count: int
    observed_arrival_rate_rps: float
    workload_arrival_rate_rps: float
    flow_balance_relative_error: float | None
    cpu_demand_window_means_seconds: tuple[float, ...]
    cpu_demand_mean_seconds: float | None
    cpu_demand_safe_seconds: float | None
    cpu_demand_window_scv: float | None
    observed_cpu_usage_cores_per_replica: tuple[float, ...]
    observed_throughput_per_replica_rps: tuple[float, ...]
    maximum_healthy_throughput_per_replica_rps: float | None
    capacity_envelope: tuple[CapacityEnvelopePoint, ...]
    observed_mean_active_requests_per_replica: tuple[float, ...]
    error_rate: float
    error_rate_4xx: float
    error_rate_5xx: float
    throttling_ratio: float
    mean_memory_bytes_per_replica: float | None
    maximum_memory_bytes_per_replica: float | None
    latency: LatencySummary
    outgoing_call_rates: tuple[tuple[str, float], ...]
    routing_probabilities: tuple[tuple[str, float], ...]
    visits_per_service_request: tuple[tuple[str, float], ...]
    measurement_window_count: int
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class ExternalRequestClassCharacterization:
    request_class: str
    observed_arrival_rate_rps: float | None
    error_rate_4xx: float | None
    error_rate_5xx: float | None
    latency: LatencySummary
    boundary_latency: LatencyBoundaryCharacterization


@dataclass(frozen=True)
class DeploymentCharacterization:
    input_fingerprint: str
    measurement_methodology_version: str
    rate_source: str
    services: tuple[ServiceCharacterization, ...]
    observed_external_arrival_rate_rps: float | None
    external_request_classes: tuple[ExternalRequestClassCharacterization, ...]
    request_class_visits: tuple[tuple[str, tuple[tuple[str, float], ...]], ...]
    request_class_calls: tuple[
        tuple[str, tuple[tuple[str, tuple[tuple[str, float], ...]], ...]], ...
    ]
    aligned_input_windows: tuple[AlignedInputWindow, ...]
    capabilities: tuple[tuple[str, bool], ...]
    warnings: tuple[str, ...]

    @property
    def service_map(self):
        return {service.service: service for service in self.services}


@dataclass(frozen=True)
class SizingPolicy:
    target_cpu_utilization: float = 0.65
    empirical_capacity_utilization: float = DEFAULT_MAXIMUM_CAPACITY_UTILIZATION
    cpu_demand_quantile: float = 0.90
    demand_uncertainty_multiplier: float = 1.10
    minimum_measurement_windows: int = 3
    maximum_accepted_throttling_ratio: float = 0.05
    capacity_confidence_level: float = 0.95
    minimum_capacity_runs_per_replica: int = 3


class StabilityFloorStatus(str, Enum):
    """Evidence status for a statistically controlled replica floor."""

    CONFIDENCE_BOUNDED_STABLE = "CONFIDENCE_BOUNDED_STABLE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    OUTSIDE_REPLICA_BOUNDS = "OUTSIDE_REPLICA_BOUNDS"


class StatisticalStabilityError(RuntimeError):
    """Raised when evidence cannot establish the requested stability floor."""


@dataclass(frozen=True)
class CapacityPointConfidenceBound:
    """One-sided per-replica capacity bound at one tested replica count."""

    replicas: int
    healthy_run_count: int
    completed_rates_rps_per_replica: tuple[float, ...]
    mean_completed_rate_rps_per_replica: float
    lower_bound_rps_per_replica: float
    confidence_level: float


@dataclass(frozen=True)
class ServiceStabilityAssessment:
    """Evidence used to choose the stable lower bound for one service."""

    service: str
    status: StabilityFloorStatus
    analytical_minimum_replicas: int
    maximum_replicas: int
    required_replicas: int | None
    arrival_rate_samples_rps: tuple[float, ...]
    arrival_rate_mean_rps: float | None
    arrival_rate_upper_bound_rps: float | None
    capacity_bounds: tuple[CapacityPointConfidenceBound, ...]
    service_rate_lower_bound_rps_per_replica: float | None
    usable_service_rate_lower_bound_rps_per_replica: float | None
    stability_margin_lower_bound_rps: float | None
    limiting_reason: str | None
    warnings: tuple[str, ...]

    @property
    def confidence_bounded_capacity_utilization(self):
        """Return the conservative offered-load ratio at the selected floor."""
        if self.arrival_rate_upper_bound_rps == 0.0:
            return 0.0
        if (
            self.required_replicas is None
            or self.arrival_rate_upper_bound_rps is None
            or self.service_rate_lower_bound_rps_per_replica is None
            or self.service_rate_lower_bound_rps_per_replica <= 0.0
        ):
            return None
        return self.arrival_rate_upper_bound_rps / (
            self.required_replicas * self.service_rate_lower_bound_rps_per_replica
        )


@dataclass(frozen=True)
class StatisticalStabilityFloor:
    """Joint-confidence lower replica bound for one measured workload.

    ``CONFIDENCE_BOUNDED_STABLE`` is conditional on the declared workload
    envelope, independent observation-window/run assumptions, and the
    per-replica capacity-scaling model.  It is not a proof against arbitrary
    future traffic or hidden shared-resource changes.
    """

    input_fingerprint: str
    method: str
    status: StabilityFloorStatus
    overall_confidence_level: float
    individual_bound_confidence_level: float
    multiplicity_correction: str
    maximum_capacity_utilization: float
    analytical_minimum_configuration: tuple[int, ...]
    minimum_stable_configuration: tuple[int, ...] | None
    maximum_configuration: tuple[int, ...]
    services: tuple[ServiceStabilityAssessment, ...]
    assumptions: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def configuration(self):
        if self.minimum_stable_configuration is None:
            raise StatisticalStabilityError(
                "The evidence did not establish a confidence-bounded stable "
                "configuration."
            )
        return self.minimum_stable_configuration

    def assert_confidence_bounded_stable(self):
        if self.status != StabilityFloorStatus.CONFIDENCE_BOUNDED_STABLE:
            details = "; ".join(
                f"{service.service}: {service.limiting_reason}"
                for service in self.services
                if service.status != StabilityFloorStatus.CONFIDENCE_BOUNDED_STABLE
            )
            raise StatisticalStabilityError(
                "No statistically controlled stability floor was established"
                + (f" ({details})" if details else ".")
            )

    def assert_robustly_stable(self):
        """Backward-compatible alias for the explicitly conditional check."""
        self.assert_confidence_bounded_stable()


@dataclass(frozen=True)
class ServiceSizingRecommendation:
    service: str
    recommended_replicas: int
    cpu_based_replicas: int | None
    empirical_capacity_based_replicas: int | None
    empirical_capacity_evidence_replicas: tuple[int, ...]
    empirical_capacity_extrapolated: bool
    cpu_capacity_cores_per_replica: float
    cpu_capacity_basis: str
    workload_arrival_rate_rps: float
    estimated_cpu_utilization: float | None
    limiting_factors: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class InitialSizingRecommendation:
    input_fingerprint: str
    policy_fingerprint: str
    method: str
    confidence: str
    services: tuple[ServiceSizingRecommendation, ...]
    warnings: tuple[str, ...]

    @property
    def replicas(self):
        return {
            service.service: service.recommended_replicas for service in self.services
        }

    def configuration(self, service_order):
        replicas = self.replicas
        return tuple(replicas[name] for name in service_order)

    def accepts_configuration(self, configuration, service_order):
        minimum = self.configuration(service_order)
        return len(configuration) == len(minimum) and all(
            supplied >= required for supplied, required in zip(configuration, minimum)
        )


@dataclass(frozen=True)
class AnalyticalWorkloadVariant:
    name: str
    workload: dict
    assumptions: tuple[str, ...]
    uncertainty_dimensions: tuple[UncertaintyDimension, ...] = ()
    model_issues: tuple[str, ...] = ()

    @property
    def model_complete(self):
        return not self.model_issues


@dataclass(frozen=True)
class PreparedInitialSizing:
    inputs: PortableSizingInputs
    characterization: DeploymentCharacterization
    recommendation: InitialSizingRecommendation
    risk_assessment: InitialSizingRiskAssessment | None = None

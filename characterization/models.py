"""Immutable contracts for portable deployment characterization and sizing."""

from __future__ import annotations

from dataclasses import dataclass


MEASUREMENT_METHODOLOGY_VERSION = "prometheus-istio-kubernetes-v1"


@dataclass(frozen=True)
class ServiceDeployment:
    name: str
    namespace: str
    cpu_request_cores: float
    cpu_limit_cores: float | None
    memory_request_bytes: float | None
    memory_limit_bytes: float | None

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
class DeploymentSpec:
    services: tuple[ServiceDeployment, ...]

    @property
    def service_names(self):
        return tuple(service.name for service in self.services)

    def service(self, name):
        for service in self.services:
            if service.name == name:
                return service
        raise KeyError(name)


@dataclass(frozen=True)
class TargetRequestClass:
    name: str
    concurrency_weight: float
    entry_service_weights: tuple[tuple[str, float], ...]
    visits: tuple[tuple[str, float], ...]
    calls: tuple[tuple[str, tuple[tuple[str, float], ...]], ...]
    call_execution: str | None
    fixed_latency_overhead_seconds: float
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
    request_classes: tuple[TargetRequestClass, ...]


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
    deployment: DeploymentSpec
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
    capabilities: tuple[tuple[str, bool], ...]
    warnings: tuple[str, ...]

    @property
    def service_map(self):
        return {service.service: service for service in self.services}


@dataclass(frozen=True)
class SizingPolicy:
    target_cpu_utilization: float = 0.65
    empirical_capacity_utilization: float = 0.85
    cpu_demand_quantile: float = 0.90
    demand_uncertainty_multiplier: float = 1.10
    minimum_measurement_windows: int = 3
    maximum_accepted_throttling_ratio: float = 0.05
    capacity_confidence_level: float = 0.95
    minimum_capacity_runs_per_replica: int = 3


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
class SimulationWorkloadVariant:
    name: str
    workload: dict
    assumptions: tuple[str, ...]


@dataclass(frozen=True)
class PreparedInitialSizing:
    inputs: PortableSizingInputs
    characterization: DeploymentCharacterization
    recommendation: InitialSizingRecommendation

import hashlib
import json
import math
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path

import yaml

from native_backend import SIMULATOR_SEMANTICS_VERSION

from .seeds import SEED_POLICY_VERSION, SeedPolicy, default_seed_policy


CALIBRATION_PROFILE_VERSION = 6
P95_CONFIDENCE_INTERVAL_METHOD = "replication-cluster-percentile-bootstrap-v1"
MEAN_CONFIDENCE_INTERVAL_METHOD = "replication-cluster-mean-bootstrap-v1"


class CalibrationProfileMismatchError(RuntimeError):
    """Raised when a persisted profile is invalid for the current experiment."""


@dataclass(frozen=True)
class CalibrationOptions:
    time_unit: str = "seconds"
    warmup_replications: int = 30
    warmup_validation_replications: int = 100
    warmup_window_length: int = 10
    warmup_reference_tail_fraction: float = 0.20
    warmup_required_stable_windows: int = 3
    warmup_relative_tolerance: float = 0.05
    warmup_sensitivity_tolerance: float = 0.10
    warmup_max_horizon_doublings: int = 5
    warmup_max_validation_rounds: int = 1
    warmup_max_validation_horizon_extensions: int = 2
    measurement_calibration_replications: int = 30
    target_samples_per_class: int = 2_000
    minimum_eligible_samples_per_class: int = 20_000
    minimum_nonempty_replications_per_class: int = 20
    measurement_duration_safety_factor: float = 1.20
    measurement_max_doublings: int = 3
    relative_ci_half_width_target: float = 0.05
    absolute_ci_half_width_target: float = 0.01
    measurement_stability_tolerance: float = 0.05
    ci_width_stability_tolerance: float = 0.10
    minimum_optimization_replications: int = 30
    maximum_optimization_replications: int = 60
    replication_batch_size: int = 10
    overall_alpha: float = 0.05
    bootstrap_resample_count: int = 5_000
    minimum_bootstrap_tail_draws: int = 5
    final_validation_replications: int = 30
    maximum_drain_time: float = 10_000.0
    seed_policy: SeedPolicy = field(default_factory=default_seed_policy)

    def __post_init__(self):
        positive_integers = {
            "warmup_replications": self.warmup_replications,
            "warmup_validation_replications": self.warmup_validation_replications,
            "warmup_window_length": self.warmup_window_length,
            "warmup_required_stable_windows": self.warmup_required_stable_windows,
            "warmup_max_horizon_doublings": self.warmup_max_horizon_doublings,
            "warmup_max_validation_rounds": self.warmup_max_validation_rounds,
            "measurement_calibration_replications": (
                self.measurement_calibration_replications
            ),
            "target_samples_per_class": self.target_samples_per_class,
            "minimum_eligible_samples_per_class": (
                self.minimum_eligible_samples_per_class
            ),
            "minimum_nonempty_replications_per_class": (
                self.minimum_nonempty_replications_per_class
            ),
            "minimum_optimization_replications": (
                self.minimum_optimization_replications
            ),
            "maximum_optimization_replications": (
                self.maximum_optimization_replications
            ),
            "replication_batch_size": self.replication_batch_size,
            "bootstrap_resample_count": self.bootstrap_resample_count,
            "minimum_bootstrap_tail_draws": self.minimum_bootstrap_tail_draws,
            "final_validation_replications": self.final_validation_replications,
        }
        for name, value in positive_integers.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer.")
        if (
            isinstance(self.warmup_max_validation_horizon_extensions, bool)
            or not isinstance(self.warmup_max_validation_horizon_extensions, int)
            or self.warmup_max_validation_horizon_extensions < 0
        ):
            raise ValueError(
                "warmup_max_validation_horizon_extensions must be non-negative."
            )
        if (
            self.minimum_optimization_replications
            > self.maximum_optimization_replications
        ):
            raise ValueError("R_min cannot exceed R_max.")
        if (
            self.minimum_nonempty_replications_per_class
            > self.measurement_calibration_replications
        ):
            raise ValueError(
                "Minimum nonempty replications cannot exceed measurement pilots."
            )
        if self.final_validation_replications < self.minimum_optimization_replications:
            raise ValueError("Final validation must use at least R_min replications.")
        if not self.time_unit.strip():
            raise ValueError("time_unit cannot be empty.")
        unit_intervals = {
            "warmup_reference_tail_fraction": self.warmup_reference_tail_fraction,
            "warmup_relative_tolerance": self.warmup_relative_tolerance,
            "warmup_sensitivity_tolerance": self.warmup_sensitivity_tolerance,
            "relative_ci_half_width_target": self.relative_ci_half_width_target,
            "measurement_stability_tolerance": self.measurement_stability_tolerance,
            "ci_width_stability_tolerance": self.ci_width_stability_tolerance,
            "overall_alpha": self.overall_alpha,
        }
        for name, value in unit_intervals.items():
            if not math.isfinite(value) or not 0 < value < 1:
                raise ValueError(f"{name} must lie strictly between zero and one.")
        if (
            not math.isfinite(self.absolute_ci_half_width_target)
            or self.absolute_ci_half_width_target <= 0
        ):
            raise ValueError("absolute_ci_half_width_target must be positive.")
        if (
            not math.isfinite(self.measurement_duration_safety_factor)
            or self.measurement_duration_safety_factor < 1
        ):
            raise ValueError("measurement_duration_safety_factor must be at least one.")
        if (
            isinstance(self.measurement_max_doublings, bool)
            or not isinstance(self.measurement_max_doublings, int)
            or self.measurement_max_doublings < 1
        ):
            raise ValueError("measurement_max_doublings must be at least one.")
        if not math.isfinite(self.maximum_drain_time) or self.maximum_drain_time <= 0:
            raise ValueError("maximum_drain_time must be positive.")

    @property
    def maximum_sequential_looks(self):
        remaining = (
            self.maximum_optimization_replications
            - self.minimum_optimization_replications
        )
        return 1 + math.ceil(remaining / self.replication_batch_size)


@dataclass(frozen=True)
class CalibrationProfile:
    profile_version: int
    topology_fingerprint: str
    workload_fingerprint: str
    simulator_semantics_version: str
    minimum_replica_configuration: tuple[int, ...]
    maximum_replica_configuration: tuple[int, ...]
    warmup_duration: float
    measurement_duration: float
    measurement_duration_candidates: tuple[float, ...]
    warmup_bucket_width: float
    warmup_window_length: int
    warmup_reference_tail_fraction: float
    warmup_required_stable_windows: int
    warmup_relative_tolerance: float
    warmup_replication_count: int
    minimum_optimization_replications: int
    maximum_optimization_replications: int
    replication_batch_size: int
    maximum_sequential_looks: int
    overall_alpha: float
    request_class_slos: tuple[tuple[str, float], ...]
    p95_confidence_interval_method: str
    bootstrap_resample_count: int
    minimum_bootstrap_tail_draws: int
    target_samples_per_class: int
    minimum_eligible_samples_per_request_class: int
    minimum_nonempty_replications_per_request_class: int
    relative_ci_half_width_target: float
    absolute_ci_half_width_target: float
    measurement_stability_tolerance: float
    ci_width_stability_tolerance: float
    calibration_seeds: tuple[int, ...]
    optimization_seed_policy: SeedPolicy
    final_validation_seeds: tuple[int, ...]
    final_validation_replication_count: int
    representative_configurations: tuple[tuple[int, ...], ...]
    maximum_drain_time: float
    creation_timestamp: str
    calibration_diagnostics: str
    profile_fingerprint: str
    latency_slo_kind: str = "percentile"

    @property
    def confidence_level(self):
        return 1.0 - self.overall_alpha

    @property
    def confidence_interval_method(self):
        """Generic name; the stored field name is retained for compatibility."""
        return self.p95_confidence_interval_method

    @property
    def slos(self):
        return dict(self.request_class_slos)

    @property
    def diagnostics(self):
        return json.loads(self.calibration_diagnostics)

    def optimization_seeds(self, count=None):
        if count is None:
            count = self.maximum_optimization_replications
        return self.optimization_seed_policy.seeds("optimization", count)

    def validate_integrity(self):
        if self.profile_version != CALIBRATION_PROFILE_VERSION:
            raise CalibrationProfileMismatchError(
                f"Unsupported calibration profile version {self.profile_version}."
            )
        expected = _profile_fingerprint(self)
        if self.profile_fingerprint != expected:
            raise CalibrationProfileMismatchError(
                "Calibration profile fingerprint does not match its contents."
            )
        if self.latency_slo_kind not in {"mean", "percentile"}:
            raise CalibrationProfileMismatchError("Unsupported latency SLO kind.")
        expected_interval_method = (
            MEAN_CONFIDENCE_INTERVAL_METHOD
            if self.latency_slo_kind == "mean"
            else P95_CONFIDENCE_INTERVAL_METHOD
        )
        if self.p95_confidence_interval_method != expected_interval_method:
            raise CalibrationProfileMismatchError(
                "Unsupported latency confidence-interval method."
            )
        if self.optimization_seed_policy.version != SEED_POLICY_VERSION:
            raise CalibrationProfileMismatchError("Unsupported seed-policy version.")
        if (
            not math.isfinite(self.warmup_duration)
            or self.warmup_duration < 0
            or not math.isfinite(self.measurement_duration)
            or self.measurement_duration <= 0
            or not math.isfinite(self.warmup_bucket_width)
            or self.warmup_bucket_width <= 0
            or not math.isfinite(self.maximum_drain_time)
            or self.maximum_drain_time <= 0
        ):
            raise CalibrationProfileMismatchError(
                "Profile durations and bucket width are invalid."
            )
        if (
            not self.measurement_duration_candidates
            or self.measurement_duration not in self.measurement_duration_candidates
            or any(
                not math.isfinite(value) or value <= 0
                for value in self.measurement_duration_candidates
            )
            or any(
                right <= left
                for left, right in zip(
                    self.measurement_duration_candidates,
                    self.measurement_duration_candidates[1:],
                )
            )
        ):
            raise CalibrationProfileMismatchError(
                "Measurement-duration candidates are invalid."
            )
        service_count = len(self.minimum_replica_configuration)
        if (
            service_count == 0
            or len(self.maximum_replica_configuration) != service_count
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in (
                    self.minimum_replica_configuration
                    + self.maximum_replica_configuration
                )
            )
            or any(
                maximum < minimum
                for minimum, maximum in zip(
                    self.minimum_replica_configuration,
                    self.maximum_replica_configuration,
                )
            )
        ):
            raise CalibrationProfileMismatchError("Replica bounds are invalid.")
        if (
            not self.representative_configurations
            or self.minimum_replica_configuration
            not in self.representative_configurations
            or self.maximum_replica_configuration
            not in self.representative_configurations
            or any(
                len(configuration) != service_count
                or any(
                    value < minimum or value > maximum
                    for value, minimum, maximum in zip(
                        configuration,
                        self.minimum_replica_configuration,
                        self.maximum_replica_configuration,
                    )
                )
                for configuration in self.representative_configurations
            )
        ):
            raise CalibrationProfileMismatchError(
                "Representative configurations do not cover the calibrated bounds."
            )
        positive_counts = (
            self.warmup_window_length,
            self.warmup_required_stable_windows,
            self.warmup_replication_count,
            self.minimum_optimization_replications,
            self.maximum_optimization_replications,
            self.replication_batch_size,
            self.maximum_sequential_looks,
            self.bootstrap_resample_count,
            self.minimum_bootstrap_tail_draws,
            self.target_samples_per_class,
            self.minimum_eligible_samples_per_request_class,
            self.minimum_nonempty_replications_per_request_class,
            self.final_validation_replication_count,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in positive_counts
        ):
            raise CalibrationProfileMismatchError(
                "Profile replication and sample counts must be positive integers."
            )
        expected_looks = 1 + math.ceil(
            (
                self.maximum_optimization_replications
                - self.minimum_optimization_replications
            )
            / self.replication_batch_size
        )
        if (
            self.minimum_optimization_replications
            > self.maximum_optimization_replications
            or self.maximum_sequential_looks != expected_looks
            or self.final_validation_replication_count
            < self.minimum_optimization_replications
            or self.minimum_nonempty_replications_per_request_class
            > self.maximum_optimization_replications
            or self.minimum_nonempty_replications_per_request_class
            > self.final_validation_replication_count
        ):
            raise CalibrationProfileMismatchError(
                "Replication schedule or independent-data minima are inconsistent."
            )
        if len(self.final_validation_seeds) != self.final_validation_replication_count:
            raise CalibrationProfileMismatchError(
                "Final-validation seed count does not match the profile."
            )
        if len(set(self.final_validation_seeds)) != len(self.final_validation_seeds):
            raise CalibrationProfileMismatchError(
                "Final-validation seeds are not unique."
            )
        final_namespace = self.optimization_seed_policy.namespace("final_validation")
        if any(
            not final_namespace.contains(seed) for seed in self.final_validation_seeds
        ):
            raise CalibrationProfileMismatchError(
                "Final-validation seeds are outside their namespace."
            )
        if len(set(self.calibration_seeds)) != len(self.calibration_seeds):
            raise CalibrationProfileMismatchError("Calibration seeds are not unique.")
        calibration_namespaces = tuple(
            self.optimization_seed_policy.namespace(name)
            for name in (
                "warmup_calibration",
                "warmup_validation",
                "measurement_calibration",
            )
        )
        if any(
            not any(namespace.contains(seed) for namespace in calibration_namespaces)
            for seed in self.calibration_seeds
        ):
            raise CalibrationProfileMismatchError(
                "Calibration seeds are outside calibration namespaces."
            )
        class_count = len(self.request_class_slos)
        if (
            class_count == 0
            or len({name for name, _ in self.request_class_slos}) != class_count
            or any(
                not name or not math.isfinite(slo) or slo <= 0
                for name, slo in self.request_class_slos
            )
        ):
            raise CalibrationProfileMismatchError(
                "Profile request-class SLOs are invalid."
            )
        if not 0 < self.overall_alpha < 1:
            raise CalibrationProfileMismatchError("Profile overall alpha is invalid.")
        tail_alpha = self.overall_alpha / (
            2 * self.maximum_sequential_looks * class_count
        )
        if (
            self.bootstrap_resample_count * tail_alpha
            < self.minimum_bootstrap_tail_draws
        ):
            raise CalibrationProfileMismatchError(
                "Bootstrap resample count has insufficient sequential tail resolution."
            )
        return self

    def to_dict(self):
        return {
            "profile_version": self.profile_version,
            "topology_fingerprint": self.topology_fingerprint,
            "workload_fingerprint": self.workload_fingerprint,
            "simulator_semantics_version": self.simulator_semantics_version,
            "bounded_replica_configuration_space": {
                "minimum": list(self.minimum_replica_configuration),
                "maximum": list(self.maximum_replica_configuration),
            },
            "warmup_duration": self.warmup_duration,
            "measurement_duration": self.measurement_duration,
            "measurement_duration_candidates": list(
                self.measurement_duration_candidates
            ),
            "warmup_bucket_width": self.warmup_bucket_width,
            "warmup_window_length": self.warmup_window_length,
            "warmup_reference_tail_fraction": self.warmup_reference_tail_fraction,
            "warmup_required_stable_windows": self.warmup_required_stable_windows,
            "warmup_relative_tolerance": self.warmup_relative_tolerance,
            "warmup_replication_count": self.warmup_replication_count,
            "minimum_optimization_replications": (
                self.minimum_optimization_replications
            ),
            "maximum_optimization_replications": (
                self.maximum_optimization_replications
            ),
            "replication_batch_size": self.replication_batch_size,
            "maximum_sequential_looks": self.maximum_sequential_looks,
            "overall_alpha": self.overall_alpha,
            "confidence_level": self.confidence_level,
            "request_class_slos": self.slos,
            "latency_slo_kind": self.latency_slo_kind,
            "p95_confidence_interval_method": (self.p95_confidence_interval_method),
            "bootstrap_resample_count": self.bootstrap_resample_count,
            "minimum_bootstrap_tail_draws": self.minimum_bootstrap_tail_draws,
            "target_samples_per_class": self.target_samples_per_class,
            "minimum_eligible_samples_per_request_class": (
                self.minimum_eligible_samples_per_request_class
            ),
            "minimum_nonempty_replications_per_request_class": (
                self.minimum_nonempty_replications_per_request_class
            ),
            "relative_ci_half_width_target": self.relative_ci_half_width_target,
            "absolute_ci_half_width_target": self.absolute_ci_half_width_target,
            "measurement_stability_tolerance": self.measurement_stability_tolerance,
            "ci_width_stability_tolerance": self.ci_width_stability_tolerance,
            "calibration_seeds": list(self.calibration_seeds),
            "optimization_seed_policy": self.optimization_seed_policy.to_dict(),
            "final_validation_seeds": list(self.final_validation_seeds),
            "final_validation_replication_count": (
                self.final_validation_replication_count
            ),
            "representative_configurations": [
                list(configuration)
                for configuration in self.representative_configurations
            ],
            "maximum_drain_time": self.maximum_drain_time,
            "creation_timestamp": self.creation_timestamp,
            "calibration_diagnostics": self.diagnostics,
            "profile_fingerprint": self.profile_fingerprint,
        }

    @classmethod
    def from_dict(cls, value):
        bounds = value["bounded_replica_configuration_space"]
        profile = cls(
            profile_version=int(value["profile_version"]),
            topology_fingerprint=str(value["topology_fingerprint"]),
            workload_fingerprint=str(value["workload_fingerprint"]),
            simulator_semantics_version=str(value["simulator_semantics_version"]),
            minimum_replica_configuration=tuple(int(v) for v in bounds["minimum"]),
            maximum_replica_configuration=tuple(int(v) for v in bounds["maximum"]),
            warmup_duration=float(value["warmup_duration"]),
            measurement_duration=float(value["measurement_duration"]),
            measurement_duration_candidates=tuple(
                float(v) for v in value["measurement_duration_candidates"]
            ),
            warmup_bucket_width=float(value["warmup_bucket_width"]),
            warmup_window_length=int(value["warmup_window_length"]),
            warmup_reference_tail_fraction=float(
                value["warmup_reference_tail_fraction"]
            ),
            warmup_required_stable_windows=int(value["warmup_required_stable_windows"]),
            warmup_relative_tolerance=float(value["warmup_relative_tolerance"]),
            warmup_replication_count=int(value["warmup_replication_count"]),
            minimum_optimization_replications=int(
                value["minimum_optimization_replications"]
            ),
            maximum_optimization_replications=int(
                value["maximum_optimization_replications"]
            ),
            replication_batch_size=int(value["replication_batch_size"]),
            maximum_sequential_looks=int(value["maximum_sequential_looks"]),
            overall_alpha=float(value["overall_alpha"]),
            request_class_slos=tuple(
                sorted(
                    (str(name), float(slo))
                    for name, slo in value["request_class_slos"].items()
                )
            ),
            p95_confidence_interval_method=str(value["p95_confidence_interval_method"]),
            bootstrap_resample_count=int(value["bootstrap_resample_count"]),
            minimum_bootstrap_tail_draws=int(value["minimum_bootstrap_tail_draws"]),
            target_samples_per_class=int(value["target_samples_per_class"]),
            minimum_eligible_samples_per_request_class=int(
                value["minimum_eligible_samples_per_request_class"]
            ),
            minimum_nonempty_replications_per_request_class=int(
                value["minimum_nonempty_replications_per_request_class"]
            ),
            relative_ci_half_width_target=float(value["relative_ci_half_width_target"]),
            absolute_ci_half_width_target=float(value["absolute_ci_half_width_target"]),
            measurement_stability_tolerance=float(
                value["measurement_stability_tolerance"]
            ),
            ci_width_stability_tolerance=float(value["ci_width_stability_tolerance"]),
            calibration_seeds=tuple(int(v) for v in value["calibration_seeds"]),
            optimization_seed_policy=SeedPolicy.from_dict(
                value["optimization_seed_policy"]
            ),
            final_validation_seeds=tuple(
                int(v) for v in value["final_validation_seeds"]
            ),
            final_validation_replication_count=int(
                value["final_validation_replication_count"]
            ),
            representative_configurations=tuple(
                tuple(int(v) for v in configuration)
                for configuration in value["representative_configurations"]
            ),
            maximum_drain_time=float(value["maximum_drain_time"]),
            creation_timestamp=str(value["creation_timestamp"]),
            calibration_diagnostics=_canonical_json(
                value.get("calibration_diagnostics", {})
            ),
            profile_fingerprint=str(value["profile_fingerprint"]),
            latency_slo_kind=str(value.get("latency_slo_kind", "percentile")),
        )
        return profile.validate_integrity()


def _canonical_json(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _fingerprint(value):
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _distribution_fingerprint(specification):
    nonsemantic_fields = {"source", "column", "timestamp_column", "library"}
    return {
        "distribution": specification.distribution,
        "mean": float(specification.mean),
        "scv": float(specification.scv),
        "parameters": [
            (name, value)
            for name, value in specification.parameters
            if name not in nonsemantic_fields
        ],
        "samples": list(specification.samples),
    }


def experiment_fingerprints(system):
    services = tuple(
        sorted(
            system.get_services().values(),
            key=lambda item: item.get_id(),
        )
    )
    request_classes = tuple(system.get_request_classes())
    visit_ratios = getattr(system, "get_request_class_visit_ratios", lambda: {})()
    workflows = getattr(system, "get_request_class_workflows", lambda: {})()
    call_graphs = getattr(system, "get_request_class_call_graphs", lambda: {})()
    call_execution_modes = getattr(
        system,
        "get_request_class_call_execution_modes",
        lambda: {},
    )()
    concurrency_limits = getattr(
        system,
        "get_service_concurrency_limits_per_replica",
        lambda: {service.get_name(): 1 for service in services},
    )()
    processing_slots = getattr(
        system,
        "get_service_processing_slots_per_replica",
        lambda: {service.get_name(): 1 for service in services},
    )()
    fixed_latency_overheads = getattr(
        system,
        "get_request_class_fixed_latency_overheads",
        lambda: {},
    )()
    end_to_end_timeouts = getattr(
        system,
        "get_request_class_end_to_end_timeouts",
        lambda: {},
    )()
    topology = {
        "network_type": system.get_type().value,
        "services": [
            {
                "id": service.get_id(),
                "name": service.get_name(),
                "concurrency_limit_per_replica": concurrency_limits[
                    service.get_name()
                ],
                "processing_slots_per_replica": processing_slots[
                    service.get_name()
                ],
            }
            for service in services
        ],
        "request_classes": [
            {
                "name": request_class,
                "routes": [
                    {
                        "service": service.get_name(),
                        "edges": [
                            {
                                "target": (
                                    edge.get_target().get_name()
                                    if edge.get_target() is not None
                                    else None
                                ),
                                "probability": float(edge.get_routing_prob()),
                            }
                            for edge in service.get_adjacency_list(request_class)
                        ],
                    }
                    for service in services
                ],
                "visits": [
                    {
                        "service": service_name,
                        "expected_visits_per_external_request": float(value),
                    }
                    for service_name, value in sorted(
                        visit_ratios.get(request_class, {}).items()
                    )
                ],
                "workflow": [
                    {
                        "service": visit.service,
                        "mean_visits": float(visit.mean_visits),
                    }
                    for visit in workflows.get(request_class, ())
                ],
                "calls": [
                    {
                        "source": source,
                        "targets": [
                            {
                                "target": target,
                                "mean_calls": float(mean_calls),
                            }
                            for target, mean_calls in targets.items()
                        ],
                    }
                    for source, targets in sorted(
                        call_graphs.get(request_class, {}).items()
                    )
                ],
                "call_execution": call_execution_modes.get(request_class),
            }
            for request_class in request_classes
        ],
    }
    try:
        latency_slos = system.get_request_class_latency_slos()
    except AttributeError:
        latency_slos = {}
    workload = {
        "request_class_fixed_latency_overheads": [
            {
                "request_class": request_class,
                "seconds": float(fixed_latency_overheads.get(request_class, 0.0)),
            }
            for request_class in request_classes
        ],
        "request_class_end_to_end_timeouts": [
            {
                "request_class": request_class,
                "seconds": float(end_to_end_timeouts.get(request_class, 0.0)),
            }
            for request_class in request_classes
        ],
        "request_class_latency_slos": [
            {
                "request_class": request_class,
                "constraint": latency_slos[request_class],
            }
            for request_class in request_classes
            if request_class in latency_slos
        ],
        "traffic_model": {},
    }
    service_specs = system.get_service_time_distribution_specs()
    class_service_specs = getattr(
        system,
        "get_request_class_service_time_distribution_specs",
        lambda: {
            request_class: service_specs for request_class in request_classes
        },
    )()
    arrival_specs = system.get_external_arrival_distribution_specs()
    workload["traffic_model"] = {
        "service_times": [
            {
                "service": service.get_name(),
                "specification": _distribution_fingerprint(
                    service_specs[service.get_name()]
                ),
            }
            for service in services
        ],
        "request_class_service_times": [
            {
                "request_class": request_class,
                "services": [
                    {
                        "service": service_name,
                        "specification": _distribution_fingerprint(specification),
                    }
                    for service_name, specification in sorted(
                        class_service_specs[request_class].items()
                    )
                ],
            }
            for request_class in request_classes
        ],
        "external_arrivals": [
            {
                "request_class": request_class,
                "streams": [
                    {
                        "service": service_name,
                        "specification": _distribution_fingerprint(spec),
                    }
                    for service_name, spec in sorted(
                        arrival_specs[request_class].items()
                    )
                ],
            }
            for request_class in request_classes
        ],
    }
    return _fingerprint(topology), _fingerprint(workload)


def _profile_fingerprint(profile):
    value = profile.to_dict()
    value.pop("profile_fingerprint", None)
    value.pop("creation_timestamp", None)
    value.pop("calibration_diagnostics", None)
    return _fingerprint(value)


def _normalize_configuration(configuration, service_count, label):
    configuration = tuple(configuration)
    if len(configuration) != service_count:
        raise ValueError(f"{label} must define every service.")
    if any(
        isinstance(v, bool) or not isinstance(v, int) or v <= 0 for v in configuration
    ):
        raise ValueError(f"{label} values must be positive integers.")
    return configuration


def build_calibration_profile(
    system,
    *,
    maximum_configuration,
    request_class_slos,
    warmup_duration,
    measurement_duration,
    measurement_duration_candidates,
    warmup_bucket_width,
    representative_configurations,
    calibration_diagnostics,
    options=None,
    calibration_seeds=None,
    creation_timestamp=None,
    latency_slo_kind="percentile",
):
    options = options or CalibrationOptions()
    service_count = len(system.get_services())
    minimum = _normalize_configuration(
        system.get_minimum_stable_replicas(), service_count, "minimum configuration"
    )
    maximum = _normalize_configuration(
        maximum_configuration, service_count, "maximum configuration"
    )
    if any(high < low for low, high in zip(minimum, maximum)):
        raise ValueError("Maximum configuration cannot be below the stable minimum.")
    slos = tuple(
        sorted((str(name), float(slo)) for name, slo in request_class_slos.items())
    )
    request_classes = set(system.get_request_classes())
    slo_classes = {name for name, _ in slos}
    if not slo_classes or not slo_classes <= request_classes:
        raise ValueError(
            "SLO definitions must be a nonempty subset of the request classes."
        )
    if any(not math.isfinite(slo) or slo <= 0 for _, slo in slos):
        raise ValueError("Request-class SLOs must be finite and positive.")
    if latency_slo_kind not in {"mean", "percentile"}:
        raise ValueError("latency_slo_kind must be mean or percentile.")
    if not math.isfinite(warmup_duration) or warmup_duration < 0:
        raise ValueError("warmup_duration must be finite and non-negative.")
    if not math.isfinite(measurement_duration) or measurement_duration <= 0:
        raise ValueError("measurement_duration must be finite and positive.")
    candidates = tuple(float(value) for value in measurement_duration_candidates)
    if not candidates or any(not math.isfinite(v) or v <= 0 for v in candidates):
        raise ValueError("Measurement duration candidates must be finite and positive.")
    if float(measurement_duration) not in candidates:
        raise ValueError("Accepted measurement duration must be one of the candidates.")
    representatives = tuple(
        dict.fromkeys(
            _normalize_configuration(
                item,
                service_count,
                "representative configuration",
            )
            for item in representative_configurations
        )
    )
    for configuration in representatives:
        if any(
            value < low or value > high
            for value, low, high in zip(configuration, minimum, maximum)
        ):
            raise ValueError("Representative configuration lies outside the bounds.")
    seed_policy = options.seed_policy
    if calibration_seeds is None:
        calibration_seeds = seed_policy.seeds(
            "warmup_calibration", options.warmup_replications
        )
    final_seeds = seed_policy.seeds(
        "final_validation", options.final_validation_replications
    )
    topology_fingerprint, workload_fingerprint = experiment_fingerprints(system)
    profile = CalibrationProfile(
        profile_version=CALIBRATION_PROFILE_VERSION,
        topology_fingerprint=topology_fingerprint,
        workload_fingerprint=workload_fingerprint,
        simulator_semantics_version=SIMULATOR_SEMANTICS_VERSION,
        minimum_replica_configuration=minimum,
        maximum_replica_configuration=maximum,
        warmup_duration=float(warmup_duration),
        measurement_duration=float(measurement_duration),
        measurement_duration_candidates=candidates,
        warmup_bucket_width=float(warmup_bucket_width),
        warmup_window_length=options.warmup_window_length,
        warmup_reference_tail_fraction=options.warmup_reference_tail_fraction,
        warmup_required_stable_windows=options.warmup_required_stable_windows,
        warmup_relative_tolerance=options.warmup_relative_tolerance,
        warmup_replication_count=options.warmup_replications,
        minimum_optimization_replications=options.minimum_optimization_replications,
        maximum_optimization_replications=options.maximum_optimization_replications,
        replication_batch_size=options.replication_batch_size,
        maximum_sequential_looks=options.maximum_sequential_looks,
        overall_alpha=options.overall_alpha,
        request_class_slos=slos,
        p95_confidence_interval_method=(
            MEAN_CONFIDENCE_INTERVAL_METHOD
            if latency_slo_kind == "mean"
            else P95_CONFIDENCE_INTERVAL_METHOD
        ),
        bootstrap_resample_count=options.bootstrap_resample_count,
        minimum_bootstrap_tail_draws=options.minimum_bootstrap_tail_draws,
        target_samples_per_class=options.target_samples_per_class,
        minimum_eligible_samples_per_request_class=(
            options.minimum_eligible_samples_per_class
        ),
        minimum_nonempty_replications_per_request_class=(
            options.minimum_nonempty_replications_per_class
        ),
        relative_ci_half_width_target=options.relative_ci_half_width_target,
        absolute_ci_half_width_target=options.absolute_ci_half_width_target,
        measurement_stability_tolerance=options.measurement_stability_tolerance,
        ci_width_stability_tolerance=options.ci_width_stability_tolerance,
        calibration_seeds=tuple(calibration_seeds),
        optimization_seed_policy=seed_policy,
        final_validation_seeds=final_seeds,
        final_validation_replication_count=options.final_validation_replications,
        representative_configurations=representatives,
        maximum_drain_time=options.maximum_drain_time,
        creation_timestamp=(
            creation_timestamp
            or datetime.now(timezone.utc).isoformat(timespec="seconds")
        ),
        calibration_diagnostics=_canonical_json(calibration_diagnostics),
        profile_fingerprint="",
        latency_slo_kind=latency_slo_kind,
    )
    profile = replace(profile, profile_fingerprint=_profile_fingerprint(profile))
    return profile.validate_integrity()


def validate_profile_for_experiment(
    profile,
    system,
    *,
    request_class_slos=None,
    maximum_configuration=None,
):
    profile.validate_integrity()
    topology, workload = experiment_fingerprints(system)
    mismatches = []
    if profile.topology_fingerprint != topology:
        mismatches.append("topology")
    if profile.workload_fingerprint != workload:
        mismatches.append("workload")
    if profile.simulator_semantics_version != SIMULATOR_SEMANTICS_VERSION:
        mismatches.append("simulator semantics")
    if request_class_slos is not None:
        current_slos = tuple(
            sorted((str(name), float(slo)) for name, slo in request_class_slos.items())
        )
        if current_slos != profile.request_class_slos:
            mismatches.append("request-class SLOs")
    if maximum_configuration is not None:
        current_maximum = _normalize_configuration(
            maximum_configuration,
            len(profile.maximum_replica_configuration),
            "maximum configuration",
        )
        if current_maximum != profile.maximum_replica_configuration:
            mismatches.append("replica bounds")
    if mismatches:
        raise CalibrationProfileMismatchError(
            "Calibration profile is invalid for the current "
            + ", ".join(mismatches)
            + "."
        )
    return profile


def save_calibration_profile(profile, path):
    profile.validate_integrity()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output:
        yaml.safe_dump(profile.to_dict(), output, sort_keys=True)
    return path


def load_calibration_profile(path):
    with Path(path).open("r", encoding="utf-8") as source:
        value = yaml.safe_load(source)
    if not isinstance(value, dict):
        raise CalibrationProfileMismatchError("Calibration profile must be a mapping.")
    return CalibrationProfile.from_dict(value)

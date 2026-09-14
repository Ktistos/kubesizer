from dataclasses import dataclass
from enum import Enum

from calibration import validate_profile_for_experiment

from metrics import (
    ClusterBootstrapP95Interval,
    IndependentReplicationRateInterval,
    fixed_experiment_tail_alpha,
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
    independent_replication_rate_interval,
    ReliabilityStatus,
    RequestClassP95Reliability,
    evaluate_request_class_p95_reliability,
)


MINIMUM_FINAL_VALIDATION_REPLICATIONS = 30


class FinalReplicaValidationError(RuntimeError):
    """Raised when a selected configuration fails fresh-seed validation."""

    def __init__(self, message, result=None):
        super().__init__(message)
        self.result = result


@dataclass(frozen=True)
class FinalReplicaValidationResult:
    replica_configuration: tuple[int, ...]
    reliability: RequestClassP95Reliability

    @property
    def passed(self):
        return self.reliability.status == ReliabilityStatus.RELIABLY_FEASIBLE


def validate_selected_replica_configuration(
    system,
    configuration,
    *,
    warmup_duration,
    measurement_duration,
    request_class_slos,
    seeds,
    workers=1,
    confidence_level=0.95,
    excluded_seeds=(),
):
    """Validate a locked optimizer result using a completely fresh seed set."""
    configuration = tuple(configuration)
    seeds = tuple(seeds)
    if len(seeds) < MINIMUM_FINAL_VALIDATION_REPLICATIONS:
        raise ValueError(
            "Final validation requires at least "
            f"{MINIMUM_FINAL_VALIDATION_REPLICATIONS} independent seeds."
        )
    if len(set(seeds)) != len(seeds):
        raise ValueError("Validation seeds must be unique.")
    overlap = set(seeds).intersection(excluded_seeds)
    if overlap:
        raise ValueError(
            "Final-validation seeds overlap optimizer seeds: "
            + ", ".join(str(seed) for seed in sorted(overlap))
        )

    system.configure_system_replicas(configuration)
    if not system.solve().get_stability():
        raise FinalReplicaValidationError(
            "The selected configuration is analytically unstable."
        )
    replications = system.simulate_measurement_replications(
        warmup_duration=warmup_duration,
        measurement_duration=measurement_duration,
        seeds=seeds,
        workers=workers,
    )
    try:
        reliability = evaluate_request_class_p95_reliability(
            replications,
            request_class_slos,
            confidence_level=confidence_level,
            adjust_for_multiple_classes=True,
        )
    except ValueError as error:
        raise FinalReplicaValidationError(
            f"Final validation could not calculate reliable p95 intervals: {error}"
        ) from error
    result = FinalReplicaValidationResult(
        replica_configuration=configuration,
        reliability=reliability,
    )
    if not result.passed:
        failing = ", ".join(
            f"{name}={interval.status.value}"
            for name, interval in reliability.request_class_intervals.items()
            if interval.status != ReliabilityStatus.RELIABLY_FEASIBLE
        )
        raise FinalReplicaValidationError(
            "Selected configuration failed independent final validation: " + failing,
            result=result,
        )
    return result


class FinalValidationClassification(str, Enum):
    VALIDATED_FEASIBLE = "VALIDATED_FEASIBLE"
    FAILED_SLO_VALIDATION = "FAILED_SLO_VALIDATION"
    ANALYTICALLY_UNSTABLE = "ANALYTICALLY_UNSTABLE"
    SIMULATION_FAILURE = "SIMULATION_FAILURE"
    CALIBRATION_FAILURE = "CALIBRATION_FAILURE"


@dataclass(frozen=True)
class CalibratedFinalValidationResult:
    replica_configuration: tuple[int, ...]
    classification: FinalValidationClassification
    replication_count: int
    seeds: tuple[int, ...]
    request_class_intervals: tuple[tuple[str, ClusterBootstrapP95Interval], ...]
    failure_rate_intervals: tuple[tuple[str, IndependentReplicationRateInterval], ...]
    eligible_request_counts: tuple[tuple[str, int], ...]
    warmup_duration: float
    measurement_duration: float
    calibration_profile_fingerprint: str
    simulation_diagnostics: tuple[tuple[str, float | int], ...]
    failure_reason: str | None = None

    @property
    def passed(self):
        return self.classification == FinalValidationClassification.VALIDATED_FEASIBLE

    @property
    def intervals(self):
        return dict(self.request_class_intervals)

    @property
    def failure_intervals(self):
        return dict(self.failure_rate_intervals)


def validate_final_configuration(
    system,
    configuration,
    calibration_profile,
    *,
    workers=1,
    progress_callback=None,
):
    """Run one fixed fresh-seed holdout using familywise cluster-bootstrap CIs."""
    configuration = tuple(configuration)
    profile = calibration_profile
    if progress_callback is not None and not callable(progress_callback):
        raise TypeError("progress_callback must be callable or None.")
    base = dict(
        replica_configuration=configuration,
        replication_count=0,
        seeds=(),
        request_class_intervals=(),
        failure_rate_intervals=(),
        eligible_request_counts=(),
        warmup_duration=profile.warmup_duration,
        measurement_duration=profile.measurement_duration,
        calibration_profile_fingerprint=profile.profile_fingerprint,
        simulation_diagnostics=(),
    )
    try:
        validate_profile_for_experiment(
            profile,
            system,
            request_class_slos=profile.slos,
            request_class_failure_rate_slos=profile.failure_rate_slos,
            maximum_configuration=profile.maximum_replica_configuration,
        )
    except Exception as error:
        return CalibratedFinalValidationResult(
            classification=FinalValidationClassification.CALIBRATION_FAILURE,
            failure_reason=str(error),
            **base,
        )
    if (
        len(configuration) != len(profile.maximum_replica_configuration)
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in configuration
        )
        or any(
            value > maximum
            for value, maximum in zip(
                configuration,
                profile.maximum_replica_configuration,
            )
        )
    ):
        return CalibratedFinalValidationResult(
            classification=FinalValidationClassification.CALIBRATION_FAILURE,
            failure_reason="Configuration lies outside calibrated replica bounds.",
            **base,
        )
    original = tuple(system.get_system_replicas())
    try:
        system.configure_system_replicas(configuration)
        if not system.solve().get_stability():
            return CalibratedFinalValidationResult(
                classification=FinalValidationClassification.ANALYTICALLY_UNSTABLE,
                failure_reason="Selected configuration is analytically unstable.",
                **base,
            )
        seeds = tuple(profile.final_validation_seeds)
        optimization_seeds = set(profile.optimization_seeds())
        if optimization_seeds.intersection(seeds):
            return CalibratedFinalValidationResult(
                classification=FinalValidationClassification.CALIBRATION_FAILURE,
                failure_reason="Final-validation seeds overlap optimization seeds.",
                **base,
            )
        total_replications = len(seeds)
        if progress_callback is not None:
            progress_callback(
                "final_validation_started",
                {
                    "configuration": configuration,
                    "completed_replications": 0,
                    "total_replications": total_replications,
                },
            )
        try:
            replications = tuple(
                system.simulate_measurement_replications(
                    warmup_duration=profile.warmup_duration,
                    measurement_duration=profile.measurement_duration,
                    seeds=seeds,
                    workers=workers,
                    maximum_drain_time=profile.maximum_drain_time,
                )
            )
            if progress_callback is not None:
                progress_callback(
                    "final_validation_replications_completed",
                    {
                        "configuration": configuration,
                        "completed_replications": len(replications),
                        "total_replications": total_replications,
                    },
                )
        except Exception as error:
            return CalibratedFinalValidationResult(
                classification=FinalValidationClassification.SIMULATION_FAILURE,
                failure_reason=str(error),
                **base,
            )
        if tuple(item.seed for item in replications) != seeds:
            return CalibratedFinalValidationResult(
                replica_configuration=configuration,
                classification=FinalValidationClassification.SIMULATION_FAILURE,
                replication_count=0,
                seeds=(),
                request_class_intervals=(),
                failure_rate_intervals=(),
                eligible_request_counts=(),
                warmup_duration=profile.warmup_duration,
                measurement_duration=profile.measurement_duration,
                calibration_profile_fingerprint=profile.profile_fingerprint,
                simulation_diagnostics=(),
                failure_reason="Simulator returned a different final seed sequence.",
            )
        tail_alpha = fixed_experiment_tail_alpha(
            profile.overall_alpha,
            len(profile.request_class_slos)
            + len(profile.request_class_failure_rate_slos),
        )
        intervals = []
        counts = []
        for request_class, slo in profile.request_class_slos:
            try:
                clusters = tuple(
                    tuple(item.request_class_latencies[request_class])
                    for item in replications
                )
            except (AttributeError, KeyError, TypeError):
                return CalibratedFinalValidationResult(
                    replica_configuration=configuration,
                    classification=FinalValidationClassification.SIMULATION_FAILURE,
                    replication_count=len(replications),
                    seeds=seeds,
                    request_class_intervals=tuple(intervals),
                    failure_rate_intervals=(),
                    eligible_request_counts=tuple(counts),
                    warmup_duration=profile.warmup_duration,
                    measurement_duration=profile.measurement_duration,
                    calibration_profile_fingerprint=profile.profile_fingerprint,
                    simulation_diagnostics=(),
                    failure_reason=(f"Missing raw final cohort for {request_class!r}."),
                )
            total = sum(len(cluster) for cluster in clusters)
            nonempty = sum(bool(cluster) for cluster in clusters)
            counts.append((request_class, total))
            if (
                total < profile.minimum_eligible_samples_per_request_class
                or nonempty < profile.minimum_nonempty_replications_per_request_class
            ):
                return CalibratedFinalValidationResult(
                    replica_configuration=configuration,
                    classification=FinalValidationClassification.SIMULATION_FAILURE,
                    replication_count=len(replications),
                    seeds=seeds,
                    request_class_intervals=tuple(intervals),
                    failure_rate_intervals=(),
                    eligible_request_counts=tuple(counts),
                    warmup_duration=profile.warmup_duration,
                    measurement_duration=profile.measurement_duration,
                    calibration_profile_fingerprint=profile.profile_fingerprint,
                    simulation_diagnostics=(),
                    failure_reason=(
                        f"Final cohort for {request_class!r} does not meet minimum "
                        "sample requirements."
                    ),
                )
            try:
                bootstrap = (
                    replication_cluster_bootstrap_mean
                    if profile.latency_slo_kind == "mean"
                    else replication_cluster_bootstrap_p95
                )
                interval = bootstrap(
                    clusters,
                    request_class=request_class,
                    slo=slo,
                    resample_count=profile.bootstrap_resample_count,
                    seed=profile.optimization_seed_policy.deterministic_seed(
                        "bootstrap",
                        "final-validation",
                        profile.profile_fingerprint,
                        configuration,
                        request_class,
                    ),
                    lower_tail_probability=tail_alpha,
                    upper_tail_probability=1.0 - tail_alpha,
                    minimum_tail_draws=profile.minimum_bootstrap_tail_draws,
                )
            except ValueError as error:
                return CalibratedFinalValidationResult(
                    replica_configuration=configuration,
                    classification=FinalValidationClassification.SIMULATION_FAILURE,
                    replication_count=len(replications),
                    seeds=seeds,
                    request_class_intervals=tuple(intervals),
                    failure_rate_intervals=(),
                    eligible_request_counts=tuple(counts),
                    warmup_duration=profile.warmup_duration,
                    measurement_duration=profile.measurement_duration,
                    calibration_profile_fingerprint=profile.profile_fingerprint,
                    simulation_diagnostics=(),
                    failure_reason=str(error),
                )
            intervals.append((request_class, interval))
        failure_intervals = []
        for request_class, slo in profile.request_class_failure_rate_slos:
            try:
                clusters = tuple(
                    tuple(item.request_class_failures[request_class])
                    for item in replications
                )
                latency_sizes = tuple(
                    len(item.request_class_latencies[request_class])
                    for item in replications
                )
            except (AttributeError, KeyError, TypeError):
                return CalibratedFinalValidationResult(
                    replica_configuration=configuration,
                    classification=FinalValidationClassification.SIMULATION_FAILURE,
                    replication_count=len(replications),
                    seeds=seeds,
                    request_class_intervals=tuple(intervals),
                    failure_rate_intervals=tuple(failure_intervals),
                    eligible_request_counts=tuple(counts),
                    warmup_duration=profile.warmup_duration,
                    measurement_duration=profile.measurement_duration,
                    calibration_profile_fingerprint=profile.profile_fingerprint,
                    simulation_diagnostics=(),
                    failure_reason=(
                        f"Missing raw final failure cohort for {request_class!r}."
                    ),
                )
            if tuple(len(cluster) for cluster in clusters) != latency_sizes:
                return CalibratedFinalValidationResult(
                    replica_configuration=configuration,
                    classification=FinalValidationClassification.SIMULATION_FAILURE,
                    replication_count=len(replications),
                    seeds=seeds,
                    request_class_intervals=tuple(intervals),
                    failure_rate_intervals=tuple(failure_intervals),
                    eligible_request_counts=tuple(counts),
                    warmup_duration=profile.warmup_duration,
                    measurement_duration=profile.measurement_duration,
                    calibration_profile_fingerprint=profile.profile_fingerprint,
                    simulation_diagnostics=(),
                    failure_reason=(
                        f"Failure and latency cohorts disagree for {request_class!r}."
                    ),
                )
            if sum(bool(cluster) for cluster in clusters) < (
                profile.minimum_nonempty_replications_per_request_class
            ):
                return CalibratedFinalValidationResult(
                    replica_configuration=configuration,
                    classification=FinalValidationClassification.SIMULATION_FAILURE,
                    replication_count=len(replications),
                    seeds=seeds,
                    request_class_intervals=tuple(intervals),
                    failure_rate_intervals=tuple(failure_intervals),
                    eligible_request_counts=tuple(counts),
                    warmup_duration=profile.warmup_duration,
                    measurement_duration=profile.measurement_duration,
                    calibration_profile_fingerprint=profile.profile_fingerprint,
                    simulation_diagnostics=(),
                    failure_reason=(
                        f"Final failure cohort for {request_class!r} does not "
                        "meet the nonempty-replication requirement."
                    ),
                )
            failure_intervals.append(
                (
                    request_class,
                    independent_replication_rate_interval(
                        clusters,
                        request_class=request_class,
                        slo=slo,
                        per_tail_alpha=tail_alpha,
                    ),
                )
            )
        passed = all(
            interval.upper_bound <= interval.slo for _, interval in intervals
        ) and all(
            interval.upper_bound <= interval.slo for _, interval in failure_intervals
        )
        diagnostics = (
            (
                "processed_event_count",
                sum(
                    int(getattr(item, "processed_event_count", 0))
                    for item in replications
                ),
            ),
            (
                "external_arrival_count",
                sum(
                    int(getattr(item, "external_arrival_count", 0))
                    for item in replications
                ),
            ),
            (
                "completed_request_count",
                sum(
                    int(getattr(item, "completed_request_count", 0))
                    for item in replications
                ),
            ),
            (
                "failed_measured_request_count",
                sum(
                    sum(
                        int(value)
                        for value in getattr(
                            item,
                            "failed_measured_request_counts",
                            {},
                        ).values()
                    )
                    for item in replications
                ),
            ),
            (
                "sampled_input_block_count",
                sum(
                    getattr(item, "sampled_input_block_start", None) is not None
                    for item in replications
                ),
            ),
        )
        return CalibratedFinalValidationResult(
            replica_configuration=configuration,
            classification=(
                FinalValidationClassification.VALIDATED_FEASIBLE
                if passed
                else FinalValidationClassification.FAILED_SLO_VALIDATION
            ),
            replication_count=len(seeds),
            seeds=seeds,
            request_class_intervals=tuple(intervals),
            failure_rate_intervals=tuple(failure_intervals),
            eligible_request_counts=tuple(counts),
            warmup_duration=profile.warmup_duration,
            measurement_duration=profile.measurement_duration,
            calibration_profile_fingerprint=profile.profile_fingerprint,
            simulation_diagnostics=diagnostics,
            failure_reason=(
                None
                if passed
                else (
                    "At least one fresh-seed latency or terminal-failure-rate upper "
                    "bound exceeds its SLO."
                )
            ),
        )
    finally:
        system.configure_system_replicas(original)

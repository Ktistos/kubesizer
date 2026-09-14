from dataclasses import dataclass
from enum import Enum

from calibration import validate_profile_for_experiment

from metrics import (
    ClusterBootstrapP95Interval,
    IndependentReplicationRateInterval,
    fixed_experiment_tail_alpha,
)

from .replication_evidence import (
    ReplicationEvidenceError,
    calculate_replication_evidence,
    replication_diagnostics,
)


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

    def result(
        classification,
        failure_reason=None,
        *,
        replication_count=0,
        seeds=(),
        intervals=(),
        failure_intervals=(),
        counts=(),
        diagnostics=(),
    ):
        return CalibratedFinalValidationResult(
            replica_configuration=configuration,
            classification=classification,
            replication_count=replication_count,
            seeds=seeds,
            request_class_intervals=tuple(intervals),
            failure_rate_intervals=tuple(failure_intervals),
            eligible_request_counts=tuple(counts),
            warmup_duration=profile.warmup_duration,
            measurement_duration=profile.measurement_duration,
            calibration_profile_fingerprint=profile.profile_fingerprint,
            simulation_diagnostics=diagnostics,
            failure_reason=failure_reason,
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
        return result(
            FinalValidationClassification.CALIBRATION_FAILURE,
            str(error),
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
        return result(
            FinalValidationClassification.CALIBRATION_FAILURE,
            "Configuration lies outside calibrated replica bounds.",
        )
    original = tuple(system.get_system_replicas())
    try:
        system.configure_system_replicas(configuration)
        if not system.solve().get_stability():
            return result(
                FinalValidationClassification.ANALYTICALLY_UNSTABLE,
                "Selected configuration is analytically unstable.",
            )
        seeds = tuple(profile.final_validation_seeds)
        optimization_seeds = set(profile.optimization_seeds())
        if optimization_seeds.intersection(seeds):
            return result(
                FinalValidationClassification.CALIBRATION_FAILURE,
                "Final-validation seeds overlap optimization seeds.",
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
            return result(
                FinalValidationClassification.SIMULATION_FAILURE,
                str(error),
            )
        if tuple(item.seed for item in replications) != seeds:
            return result(
                FinalValidationClassification.SIMULATION_FAILURE,
                "Simulator returned a different final seed sequence.",
            )
        tail_alpha = fixed_experiment_tail_alpha(
            profile.overall_alpha,
            len(profile.request_class_slos)
            + len(profile.request_class_failure_rate_slos),
        )
        try:
            evidence = calculate_replication_evidence(
                profile,
                configuration,
                replications,
                tail_alpha=tail_alpha,
                seed_scope="final-validation",
                require_complete=True,
            )
        except ReplicationEvidenceError as error:
            evidence = error.evidence
            return result(
                FinalValidationClassification.SIMULATION_FAILURE,
                str(error),
                replication_count=len(replications),
                seeds=seeds,
                intervals=evidence.latency_intervals,
                failure_intervals=evidence.failure_intervals,
                counts=evidence.eligible_request_counts,
            )
        intervals = evidence.latency_intervals
        failure_intervals = evidence.failure_intervals
        counts = evidence.eligible_request_counts
        passed = all(
            interval.upper_bound <= interval.slo for _, interval in intervals
        ) and all(
            interval.upper_bound <= interval.slo for _, interval in failure_intervals
        )
        diagnostics = replication_diagnostics(replications) + (
            (
                "sampled_input_block_count",
                sum(
                    getattr(item, "sampled_input_block_start", None) is not None
                    for item in replications
                ),
            ),
        )
        return result(
            (
                FinalValidationClassification.VALIDATED_FEASIBLE
                if passed
                else FinalValidationClassification.FAILED_SLO_VALIDATION
            ),
            (
                None
                if passed
                else (
                    "At least one fresh-seed latency or terminal-failure-rate upper "
                    "bound exceeds its SLO."
                )
            ),
            replication_count=len(seeds),
            seeds=seeds,
            intervals=intervals,
            failure_intervals=failure_intervals,
            counts=counts,
            diagnostics=diagnostics,
        )
    finally:
        system.configure_system_replicas(original)

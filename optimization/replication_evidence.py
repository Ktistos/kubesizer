import math
import statistics
from dataclasses import dataclass
from enum import Enum

from metrics import (
    ClusterBootstrapP95Interval,
    IndependentReplicationRateInterval,
    independent_replication_rate_interval,
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
)


@dataclass(frozen=True)
class ReplicationEvidence:
    latency_intervals: tuple[tuple[str, ClusterBootstrapP95Interval], ...]
    failure_intervals: tuple[tuple[str, IndependentReplicationRateInterval], ...]
    eligible_request_counts: tuple[tuple[str, int], ...]


class ReplicationEvidenceError(ValueError):
    """Invalid or insufficient replication cohorts, with partial evidence."""

    def __init__(self, message, evidence):
        super().__init__(message)
        self.evidence = evidence


class LatencyGrowthStatus(str, Enum):
    """Finite-window screen for an unidentifiable stationary latency."""

    GROWTH_DETECTED = "GROWTH_DETECTED"
    NO_MATERIAL_GROWTH_DETECTED = "NO_MATERIAL_GROWTH_DETECTED"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True)
class LatencyGrowthDiagnostic:
    request_class: str
    status: LatencyGrowthStatus
    replication_count: int
    early_mean_seconds: float | None = None
    late_mean_seconds: float | None = None
    late_minus_early_seconds: float | None = None
    contrast_standard_error_seconds: float | None = None
    practical_threshold_seconds: float | None = None
    reason: str | None = None


def latency_growth_diagnostics(
    replications,
    request_classes,
    *,
    warmup_duration,
    measurement_duration,
    minimum_requests_per_half=30,
    relative_tolerance=0.10,
    standard_error_multiplier=3.0,
):
    """Screen independent-replication early/late latency contrasts.

    This does not prove stationarity.  It only rejects a stationary finite-mean
    interpretation when late-arrival cohorts grow by a practically material
    amount with a conservative standard-error margin.  Requests, rather than
    time buckets, are never treated as independent observations.
    """
    replications = tuple(replications)
    request_classes = tuple(request_classes)
    numeric = {
        "warmup_duration": warmup_duration,
        "measurement_duration": measurement_duration,
        "relative_tolerance": relative_tolerance,
        "standard_error_multiplier": standard_error_multiplier,
    }
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        for value in numeric.values()
    ):
        raise ValueError("Latency-growth screen parameters must be finite numbers.")
    if warmup_duration < 0.0 or measurement_duration <= 0.0:
        raise ValueError("Warm-up must be non-negative and measurement positive.")
    if relative_tolerance <= 0.0 or standard_error_multiplier <= 0.0:
        raise ValueError("Latency-growth tolerances must be positive.")
    if (
        isinstance(minimum_requests_per_half, bool)
        or not isinstance(minimum_requests_per_half, int)
        or minimum_requests_per_half <= 0
    ):
        raise ValueError("minimum_requests_per_half must be a positive integer.")
    if not request_classes:
        raise ValueError("At least one request class is required.")

    split = warmup_duration + measurement_duration / 2.0
    end = warmup_duration + measurement_duration
    results = []
    for request_class in request_classes:
        pairs = []
        reason = None
        for replication in replications:
            try:
                arrivals = tuple(
                    replication.request_class_arrival_times[request_class]
                )
                latencies = tuple(replication.request_class_latencies[request_class])
            except (AttributeError, KeyError, TypeError):
                reason = "raw request arrival/latency cohorts are unavailable"
                break
            if len(arrivals) != len(latencies):
                raise ValueError(
                    f"Arrival and latency cohorts disagree for {request_class!r}."
                )
            if any(
                not math.isfinite(arrival)
                or not warmup_duration <= arrival < end
                for arrival in arrivals
            ):
                raise ValueError(
                    f"Arrival outside the measured cohort for {request_class!r}."
                )
            if any(not math.isfinite(value) or value < 0.0 for value in latencies):
                raise ValueError(f"Invalid latency for {request_class!r}.")
            early = tuple(
                value
                for arrival, value in zip(arrivals, latencies, strict=True)
                if arrival < split
            )
            late = tuple(
                value
                for arrival, value in zip(arrivals, latencies, strict=True)
                if arrival >= split
            )
            if min(len(early), len(late)) < minimum_requests_per_half:
                reason = (
                    "fewer than the required requests occur in one measurement half"
                )
                break
            pairs.append((statistics.fmean(early), statistics.fmean(late)))

        if reason is not None or len(pairs) < 3:
            results.append(
                LatencyGrowthDiagnostic(
                    request_class=request_class,
                    status=LatencyGrowthStatus.INCONCLUSIVE,
                    replication_count=len(pairs),
                    reason=reason or "fewer than three independent replications",
                )
            )
            continue

        early_mean = statistics.fmean(pair[0] for pair in pairs)
        late_mean = statistics.fmean(pair[1] for pair in pairs)
        differences = tuple(late - early for early, late in pairs)
        difference = statistics.fmean(differences)
        standard_error = statistics.stdev(differences) / math.sqrt(len(pairs))
        threshold = relative_tolerance * (early_mean + late_mean) / 2.0
        if difference - standard_error_multiplier * standard_error > threshold:
            status = LatencyGrowthStatus.GROWTH_DETECTED
        elif (
            abs(difference) + standard_error_multiplier * standard_error
            <= threshold
        ):
            status = LatencyGrowthStatus.NO_MATERIAL_GROWTH_DETECTED
        else:
            status = LatencyGrowthStatus.INCONCLUSIVE
        results.append(
            LatencyGrowthDiagnostic(
                request_class=request_class,
                status=status,
                replication_count=len(pairs),
                early_mean_seconds=early_mean,
                late_mean_seconds=late_mean,
                late_minus_early_seconds=difference,
                contrast_standard_error_seconds=standard_error,
                practical_threshold_seconds=threshold,
            )
        )
    return tuple(results)


def calculate_replication_evidence(
    profile,
    configuration,
    replications,
    *,
    tail_alpha,
    seed_scope,
    seed_suffix=(),
    require_complete=False,
):
    """Build latency and failure intervals from the same replication cohorts."""
    replications = tuple(replications)
    latency_intervals = []
    failure_intervals = []
    counts = []

    def evidence():
        return ReplicationEvidence(
            tuple(latency_intervals),
            tuple(failure_intervals),
            tuple(counts),
        )

    def fail(message, cause=None):
        error = ReplicationEvidenceError(message, evidence())
        if cause is None:
            raise error
        raise error from cause

    bootstrap = (
        replication_cluster_bootstrap_mean
        if profile.latency_slo_kind == "mean"
        else replication_cluster_bootstrap_p95
    )
    for request_class, slo in profile.request_class_slos:
        try:
            clusters = tuple(
                tuple(item.request_class_latencies[request_class])
                for item in replications
            )
        except (AttributeError, KeyError, TypeError) as error:
            message = (
                f"Missing raw final cohort for {request_class!r}."
                if require_complete
                else f"Replication output has no raw cohort for {request_class!r}."
            )
            fail(message, error)
        total = sum(len(cluster) for cluster in clusters)
        nonempty = sum(bool(cluster) for cluster in clusters)
        counts.append((request_class, total))
        sufficiently_sampled = (
            total >= profile.minimum_eligible_samples_per_request_class
            and nonempty >= profile.minimum_nonempty_replications_per_request_class
        )
        if not sufficiently_sampled:
            if require_complete:
                fail(
                    f"Final cohort for {request_class!r} does not meet minimum "
                    "sample requirements."
                )
            continue
        try:
            interval = bootstrap(
                clusters,
                request_class=request_class,
                slo=slo,
                resample_count=profile.bootstrap_resample_count,
                seed=profile.optimization_seed_policy.deterministic_seed(
                    "bootstrap",
                    seed_scope,
                    profile.profile_fingerprint,
                    configuration,
                    request_class,
                    *seed_suffix,
                ),
                lower_tail_probability=tail_alpha,
                upper_tail_probability=1.0 - tail_alpha,
                minimum_tail_draws=profile.minimum_bootstrap_tail_draws,
            )
        except ValueError as error:
            fail(str(error), error)
        latency_intervals.append((request_class, interval))

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
        except (AttributeError, KeyError, TypeError) as error:
            message = (
                f"Missing raw final failure cohort for {request_class!r}."
                if require_complete
                else f"Replication output has no failure cohort for {request_class!r}."
            )
            fail(message, error)
        if tuple(len(cluster) for cluster in clusters) != latency_sizes:
            fail(f"Failure and latency cohorts disagree for {request_class!r}.")
        if sum(bool(cluster) for cluster in clusters) < (
            profile.minimum_nonempty_replications_per_request_class
        ):
            if require_complete:
                fail(
                    f"Final failure cohort for {request_class!r} does not "
                    "meet the nonempty-replication requirement."
                )
            continue
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
    return evidence()


def replication_diagnostics(replications):
    """Aggregate the diagnostics common to optimization and final validation."""
    replications = tuple(replications)
    return (
        (
            "processed_event_count",
            sum(
                int(getattr(item, "processed_event_count", 0)) for item in replications
            ),
        ),
        (
            "external_arrival_count",
            sum(
                int(getattr(item, "external_arrival_count", 0)) for item in replications
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
                        item, "failed_measured_request_counts", {}
                    ).values()
                )
                for item in replications
            ),
        ),
    )

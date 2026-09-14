from dataclasses import dataclass

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

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class IndependentReplicationRateInterval:
    """Distribution-free interval for a mean rate across independent runs."""

    request_class: str
    slo: float
    replication_count: int
    nonempty_replication_count: int
    eligible_request_count: int
    failed_request_count: int
    rates_by_replication: tuple[float, ...]
    estimate: float
    pooled_estimate: float
    lower_bound: float
    upper_bound: float
    half_width: float
    per_tail_alpha: float
    method: str = "independent-replication-empirical-bernstein-v1"

    @property
    def slo_margin(self):
        return self.slo - self.upper_bound


def independent_replication_rate_interval(
    failure_clusters,
    *,
    request_class,
    slo,
    per_tail_alpha,
):
    """Bound the expected per-run failure fraction without iid-request claims.

    Each nonempty replication contributes one bounded observation in ``[0, 1]``.
    The empirical-Bernstein radius is deliberately nonzero when every observed
    run has zero failures; ordinary resampling cannot reveal an unseen event.
    """

    clusters = tuple(tuple(cluster) for cluster in failure_clusters)
    if len(clusters) < 2:
        raise ValueError("A replication-rate interval requires at least two runs.")
    if any(
        any(value not in (False, True, 0, 1) for value in cluster)
        for cluster in clusters
    ):
        raise ValueError("Failure clusters must contain only binary outcomes.")
    if not math.isfinite(float(slo)) or not 0.0 <= float(slo) < 1.0:
        raise ValueError("Failure-rate SLO must be finite and lie in [0, 1).")
    if (
        not math.isfinite(float(per_tail_alpha))
        or not 0.0 < float(per_tail_alpha) < 0.5
    ):
        raise ValueError("per_tail_alpha must lie strictly between zero and 0.5.")

    nonempty = tuple(cluster for cluster in clusters if cluster)
    if len(nonempty) < 2:
        raise ValueError(
            "A replication-rate interval requires at least two nonempty runs."
        )
    rates = tuple(
        sum(bool(value) for value in cluster) / len(cluster) for cluster in nonempty
    )
    count = len(rates)
    estimate = sum(rates) / count
    variance = sum((value - estimate) ** 2 for value in rates) / (count - 1)

    # Maurer-Pontil empirical-Bernstein form for independent observations
    # bounded to [0, 1].  Using delta=2*per_tail_alpha gives a simultaneous
    # two-sided interval under the caller's two-tail Bonferroni allocation.
    log_term = math.log(1.0 / float(per_tail_alpha))
    radius = math.sqrt(2.0 * variance * log_term / count) + (
        7.0 * log_term / (3.0 * (count - 1))
    )
    total = sum(len(cluster) for cluster in nonempty)
    failed = sum(sum(bool(value) for value in cluster) for cluster in nonempty)
    lower = max(0.0, estimate - radius)
    upper = min(1.0, estimate + radius)
    return IndependentReplicationRateInterval(
        request_class=str(request_class),
        slo=float(slo),
        replication_count=len(clusters),
        nonempty_replication_count=count,
        eligible_request_count=total,
        failed_request_count=failed,
        rates_by_replication=rates,
        estimate=estimate,
        pooled_estimate=failed / total,
        lower_bound=lower,
        upper_bound=upper,
        half_width=(upper - lower) / 2.0,
        per_tail_alpha=float(per_tail_alpha),
    )

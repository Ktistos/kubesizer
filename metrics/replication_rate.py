import math
from dataclasses import dataclass


FAILURE_RATE_CONFIDENCE_INTERVAL_METHOD = (
    "independent-replication-kl-hoeffding-v1"
)


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
    method: str = FAILURE_RATE_CONFIDENCE_INTERVAL_METHOD

    @property
    def slo_margin(self):
        return self.slo - self.upper_bound


def minimum_zero_failure_replications(slo, per_tail_alpha):
    """Return runs needed for a zero-observation KL-Hoeffding upper bound."""
    slo = float(slo)
    per_tail_alpha = float(per_tail_alpha)
    if not math.isfinite(slo) or not 0.0 < slo < 1.0:
        raise ValueError(
            "A certifiable failure-rate SLO must lie strictly between zero and one."
        )
    if not math.isfinite(per_tail_alpha) or not 0.0 < per_tail_alpha < 0.5:
        raise ValueError("per_tail_alpha must lie strictly between zero and 0.5.")
    return max(
        2,
        math.ceil(math.log(per_tail_alpha) / math.log1p(-slo)),
    )


def _binary_relative_entropy(observed, candidate):
    if observed == 0.0:
        return -math.log1p(-candidate)
    if observed == 1.0:
        return -math.log(candidate)
    return (
        observed * math.log(observed / candidate)
        + (1.0 - observed)
        * math.log((1.0 - observed) / (1.0 - candidate))
    )


def _kl_hoeffding_bounds(observed, count, per_tail_alpha):
    target = math.log(1.0 / per_tail_alpha) / count
    if observed == 0.0:
        lower = 0.0
        upper = 1.0 - math.exp(-target)
        return lower, upper
    if observed == 1.0:
        lower = math.exp(-target)
        upper = 1.0
        return lower, upper

    low = 0.0
    high = observed
    for _ in range(80):
        middle = (low + high) / 2.0
        if _binary_relative_entropy(observed, middle) > target:
            low = middle
        else:
            high = middle
    lower = high

    low = observed
    high = 1.0
    for _ in range(80):
        middle = (low + high) / 2.0
        if _binary_relative_entropy(observed, middle) > target:
            high = middle
        else:
            low = middle
    return lower, low


def independent_replication_rate_interval(
    failure_clusters,
    *,
    request_class,
    slo,
    per_tail_alpha,
):
    """Bound the expected per-run failure fraction without iid-request claims.

    Each nonempty replication contributes one bounded observation in ``[0, 1]``.
    Inverting the Bernoulli-KL form of Hoeffding's inequality gives a finite-
    sample, distribution-free bound for independent bounded replication rates.
    It remains nonzero when every observed run has zero failures, but is much
    tighter than the previous additive empirical-Bernstein correction.
    """

    clusters = tuple(tuple(cluster) for cluster in failure_clusters)
    if len(clusters) < 2:
        raise ValueError("A replication-rate interval requires at least two runs.")
    if any(
        any(value not in (False, True, 0, 1) for value in cluster)
        for cluster in clusters
    ):
        raise ValueError("Failure clusters must contain only binary outcomes.")
    if not math.isfinite(float(slo)) or not 0.0 < float(slo) < 1.0:
        raise ValueError(
            "Failure-rate SLO must be finite and lie strictly between zero and one."
        )
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
    total = sum(len(cluster) for cluster in nonempty)
    failed = sum(sum(bool(value) for value in cluster) for cluster in nonempty)
    lower, upper = _kl_hoeffding_bounds(
        estimate,
        count,
        float(per_tail_alpha),
    )
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

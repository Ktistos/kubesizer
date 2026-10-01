import math
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class ClusterBootstrapP95Interval:
    request_class: str
    slo: float
    replication_count: int
    nonempty_replication_count: int
    eligible_request_count: int
    eligible_request_counts_by_replication: tuple[int, ...]
    p95_estimate: float
    lower_bound: float
    upper_bound: float
    half_width: float
    relative_half_width: float
    lower_tail_probability: float
    upper_tail_probability: float
    bootstrap_resample_count: int
    bootstrap_seed: int
    bootstrap_p95_values: tuple[float, ...]
    latency_statistic: str = "p95"

    @property
    def estimate(self):
        return self.p95_estimate

    @property
    def bootstrap_values(self):
        return self.bootstrap_p95_values

    @property
    def slo_margin(self):
        """Positive values mean the simultaneous upper bound passes the SLO."""
        return self.slo - self.upper_bound


@dataclass(frozen=True)
class ClusterBootstrapRateInterval:
    request_class: str
    replication_count: int
    eligible_request_count: int
    failed_request_count: int
    estimate: float
    lower_bound: float
    upper_bound: float
    half_width: float
    lower_tail_probability: float
    upper_tail_probability: float
    bootstrap_resample_count: int
    bootstrap_seed: int
    bootstrap_values: tuple[float, ...]


def percentile(values, probability=0.95):
    """Linear sample percentile matching the native simulator definition."""
    values = np.asarray(tuple(values), dtype=float)
    if values.ndim != 1 or values.size == 0:
        raise ValueError("A percentile requires a nonempty one-dimensional sample.")
    if not np.all(np.isfinite(values)) or np.any(values < 0):
        raise ValueError("Latency observations must be finite and non-negative.")
    if not 0 <= probability <= 1:
        raise ValueError("probability must lie in [0, 1].")
    return float(np.quantile(values, probability, method="linear"))


def sequential_tail_alpha(overall_alpha, maximum_looks, request_class_count):
    """Bonferroni tail allocation across two tails, looks, and classes."""
    if not math.isfinite(overall_alpha) or not 0 < overall_alpha < 1:
        raise ValueError("overall_alpha must lie strictly between zero and one.")
    for name, value in (
        ("maximum_looks", maximum_looks),
        ("request_class_count", request_class_count),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer.")
    return overall_alpha / (2 * maximum_looks * request_class_count)


def fixed_experiment_tail_alpha(overall_alpha, request_class_count):
    return sequential_tail_alpha(overall_alpha, 1, request_class_count)


def draw_cluster_bootstrap_indices(replication_count, resample_count, seed):
    """Draw whole-replication identities for deterministic test diagnostics."""
    for name, value in (
        ("replication_count", replication_count),
        ("resample_count", resample_count),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer.")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("seed must be a non-negative integer.")
    generator = np.random.default_rng(seed)
    return tuple(
        tuple(int(value) for value in row)
        for row in generator.integers(
            0,
            replication_count,
            size=(resample_count, replication_count),
        )
    )


def _linear_quantile_from_sorted(values, probability):
    position = (len(values) - 1) * probability
    lower = math.floor(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return float(values[lower] * (1.0 - weight) + values[upper] * weight)


def _weighted_order_statistics(
    sorted_clusters,
    multiplicities,
    ranks,
    pooled_values,
):
    """Select exact ranks from differently weighted sorted clusters.

    A bootstrap draw repeats complete replication clusters. Binary search over
    the pooled observation values lets each draw find its order statistic from
    per-cluster cumulative counts instead of expanding and scanning the full
    resampled latency cohort.
    """
    low = np.zeros(len(ranks), dtype=np.int64)
    high = np.full(len(ranks), len(pooled_values) - 1, dtype=np.int64)
    while np.any(low < high):
        middle = (low + high) // 2
        candidates = pooled_values[middle]
        counts = np.zeros(len(ranks), dtype=np.int64)
        for cluster_index, cluster in enumerate(sorted_clusters):
            counts += multiplicities[:, cluster_index].astype(
                np.int64
            ) * np.searchsorted(cluster, candidates, side="right")
        rank_found = counts > ranks
        high = np.where(rank_found, middle, high)
        low = np.where(rank_found, low, middle + 1)
    return pooled_values[low]


def _draw_nonempty_cluster_multiplicities(
    cluster_sizes,
    *,
    resample_count,
    seed,
):
    replication_count = len(cluster_sizes)
    generator = np.random.default_rng(seed)
    accepted = []
    accepted_count = 0
    while accepted_count < resample_count:
        required = resample_count - accepted_count
        sampled = generator.integers(
            0,
            replication_count,
            size=(required, replication_count),
        )
        multiplicities = np.zeros(
            (required, replication_count),
            dtype=np.int16,
        )
        rows = np.repeat(np.arange(required), replication_count)
        np.add.at(multiplicities, (rows, sampled.reshape(-1)), 1)
        totals = multiplicities.astype(np.int64) @ cluster_sizes
        nonempty = multiplicities[totals > 0]
        if len(nonempty):
            accepted.append(nonempty)
            accepted_count += len(nonempty)
    return np.concatenate(accepted, axis=0)[:resample_count]


def _weighted_bootstrap_p95(
    clusters,
    *,
    resample_count,
    seed,
):
    """Calculate exact cluster-bootstrap p95 values without cohort expansion."""
    sorted_clusters = tuple(np.sort(cluster) for cluster in clusters)
    pooled_values = np.sort(np.concatenate(sorted_clusters))
    cluster_sizes = np.asarray(
        [len(cluster) for cluster in sorted_clusters],
        dtype=np.int64,
    )
    multiplicities = _draw_nonempty_cluster_multiplicities(
        cluster_sizes,
        resample_count=resample_count,
        seed=seed,
    )
    totals = multiplicities.astype(np.int64) @ cluster_sizes
    positions = (totals - 1) * 0.95
    lower_ranks = np.floor(positions).astype(np.int64)
    upper_ranks = np.minimum(lower_ranks + 1, totals - 1)
    weights = positions - lower_ranks
    lower_values = _weighted_order_statistics(
        sorted_clusters,
        multiplicities,
        lower_ranks,
        pooled_values,
    )
    upper_values = _weighted_order_statistics(
        sorted_clusters,
        multiplicities,
        upper_ranks,
        pooled_values,
    )
    bootstrap_values = lower_values * (1.0 - weights) + upper_values * weights
    return pooled_values, tuple(float(value) for value in bootstrap_values)


def replication_cluster_bootstrap_p95(
    latency_clusters,
    *,
    request_class,
    slo,
    resample_count,
    seed,
    lower_tail_probability,
    upper_tail_probability,
    minimum_tail_draws=5,
):
    """Estimate pooled p95 uncertainty by resampling complete replications."""
    clusters = tuple(
        np.fromiter(cluster, dtype=float, count=len(cluster))
        for cluster in latency_clusters
    )
    if len(clusters) < 2:
        raise ValueError("Cluster bootstrap requires at least two replications.")
    if any(cluster.ndim != 1 for cluster in clusters):
        raise ValueError("Every latency cluster must be one-dimensional.")
    if any(
        np.any(~np.isfinite(cluster)) or np.any(cluster < 0) for cluster in clusters
    ):
        raise ValueError("Latency clusters must be finite and non-negative.")
    if sum(len(cluster) for cluster in clusters) == 0:
        raise ValueError("Cluster bootstrap requires at least one latency.")
    if not math.isfinite(float(slo)) or float(slo) <= 0:
        raise ValueError("slo must be finite and positive.")
    if not 0 < lower_tail_probability < upper_tail_probability < 1:
        raise ValueError("Bootstrap interval probabilities are invalid.")
    if (
        isinstance(resample_count, bool)
        or not isinstance(resample_count, int)
        or resample_count <= 0
    ):
        raise ValueError("resample_count must be a positive integer.")
    if (
        isinstance(minimum_tail_draws, bool)
        or not isinstance(minimum_tail_draws, int)
        or minimum_tail_draws <= 0
    ):
        raise ValueError("minimum_tail_draws must be a positive integer.")
    tail_resolution = (
        min(
            lower_tail_probability,
            1.0 - upper_tail_probability,
        )
        * resample_count
    )
    if tail_resolution < minimum_tail_draws:
        raise ValueError(
            "Bootstrap resample count is insufficient for the requested tail "
            "probabilities."
        )
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("Bootstrap seed must be a non-negative integer.")

    sorted_values, bootstrap_values = _weighted_bootstrap_p95(
        clusters,
        resample_count=resample_count,
        seed=seed,
    )
    point = _linear_quantile_from_sorted(sorted_values, 0.95)
    lower = percentile(bootstrap_values, lower_tail_probability)
    upper = percentile(bootstrap_values, upper_tail_probability)
    half_width = (upper - lower) / 2.0
    return ClusterBootstrapP95Interval(
        request_class=str(request_class),
        slo=float(slo),
        replication_count=len(clusters),
        nonempty_replication_count=sum(bool(len(cluster)) for cluster in clusters),
        eligible_request_count=sum(len(cluster) for cluster in clusters),
        eligible_request_counts_by_replication=tuple(
            len(cluster) for cluster in clusters
        ),
        p95_estimate=point,
        lower_bound=lower,
        upper_bound=upper,
        half_width=half_width,
        relative_half_width=(
            half_width / point if point else (0.0 if half_width == 0 else math.inf)
        ),
        lower_tail_probability=lower_tail_probability,
        upper_tail_probability=upper_tail_probability,
        bootstrap_resample_count=resample_count,
        bootstrap_seed=seed,
        bootstrap_p95_values=bootstrap_values,
    )


def replication_cluster_bootstrap_mean(
    latency_clusters,
    *,
    request_class,
    slo,
    resample_count,
    seed,
    lower_tail_probability,
    upper_tail_probability,
    minimum_tail_draws=5,
):
    """Estimate pooled mean uncertainty by resampling complete replications."""
    clusters = tuple(
        np.fromiter(cluster, dtype=float, count=len(cluster))
        for cluster in latency_clusters
    )
    if len(clusters) < 2:
        raise ValueError("Cluster bootstrap requires at least two replications.")
    if any(cluster.ndim != 1 for cluster in clusters):
        raise ValueError("Every latency cluster must be one-dimensional.")
    if any(
        np.any(~np.isfinite(cluster)) or np.any(cluster < 0) for cluster in clusters
    ):
        raise ValueError("Latency clusters must be finite and non-negative.")
    total_count = sum(len(cluster) for cluster in clusters)
    if total_count == 0:
        raise ValueError("Cluster bootstrap requires at least one latency.")
    if not math.isfinite(float(slo)) or float(slo) <= 0:
        raise ValueError("slo must be finite and positive.")
    if not 0 < lower_tail_probability < upper_tail_probability < 1:
        raise ValueError("Bootstrap interval probabilities are invalid.")
    if (
        isinstance(resample_count, bool)
        or not isinstance(resample_count, int)
        or resample_count <= 0
    ):
        raise ValueError("resample_count must be a positive integer.")
    if (
        isinstance(minimum_tail_draws, bool)
        or not isinstance(minimum_tail_draws, int)
        or minimum_tail_draws <= 0
    ):
        raise ValueError("minimum_tail_draws must be a positive integer.")
    tail_resolution = (
        min(
            lower_tail_probability,
            1.0 - upper_tail_probability,
        )
        * resample_count
    )
    if tail_resolution < minimum_tail_draws:
        raise ValueError(
            "Bootstrap resample count is insufficient for the requested tail "
            "probabilities."
        )
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("Bootstrap seed must be a non-negative integer.")

    cluster_sizes = np.asarray([len(cluster) for cluster in clusters], dtype=np.int64)
    cluster_sums = np.asarray([float(np.sum(cluster)) for cluster in clusters])
    multiplicities = _draw_nonempty_cluster_multiplicities(
        cluster_sizes,
        resample_count=resample_count,
        seed=seed,
    ).astype(np.int64)
    bootstrap_counts = multiplicities @ cluster_sizes
    bootstrap_values = tuple(
        float(value) for value in (multiplicities @ cluster_sums) / bootstrap_counts
    )
    point = sum(cluster_sums) / total_count
    lower = percentile(bootstrap_values, lower_tail_probability)
    upper = percentile(bootstrap_values, upper_tail_probability)
    half_width = (upper - lower) / 2.0
    return ClusterBootstrapP95Interval(
        request_class=str(request_class),
        slo=float(slo),
        replication_count=len(clusters),
        nonempty_replication_count=sum(bool(len(cluster)) for cluster in clusters),
        eligible_request_count=total_count,
        eligible_request_counts_by_replication=tuple(
            len(cluster) for cluster in clusters
        ),
        p95_estimate=point,
        lower_bound=lower,
        upper_bound=upper,
        half_width=half_width,
        relative_half_width=(
            half_width / point if point else (0.0 if half_width == 0 else math.inf)
        ),
        lower_tail_probability=lower_tail_probability,
        upper_tail_probability=upper_tail_probability,
        bootstrap_resample_count=resample_count,
        bootstrap_seed=seed,
        bootstrap_p95_values=bootstrap_values,
        latency_statistic="mean",
    )


def replication_cluster_bootstrap_rate(
    failure_clusters,
    *,
    request_class,
    resample_count,
    seed,
    lower_tail_probability,
    upper_tail_probability,
    minimum_tail_draws=5,
):
    """Estimate a pooled terminal-failure rate by resampling whole runs."""
    clusters = tuple(tuple(cluster) for cluster in failure_clusters)
    if len(clusters) < 2:
        raise ValueError("Cluster bootstrap requires at least two replications.")
    if any(
        any(value not in (False, True, 0, 1) for value in cluster)
        for cluster in clusters
    ):
        raise ValueError("Failure clusters must contain only binary outcomes.")
    sizes = np.asarray([len(cluster) for cluster in clusters], dtype=np.int64)
    total = int(np.sum(sizes))
    if total == 0:
        raise ValueError("Cluster bootstrap requires at least one outcome.")
    failures = np.asarray(
        [sum(bool(value) for value in cluster) for cluster in clusters],
        dtype=np.int64,
    )
    if not 0 < lower_tail_probability < upper_tail_probability < 1:
        raise ValueError("Bootstrap interval probabilities are invalid.")
    if (
        isinstance(resample_count, bool)
        or not isinstance(resample_count, int)
        or resample_count <= 0
    ):
        raise ValueError("resample_count must be a positive integer.")
    if (
        isinstance(minimum_tail_draws, bool)
        or not isinstance(minimum_tail_draws, int)
        or minimum_tail_draws <= 0
    ):
        raise ValueError("minimum_tail_draws must be a positive integer.")
    tail_resolution = (
        min(
            lower_tail_probability,
            1.0 - upper_tail_probability,
        )
        * resample_count
    )
    if tail_resolution < minimum_tail_draws:
        raise ValueError(
            "Bootstrap resample count is insufficient for the requested tail "
            "probabilities."
        )
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("Bootstrap seed must be a non-negative integer.")

    multiplicities = _draw_nonempty_cluster_multiplicities(
        sizes,
        resample_count=resample_count,
        seed=seed,
    ).astype(np.int64)
    bootstrap_totals = multiplicities @ sizes
    bootstrap_failures = multiplicities @ failures
    bootstrap_values = tuple(
        float(value) for value in bootstrap_failures / bootstrap_totals
    )
    point = int(np.sum(failures)) / total
    lower = percentile(bootstrap_values, lower_tail_probability)
    upper = percentile(bootstrap_values, upper_tail_probability)
    return ClusterBootstrapRateInterval(
        request_class=str(request_class),
        replication_count=len(clusters),
        eligible_request_count=total,
        failed_request_count=int(np.sum(failures)),
        estimate=point,
        lower_bound=lower,
        upper_bound=upper,
        half_width=(upper - lower) / 2.0,
        lower_tail_probability=lower_tail_probability,
        upper_tail_probability=upper_tail_probability,
        bootstrap_resample_count=resample_count,
        bootstrap_seed=seed,
        bootstrap_values=bootstrap_values,
    )

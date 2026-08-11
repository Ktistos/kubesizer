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

    @property
    def slo_margin(self):
        """Positive values mean the simultaneous upper bound passes the SLO."""
        return self.slo - self.upper_bound


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


def _weighted_bootstrap_p95(
    clusters,
    *,
    resample_count,
    seed,
):
    """Calculate exact cluster-bootstrap p95 values in bounded memory batches."""
    replication_count = len(clusters)
    pooled_values = np.concatenate(clusters)
    pooled_cluster_ids = np.concatenate(
        [
            np.full(len(cluster), index, dtype=np.int16)
            for index, cluster in enumerate(clusters)
        ]
    )
    order = np.argsort(pooled_values, kind="stable")
    sorted_values = pooled_values[order]
    sorted_cluster_ids = pooled_cluster_ids[order]
    generator = np.random.default_rng(seed)

    # Advanced indexing plus int64 cumulative weights uses about ten bytes per
    # observation and bootstrap row. Bound a working batch near 64 MiB.
    bytes_per_row = max(1, len(sorted_values) * 10)
    batch_size = max(1, min(64, 64_000_000 // bytes_per_row))
    bootstrap_values = []
    while len(bootstrap_values) < resample_count:
        count = min(batch_size, resample_count - len(bootstrap_values))
        sampled = generator.integers(
            0,
            replication_count,
            size=(count, replication_count),
        )
        multiplicities = np.zeros((count, replication_count), dtype=np.int16)
        rows = np.repeat(np.arange(count), replication_count)
        np.add.at(multiplicities, (rows, sampled.reshape(-1)), 1)
        cumulative = np.cumsum(
            multiplicities[:, sorted_cluster_ids],
            axis=1,
            dtype=np.int64,
        )
        totals = cumulative[:, -1]
        positions = (totals - 1) * 0.95
        lower_ranks = np.floor(np.maximum(positions, 0.0)).astype(np.int64)
        upper_ranks = np.minimum(lower_ranks + 1, np.maximum(totals - 1, 0))
        weights = positions - np.floor(positions)
        for row_index in range(count):
            # A legitimate cluster draw can contain no observations when the
            # request class is rare. Condition the quantile bootstrap on a
            # nonempty sampled cohort by redrawing that row.
            if totals[row_index] <= 0:
                continue
            lower_index = int(
                np.searchsorted(
                    cumulative[row_index],
                    lower_ranks[row_index] + 1,
                    side="left",
                )
            )
            upper_index = int(
                np.searchsorted(
                    cumulative[row_index],
                    upper_ranks[row_index] + 1,
                    side="left",
                )
            )
            bootstrap_values.append(
                float(
                    sorted_values[lower_index] * (1.0 - weights[row_index])
                    + sorted_values[upper_index] * weights[row_index]
                )
            )
    return sorted_values, tuple(bootstrap_values)


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
    clusters = tuple(np.asarray(tuple(cluster), dtype=float) for cluster in latency_clusters)
    if len(clusters) < 2:
        raise ValueError("Cluster bootstrap requires at least two replications.")
    if any(cluster.ndim != 1 for cluster in clusters):
        raise ValueError("Every latency cluster must be one-dimensional.")
    if any(
        np.any(~np.isfinite(cluster)) or np.any(cluster < 0)
        for cluster in clusters
    ):
        raise ValueError("Latency clusters must be finite and non-negative.")
    if sum(len(cluster) for cluster in clusters) == 0:
        raise ValueError("Cluster bootstrap requires at least one latency.")
    if not math.isfinite(float(slo)) or float(slo) <= 0:
        raise ValueError("slo must be finite and positive.")
    if (
        not 0 < lower_tail_probability < upper_tail_probability < 1
    ):
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
    tail_resolution = min(
        lower_tail_probability,
        1.0 - upper_tail_probability,
    ) * resample_count
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
        eligible_request_counts_by_replication=tuple(len(cluster) for cluster in clusters),
        p95_estimate=point,
        lower_bound=lower,
        upper_bound=upper,
        half_width=half_width,
        relative_half_width=(
            half_width / point
            if point
            else (0.0 if half_width == 0 else math.inf)
        ),
        lower_tail_probability=lower_tail_probability,
        upper_tail_probability=upper_tail_probability,
        bootstrap_resample_count=resample_count,
        bootstrap_seed=seed,
        bootstrap_p95_values=bootstrap_values,
    )

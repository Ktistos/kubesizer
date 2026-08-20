import numpy as np
import pytest

from metrics import (
    draw_cluster_bootstrap_indices,
    percentile,
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
    sequential_tail_alpha,
)


def bootstrap(clusters, *, seed=1234, resamples=1_000):
    return replication_cluster_bootstrap_p95(
        clusters,
        request_class="checkout",
        slo=20.0,
        resample_count=resamples,
        seed=seed,
        lower_tail_probability=0.01,
        upper_tail_probability=0.99,
        minimum_tail_draws=5,
    )


def test_point_estimate_is_pooled_p95_not_mean_replication_p95():
    clusters = (
        tuple(range(100)),
        tuple(100.0 for _ in range(10)),
    )

    interval = bootstrap(clusters)

    pooled = percentile(clusters[0] + clusters[1], 0.95)
    mean_replication_p95 = np.mean(
        [percentile(cluster, 0.95) for cluster in clusters]
    )
    assert interval.p95_estimate == pytest.approx(pooled)
    assert interval.p95_estimate != pytest.approx(mean_replication_p95)
    assert interval.eligible_request_count == 110
    assert interval.eligible_request_counts_by_replication == (100, 10)


def test_cluster_bootstrap_mean_uses_pooled_mean_and_complete_replications():
    clusters = ((1.0, 2.0), (10.0, 20.0, 30.0))
    interval = replication_cluster_bootstrap_mean(
        clusters,
        request_class="checkout",
        slo=20.0,
        resample_count=1_000,
        seed=41,
        lower_tail_probability=0.01,
        upper_tail_probability=0.99,
        minimum_tail_draws=5,
    )
    draws = draw_cluster_bootstrap_indices(2, 1_000, seed=41)
    expected = tuple(
        np.mean(
            tuple(
                value
                for cluster_index in draw
                for value in clusters[cluster_index]
            )
        )
        for draw in draws
    )

    assert interval.latency_statistic == "mean"
    assert interval.estimate == pytest.approx(np.mean(clusters[0] + clusters[1]))
    assert interval.bootstrap_values == pytest.approx(expected)


def test_bootstrap_draws_replication_indices_as_complete_clusters():
    draws = draw_cluster_bootstrap_indices(3, 8, seed=9)

    assert len(draws) == 8
    assert all(len(draw) == 3 for draw in draws)
    assert all(0 <= index < 3 for draw in draws for index in draw)

    interval = bootstrap(((0.0, 0.0), (100.0, 100.0)))
    # Whole-cluster sampling can only produce all-low, mixed, or all-high
    # samples. At p95 all mixed samples also evaluate to 100.
    assert set(interval.bootstrap_p95_values) <= {0.0, 100.0}


def test_fast_weighted_selection_matches_expanded_cluster_draws():
    clusters = (
        (1.0, 3.0, 8.0),
        (2.0, 4.0),
        (5.0, 6.0, 7.0, 9.0),
    )
    interval = bootstrap(clusters, seed=41)
    draws = draw_cluster_bootstrap_indices(
        len(clusters),
        len(interval.bootstrap_p95_values),
        seed=41,
    )
    expected = tuple(
        percentile(
            tuple(
                value
                for cluster_index in draw
                for value in clusters[cluster_index]
            ),
            0.95,
        )
        for draw in draws
    )

    assert interval.bootstrap_p95_values == pytest.approx(expected)


def test_bootstrap_seed_reproduces_interval_and_distribution():
    clusters = ((1.0, 2.0, 3.0), (4.0, 5.0), (2.0, 8.0))

    first = bootstrap(clusters, seed=77)
    second = bootstrap(clusters, seed=77)
    different = bootstrap(clusters, seed=78)

    assert first == second
    assert first.bootstrap_p95_values != different.bootstrap_p95_values


def test_insufficient_bootstrap_tail_resolution_fails_clearly():
    with pytest.raises(ValueError, match="insufficient"):
        replication_cluster_bootstrap_p95(
            ((1.0,), (2.0,)),
            request_class="checkout",
            slo=3.0,
            resample_count=100,
            seed=1,
            lower_tail_probability=0.001,
            upper_tail_probability=0.999,
            minimum_tail_draws=5,
        )


def test_sequential_alpha_allocation_controls_looks_classes_and_tails():
    alpha_tail = sequential_tail_alpha(
        overall_alpha=0.05,
        maximum_looks=4,
        request_class_count=3,
    )

    assert alpha_tail == pytest.approx(0.05 / (2 * 4 * 3))
    assert 2 * 4 * 3 * alpha_tail == pytest.approx(0.05)


def test_empty_or_invalid_clusters_fail():
    with pytest.raises(ValueError, match="at least one latency"):
        bootstrap(((), ()))
    with pytest.raises(ValueError, match="finite"):
        bootstrap(((1.0,), (float("nan"),)))


def test_bootstrap_redraws_all_empty_cluster_samples_for_rare_classes():
    interval = bootstrap(((), (), (1.0, 2.0, 3.0)), seed=19)

    assert len(interval.bootstrap_p95_values) == 1_000
    assert all(value >= 1.0 for value in interval.bootstrap_p95_values)
    assert interval.nonempty_replication_count == 1

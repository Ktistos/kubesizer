import math

import pytest

from metrics import time_weighted_bucket_averages


def test_interval_entirely_inside_one_bucket():
    history = [(0.0, 0), (2.0, 3), (4.0, 0)]

    buckets = time_weighted_bucket_averages(history, bucket_width=5.0, horizon=5.0)

    assert buckets == pytest.approx([1.2])


def test_interval_ending_exactly_on_bucket_boundary():
    history = [(0.0, 2), (5.0, 0)]

    buckets = time_weighted_bucket_averages(history, bucket_width=5.0, horizon=10.0)

    assert buckets == pytest.approx([2.0, 0.0])


def test_interval_crossing_one_bucket_boundary():
    history = [(0.0, 0), (4.0, 2), (6.0, 0)]

    buckets = time_weighted_bucket_averages(history, bucket_width=5.0, horizon=10.0)

    assert buckets == pytest.approx([0.4, 0.4])


def test_interval_crossing_multiple_bucket_boundaries():
    history = [(0.0, 3), (12.0, 0)]

    buckets = time_weighted_bucket_averages(history, bucket_width=5.0, horizon=15.0)

    assert buckets == pytest.approx([3.0, 3.0, 1.2])


def test_hand_computed_fixture_and_area_conservation():
    history = [
        (0.0, 0),
        (2.0, 1),
        (3.0, 2),
        (7.0, 1),
        (9.0, 0),
    ]

    buckets = time_weighted_bucket_averages(history, bucket_width=5.0, horizon=10.0)

    assert buckets == pytest.approx([1.0, 1.2])
    bucket_area = sum(bucket * 5.0 for bucket in buckets)
    original_area = 0 * 2 + 1 * 1 + 2 * 4 + 1 * 2 + 0 * 1
    assert bucket_area == pytest.approx(original_area)


def test_final_state_is_flushed_to_horizon():
    history = [(0.0, 0), (2.0, 2)]

    buckets = time_weighted_bucket_averages(history, bucket_width=5.0, horizon=10.0)

    assert buckets == pytest.approx([1.2, 2.0])


def test_incomplete_final_bucket_is_discarded():
    history = [(0.0, 2)]

    buckets = time_weighted_bucket_averages(history, bucket_width=4.0, horizon=10.0)

    assert buckets == pytest.approx([2.0, 2.0])


@pytest.mark.parametrize(
    ("history", "bucket_width", "horizon", "message"),
    [
        ([], 1.0, 10.0, "initial state"),
        ([(1.0, 0)], 1.0, 10.0, "begin at time zero"),
        ([(0.0, 0), (0.0, 1)], 1.0, 10.0, "strictly increasing"),
        ([(0.0, -1)], 1.0, 10.0, "non-negative"),
        ([(0.0, 0)], 0.0, 10.0, "greater than zero"),
        ([(0.0, 0)], 1.0, 0.0, "greater than zero"),
        ([(0.0, math.inf)], 1.0, 10.0, "finite number"),
    ],
)
def test_invalid_inputs_are_rejected(history, bucket_width, horizon, message):
    with pytest.raises(ValueError, match=message):
        time_weighted_bucket_averages(history, bucket_width, horizon)

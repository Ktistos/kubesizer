import pytest

from metrics import independent_replication_rate_interval


def test_zero_observed_failures_do_not_create_a_zero_width_certificate():
    interval = independent_replication_rate_interval(
        tuple((False,) * 100 for _ in range(30)),
        request_class="checkout",
        slo=0.05,
        per_tail_alpha=0.025,
    )

    assert interval.estimate == 0.0
    assert interval.pooled_estimate == 0.0
    assert interval.lower_bound == 0.0
    assert interval.upper_bound > 0.0
    assert interval.failed_request_count == 0


def test_rate_interval_uses_complete_replication_fractions():
    interval = independent_replication_rate_interval(
        ((False, False), (True, False, False, False)),
        request_class="checkout",
        slo=0.5,
        per_tail_alpha=0.025,
    )

    assert interval.rates_by_replication == (0.0, 0.25)
    assert interval.estimate == pytest.approx(0.125)
    assert interval.pooled_estimate == pytest.approx(1 / 6)
    assert interval.eligible_request_count == 6
    assert interval.failed_request_count == 1


def test_rate_interval_rejects_nonbinary_outcomes():
    with pytest.raises(ValueError, match="binary"):
        independent_replication_rate_interval(
            ((False,), (2,)),
            request_class="checkout",
            slo=0.1,
            per_tail_alpha=0.025,
        )

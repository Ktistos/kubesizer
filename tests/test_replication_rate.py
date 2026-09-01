import pytest

from metrics import (
    independent_replication_rate_interval,
    minimum_zero_failure_replications,
)


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


def test_kl_hoeffding_zero_failure_bound_is_tighter_and_auditable():
    interval = independent_replication_rate_interval(
        tuple((False,) * 2_000 for _ in range(60)),
        request_class="checkout",
        slo=0.10,
        per_tail_alpha=0.003125,
    )

    assert interval.method == "independent-replication-kl-hoeffding-v1"
    assert interval.upper_bound == pytest.approx(
        1.0 - 0.003125 ** (1.0 / 60)
    )
    assert interval.upper_bound < 0.10
    assert minimum_zero_failure_replications(0.10, 0.003125) == 55


def test_zero_failure_slo_cannot_be_certified_with_finite_replications():
    with pytest.raises(ValueError, match="strictly between zero and one"):
        minimum_zero_failure_replications(0.0, 0.025)

    with pytest.raises(ValueError, match="strictly between zero and one"):
        independent_replication_rate_interval(
            ((False,), (False,)),
            request_class="checkout",
            slo=0.0,
            per_tail_alpha=0.025,
        )

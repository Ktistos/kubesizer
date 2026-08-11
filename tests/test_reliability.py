from types import SimpleNamespace

import pytest

from metrics import (
    ReliabilityStatus,
    evaluate_request_class_p95_reliability,
    student_t_cdf,
    student_t_quantile,
)


def replications(values_by_class, *, seed_start=100, configuration=(2,)):
    count = len(next(iter(values_by_class.values())))
    assert all(len(values) == count for values in values_by_class.values())
    return tuple(
        SimpleNamespace(
            seed=seed_start + index,
            replica_configuration=configuration,
            warmup_duration=10.0,
            measurement_duration=100.0,
            measured_request_counts={key: 2_000 for key in values_by_class},
            request_class_p95_latencies={
                key: values[index] for key, values in values_by_class.items()
            },
        )
        for index in range(count)
    )


def test_student_t_matches_known_critical_values():
    assert student_t_quantile(0.975, 29) == pytest.approx(
        2.045229642,
        rel=1e-9,
    )
    assert student_t_quantile(0.975, 1) == pytest.approx(
        12.706204736,
        rel=1e-9,
    )
    assert student_t_cdf(student_t_quantile(0.995, 29), 29) == pytest.approx(
        0.995,
        abs=1e-12,
    )


@pytest.mark.parametrize(
    ("values", "expected_status"),
    (
        ([0.75 + (index % 3) * 0.01 for index in range(30)], ReliabilityStatus.RELIABLY_FEASIBLE),
        ([1.20 + (index % 3) * 0.01 for index in range(30)], ReliabilityStatus.RELIABLY_INFEASIBLE),
        ([0.90 if index % 2 else 1.10 for index in range(30)], ReliabilityStatus.UNCERTAIN),
    ),
)
def test_request_class_interval_classifies_against_slo(values, expected_status):
    result = evaluate_request_class_p95_reliability(
        replications({"checkout": values}),
        {"checkout": 1.0},
    )

    interval = result.request_class_intervals["checkout"]
    assert interval.status == expected_status
    assert result.status == expected_status
    assert result.feasible == (
        expected_status == ReliabilityStatus.RELIABLY_FEASIBLE
    )
    assert interval.replication_count == 30
    assert interval.standard_error == pytest.approx(
        interval.standard_deviation / 30**0.5
    )
    assert interval.upper_bound - interval.mean_p95 == pytest.approx(
        interval.half_width
    )


def test_bonferroni_intervals_are_simultaneous_across_request_classes():
    values = [0.8 + (index % 4) * 0.02 for index in range(30)]
    runs = replications({"checkout": values, "search": values})
    adjusted = evaluate_request_class_p95_reliability(
        runs,
        {"checkout": 1.0, "search": 1.0},
    )
    unadjusted = evaluate_request_class_p95_reliability(
        runs,
        {"checkout": 1.0, "search": 1.0},
        adjust_for_multiple_classes=False,
    )

    assert adjusted.multiple_class_adjustment == "bonferroni"
    assert adjusted.per_interval_confidence_level == pytest.approx(0.975)
    assert (
        adjusted.request_class_intervals["checkout"].half_width
        > unadjusted.request_class_intervals["checkout"].half_width
    )


def test_any_reliably_infeasible_class_makes_configuration_infeasible():
    result = evaluate_request_class_p95_reliability(
        replications(
            {
                "checkout": [0.7] * 30,
                "search": [1.3] * 30,
            }
        ),
        {"checkout": 1.0, "search": 1.0},
    )

    assert result.request_class_intervals["checkout"].status == (
        ReliabilityStatus.RELIABLY_FEASIBLE
    )
    assert result.request_class_intervals["search"].status == (
        ReliabilityStatus.RELIABLY_INFEASIBLE
    )
    assert result.status == ReliabilityStatus.RELIABLY_INFEASIBLE


def test_reliability_rejects_missing_or_nonindependent_observations():
    with pytest.raises(ValueError, match="At least two"):
        evaluate_request_class_p95_reliability(
            replications({"checkout": [0.5]}),
            {"checkout": 1.0},
        )

    duplicate_seeds = list(replications({"checkout": [0.5, 0.6]}))
    duplicate_seeds[1].seed = duplicate_seeds[0].seed
    with pytest.raises(ValueError, match="unique"):
        evaluate_request_class_p95_reliability(
            duplicate_seeds,
            {"checkout": 1.0},
        )

    missing = replications({"search": [0.5, 0.6]})
    with pytest.raises(ValueError, match="has no p95"):
        evaluate_request_class_p95_reliability(
            missing,
            {"checkout": 1.0},
        )

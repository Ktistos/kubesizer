import pytest

from performance_models import QueueingNetworkType, System
from predictive_validation import (
    PredictiveValidationCase,
    PredictiveValidationOptions,
    PredictiveValidationStatus,
    validate_simulator_predictions,
)


def _system(*, arrival_rate=1.0, service_time=0.1, timeout=0.0):
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "routing": {},
                **(
                    {"end_to_end_timeout_seconds": timeout}
                    if timeout
                    else {}
                ),
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "api": {
                        "distribution": "deterministic",
                        "rate": arrival_rate,
                    }
                }
            },
            "service_times": {
                "api": {
                    "distribution": "deterministic",
                    "mean": service_time,
                }
            },
        },
    }
    return System("predictive-validation", workload, QueueingNetworkType.OPEN)


def _options(**overrides):
    values = {
        "warmup_duration": 0.0,
        "measurement_duration": 10.5,
        "replication_count": 3,
        "bootstrap_resample_count": 200,
        "minimum_cases": 2,
        "absolute_error_tolerance_seconds": 0.001,
    }
    values.update(overrides)
    return PredictiveValidationOptions(**values)


def _case(name, system, observed, *, error_rate=0.0, statistic="mean"):
    return PredictiveValidationCase.from_mappings(
        name,
        system,
        (1,),
        {"request": observed},
        statistic=statistic,
        observed_request_class_error_rates={"request": error_rate},
    )


def test_matching_holdouts_produce_a_target_bound_validation_certificate():
    target = _system()
    supporting = _system(arrival_rate=2.0)

    result = validate_simulator_predictions(
        target,
        (
            _case("target-holdout", _system(), 0.1),
            _case("supporting-load", supporting, 0.1),
        ),
        options=_options(),
    )

    assert result.status is PredictiveValidationStatus.VALIDATED
    assert result.accepted
    assert all(case.passed for case in result.cases)
    assert all(
        diagnostic.predicted_seconds == pytest.approx(0.1)
        for case in result.cases
        for diagnostic in case.diagnostics
    )
    result.assert_valid_for(target)
    assert len(result.certificate_fingerprint) == 64


def test_material_latency_underprediction_rejects_the_model():
    target = _system()

    result = validate_simulator_predictions(
        target,
        (
            _case("target-holdout", _system(), 0.2),
            _case("repeat-holdout", _system(), 0.2),
        ),
        options=_options(),
    )

    assert result.status is PredictiveValidationStatus.REJECTED
    assert not result.accepted
    assert all(not case.passed for case in result.cases)
    with pytest.raises(ValueError, match="has not passed"):
        result.assert_valid_for(target)


def test_observed_failure_mode_cannot_be_hidden_by_success_latency():
    target = _system()

    result = validate_simulator_predictions(
        target,
        (
            _case("target-holdout", _system(), 0.1, error_rate=0.05),
            _case("repeat-holdout", _system(), 0.1, error_rate=0.0),
        ),
        options=_options(maximum_observed_error_rate=0.01),
    )

    assert result.status is PredictiveValidationStatus.REJECTED
    assert "failures exceed" in result.cases[0].reasons[0]


def test_explicit_deadline_enables_validation_of_terminal_failure_rate():
    target = _system(timeout=0.05)

    result = validate_simulator_predictions(
        target,
        (
            _case(
                "target-timeout",
                _system(timeout=0.05),
                0.05,
                error_rate=1.0,
            ),
            _case(
                "support-timeout",
                _system(arrival_rate=2.0, timeout=0.05),
                0.05,
                error_rate=1.0,
            ),
        ),
        options=_options(
            maximum_observed_error_rate=0.01,
            maximum_absolute_error_rate_difference=0.001,
            maximum_error_rate_ci_half_width=0.001,
        ),
    )

    assert result.status is PredictiveValidationStatus.VALIDATED
    assert all(case.error_rate_diagnostics for case in result.cases)
    assert all(
        diagnostic.predicted_rate == pytest.approx(1.0)
        for case in result.cases
        for diagnostic in case.error_rate_diagnostics
    )


def test_missing_cases_and_wrong_target_workload_fail_explicitly():
    target = _system()
    insufficient = validate_simulator_predictions(
        target,
        (_case("only", _system(), 0.1),),
        options=_options(),
    )

    assert insufficient.status is PredictiveValidationStatus.INSUFFICIENT_EVIDENCE

    accepted = validate_simulator_predictions(
        target,
        (
            _case("target", _system(), 0.1),
            _case("support", _system(arrival_rate=2.0), 0.1),
        ),
        options=_options(),
    )
    with pytest.raises(ValueError, match="different topology or workload"):
        accepted.assert_valid_for(_system(arrival_rate=3.0))

"""Regression tests for replay diagnostics, independent of campaign files."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


spec = importlib.util.spec_from_file_location(
    "audit_simulation_semantics",
    Path(__file__).resolve().parents[1] / "scripts/audit_simulation_semantics.py",
)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def row(run, method, prediction):
    return {
        "run": run,
        "request_class": "request",
        "method": method,
        "primary_latency_eligible": "True",
        "prediction_available": "True" if prediction is not None else "False",
        "observed_successful_mean_ms": "100",
        "predicted_mean_ms": str(prediction) if prediction is not None else "",
    }


def test_common_accuracy_does_not_hide_different_prediction_coverage():
    analytical, simulation = audit.METHODS
    result = audit.accuracy_comparison(
        [
            row("a", analytical, 110),
            row("a", simulation, 120),
            row("b", analytical, None),
            row("b", simulation, 10000),
        ]
    )
    assert result["eligible_observations"] == 2
    assert result["matched_observations"] == 1
    assert result["methods"][analytical]["finite_observations"] == 1
    assert result["methods"][simulation]["finite_observations"] == 2
    assert result["methods"][analytical]["matched_mape_percent"] == pytest.approx(10)
    assert result["methods"][simulation]["matched_mape_percent"] == pytest.approx(20)


def test_empty_intersection_is_not_zero_error():
    result = audit.accuracy_comparison([row("a", audit.METHODS[0], None)])
    assert result["matched_observations"] == 0
    assert result["methods"][audit.METHODS[0]]["matched_mape_percent"] is None


def test_duplicate_score_rows_fail():
    one = row("a", audit.METHODS[0], 110)
    with pytest.raises(ValueError, match="Duplicate"):
        audit.accuracy_comparison([one, one])


@pytest.mark.parametrize("prediction", [float("nan"), float("inf"), -1])
def test_nonfinite_or_negative_predictions_fail(prediction):
    with pytest.raises(ValueError, match="invalid latency"):
        audit.accuracy_comparison([row("a", audit.METHODS[0], prediction)])


def test_different_observations_cannot_be_paired():
    a, b = (row("a", m, 110) for m in audit.METHODS)
    b["observed_successful_mean_ms"] = "200"
    with pytest.raises(ValueError, match="disagree"):
        audit.accuracy_comparison([a, b])


@pytest.mark.parametrize(
    "pairs,status",
    [
        ([(1, 3), (1.1, 3.1), (0.9, 2.9)], "growth_detected"),
        ([(1, 1), (1.1, 1.1), (0.9, 0.9)], "no_material_drift_detected"),
        ([(3, 1), (3.1, 1.1), (2.9, 0.9)], "inconclusive"),
        ([(0, 0)] * 3, "no_material_drift_detected"),
        ([(1, 3), (1, 3)], "inconclusive"),
        ([(1, 5), (5, 1), (1, 2)], "inconclusive"),
    ],
)
def test_independent_contrast_screen(pairs, status):
    assert audit.trend_summary(pairs)["status"] == status


def record(*, growing=False):
    return SimpleNamespace(
        tagged_requests_in_flight_at_end=0,
        failed_measured_request_counts={"request": 0},
        measured_request_counts={"request": 60},
        request_class_arrival_times={"request": tuple(range(60))},
        request_class_latencies={
            "request": (1.0,) * 30 + (3.0 if growing else 1.0,) * 30
        },
        warmup_duration=0,
        measurement_duration=60,
        active_requests_at_end=100 if growing else 2,
    )


def test_complete_cohort_can_still_have_growing_latency():
    result = audit.cohort_summary([record(growing=True) for _ in range(3)])
    assert result["status"] == "growth_detected"
    assert result["classes"]["request"]["finite_window_mean_ms"] == 2000
    assert "stable" not in result


def test_failure_is_not_a_good_stationarity_result():
    r = record()
    r.failed_measured_request_counts["request"] = 1
    assert audit.cohort_summary([r] * 3)["status"] == "incomplete_or_failed"


def test_incomplete_cohort_is_rejected():
    r = record()
    r.tagged_requests_in_flight_at_end = 1
    assert audit.cohort_summary([r] * 3)["status"] == "incomplete_or_failed"


def test_completion_order_does_not_change_arrival_cohort_halves():
    ordered = record(growing=True)
    shuffled = record(growing=True)
    shuffled.request_class_arrival_times = {
        "request": tuple(reversed(ordered.request_class_arrival_times["request"]))
    }
    shuffled.request_class_latencies = {
        "request": tuple(reversed(ordered.request_class_latencies["request"]))
    }
    assert audit.cohort_summary([ordered] * 3) == audit.cohort_summary([shuffled] * 3)


def test_out_of_window_arrival_is_rejected():
    r = record()
    r.request_class_arrival_times["request"] = (60,) + tuple(range(1, 60))
    with pytest.raises(ValueError, match="outside"):
        audit.cohort_summary([r] * 3)


def test_variants_never_mutate_frozen_input_and_change_one_field_only():
    model = {
        "traffic_model": {
            "service_times": {
                name: {
                    "execution_model": "processor_sharing",
                    "concurrency_limit_per_replica": limit,
                }
                for name, limit in (("repeat-front", 8), ("identity", 3))
            }
        }
    }
    variants = list(audit.diagnostic_variants({"model": model}))
    assert len(variants) == 5
    assert variants[0][1] == model
    for _, variant in variants[1:]:
        assert (
            sum(
                value != model["traffic_model"]["service_times"][name][field]
                for name, fields in variant["traffic_model"]["service_times"].items()
                for field, value in fields.items()
            )
            == 1
        )
    assert (
        model["traffic_model"]["service_times"]["identity"][
            "concurrency_limit_per_replica"
        ]
        == 3
    )


def test_state_trends_exclude_warmup_and_use_replications_as_units():
    def ensemble():
        return SimpleNamespace(
            service_names=("api",),
            replica_configuration=(1,),
            bucket_width=30,
            horizon=180,
            active_requests=(999, 999, 10, 10, 30, 30),
            queue_length_by_service=((999, 999, 9, 9, 29, 29),),
            busy_replicas_by_service=((1, 1, 1, 1, 1, 1),),
            external_arrival_count=1800,
            completed_request_count=1770,
        )

    result = audit.state_summary([ensemble() for _ in range(3)], 60)
    assert result["mean_buckets"]["active_requests"] == [10, 10, 30, 30]
    trend = result["trends"]["active_requests"]
    assert trend["status"] == "growth_detected"
    assert trend["replications"] == 3
    assert trend["early_mean"] == 10
    assert trend["late_mean"] == 30


def test_state_screen_does_not_silently_round_warmup():
    ensemble = SimpleNamespace(service_names=("api",), bucket_width=30)
    with pytest.raises(ValueError, match="align"):
        audit.state_summary([ensemble] * 3, 35)


def test_worker_holding_time_uses_covered_not_nominal_duration():
    pod = {"admitted_fraction": 0.5, "coverage_fraction": 0.8, "completed": 1200}
    assert audit.worker_holding_ms(pod, slots=3, window_seconds=100) == pytest.approx(
        100
    )


@pytest.mark.parametrize("coverage,completed", [(0, 10), (1.1, 10), (1, 0)])
def test_worker_holding_requires_valid_coverage(coverage, completed):
    with pytest.raises(ValueError, match="covered"):
        audit.worker_holding_ms(
            {
                "admitted_fraction": 0.5,
                "coverage_fraction": coverage,
                "completed": completed,
            },
            3,
            100,
        )

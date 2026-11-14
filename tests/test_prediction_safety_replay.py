"""Check replay scoring without needing any archived campaign files."""

import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location(
    "prediction_safety_replay",
    Path(__file__).resolve().parents[1] / "scripts/replay_prediction_safety_fixes.py",
)
replay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(replay)


def test_accuracy_separates_coverage_from_matched_error():
    result = replay.accuracy(
        [
            {"eligible": True, "observed_ms": 100, "before": 110, "after": 120},
            {"eligible": True, "observed_ms": 100, "before": 900, "after": None},
            {"eligible": False, "observed_ms": 100, "before": 999, "after": 999},
        ],
        ("before", "after"),
    )
    assert result["eligible"] == 2
    assert result["matched"] == 1
    assert result["full"]["before"]["finite"] == 2
    assert result["full"]["after"]["finite"] == 1
    assert result["full"]["before"]["mape_percent"] == pytest.approx(405)
    assert result["matched_scores"]["before"]["mape_percent"] == pytest.approx(10)
    assert result["matched_scores"]["after"]["mape_percent"] == pytest.approx(20)


def test_absent_predictions_are_not_perfect_predictions():
    result = replay.accuracy(
        [{"eligible": True, "observed_ms": 100, "before": None, "after": None}],
        ("before", "after"),
    )
    assert result["matched"] == 0
    assert result["full"]["after"]["mape_percent"] is None


@pytest.mark.parametrize(
    "prediction,bad,expected",
    [
        ({"available": False, "means_ms": {"a": 1}}, True, "abstained"),
        ({"available": True, "means_ms": {}}, True, "abstained"),
        ({"available": True, "means_ms": {"a": 101}}, False, "predicted_infeasible"),
        ({"available": True, "means_ms": {"a": 100}}, False, "accepted_safe"),
        ({"available": True, "means_ms": {"a": 100}}, True, "false_safe"),
    ],
)
def test_safety_is_not_inferred_from_missing_predictions(prediction, bad, expected):
    assert replay.decision(prediction, {"a": 100}, bad) == expected


def test_all_request_classes_must_meet_their_slos():
    prediction = {"available": True, "means_ms": {"a": 10, "b": 51}}
    assert (
        replay.decision(prediction, {"a": 20, "b": 50}, False)
        == "predicted_infeasible"
    )

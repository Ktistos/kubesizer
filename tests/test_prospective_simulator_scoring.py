import csv
import json

import pytest

from scripts.score_prospective_simulator_comparison import score


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def population(tmp_path):
    matrix = tmp_path / "matrix"
    matrix.mkdir()
    cells = [
        {"cell_id": "t/f/r1/l0500", "track": "t", "family": "f", "replicas": 1,
         "load_ratio": 0.5, "arrival_rate_rps": 50, "semantic_status": "strict",
         "expected_repetitions": 3},
        {"cell_id": "t/f/r2/l0500", "track": "t", "family": "f", "replicas": 2,
         "load_ratio": 0.5, "arrival_rate_rps": 50, "semantic_status": "strict",
         "expected_repetitions": 3},
    ]
    write_csv(matrix / "cells.csv", cells)
    write_csv(matrix / "engine-eligibility.csv", [
        {"track": "t", "family": "*", "engine": "engine", "status": "strict"}
    ])
    write_csv(matrix / "decisions.csv", [{
        "decision_id": "d", "track": "t", "family": "f", "load_ratio": 0.5,
        "slo_seconds": 0.04,
        "candidate_cell_ids": json.dumps([row["cell_id"] for row in cells]),
    }])
    observed = tmp_path / "observed.csv"
    write_csv(observed, [
        {"cell_id": cells[0]["cell_id"], "observed_repetitions": 3,
         "cohort_count": 300, "cohort_completed": 300, "mean_seconds": 0.05,
         "p95_seconds": 0.08, "stable": True, "health_passed": True},
        {"cell_id": cells[1]["cell_id"], "observed_repetitions": 3,
         "cohort_count": 300, "cohort_completed": 300, "mean_seconds": 0.03,
         "p95_seconds": 0.05, "stable": True, "health_passed": True},
    ])
    predictions = tmp_path / "predictions.csv"
    write_csv(predictions, [
        {"cell_id": cells[0]["cell_id"], "engine": "engine", "status": "predicted",
         "mean_seconds": 0.04, "p95_seconds": 0.07, "stable": True,
         "information_condition": "portable_low_load_measurements_only"},
        {"cell_id": cells[1]["cell_id"], "engine": "engine", "status": "predicted",
         "mean_seconds": 0.025, "p95_seconds": 0.04, "stable": True,
         "information_condition": "portable_low_load_measurements_only"},
    ])
    return matrix, observed, predictions


def test_scoring_reports_accuracy_and_unsafe_cheapest_recommendation(tmp_path):
    result = score(*population(tmp_path), bootstrap_resamples=100)
    accuracy = result["accuracy"][0]
    assert accuracy["predicted_cells"] == 2
    assert accuracy["coverage_percent"] == 100
    assert accuracy["mean_ape_percent"] == pytest.approx((20 + 100 / 6) / 2)
    decision = result["decisions"][0]
    assert decision["predicted_choice"] == "t/f/r1/l0500"
    assert decision["observed_choice"] == "t/f/r2/l0500"
    assert decision["false_safe"] is True


def test_incomplete_observed_cohort_is_not_latency_scored(tmp_path):
    matrix, observed, predictions = population(tmp_path)
    rows = list(csv.DictReader(observed.open()))
    rows[0]["cohort_completed"] = "299"
    write_csv(observed, rows)
    result = score(matrix, observed, predictions, bootstrap_resamples=100)
    assert result["accuracy"][0]["eligible_cells"] == 1
    assert result["accuracy"][0]["predicted_cells"] == 1


def test_prediction_for_ineligible_engine_is_rejected(tmp_path):
    matrix, observed, predictions = population(tmp_path)
    rows = list(csv.DictReader(predictions.open()))
    rows[0]["engine"] = "wrong"
    write_csv(predictions, rows)
    with pytest.raises(ValueError, match="not eligible"):
        score(matrix, observed, predictions, bootstrap_resamples=100)


def test_accuracy_separates_strict_and_replica_routing_cells(tmp_path):
    matrix, observed, predictions = population(tmp_path)
    rows = list(csv.DictReader((matrix / "cells.csv").open()))
    rows[1]["semantic_status"] = "replica_routing_capability"
    write_csv(matrix / "cells.csv", rows)
    result = score(matrix, observed, predictions, bootstrap_resamples=100)
    assert {(row["cell_semantic_status"], row["predicted_cells"])
            for row in result["accuracy"]} == {
        ("strict", 1), ("replica_routing_capability", 1),
    }

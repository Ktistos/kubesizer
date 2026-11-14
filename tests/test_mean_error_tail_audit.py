import pytest

from scripts.audit_mean_error_tail import (
    aggregate_cells,
    error_summary,
    nearest_rank,
)


def row(run, observed, predicted):
    return {
        "run": run,
        "family": "chain",
        "configuration": "c1",
        "load_point": "u80",
        "request_class": "request",
        "method": "simulation_fitted",
        "primary_latency_eligible": "True",
        "prediction_available": "True",
        "observed_successful_mean_ms": str(observed),
        "predicted_mean_ms": str(predicted),
    }


def test_cell_scoring_averages_repetitions_before_computing_error():
    cells = aggregate_cells(
        (row("r1", 100, 150), row("r2", 200, 150)),
        "simulation_fitted",
        {("chain", "u80"): "growth_detected"},
    )

    assert len(cells) == 1
    assert cells[0]["repetition_count"] == 2
    assert cells[0]["observed_replication_mean_ms"] == pytest.approx(150)
    assert cells[0]["ape_percent"] == pytest.approx(0)
    assert cells[0]["diagnostic_status"] == "growth_detected"


def test_cell_scoring_rejects_a_prediction_changed_between_repetitions():
    with pytest.raises(ValueError, match="changed across repetitions"):
        aggregate_cells(
            (row("r1", 100, 150), row("r2", 100, 151)),
            "simulation_fitted",
        )


def test_error_summary_reports_tail_without_hiding_it():
    summary = error_summary((1, 2, 3, 100))

    assert summary["median_ape_percent"] == pytest.approx(2.5)
    assert summary["mean_ape_percent"] == pytest.approx(26.5)
    assert summary["maximum_ape_percent"] == 100
    assert nearest_rank((1, 2, 3, 4, 5), 0.95) == 5

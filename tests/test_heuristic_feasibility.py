from types import SimpleNamespace

import pytest

from optimization import FeasibilityStatus, evaluation_feasibility_status
from optimization.feasibility import (
    EvaluationBudgetExhausted,
    HeuristicEvaluationController,
    certified_bound_gaps,
)


def test_statistical_unknown_overrides_compatibility_feasible_flag():
    evaluation = SimpleNamespace(
        feasible=True,
        classification=SimpleNamespace(value="INCONCLUSIVE"),
    )

    assert evaluation_feasibility_status(evaluation) == FeasibilityStatus.UNKNOWN


def test_deterministic_validity_adapts_to_three_state_feasibility():
    assert (
        evaluation_feasibility_status(SimpleNamespace(valid=True))
        == FeasibilityStatus.FEASIBLE
    )
    assert (
        evaluation_feasibility_status(SimpleNamespace(valid=False))
        == FeasibilityStatus.INFEASIBLE
    )


def test_evaluation_controller_caches_and_refines_unknown_only_explicitly():
    calls = {"evaluate": 0, "refine": 0}

    def evaluate():
        calls["evaluate"] += 1
        return SimpleNamespace(
            classification=SimpleNamespace(value="INCONCLUSIVE"),
            replications_used=2,
        )

    def refine(_previous):
        calls["refine"] += 1
        return SimpleNamespace(
            classification=SimpleNamespace(value="STATISTICALLY_FEASIBLE"),
            replications_used=4,
        )

    controller = HeuristicEvaluationController(
        maximum_unique_evaluations=2,
        maximum_total_evaluator_calls=3,
        time_budget_seconds=10,
    )
    first, _ = controller.evaluate(("same",), evaluate)
    second, _ = controller.evaluate(
        ("same",),
        evaluate,
        refiner=refine,
        refine_unknown=True,
    )
    third, _ = controller.evaluate(("same",), evaluate)

    assert evaluation_feasibility_status(first) == FeasibilityStatus.UNKNOWN
    assert evaluation_feasibility_status(second) == FeasibilityStatus.FEASIBLE
    assert third is second
    assert calls == {"evaluate": 1, "refine": 1}
    assert controller.unique_evaluation_count == 1
    assert controller.total_evaluator_calls == 2
    assert controller.simulation_replications == 4


def test_simulation_replication_budget_stops_new_evaluations():
    controller = HeuristicEvaluationController(
        maximum_unique_evaluations=10,
        maximum_total_evaluator_calls=10,
        maximum_simulation_replications=3,
        time_budget_seconds=10,
    )
    controller.evaluate(
        ("first",),
        lambda: SimpleNamespace(valid=True, replications_used=3),
    )

    with pytest.raises(EvaluationBudgetExhausted) as captured:
        controller.evaluate(
            ("second",),
            lambda: SimpleNamespace(valid=True, replications_used=1),
        )

    assert captured.value.reason.value == ("SIMULATION_REPLICATION_BUDGET_EXHAUSTED")


def test_zero_lower_bound_has_no_relative_certificate():
    absolute, relative = certified_bound_gaps(2.0, 0.0)

    assert absolute == 2.0
    assert relative is None

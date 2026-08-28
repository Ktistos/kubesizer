from types import SimpleNamespace

import pytest

import statistical_workflow


class _Service:
    def __init__(self, service_id, name):
        self._id = service_id
        self._name = name

    def get_id(self):
        return self._id

    def get_name(self):
        return self._name


class _System:
    def get_services(self):
        return {
            "api": _Service(0, "api"),
            "worker": _Service(1, "worker"),
        }


def test_public_recommendation_distinguishes_proven_bounded_optimum(monkeypatch):
    certificate = object()
    optimization = SimpleNamespace(
        configuration=(2, 1),
        optimality_proven=True,
    )
    final = SimpleNamespace(passed=True)
    monkeypatch.setattr(
        statistical_workflow,
        "optimize_with_predictively_validated_simulation",
        lambda *_args, **_kwargs: optimization,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "validate_final_configuration",
        lambda *_args, **_kwargs: final,
    )

    result = statistical_workflow.recommend_replicas_with_validated_simulation(
        _System(),
        object(),
        certificate,
    )

    assert result.status == "VALIDATED_BOUNDED_MODEL_OPTIMUM"
    assert result.recommendable
    assert result.service_replicas == (("api", 2), ("worker", 1))
    assert result.bounded_model_optimal
    assert "declared bounds" in result.guarantee


def test_public_recommendation_withholds_failed_final_holdout(monkeypatch):
    optimization = SimpleNamespace(
        configuration=(1, 1),
        optimality_proven=False,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "optimize_with_predictively_validated_simulation",
        lambda *_args, **_kwargs: optimization,
    )
    monkeypatch.setattr(
        statistical_workflow,
        "validate_final_configuration",
        lambda *_args, **_kwargs: SimpleNamespace(passed=False),
    )

    result = statistical_workflow.recommend_replicas_with_validated_simulation(
        _System(),
        object(),
        object(),
    )

    assert result.status == "FINAL_VALIDATION_FAILED"
    assert not result.recommendable
    assert not result.bounded_model_optimal


def test_validated_recommendation_exposes_margin_and_prediction_risk():
    interval = SimpleNamespace(
        slo=0.2,
        estimate=0.17,
        lower_bound=0.16,
        upper_bound=0.195,
        latency_statistic="mean",
    )
    diagnostic = SimpleNamespace(
        request_class="browse",
        relative_error=-0.08,
        absolute_error_seconds=0.012,
    )
    result = statistical_workflow.ValidatedReplicaRecommendation(
        service_replicas=(("api", 2),),
        status="VALIDATED_FEASIBLE_HEURISTIC",
        optimization=SimpleNamespace(unresolved_lower_cost_states=((1,),)),
        final_validation=SimpleNamespace(
            request_class_intervals=(("browse", interval),)
        ),
        predictive_validation=SimpleNamespace(
            status=SimpleNamespace(value="VALIDATED"),
            cases=(SimpleNamespace(diagnostics=(diagnostic,)),),
        ),
        bounded_model_optimal=False,
        guarantee="validated heuristic",
    )

    summary = result.evidence_summary
    request = summary.request_classes[0]

    assert summary.status == "VALIDATED_HEURISTIC"
    assert request.simultaneous_slo_margin_seconds == pytest.approx(0.005)
    assert request.maximum_absolute_relative_predictive_error == pytest.approx(0.08)
    assert any("less than 5%" in warning for warning in summary.warnings)
    assert summary.unresolved_lower_cost_configurations == ((1,),)

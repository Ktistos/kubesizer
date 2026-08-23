from types import SimpleNamespace

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

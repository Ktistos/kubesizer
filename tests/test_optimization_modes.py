import pytest

from optimization import (
    OptimizationMode,
    OptimizationModes,
    WorkflowOptimizationMode,
    optimization_workflow_mode,
)


def test_uniform_mode_applies_to_both_optimizers():
    modes = OptimizationModes.uniform("exact")

    assert modes.replica == OptimizationMode.EXACT
    assert modes.placement == OptimizationMode.EXACT
    assert modes.workflow_mode == "EXACT"


def test_explicit_different_modes_are_hybrid():
    modes = OptimizationModes(
        mode="hybrid",
        replica="exact",
        placement="scalable",
    )

    assert modes.mode == WorkflowOptimizationMode.HYBRID
    assert modes.replica == OptimizationMode.EXACT
    assert modes.placement == OptimizationMode.SCALABLE
    assert modes.workflow_mode == "HYBRID"
    assert optimization_workflow_mode("scalable", "exact") == "HYBRID"


@pytest.mark.parametrize("missing", ["replica", "placement"])
def test_hybrid_requires_both_individual_modes(missing):
    arguments = {
        "mode": WorkflowOptimizationMode.HYBRID,
        "replica": OptimizationMode.EXACT,
        "placement": OptimizationMode.SCALABLE,
    }
    arguments[missing] = None

    with pytest.raises(ValueError, match="requires explicit"):
        OptimizationModes(**arguments)


def test_hybrid_rejects_auto_or_matching_individual_modes():
    with pytest.raises(ValueError, match="must be EXACT or SCALABLE"):
        OptimizationModes(mode="hybrid", replica="auto", placement="exact")
    with pytest.raises(ValueError, match="to differ"):
        OptimizationModes(mode="hybrid", replica="exact", placement="exact")


def test_nonhybrid_mode_rejects_individual_overrides():
    with pytest.raises(ValueError, match="only be specified in HYBRID"):
        OptimizationModes(mode="auto", replica="exact", placement="scalable")

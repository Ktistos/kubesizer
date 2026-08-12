import math

from .branch_and_bound_node_placement import BranchAndBoundNodePlacementOptimizer
from .mode import (
    DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT,
    OptimizationMode,
    ScalablePlacementSearchOptions,
    normalize_optimization_mode,
)
from .scalable_node_placement import ScalableNodePlacementOptimizer


def replica_configuration_count(calibration_profile):
    return math.prod(
        maximum - minimum + 1
        for minimum, maximum in zip(
            calibration_profile.minimum_replica_configuration,
            calibration_profile.maximum_replica_configuration,
        )
    )


def resolve_optimization_mode(mode, search_space_size, exact_limit):
    mode = normalize_optimization_mode(mode)
    if isinstance(exact_limit, bool) or not isinstance(exact_limit, int) or exact_limit <= 0:
        raise ValueError("exact_limit must be a positive integer.")
    if mode == OptimizationMode.AUTO:
        return (
            OptimizationMode.EXACT
            if search_space_size <= exact_limit
            else OptimizationMode.SCALABLE
        )
    return mode


def optimize_node_placement(
    problem,
    *,
    mode=OptimizationMode.AUTO,
    exact_configuration_limit=DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT,
    scalable_options=None,
):
    """Select exact branch-and-bound or bounded scalable placement."""
    search_space_size = problem.configuration_count()
    selected_mode = resolve_optimization_mode(
        mode,
        search_space_size,
        exact_configuration_limit,
    )
    if selected_mode == OptimizationMode.EXACT:
        return BranchAndBoundNodePlacementOptimizer().optimize(problem)
    options = scalable_options or ScalablePlacementSearchOptions()
    return ScalableNodePlacementOptimizer(
        beam_width=options.beam_width,
        candidate_distributions_per_service=(
            options.candidate_distributions_per_service
        ),
        maximum_partial_states=options.maximum_partial_states,
        maximum_complete_evaluations=options.maximum_complete_evaluations,
        maximum_local_iterations=options.maximum_local_iterations,
        time_budget_seconds=options.time_budget_seconds,
    ).optimize(problem)

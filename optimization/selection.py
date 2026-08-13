from .branch_and_bound_node_placement import BranchAndBoundNodePlacementOptimizer
from .mode import (
    DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT,
    DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    OptimizationMode,
    ScalablePlacementSearchOptions,
    ScalableReplicaSearchOptions,
    normalize_optimization_mode,
)
from .scalable_node_placement import ScalableNodePlacementOptimizer
from .scalable_replica import ScalableReplicaOptimizer
from .uniform_cost import UniformCostReplicaOptimizer


def resolve_optimization_mode(mode, search_space_size, exact_limit):
    mode = normalize_optimization_mode(mode)
    if (
        isinstance(search_space_size, bool)
        or not isinstance(search_space_size, int)
        or search_space_size <= 0
    ):
        raise ValueError("search_space_size must be a positive integer.")
    if (
        isinstance(exact_limit, bool)
        or not isinstance(exact_limit, int)
        or exact_limit <= 0
    ):
        raise ValueError("exact_limit must be a positive integer.")
    if mode == OptimizationMode.AUTO:
        return (
            OptimizationMode.EXACT
            if search_space_size <= exact_limit
            else OptimizationMode.SCALABLE
        )
    return mode


def optimize_replica_configuration(
    problem,
    *,
    mode=OptimizationMode.AUTO,
    exact_configuration_limit=DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    scalable_options=None,
):
    """Select exact uniform-cost or bounded scalable replica search."""
    selected_mode = resolve_optimization_mode(
        mode,
        problem.configuration_count(),
        exact_configuration_limit,
    )
    if selected_mode == OptimizationMode.EXACT:
        return UniformCostReplicaOptimizer().optimize(problem)
    options = scalable_options or ScalableReplicaSearchOptions()
    return ScalableReplicaOptimizer(
        maximum_configuration_evaluations=(
            options.maximum_configuration_evaluations
        ),
        time_budget_seconds=options.time_budget_seconds,
        candidate_width=options.candidate_width,
    ).optimize(problem)


def optimize_node_placement(
    problem,
    *,
    mode=OptimizationMode.AUTO,
    exact_configuration_limit=DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT,
    scalable_options=None,
):
    """Select exact branch-and-bound or bounded scalable placement."""
    selected_mode = resolve_optimization_mode(
        mode,
        problem.configuration_count(),
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

import math

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


def replica_configuration_count(calibration_profile):
    return math.prod(
        maximum - minimum + 1
        for minimum, maximum in zip(
            calibration_profile.minimum_replica_configuration,
            calibration_profile.maximum_replica_configuration,
        )
    )


def optimization_mode_decision(mode, search_space_size, exact_limit):
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
        selected = (
            OptimizationMode.EXACT
            if search_space_size <= exact_limit
            else OptimizationMode.SCALABLE
        )
        comparator = "<=" if selected == OptimizationMode.EXACT else ">"
        reason = (
            f"AUTO selected {selected.value}: estimated search space "
            f"{search_space_size:,} {comparator} exact threshold "
            f"{exact_limit:,}."
        )
        return selected, reason
    return mode, f"Caller explicitly selected {mode.value} mode."


def resolve_optimization_mode(mode, search_space_size, exact_limit):
    return optimization_mode_decision(mode, search_space_size, exact_limit)[0]


def _scalable_replica_optimizer(options, selection_reason):
    return ScalableReplicaOptimizer(
        maximum_configuration_evaluations=options.maximum_configuration_evaluations,
        maximum_total_evaluator_calls=options.maximum_total_evaluator_calls,
        maximum_simulation_replications=options.maximum_simulation_replications,
        time_budget_seconds=options.time_budget_seconds,
        candidate_width=options.candidate_width,
        restricted_candidate_fraction=options.restricted_candidate_fraction,
        maximum_restarts=options.maximum_restarts,
        maximum_no_improvement=options.maximum_no_improvement,
        maximum_vns_neighborhood=options.maximum_vns_neighborhood,
        vns_attempts_per_neighborhood=options.vns_attempts_per_neighborhood,
        random_seed=options.random_seed,
        removal_order=options.removal_order,
        archive_equal_cost=options.archive_equal_cost,
        collect_trace=options.collect_trace,
        selection_reason=selection_reason,
    )


def optimize_replica_configuration(
    problem,
    *,
    mode=OptimizationMode.AUTO,
    exact_configuration_limit=DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    scalable_options=None,
):
    """Select exact uniform-cost or bounded multi-start/VNS replica search."""
    count_method = getattr(problem, "configuration_count", None)
    if count_method is not None:
        search_space_size = count_method()
    elif hasattr(problem, "maximum_configuration"):
        minimum = (
            problem.stable_configuration()
            if hasattr(problem, "stable_configuration")
            else problem.initial_state()
        )
        search_space_size = math.prod(
            maximum - low + 1
            for low, maximum in zip(minimum, problem.maximum_configuration())
        )
    else:
        raise TypeError(
            "AUTO replica selection requires a configuration-count or bounds API."
        )
    selected, reason = optimization_mode_decision(
        mode, search_space_size, exact_configuration_limit
    )
    if selected == OptimizationMode.EXACT:
        optimizer = UniformCostReplicaOptimizer()
        optimizer.selection_reason = reason
        return optimizer.optimize(problem)
    return _scalable_replica_optimizer(
        scalable_options or ScalableReplicaSearchOptions(),
        reason,
    ).optimize(problem)


def optimize_node_placement(
    problem,
    *,
    mode=OptimizationMode.AUTO,
    exact_configuration_limit=DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT,
    scalable_options=None,
):
    """Select exact branch-and-bound or bounded beam/VNS/LNS placement."""
    selected, reason = optimization_mode_decision(
        mode,
        problem.configuration_count(),
        exact_configuration_limit,
    )
    if selected == OptimizationMode.EXACT:
        optimizer = BranchAndBoundNodePlacementOptimizer()
        optimizer.selection_reason = reason
        return optimizer.optimize(problem)
    options = scalable_options or ScalablePlacementSearchOptions()
    return ScalableNodePlacementOptimizer(
        beam_width=options.beam_width,
        diversity_per_signature=options.diversity_per_signature,
        candidate_distributions_per_service=(
            options.candidate_distributions_per_service
        ),
        maximum_partial_states=options.maximum_partial_states,
        maximum_complete_evaluations=options.maximum_complete_evaluations,
        maximum_total_evaluator_calls=options.maximum_total_evaluator_calls,
        maximum_simulation_replications=options.maximum_simulation_replications,
        maximum_local_iterations=options.maximum_local_iterations,
        time_budget_seconds=options.time_budget_seconds,
        maximum_restarts=options.maximum_restarts,
        maximum_no_improvement=options.maximum_no_improvement,
        random_seed=options.random_seed,
        lns_neighborhood_sizes=options.lns_neighborhood_sizes,
        lns_subset_attempts=options.lns_subset_attempts,
        lns_subproblem_budget=options.lns_subproblem_budget,
        collect_trace=options.collect_trace,
        selection_reason=reason,
    ).optimize(problem)

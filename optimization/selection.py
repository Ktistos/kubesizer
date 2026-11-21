import math
from itertools import product

from .uniform_cost import UniformCostReplicaOptimizer


def replica_configuration_count(calibration_profile):
    return math.prod(
        maximum - minimum + 1
        for minimum, maximum in zip(
            calibration_profile.minimum_replica_configuration,
            calibration_profile.maximum_replica_configuration,
        )
    )


def evaluate_replica_configurations(problem, *, progress_callback=None):
    """Evaluate the full bounded grid needed for cost and latency trade-offs."""
    count = problem.configuration_count()
    if progress_callback is not None:
        progress_callback("replica_optimization_started", {
            "completed_configurations": 0,
            "total_configurations": count,
            "search_space_configurations": count,
        })
    bounds = zip(problem.initial_state(), problem.maximum_configuration(), strict=True)
    evaluations = tuple(
        problem.evaluate(configuration)
        for configuration in product(*(range(low, high + 1) for low, high in bounds))
    )
    if progress_callback is not None:
        progress_callback("replica_optimization_completed", {
            "completed_configurations": len(evaluations),
            "search_space_configurations": count,
        })
    return evaluations


def pareto_replica_configurations(evaluations, *, accept=None):
    """Return nondominated feasible vectors under cost and worst SLO ratio.

    Only an accepted candidate can dominate another one. The objectives come
    from the search, so rejecting a finalist can expose a replacement.
    """
    candidates = sorted(
        (evaluation for evaluation in evaluations if evaluation.feasible),
        key=lambda evaluation: (
            evaluation.cost, evaluation.tie_breaker, evaluation.configuration
        ),
    )
    frontier = []
    best = (math.inf, math.inf)
    for candidate in candidates:
        rank = (candidate.tie_breaker, candidate.cost)
        if rank > best or (accept is not None and not accept(candidate)):
            continue
        frontier.append(candidate)
        best = rank
    return tuple(frontier)


def optimize_replica_configuration(
    problem,
    *,
    progress_callback=None,
):
    """Run exact uniform-cost replica search over the configured bounds."""
    if progress_callback is not None and not callable(progress_callback):
        raise TypeError("progress_callback must be callable or None.")
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
        raise TypeError("Exact replica search requires a configuration-count or bounds API.")
    if progress_callback is not None:
        progress_callback(
            "replica_optimization_started",
            {
                "completed_configurations": 0,
                "total_configurations": search_space_size,
                "search_space_configurations": search_space_size,
            },
        )
    result = UniformCostReplicaOptimizer().optimize(problem)
    if progress_callback is not None:
        progress_callback(
            "replica_optimization_completed",
            {
                "completed_configurations": getattr(
                    result,
                    "unique_evaluation_count",
                    getattr(result, "explored_states", 0),
                ),
                "total_configurations": search_space_size,
                "search_space_configurations": search_space_size,
            },
        )
    return result

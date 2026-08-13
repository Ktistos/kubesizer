import math
from dataclasses import dataclass

from .base import Optimizer
from .exceptions import NoFeasibleConfigurationError
from .result import OptimizationResult


@dataclass(frozen=True)
class BranchAndBoundNodePlacementOptimizationResult(OptimizationResult):
    """Exact placement result with branch-and-bound search diagnostics."""

    feasible_states: int
    complete_states: int
    pruned_branches: int
    pruned_complete_placements: int
    total_configurations: int
    optimality_proven: bool = True

    @property
    def placement(self):
        """Return the optimal node-oriented placement dictionary."""
        return {
            node_name: dict(service_counts)
            for node_name, service_counts in self.configuration
            if service_counts
        }


class BranchAndBoundNodePlacementOptimizer(Optimizer):
    """Find an exact placement without evaluating every complete candidate.

    The search assigns one service distribution at a time. Partial placements
    that exceed node capacity are discarded immediately. An admissible cost
    bound discards a branch only when every completion is provably worse than
    the best complete placement already found.
    """

    _BOUND_RELATIVE_TOLERANCE = 1e-12
    _BOUND_ABSOLUTE_TOLERANCE = 1e-15

    def optimize(self, problem):
        service_names = tuple(problem.get_service_names())
        distribution_options = {
            service_name: problem.get_replica_distribution_options(
                service_name
            )
            for service_name in service_names
        }
        service_order = self._traffic_aware_service_order(
            service_names,
            problem.get_edge_traffic_rates(),
            distribution_options,
        )
        total_configurations = problem.configuration_count()

        best_evaluation = None
        best_rank = None
        explored_states = 1
        feasible_states = 0
        complete_states = 0
        pruned_branches = 0
        pruned_complete_placements = 0
        assignments = {}

        remaining_completion_counts = [1] * (len(service_order) + 1)
        for index in range(len(service_order) - 1, -1, -1):
            remaining_completion_counts[index] = (
                remaining_completion_counts[index + 1]
                * len(distribution_options[service_order[index]])
            )

        def search(depth):
            nonlocal best_evaluation
            nonlocal best_rank
            nonlocal explored_states
            nonlocal feasible_states
            nonlocal complete_states
            nonlocal pruned_branches
            nonlocal pruned_complete_placements

            if depth == len(service_order):
                complete_states += 1
                configuration = problem.build_configuration(assignments)
                evaluation = problem.evaluate(configuration)
                if not evaluation.valid:
                    return

                feasible_states += 1
                candidate_rank = (
                    evaluation.total_cost,
                    evaluation.configuration,
                )
                if best_rank is None or candidate_rank < best_rank:
                    best_rank = candidate_rank
                    best_evaluation = evaluation
                return

            service_name = service_order[depth]
            candidates = []
            completions_below_candidate = remaining_completion_counts[
                depth + 1
            ]

            for distribution in distribution_options[service_name]:
                assignments[service_name] = distribution
                if not problem.partial_assignment_is_resource_feasible(
                    assignments
                ):
                    pruned_branches += 1
                    pruned_complete_placements += completions_below_candidate
                    continue
                lower_bound = problem.partial_cost_lower_bound(assignments)
                candidates.append((lower_bound, distribution))
            assignments.pop(service_name, None)

            candidates.sort(key=lambda item: (item[0], item[1]))
            for lower_bound, distribution in candidates:
                if self._is_strictly_worse_bound(
                    lower_bound,
                    None if best_evaluation is None else best_evaluation.total_cost,
                ):
                    pruned_branches += 1
                    pruned_complete_placements += completions_below_candidate
                    continue

                assignments[service_name] = distribution
                explored_states += 1
                search(depth + 1)
                assignments.pop(service_name)

        search(0)

        if best_evaluation is None:
            raise NoFeasibleConfigurationError(
                "No feasible fixed-replica node placement exists."
            )

        if complete_states + pruned_complete_placements != total_configurations:
            raise AssertionError(
                "Branch-and-bound accounting did not cover the full placement "
                "space."
            )

        configuration = best_evaluation.configuration
        return BranchAndBoundNodePlacementOptimizationResult(
            configuration=configuration,
            cost=best_evaluation.total_cost,
            metrics=best_evaluation,
            explored_states=explored_states,
            path=(configuration,),
            feasible_states=feasible_states,
            complete_states=complete_states,
            pruned_branches=pruned_branches,
            pruned_complete_placements=pruned_complete_placements,
            total_configurations=total_configurations,
        )

    @classmethod
    def _is_strictly_worse_bound(cls, lower_bound, incumbent_cost):
        if incumbent_cost is None or lower_bound <= incumbent_cost:
            return False
        return not math.isclose(
            lower_bound,
            incumbent_cost,
            rel_tol=cls._BOUND_RELATIVE_TOLERANCE,
            abs_tol=cls._BOUND_ABSOLUTE_TOLERANCE,
        )

    @staticmethod
    def _traffic_aware_service_order(
        service_names,
        edge_traffic_rates,
        distribution_options,
    ):
        """Place highly connected services early to tighten cost bounds."""
        original_index = {
            service_name: index
            for index, service_name in enumerate(service_names)
        }
        total_incident_traffic = {name: 0.0 for name in service_names}
        pair_traffic = {}
        for (source, destination), traffic_rate in edge_traffic_rates.items():
            total_incident_traffic[source] += traffic_rate
            if destination != source:
                total_incident_traffic[destination] += traffic_rate
            pair_traffic[(source, destination)] = (
                pair_traffic.get((source, destination), 0.0) + traffic_rate
            )

        order = []
        unassigned = set(service_names)
        while unassigned:
            assigned = set(order)

            def rank(service_name):
                connected_traffic = sum(
                    traffic_rate
                    for (source, destination), traffic_rate
                    in pair_traffic.items()
                    if (
                        source == service_name and destination in assigned
                    )
                    or (
                        destination == service_name and source in assigned
                    )
                )
                return (
                    connected_traffic,
                    total_incident_traffic[service_name],
                    len(distribution_options[service_name]),
                    -original_index[service_name],
                )

            selected = max(unassigned, key=rank)
            order.append(selected)
            unassigned.remove(selected)
        return tuple(order)

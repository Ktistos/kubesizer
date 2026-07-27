from dataclasses import dataclass

from .base import Optimizer
from .exceptions import NoFeasibleConfigurationError
from .result import OptimizationResult


@dataclass(frozen=True)
class NodePlacementOptimizationResult(OptimizationResult):
    """Exact node-placement result with feasible-state accounting."""

    feasible_states: int

    @property
    def placement(self):
        """Return the optimal configuration in node-oriented dictionary form."""
        return {
            node_name: dict(service_counts)
            for node_name, service_counts in self.configuration
            if service_counts
        }


class ExhaustiveNodePlacementOptimizer(Optimizer):
    """Find the global optimum by evaluating every fixed-count placement.

    This algorithm is suitable for the current small placement space and does
    not require an admissible heuristic or an external mixed-integer solver.
    """

    def optimize(self, problem) -> NodePlacementOptimizationResult:
        best_evaluation = None
        best_rank = None
        explored_states = 0
        feasible_states = 0

        for configuration in problem.iter_configurations():
            explored_states += 1
            evaluation = problem.evaluate(configuration)
            if not evaluation.valid:
                continue

            feasible_states += 1
            candidate_rank = (
                evaluation.total_cost,
                evaluation.configuration,
            )
            if best_rank is None or candidate_rank < best_rank:
                best_rank = candidate_rank
                best_evaluation = evaluation

        if best_evaluation is None:
            raise NoFeasibleConfigurationError(
                "No feasible fixed-replica node placement exists."
            )

        configuration = best_evaluation.configuration
        return NodePlacementOptimizationResult(
            configuration=configuration,
            cost=best_evaluation.total_cost,
            metrics=best_evaluation,
            explored_states=explored_states,
            path=(configuration,),
            feasible_states=feasible_states,
        )

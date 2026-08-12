import itertools
import math
import time
from dataclasses import dataclass

from .base import Optimizer
from .exceptions import NoFeasibleConfigurationError
from .result import OptimizationResult


@dataclass(frozen=True)
class ScalableNodePlacementOptimizationResult(OptimizationResult):
    """Bounded placement result that never claims global optimality."""

    feasible_states: int
    complete_states: int
    partial_states: int
    discarded_partial_states: int
    total_configurations: int
    solution_mode: str
    optimality_proven: bool
    budget_exhausted: bool
    search_truncated: bool
    locally_optimal: bool
    beam_width: int
    maximum_complete_evaluations: int
    time_budget_seconds: float

    @property
    def placement(self):
        return {
            node_name: dict(service_counts)
            for node_name, service_counts in self.configuration
            if service_counts
        }

    @property
    def pruned_complete_placements(self):
        # Beam discards are heuristic, not proof-based branch-and-bound prunes.
        return 0


class ScalableNodePlacementOptimizer(Optimizer):
    """Traffic-aware beam construction followed by bounded replica moves."""

    def __init__(
        self,
        *,
        beam_width=32,
        candidate_distributions_per_service=64,
        maximum_partial_states=100_000,
        maximum_complete_evaluations=10_000,
        maximum_local_iterations=100,
        time_budget_seconds=30.0,
        clock=None,
    ):
        integer_options = {
            "beam_width": beam_width,
            "candidate_distributions_per_service": (
                candidate_distributions_per_service
            ),
            "maximum_partial_states": maximum_partial_states,
            "maximum_complete_evaluations": maximum_complete_evaluations,
            "maximum_local_iterations": maximum_local_iterations,
        }
        for name, value in integer_options.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be positive.")
        if not math.isfinite(time_budget_seconds) or time_budget_seconds <= 0:
            raise ValueError("time_budget_seconds must be finite and positive.")
        self.beam_width = beam_width
        self.candidate_distributions_per_service = (
            candidate_distributions_per_service
        )
        self.maximum_partial_states = maximum_partial_states
        self.maximum_complete_evaluations = maximum_complete_evaluations
        self.maximum_local_iterations = maximum_local_iterations
        self.time_budget_seconds = float(time_budget_seconds)
        self._clock = clock or time.perf_counter

    @staticmethod
    def _service_order(problem):
        names = tuple(problem.get_service_names())
        traffic = problem.get_edge_traffic_rates()
        original = {name: index for index, name in enumerate(names)}
        incident = {name: 0.0 for name in names}
        for (source, destination), rate in traffic.items():
            incident[source] += rate
            if destination != source:
                incident[destination] += rate
        return tuple(
            sorted(
                names,
                key=lambda name: (-incident[name], original[name]),
            )
        )

    def _distribution_candidates(self, problem, service_name):
        node_count = len(problem.get_node_names())
        replicas = problem.get_fixed_replica_counts()[service_name]
        candidates = []
        seen = set()

        def add(distribution):
            distribution = tuple(distribution)
            if distribution not in seen:
                seen.add(distribution)
                candidates.append(distribution)

        quotient, remainder = divmod(replicas, node_count)
        for rotation in range(node_count):
            balanced = [quotient] * node_count
            for offset in range(remainder):
                balanced[(rotation + offset) % node_count] += 1
            add(balanced)

        for node_index in range(node_count):
            concentrated = [0] * node_count
            concentrated[node_index] = replicas
            add(concentrated)

        for left, right in itertools.combinations(range(node_count), 2):
            for left_count in {replicas // 2, math.ceil(replicas / 2)}:
                split = [0] * node_count
                split[left] = left_count
                split[right] = replicas - left_count
                add(split)

        if len(candidates) < self.candidate_distributions_per_service:
            for distribution in problem.iter_replica_distribution_options(
                service_name
            ):
                add(distribution)
                if len(candidates) >= self.candidate_distributions_per_service:
                    break
        return tuple(candidates[: self.candidate_distributions_per_service])

    @staticmethod
    def _assignment_key(service_order, assignments):
        return tuple(assignments[name] for name in service_order if name in assignments)

    def optimize(self, problem):
        started = self._clock()
        service_order = self._service_order(problem)
        total_configurations = problem.configuration_count()
        partial_states = 0
        discarded_partial_states = 0
        complete_states = 0
        feasible_states = 0
        budget_exhausted = False
        search_truncated = False

        def expired():
            return self._clock() - started >= self.time_budget_seconds

        beam = ((0.0, {}),)
        for service_name in service_order:
            candidates = []
            distributions = self._distribution_candidates(problem, service_name)
            replica_count = problem.get_fixed_replica_counts()[service_name]
            distribution_count = math.comb(
                replica_count + len(problem.get_node_names()) - 1,
                len(problem.get_node_names()) - 1,
            )
            if len(distributions) < distribution_count:
                search_truncated = True
            for _, assignments in beam:
                for distribution in distributions:
                    if partial_states >= self.maximum_partial_states or expired():
                        budget_exhausted = True
                        break
                    expanded = dict(assignments)
                    expanded[service_name] = distribution
                    partial_states += 1
                    if not problem.partial_assignment_is_resource_feasible(expanded):
                        discarded_partial_states += 1
                        continue
                    candidates.append(
                        (problem.partial_cost_lower_bound(expanded), expanded)
                    )
                if budget_exhausted:
                    break
            if not candidates:
                raise NoFeasibleConfigurationError(
                    "Scalable placement search found no resource-feasible partial "
                    "placement within its budget."
                )
            candidates.sort(
                key=lambda item: (
                    item[0],
                    self._assignment_key(service_order, item[1]),
                )
            )
            if len(candidates) > self.beam_width:
                search_truncated = True
                discarded_partial_states += len(candidates) - self.beam_width
            beam = tuple(candidates[: self.beam_width])
            if budget_exhausted and service_name != service_order[-1]:
                raise NoFeasibleConfigurationError(
                    "Scalable placement search exhausted its budget before "
                    "constructing a complete placement."
                )

        evaluated = {}

        def evaluate(assignments):
            nonlocal complete_states, feasible_states, budget_exhausted
            key = tuple(assignments[name] for name in service_order)
            if key in evaluated:
                return evaluated[key]
            if (
                complete_states >= self.maximum_complete_evaluations
                or expired()
            ):
                budget_exhausted = True
                return None
            result = problem.evaluate(problem.build_configuration(assignments))
            complete_states += 1
            feasible_states += int(result.valid)
            evaluated[key] = result
            return result

        best_evaluation = None
        best_assignments = None
        for _, assignments in beam:
            evaluation = evaluate(assignments)
            if evaluation is None:
                break
            if not evaluation.valid:
                continue
            if best_evaluation is None or (
                evaluation.total_cost,
                evaluation.configuration,
            ) < (
                best_evaluation.total_cost,
                best_evaluation.configuration,
            ):
                best_evaluation = evaluation
                best_assignments = dict(assignments)
        if best_evaluation is None:
            raise NoFeasibleConfigurationError(
                "Scalable placement search found no complete feasible placement "
                "within its budget."
            )

        locally_optimal = False
        local_iterations = 0
        while local_iterations < self.maximum_local_iterations:
            local_iterations += 1
            improving = []
            node_count = len(problem.get_node_names())
            for service_name in service_order:
                distribution = best_assignments[service_name]
                for source in range(node_count):
                    if distribution[source] == 0:
                        continue
                    for destination in range(node_count):
                        if source == destination:
                            continue
                        moved = list(distribution)
                        moved[source] -= 1
                        moved[destination] += 1
                        candidate = dict(best_assignments)
                        candidate[service_name] = tuple(moved)
                        if not problem.partial_assignment_is_resource_feasible(candidate):
                            continue
                        evaluation = evaluate(candidate)
                        if evaluation is None:
                            break
                        if evaluation.valid and evaluation.total_cost < (
                            best_evaluation.total_cost - 1e-15
                        ):
                            improving.append((evaluation, candidate))
                    if budget_exhausted:
                        break
                if budget_exhausted:
                    break
            if budget_exhausted:
                break
            if not improving:
                locally_optimal = True
                break
            best_evaluation, best_assignments = min(
                improving,
                key=lambda item: (
                    item[0].total_cost,
                    item[0].configuration,
                ),
            )
        if local_iterations >= self.maximum_local_iterations and not locally_optimal:
            budget_exhausted = True

        configuration = best_evaluation.configuration
        return ScalableNodePlacementOptimizationResult(
            configuration=configuration,
            cost=best_evaluation.total_cost,
            metrics=best_evaluation,
            explored_states=partial_states + complete_states,
            path=(configuration,),
            feasible_states=feasible_states,
            complete_states=complete_states,
            partial_states=partial_states,
            discarded_partial_states=discarded_partial_states,
            total_configurations=total_configurations,
            solution_mode="SCALABLE",
            optimality_proven=False,
            budget_exhausted=budget_exhausted,
            search_truncated=search_truncated,
            locally_optimal=locally_optimal,
            beam_width=self.beam_width,
            maximum_complete_evaluations=self.maximum_complete_evaluations,
            time_budget_seconds=self.time_budget_seconds,
        )

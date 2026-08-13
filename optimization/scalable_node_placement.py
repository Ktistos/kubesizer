import math
import random
from dataclasses import dataclass

from .base import Optimizer
from .exceptions import NoFeasibleConfigurationError
from .feasibility import (
    EvaluationBudgetExhausted,
    FeasibilityStatus,
    HeuristicEvaluationController,
    HeuristicTraceEntry,
    TerminationReason,
    certified_bound_gaps,
)
from .result import OptimizationResult


@dataclass(frozen=True)
class ScalableNodePlacementOptimizationResult(OptimizationResult):
    """Beam/VNS/LNS placement result with honest heuristic guarantees."""

    method: str
    objective_value: float
    feasibility_status: FeasibilityStatus
    termination_reason: TerminationReason
    evaluation_count: int
    unique_evaluation_count: int
    simulation_replications: int
    runtime_seconds: float
    restart_count: int
    random_seed: int
    budget_exhausted: bool
    deletion_local_certified: bool
    relocation_local_certified: bool
    swap_local_certified: bool
    global_optimal: bool
    lower_bound: float | None
    absolute_bound_gap: float | None
    relative_bound_gap: float | None
    trace: tuple[HeuristicTraceEntry, ...]
    selection_reason: str | None
    feasible_states: int
    complete_states: int
    partial_states: int
    discarded_partial_states: int
    total_configurations: int
    solution_mode: str
    optimality_proven: bool
    search_truncated: bool
    locally_optimal: bool
    beam_width: int
    maximum_complete_evaluations: int
    time_budget_seconds: float
    maximum_restarts: int
    lns_neighborhood_sizes: tuple[int, ...]

    @property
    def placement(self):
        return {
            node_name: dict(service_counts)
            for node_name, service_counts in self.configuration
            if service_counts
        }

    @property
    def pruned_complete_placements(self):
        return 0


class BeamVNSPlacementOptimizer(Optimizer):
    """Diverse per-replica beam construction with relocation, swap, and LNS."""

    _IMPROVEMENT_TOLERANCE = 1e-15

    def __init__(
        self,
        *,
        beam_width=32,
        diversity_per_signature=2,
        candidate_distributions_per_service=64,
        maximum_partial_states=100_000,
        maximum_complete_evaluations=10_000,
        maximum_total_evaluator_calls=None,
        maximum_simulation_replications=None,
        maximum_local_iterations=100,
        time_budget_seconds=30.0,
        maximum_restarts=4,
        maximum_no_improvement=3,
        random_seed=0,
        lns_neighborhood_sizes=(3, 4, 5),
        lns_subset_attempts=3,
        lns_subproblem_budget=1_000,
        initial_placement=None,
        collect_trace=False,
        cancellation_check=None,
        clock=None,
        selection_reason=None,
    ):
        integers = {
            "beam_width": beam_width,
            "diversity_per_signature": diversity_per_signature,
            "candidate_distributions_per_service": (
                candidate_distributions_per_service
            ),
            "maximum_partial_states": maximum_partial_states,
            "maximum_complete_evaluations": maximum_complete_evaluations,
            "maximum_local_iterations": maximum_local_iterations,
            "maximum_restarts": maximum_restarts,
            "maximum_no_improvement": maximum_no_improvement,
            "lns_subset_attempts": lns_subset_attempts,
            "lns_subproblem_budget": lns_subproblem_budget,
        }
        for name, value in integers.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer.")
        if maximum_total_evaluator_calls is not None and (
            isinstance(maximum_total_evaluator_calls, bool)
            or not isinstance(maximum_total_evaluator_calls, int)
            or maximum_total_evaluator_calls <= 0
        ):
            raise ValueError(
                "maximum_total_evaluator_calls must be a positive integer."
            )
        if not math.isfinite(time_budget_seconds) or time_budget_seconds <= 0:
            raise ValueError("time_budget_seconds must be finite and positive.")
        if not lns_neighborhood_sizes or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in lns_neighborhood_sizes
        ):
            raise ValueError("lns_neighborhood_sizes must contain positive integers.")
        if isinstance(random_seed, bool) or not isinstance(random_seed, int):
            raise ValueError("random_seed must be an integer.")

        self.beam_width = beam_width
        self.diversity_per_signature = diversity_per_signature
        self.candidate_distributions_per_service = candidate_distributions_per_service
        self.maximum_partial_states = maximum_partial_states
        self.maximum_complete_evaluations = maximum_complete_evaluations
        self.maximum_total_evaluator_calls = maximum_total_evaluator_calls
        self.maximum_simulation_replications = maximum_simulation_replications
        self.maximum_local_iterations = maximum_local_iterations
        self.time_budget_seconds = float(time_budget_seconds)
        self.maximum_restarts = maximum_restarts
        self.maximum_no_improvement = maximum_no_improvement
        self.random_seed = random_seed
        self.lns_neighborhood_sizes = tuple(lns_neighborhood_sizes)
        self.lns_subset_attempts = lns_subset_attempts
        self.lns_subproblem_budget = lns_subproblem_budget
        self.initial_placement = initial_placement
        self.collect_trace = bool(collect_trace)
        self.cancellation_check = cancellation_check
        self._clock = clock
        self.selection_reason = selection_reason

    @staticmethod
    def _key(problem, distributions):
        return tuple(tuple(distributions[name]) for name in problem.get_service_names())

    @staticmethod
    def _copy(distributions):
        return {name: tuple(value) for name, value in distributions.items()}

    def _evaluate(self, controller, problem, distributions):
        key = self._key(problem, distributions)
        return controller.evaluate(
            key,
            lambda: problem.evaluate_distributions(distributions),
        )[0]

    def _check(self, controller):
        controller.check_available(new_configuration=False)

    def _replica_order(self, problem, rng, restart):
        difficulty = problem.get_replica_difficulty_scores()
        items = list(problem.get_replica_items())
        jitter = {item: rng.random() for item in items}
        if restart % 3 == 1:
            items.sort(key=lambda item: (-difficulty[item[0]], jitter[item], item))
        elif restart % 3 == 2:
            items.sort(key=lambda item: (jitter[item], -difficulty[item[0]], item))
        else:
            items.sort(
                key=lambda item: (
                    -difficulty[item[0]],
                    -item[1],
                    jitter[item],
                )
            )
        return tuple(items)

    def _diverse_beam(self, problem, candidates):
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        retained = []
        signature_counts = {}
        for score, tie, distributions in candidates:
            signature = problem.partial_resource_signature(distributions)
            count = signature_counts.get(signature, 0)
            if count >= self.diversity_per_signature:
                continue
            signature_counts[signature] = count + 1
            retained.append((score, tie, distributions))
            if len(retained) >= self.beam_width:
                break
        return tuple(retained)

    def _construct_beam(
        self,
        problem,
        controller,
        rng,
        restart,
        partial_counter,
    ):
        node_names = problem.get_node_names()
        service_names = problem.get_service_names()
        zero = {name: (0,) * len(node_names) for name in service_names}
        beam = ((0.0, 0.0, zero),)
        heuristic_truncated = False
        for service_name, _ in self._replica_order(problem, rng, restart):
            expanded = []
            for _, _, distributions in beam:
                for node_index in range(len(node_names)):
                    try:
                        self._check(controller)
                    except EvaluationBudgetExhausted:
                        return (), True, heuristic_truncated
                    if partial_counter[0] >= self.maximum_partial_states:
                        return (), True, heuristic_truncated
                    partial_counter[0] += 1
                    candidate = self._copy(distributions)
                    service_distribution = list(candidate[service_name])
                    service_distribution[node_index] += 1
                    candidate[service_name] = tuple(service_distribution)
                    if not problem.partial_replica_assignment_is_feasible(candidate):
                        partial_counter[1] += 1
                        continue
                    expanded.append(
                        (
                            problem.partial_replica_cost_estimate(candidate),
                            rng.random(),
                            candidate,
                        )
                    )
            if not expanded:
                return (), False, heuristic_truncated
            before = len(expanded)
            beam = self._diverse_beam(problem, expanded)
            heuristic_truncated = heuristic_truncated or len(beam) < before
            partial_counter[1] += before - len(beam)
        return (
            tuple(item[2] for item in beam),
            False,
            heuristic_truncated,
        )

    def _best_relocation(self, problem, distributions, controller):
        best = None
        node_names = problem.get_node_names()
        for service_name in problem.get_service_names():
            for source_index, count in enumerate(distributions[service_name]):
                if count <= 0:
                    continue
                for destination_index in range(len(node_names)):
                    if source_index == destination_index:
                        continue
                    self._check(controller)
                    delta, candidate = problem.relocation_delta(
                        distributions,
                        service_name,
                        node_names[source_index],
                        node_names[destination_index],
                    )
                    if delta >= -self._IMPROVEMENT_TOLERANCE:
                        continue
                    rank = (delta, self._key(problem, candidate))
                    if best is None or rank < best[0]:
                        best = (rank, candidate)
        return None if best is None else best[1]

    def _best_swap(self, problem, distributions, controller):
        occupied = [
            (service_name, node_index)
            for service_name in problem.get_service_names()
            for node_index, count in enumerate(distributions[service_name])
            if count > 0
        ]
        node_names = problem.get_node_names()
        best = None
        for left_index, (left_service, left_node) in enumerate(occupied):
            for right_service, right_node in occupied[left_index + 1 :]:
                if left_node == right_node or left_service == right_service:
                    continue
                self._check(controller)
                delta, candidate = problem.swap_delta(
                    distributions,
                    left_service,
                    node_names[left_node],
                    right_service,
                    node_names[right_node],
                )
                if delta >= -self._IMPROVEMENT_TOLERANCE:
                    continue
                rank = (delta, self._key(problem, candidate))
                if best is None or rank < best[0]:
                    best = (rank, candidate)
        return None if best is None else best[1]

    def _local_descent(
        self,
        problem,
        controller,
        distributions,
        evaluation,
        restart,
        record,
    ):
        relocation_certified = False
        swap_certified = False
        iterations = 0
        while iterations < self.maximum_local_iterations:
            iterations += 1
            try:
                relocated = self._best_relocation(problem, distributions, controller)
            except EvaluationBudgetExhausted:
                return (
                    distributions,
                    evaluation,
                    False,
                    False,
                )
            if relocated is not None:
                try:
                    candidate_evaluation = self._evaluate(
                        controller, problem, relocated
                    )
                except EvaluationBudgetExhausted:
                    return distributions, evaluation, False, False
                if (
                    candidate_evaluation.valid
                    and candidate_evaluation.total_cost
                    < evaluation.total_cost - self._IMPROVEMENT_TOLERANCE
                ):
                    distributions = relocated
                    evaluation = candidate_evaluation
                    record(
                        "relocation",
                        restart,
                        distributions,
                        evaluation,
                        "accepted-relocation",
                    )
                    continue
            relocation_certified = True

            try:
                swapped = self._best_swap(problem, distributions, controller)
            except EvaluationBudgetExhausted:
                return distributions, evaluation, relocation_certified, False
            if swapped is None:
                swap_certified = True
                return (
                    distributions,
                    evaluation,
                    relocation_certified,
                    swap_certified,
                )
            try:
                candidate_evaluation = self._evaluate(controller, problem, swapped)
            except EvaluationBudgetExhausted:
                return distributions, evaluation, relocation_certified, False
            if (
                candidate_evaluation.valid
                and candidate_evaluation.total_cost
                < evaluation.total_cost - self._IMPROVEMENT_TOLERANCE
            ):
                distributions = swapped
                evaluation = candidate_evaluation
                relocation_certified = False
                record("swap", restart, distributions, evaluation, "accepted-swap")
                continue
            swap_certified = True
            return (
                distributions,
                evaluation,
                relocation_certified,
                swap_certified,
            )
        return distributions, evaluation, False, False

    @staticmethod
    def _replica_instances(distributions):
        return tuple(
            (service_name, node_index, ordinal)
            for service_name, distribution in distributions.items()
            for node_index, count in enumerate(distribution)
            for ordinal in range(count)
        )

    def _lns_subsets(self, problem, distributions, q, rng):
        replicas = list(self._replica_instances(distributions))
        if q > len(replicas):
            return ()
        difficulty = problem.get_replica_difficulty_scores()
        selected = []
        top = tuple(
            sorted(
                replicas,
                key=lambda item: (-difficulty[item[0]], item),
            )[:q]
        )
        selected.append(top)
        for _ in range(self.lns_subset_attempts - 1):
            subset = tuple(sorted(rng.sample(replicas, q)))
            if subset not in selected:
                selected.append(subset)
        return tuple(selected)

    def _repair_lns(
        self,
        problem,
        controller,
        distributions,
        selected,
        rng,
    ):
        base = self._copy(distributions)
        for service_name, node_index, _ in selected:
            values = list(base[service_name])
            values[node_index] -= 1
            base[service_name] = tuple(values)

        beam = ((problem.partial_replica_cost_estimate(base), base),)
        expanded_count = 0
        node_count = len(problem.get_node_names())
        for service_name, _, _ in selected:
            expanded = []
            for _, partial in beam:
                for node_index in range(node_count):
                    self._check(controller)
                    if expanded_count >= self.lns_subproblem_budget:
                        return ()
                    expanded_count += 1
                    candidate = self._copy(partial)
                    values = list(candidate[service_name])
                    values[node_index] += 1
                    candidate[service_name] = tuple(values)
                    if not problem.partial_replica_assignment_is_feasible(candidate):
                        continue
                    expanded.append(
                        (
                            problem.partial_replica_cost_estimate(candidate),
                            rng.random(),
                            candidate,
                        )
                    )
            if not expanded:
                return ()
            expanded.sort(
                key=lambda item: (item[0], item[1], self._key(problem, item[2]))
            )
            beam = tuple(
                (score, candidate)
                for score, _, candidate in expanded[: self.beam_width]
            )
        return tuple(candidate for _, candidate in beam)

    def _lns(
        self,
        problem,
        controller,
        distributions,
        evaluation,
        rng,
        restart,
        record,
    ):
        for q in self.lns_neighborhood_sizes:
            for selected in self._lns_subsets(problem, distributions, q, rng):
                try:
                    repairs = self._repair_lns(
                        problem, controller, distributions, selected, rng
                    )
                except EvaluationBudgetExhausted:
                    return distributions, evaluation, False
                for candidate in repairs:
                    try:
                        candidate_evaluation = self._evaluate(
                            controller, problem, candidate
                        )
                    except EvaluationBudgetExhausted:
                        return distributions, evaluation, False
                    if (
                        candidate_evaluation.valid
                        and candidate_evaluation.total_cost
                        < evaluation.total_cost - self._IMPROVEMENT_TOLERANCE
                    ):
                        record(
                            "lns",
                            restart,
                            candidate,
                            candidate_evaluation,
                            f"accepted-lns-{q}",
                        )
                        return candidate, candidate_evaluation, True
        return distributions, evaluation, False

    def optimize(self, problem):
        rng = random.Random(self.random_seed)
        controller = HeuristicEvaluationController(
            maximum_unique_evaluations=self.maximum_complete_evaluations,
            maximum_total_evaluator_calls=self.maximum_total_evaluator_calls,
            maximum_simulation_replications=(self.maximum_simulation_replications),
            time_budget_seconds=self.time_budget_seconds,
            clock=self._clock,
            cancellation_check=self.cancellation_check,
        )
        trace = []
        partial_counter = [0, 0]
        feasible_states = 0
        incumbent = None
        incumbent_evaluation = None
        incumbent_relocation = False
        incumbent_swap = False
        restart_count = 0
        no_improvement = 0
        search_truncated = False
        construction_budget_exhausted = False
        beam_failures = 0

        def record(phase, restart, distributions, evaluation, action):
            if not self.collect_trace:
                return
            trace.append(
                HeuristicTraceEntry(
                    phase=phase,
                    restart=restart,
                    configuration=self._key(problem, distributions),
                    objective_value=evaluation.total_cost,
                    feasibility_status=(
                        FeasibilityStatus.FEASIBLE
                        if evaluation.valid
                        else FeasibilityStatus.INFEASIBLE
                    ),
                    action=action,
                )
            )

        if self.initial_placement is not None:
            distributions = problem.configuration_to_distributions(
                self.initial_placement
            )
            if problem.partial_replica_assignment_is_feasible(distributions):
                evaluation = self._evaluate(controller, problem, distributions)
                if evaluation.valid:
                    feasible_states += 1
                    (
                        incumbent,
                        incumbent_evaluation,
                        incumbent_relocation,
                        incumbent_swap,
                    ) = self._local_descent(
                        problem,
                        controller,
                        distributions,
                        evaluation,
                        0,
                        record,
                    )

        for restart in range(1, self.maximum_restarts + 1):
            if controller.budget_exhausted:
                break
            restart_count = restart
            (
                complete_candidates,
                construction_budget,
                beam_truncated,
            ) = self._construct_beam(problem, controller, rng, restart, partial_counter)
            search_truncated = search_truncated or beam_truncated
            construction_budget_exhausted = (
                construction_budget_exhausted or construction_budget
            )
            if not complete_candidates:
                beam_failures += 1
                if construction_budget:
                    break
                no_improvement += 1
                continue

            restart_best = None
            restart_evaluation = None
            for distributions in complete_candidates:
                try:
                    evaluation = self._evaluate(controller, problem, distributions)
                except EvaluationBudgetExhausted:
                    construction_budget_exhausted = True
                    break
                feasible_states += int(evaluation.valid)
                if not evaluation.valid:
                    continue
                if restart_evaluation is None or (
                    evaluation.total_cost,
                    self._key(problem, distributions),
                ) < (
                    restart_evaluation.total_cost,
                    self._key(problem, restart_best),
                ):
                    restart_best = distributions
                    restart_evaluation = evaluation
            if restart_best is None:
                if controller.budget_exhausted:
                    break
                no_improvement += 1
                continue

            (
                restart_best,
                restart_evaluation,
                relocation_local,
                swap_local,
            ) = self._local_descent(
                problem,
                controller,
                restart_best,
                restart_evaluation,
                restart,
                record,
            )
            lns_best, lns_evaluation, improved = self._lns(
                problem,
                controller,
                restart_best,
                restart_evaluation,
                rng,
                restart,
                record,
            )
            if improved:
                (
                    restart_best,
                    restart_evaluation,
                    relocation_local,
                    swap_local,
                ) = self._local_descent(
                    problem,
                    controller,
                    lns_best,
                    lns_evaluation,
                    restart,
                    record,
                )

            candidate_rank = (
                restart_evaluation.total_cost,
                self._key(problem, restart_best),
            )
            incumbent_rank = (
                (
                    incumbent_evaluation.total_cost,
                    self._key(problem, incumbent),
                )
                if incumbent is not None
                else None
            )
            if incumbent_rank is None or candidate_rank < incumbent_rank:
                incumbent = restart_best
                incumbent_evaluation = restart_evaluation
                incumbent_relocation = relocation_local
                incumbent_swap = swap_local
                no_improvement = 0
                record(
                    "multi-start",
                    restart,
                    incumbent,
                    incumbent_evaluation,
                    "new-incumbent",
                )
            else:
                no_improvement += 1
            if no_improvement >= self.maximum_no_improvement:
                break

        if incumbent is None:
            reason = controller.exhaustion_reason or (
                TerminationReason.EVALUATION_BUDGET_EXHAUSTED
                if construction_budget_exhausted
                else TerminationReason.NO_FEASIBLE_SOLUTION_FOUND
            )
            error = NoFeasibleConfigurationError(
                "No complete feasible placement was found within the heuristic "
                f"search budget ({reason.value}); this is not proof that the "
                "placement problem is infeasible."
            )
            error.termination_reason = reason
            error.beam_failures = beam_failures
            error.evaluation_count = controller.total_evaluator_calls
            error.unique_evaluation_count = controller.unique_evaluation_count
            raise error

        if not controller.budget_exhausted and not construction_budget_exhausted:
            (
                incumbent,
                incumbent_evaluation,
                incumbent_relocation,
                incumbent_swap,
            ) = self._local_descent(
                problem,
                controller,
                incumbent,
                incumbent_evaluation,
                restart_count,
                record,
            )

        lower_bound = (
            problem.placement_lower_bound()
            if hasattr(problem, "placement_lower_bound")
            else None
        )
        absolute_gap, relative_gap = certified_bound_gaps(
            incumbent_evaluation.total_cost, lower_bound
        )
        local = (
            incumbent_relocation
            and incumbent_swap
            and not controller.budget_exhausted
            and not construction_budget_exhausted
        )
        if controller.exhaustion_reason is not None:
            reason = controller.exhaustion_reason
        elif construction_budget_exhausted:
            reason = TerminationReason.EVALUATION_BUDGET_EXHAUSTED
        elif no_improvement >= self.maximum_no_improvement:
            reason = TerminationReason.NO_IMPROVEMENT_LIMIT_REACHED
        elif local:
            reason = TerminationReason.LOCAL_SEARCH_COMPLETED
        else:
            reason = TerminationReason.RESTART_LIMIT_REACHED

        configuration = incumbent_evaluation.configuration
        return ScalableNodePlacementOptimizationResult(
            configuration=configuration,
            cost=incumbent_evaluation.total_cost,
            metrics=incumbent_evaluation,
            explored_states=(partial_counter[0] + controller.unique_evaluation_count),
            path=(configuration,),
            method="DIVERSITY_BEAM_VNS_LNS",
            objective_value=incumbent_evaluation.total_cost,
            feasibility_status=FeasibilityStatus.FEASIBLE,
            termination_reason=reason,
            evaluation_count=controller.total_evaluator_calls,
            unique_evaluation_count=controller.unique_evaluation_count,
            simulation_replications=controller.simulation_replications,
            runtime_seconds=controller.runtime_seconds,
            restart_count=restart_count,
            random_seed=self.random_seed,
            budget_exhausted=(
                controller.budget_exhausted or construction_budget_exhausted
            ),
            deletion_local_certified=False,
            relocation_local_certified=(
                incumbent_relocation and not controller.budget_exhausted
            ),
            swap_local_certified=(incumbent_swap and not controller.budget_exhausted),
            global_optimal=False,
            lower_bound=lower_bound,
            absolute_bound_gap=absolute_gap,
            relative_bound_gap=relative_gap,
            trace=tuple(trace),
            selection_reason=self.selection_reason,
            feasible_states=feasible_states,
            complete_states=controller.unique_evaluation_count,
            partial_states=partial_counter[0],
            discarded_partial_states=partial_counter[1],
            total_configurations=problem.configuration_count(),
            solution_mode="SCALABLE",
            optimality_proven=False,
            search_truncated=search_truncated,
            locally_optimal=local,
            beam_width=self.beam_width,
            maximum_complete_evaluations=self.maximum_complete_evaluations,
            time_budget_seconds=self.time_budget_seconds,
            maximum_restarts=self.maximum_restarts,
            lns_neighborhood_sizes=self.lns_neighborhood_sizes,
        )


class ScalableNodePlacementOptimizer(BeamVNSPlacementOptimizer):
    """Backward-compatible public name for the scalable placement heuristic."""

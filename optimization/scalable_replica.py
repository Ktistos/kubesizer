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
    evaluation_feasibility_status,
)
from .result import OptimizationResult


@dataclass(frozen=True)
class ScalableReplicaOptimizationResult(OptimizationResult):
    """Multi-start/VNS result with no global-optimality claim."""

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
    unknown_evaluation_count: int
    infeasible_evaluation_count: int
    solution_mode: str
    selection_reason: str | None
    minimality_proven: bool
    unresolved_lower_cost_states: tuple
    configuration_evaluation_budget: int
    total_evaluator_call_budget: int
    time_budget_seconds: float
    candidate_width: int
    locally_minimal: bool
    maximum_restarts: int
    maximum_vns_neighborhood: int

    @property
    def optimality_proven(self):
        return self.global_optimal


class MultiStartReplicaVNSOptimizer(Optimizer):
    """Seeded randomized greedy construction, deletion descent, and VNS.

    Analytical values rank candidates only. A candidate becomes an incumbent
    exclusively after the problem evaluator certifies it feasible.
    """

    def __init__(
        self,
        *,
        maximum_configuration_evaluations=64,
        maximum_total_evaluator_calls=None,
        maximum_simulation_replications=None,
        time_budget_seconds=300.0,
        candidate_width=8,
        restricted_candidate_fraction=0.10,
        maximum_restarts=4,
        maximum_no_improvement=3,
        maximum_vns_neighborhood=3,
        vns_attempts_per_neighborhood=2,
        random_seed=0,
        removal_order="cost",
        archive_equal_cost=False,
        candidate_ranker=None,
        collect_trace=False,
        cancellation_check=None,
        clock=None,
        selection_reason=None,
    ):
        integer_options = {
            "maximum_configuration_evaluations": maximum_configuration_evaluations,
            "candidate_width": candidate_width,
            "maximum_restarts": maximum_restarts,
            "maximum_no_improvement": maximum_no_improvement,
            "maximum_vns_neighborhood": maximum_vns_neighborhood,
            "vns_attempts_per_neighborhood": vns_attempts_per_neighborhood,
        }
        for name, value in integer_options.items():
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
        if (
            not math.isfinite(restricted_candidate_fraction)
            or not 0 <= restricted_candidate_fraction <= 1
        ):
            raise ValueError(
                "restricted_candidate_fraction must lie between zero and one."
            )
        if removal_order not in {"cost", "random"} and not callable(removal_order):
            raise ValueError("removal_order must be 'cost', 'random', or callable.")
        if isinstance(random_seed, bool) or not isinstance(random_seed, int):
            raise ValueError("random_seed must be an integer.")

        self.maximum_configuration_evaluations = maximum_configuration_evaluations
        self.maximum_total_evaluator_calls = maximum_total_evaluator_calls
        self.maximum_simulation_replications = maximum_simulation_replications
        self.time_budget_seconds = float(time_budget_seconds)
        self.candidate_width = candidate_width
        self.restricted_candidate_fraction = float(restricted_candidate_fraction)
        self.maximum_restarts = maximum_restarts
        self.maximum_no_improvement = maximum_no_improvement
        self.maximum_vns_neighborhood = maximum_vns_neighborhood
        self.vns_attempts_per_neighborhood = vns_attempts_per_neighborhood
        self.random_seed = random_seed
        self.removal_order = removal_order
        self.archive_equal_cost = bool(archive_equal_cost)
        self.candidate_ranker = candidate_ranker
        self.collect_trace = bool(collect_trace)
        self.cancellation_check = cancellation_check
        self._clock = clock
        self.selection_reason = selection_reason

    @staticmethod
    def _stable(problem):
        method = getattr(problem, "stable_configuration", None)
        return tuple(method() if method is not None else problem.initial_state())

    @staticmethod
    def _construction_start(problem, stable):
        method = getattr(problem, "construction_start_configuration", None)
        if method is None:
            return stable
        start = tuple(method())
        if len(start) != len(stable) or any(
            value < minimum for value, minimum in zip(start, stable)
        ):
            raise ValueError(
                "The construction start must be componentwise at or above the "
                "analytical stability vector."
            )
        return start

    @staticmethod
    def _cost(problem, configuration):
        method = getattr(problem, "configuration_cost", None)
        if method is not None:
            value = method(configuration)
        else:
            costs = getattr(
                problem, "replica_costs", lambda: (1,) * len(configuration)
            )()
            value = sum(count * cost for count, cost in zip(configuration, costs))
        value = float(value)
        if not math.isfinite(value) or value < 0:
            raise ValueError(
                "Replica configuration cost must be finite and nonnegative."
            )
        return value

    @classmethod
    def _addition_cost(cls, problem, current, candidate):
        return cls._cost(problem, candidate) - cls._cost(problem, current)

    @staticmethod
    def _structural(problem, configuration):
        callback = getattr(problem, "candidate_is_structurally_feasible", None)
        return callback is None or bool(callback(configuration))

    @staticmethod
    def _status(evaluation):
        return evaluation_feasibility_status(evaluation)

    @staticmethod
    def _slack(evaluation):
        statistical = getattr(evaluation, "configuration_evaluation", None)
        intervals = getattr(statistical, "intervals", {}) or {}
        if intervals:
            return max(value.upper_bound / value.slo for value in intervals.values())
        try:
            value = float(getattr(evaluation, "tie_breaker", math.inf))
        except (TypeError, ValueError):
            return math.inf
        return value if math.isfinite(value) else math.inf

    def _rank(self, problem, current, candidate):
        if self.candidate_ranker is not None:
            value = self.candidate_ranker(problem, current, candidate)
        elif hasattr(problem, "rank_replica_addition"):
            value = problem.rank_replica_addition(current, candidate)
        elif hasattr(problem, "search_tie_breaker"):
            before = float(problem.search_tie_breaker(current))
            after = float(problem.search_tie_breaker(candidate))
            value = (before - after) / self._addition_cost(problem, current, candidate)
        else:
            value = -self._addition_cost(problem, current, candidate)
        value = float(value)
        if math.isnan(value):
            raise ValueError("Replica candidate ranker returned NaN.")
        return value

    def _rcl(self, problem, current, candidates):
        ranked = sorted(
            ((self._rank(problem, current, item), item) for item in candidates),
            key=lambda item: (-item[0], item[1]),
        )
        if not ranked:
            return ()
        width = min(self.candidate_width, len(ranked))
        if self.restricted_candidate_fraction > 0 and math.isfinite(ranked[0][0]):
            allowance = self.restricted_candidate_fraction * max(1.0, abs(ranked[0][0]))
            eligible = [item for item in ranked if item[0] >= ranked[0][0] - allowance]
            width = min(len(eligible), self.candidate_width)
        return tuple(item[1] for item in ranked[:width])

    @staticmethod
    def _raw_evaluate(problem, configuration):
        if hasattr(problem, "evaluate_and_check"):
            return problem.evaluate_and_check(configuration)[0]
        return problem.evaluate(configuration)

    def _evaluate(self, controller, problem, configuration):
        configuration = tuple(configuration)
        refine_method = getattr(problem, "refine_evaluation", None)
        refiner = (
            (lambda previous: refine_method(configuration, previous))
            if refine_method is not None
            else None
        )
        return controller.evaluate(
            configuration,
            lambda: self._raw_evaluate(problem, configuration),
            refiner=refiner,
            refine_unknown=True,
        )[0]

    def _removals(self, problem, current, stable, rng):
        values = []
        for index, (count, minimum) in enumerate(zip(current, stable)):
            if count <= minimum:
                continue
            candidate = list(current)
            candidate[index] -= 1
            candidate = tuple(candidate)
            if not self._structural(problem, candidate):
                continue
            saving = self._cost(problem, current) - self._cost(problem, candidate)
            values.append((saving, index, candidate))
        if callable(self.removal_order):
            return tuple(
                sorted(
                    (item[2] for item in values),
                    key=lambda item: self.removal_order(problem, item),
                )
            )
        if self.removal_order == "random":
            rng.shuffle(values)
        else:
            values.sort(key=lambda item: (-item[0], item[1]))
        return tuple(item[2] for item in values)

    def _delete(
        self,
        controller,
        problem,
        current,
        evaluation,
        stable,
        rng,
        restart,
        record,
    ):
        current = tuple(current)
        while True:
            feasible = []
            unknown = False
            for candidate in self._removals(problem, current, stable, rng):
                try:
                    candidate_evaluation = self._evaluate(
                        controller, problem, candidate
                    )
                except EvaluationBudgetExhausted:
                    return current, evaluation, False, unknown
                status = self._status(candidate_evaluation)
                unknown = unknown or status == FeasibilityStatus.UNKNOWN
                if status == FeasibilityStatus.FEASIBLE:
                    feasible.append((candidate, candidate_evaluation))
            if not feasible:
                return (
                    current,
                    evaluation,
                    not unknown and not controller.budget_exhausted,
                    unknown,
                )
            current, evaluation = min(
                feasible,
                key=lambda item: (
                    self._cost(problem, item[0]),
                    self._slack(item[1]),
                    item[0],
                ),
            )
            record("deletion", restart, current, evaluation, "accepted-removal")

    def _construct(self, controller, problem, stable, rng, restart, record):
        current = stable
        try:
            evaluation = self._evaluate(controller, problem, current)
        except EvaluationBudgetExhausted:
            return None
        record("construction", restart, current, evaluation, "start")
        while self._status(evaluation) != FeasibilityStatus.FEASIBLE:
            candidates = tuple(
                value
                for value in problem.neighbors(current)
                if self._structural(problem, value)
            )
            rcl = self._rcl(problem, current, candidates)
            if not rcl:
                return None
            selected = rng.choice(rcl)
            try:
                evaluation = self._evaluate(controller, problem, selected)
            except EvaluationBudgetExhausted:
                return None
            current = selected
            if self._status(evaluation) == FeasibilityStatus.UNKNOWN:
                alternatives = []
                for alternative in rcl:
                    if alternative == selected:
                        continue
                    try:
                        alternative_evaluation = self._evaluate(
                            controller, problem, alternative
                        )
                    except EvaluationBudgetExhausted:
                        break
                    if (
                        self._status(alternative_evaluation)
                        == FeasibilityStatus.FEASIBLE
                    ):
                        alternatives.append((alternative, alternative_evaluation))
                if alternatives:
                    current, evaluation = min(
                        alternatives,
                        key=lambda item: (
                            self._cost(problem, item[0]),
                            self._slack(item[1]),
                            item[0],
                        ),
                    )
            record("construction", restart, current, evaluation, "accepted-addition")
        return current, evaluation

    def _perturb(self, problem, incumbent, q, rng):
        current = tuple(incumbent)
        for _ in range(q):
            candidates = tuple(
                value
                for value in problem.neighbors(current)
                if self._structural(problem, value)
            )
            rcl = self._rcl(problem, current, candidates)
            if not rcl:
                return None
            current = rcl[0] if rng.random() < 0.5 else rng.choice(rcl)
        return current

    def _vns(
        self,
        controller,
        problem,
        incumbent,
        incumbent_evaluation,
        incumbent_certified,
        stable,
        rng,
        restart,
        record,
    ):
        if not hasattr(problem, "maximum_configuration"):
            return incumbent, incumbent_evaluation, incumbent_certified, False
        certified = incumbent_certified
        unknown_seen = False
        q = 1
        failures = 0
        while (
            q <= self.maximum_vns_neighborhood
            and failures < self.maximum_no_improvement
            and not controller.budget_exhausted
        ):
            improved = False
            for _ in range(self.vns_attempts_per_neighborhood):
                perturbed = self._perturb(problem, incumbent, q, rng)
                if perturbed is None:
                    break
                try:
                    evaluation = self._evaluate(controller, problem, perturbed)
                except EvaluationBudgetExhausted:
                    return incumbent, incumbent_evaluation, False, unknown_seen
                status = self._status(evaluation)
                unknown_seen = unknown_seen or status == FeasibilityStatus.UNKNOWN
                if status != FeasibilityStatus.FEASIBLE:
                    continue
                record("vns", restart, perturbed, evaluation, f"add-{q}")
                candidate, candidate_evaluation, local, unknown = self._delete(
                    controller,
                    problem,
                    perturbed,
                    evaluation,
                    stable,
                    rng,
                    restart,
                    record,
                )
                unknown_seen = unknown_seen or unknown
                candidate_cost = self._cost(problem, candidate)
                incumbent_cost = self._cost(problem, incumbent)
                lower_cost = candidate_cost < incumbent_cost - 1e-15
                equal_better = (
                    self.archive_equal_cost
                    and math.isclose(candidate_cost, incumbent_cost)
                    and candidate != incumbent
                    and self._slack(candidate_evaluation)
                    < self._slack(incumbent_evaluation)
                )
                if lower_cost or equal_better:
                    incumbent = candidate
                    incumbent_evaluation = candidate_evaluation
                    certified = local
                    improved = lower_cost
                    action = (
                        "accepted-improvement"
                        if lower_cost
                        else "accepted-equal-cost-diversification"
                    )
                    record("vns", restart, incumbent, incumbent_evaluation, action)
                    if improved:
                        break
            if improved:
                q = 1
                failures = 0
            else:
                q += 1
                failures += 1
        return incumbent, incumbent_evaluation, certified, unknown_seen

    def optimize(self, problem):
        rng = random.Random(self.random_seed)
        controller = HeuristicEvaluationController(
            maximum_unique_evaluations=self.maximum_configuration_evaluations,
            maximum_total_evaluator_calls=self.maximum_total_evaluator_calls,
            maximum_simulation_replications=(self.maximum_simulation_replications),
            time_budget_seconds=self.time_budget_seconds,
            clock=self._clock,
            cancellation_check=self.cancellation_check,
        )
        stable = self._stable(problem)
        construction_start = self._construction_start(problem, stable)
        if not self._structural(problem, stable):
            raise NoFeasibleConfigurationError(
                "The analytical stability vector violates structural constraints."
            )
        if not self._structural(problem, construction_start):
            construction_start = stable
        lower_bound = self._cost(problem, stable)
        trace = []
        current_journey = []

        def record(phase, restart, configuration, evaluation, action):
            configuration = tuple(configuration)
            if not current_journey or current_journey[-1] != configuration:
                current_journey.append(configuration)
            if self.collect_trace:
                trace.append(
                    HeuristicTraceEntry(
                        phase=phase,
                        restart=restart,
                        configuration=configuration,
                        objective_value=self._cost(problem, configuration),
                        feasibility_status=self._status(evaluation),
                        action=action,
                    )
                )

        incumbent = None
        incumbent_evaluation = None
        incumbent_local = False
        incumbent_path = ()
        restart_count = 0
        no_improvement = 0

        for restart in range(1, self.maximum_restarts + 1):
            if controller.budget_exhausted:
                break
            restart_count = restart
            current_journey.clear()
            constructed = self._construct(
                controller,
                problem,
                construction_start,
                rng,
                restart,
                record,
            )
            if constructed is None:
                if controller.budget_exhausted:
                    break
                no_improvement += 1
                continue
            candidate, evaluation = constructed
            candidate, evaluation, local, _ = self._delete(
                controller,
                problem,
                candidate,
                evaluation,
                stable,
                rng,
                restart,
                record,
            )
            candidate, evaluation, vns_local, _ = self._vns(
                controller,
                problem,
                candidate,
                evaluation,
                local,
                stable,
                rng,
                restart,
                record,
            )
            local = vns_local
            candidate_rank = (
                self._cost(problem, candidate),
                self._slack(evaluation),
                candidate,
            )
            incumbent_rank = (
                (
                    self._cost(problem, incumbent),
                    self._slack(incumbent_evaluation),
                    incumbent,
                )
                if incumbent is not None
                else None
            )
            if incumbent_rank is None or candidate_rank < incumbent_rank:
                incumbent = candidate
                incumbent_evaluation = evaluation
                incumbent_local = local
                incumbent_path = tuple(current_journey)
                no_improvement = 0
                record("multi-start", restart, incumbent, evaluation, "new-incumbent")
            else:
                no_improvement += 1
            if self._cost(problem, incumbent) <= lower_bound + 1e-15:
                break
            if no_improvement >= self.maximum_no_improvement:
                break

        if incumbent is None:
            statuses = tuple(self._status(value) for value in controller.cache.values())
            reason = controller.exhaustion_reason or (
                TerminationReason.STATISTICALLY_UNRESOLVED
                if FeasibilityStatus.UNKNOWN in statuses
                else TerminationReason.NO_FEASIBLE_SOLUTION_FOUND
            )
            error = NoFeasibleConfigurationError(
                "No certified feasible replica configuration was found within "
                f"the heuristic search budget ({reason.value})."
            )
            error.termination_reason = reason
            error.evaluation_count = controller.total_evaluator_calls
            error.unique_evaluation_count = controller.unique_evaluation_count
            error.simulation_replications = controller.simulation_replications
            error.random_seed = self.random_seed
            raise error

        objective = self._cost(problem, incumbent)
        absolute_gap, relative_gap = certified_bound_gaps(objective, lower_bound)
        if controller.exhaustion_reason is not None:
            reason = controller.exhaustion_reason
        elif objective <= lower_bound + 1e-15:
            reason = TerminationReason.LOWER_BOUND_MATCHED
        elif no_improvement >= self.maximum_no_improvement:
            reason = TerminationReason.NO_IMPROVEMENT_LIMIT_REACHED
        elif restart_count >= self.maximum_restarts:
            reason = TerminationReason.RESTART_LIMIT_REACHED
        else:
            reason = TerminationReason.LOCAL_SEARCH_COMPLETED

        statuses = tuple(self._status(value) for value in controller.cache.values())
        unresolved_lower = tuple(
            configuration
            for configuration, evaluation in controller.cache.items()
            if self._cost(problem, configuration) < objective
            and self._status(evaluation) == FeasibilityStatus.UNKNOWN
        )
        local_certified = incumbent_local and not controller.budget_exhausted
        return ScalableReplicaOptimizationResult(
            configuration=incumbent,
            cost=objective - lower_bound,
            metrics=incumbent_evaluation,
            explored_states=controller.unique_evaluation_count,
            path=incumbent_path,
            method="MULTI_START_RANDOMIZED_GREEDY_VNS",
            objective_value=objective,
            feasibility_status=FeasibilityStatus.FEASIBLE,
            termination_reason=reason,
            evaluation_count=controller.total_evaluator_calls,
            unique_evaluation_count=controller.unique_evaluation_count,
            simulation_replications=controller.simulation_replications,
            runtime_seconds=controller.runtime_seconds,
            restart_count=restart_count,
            random_seed=self.random_seed,
            budget_exhausted=controller.budget_exhausted,
            deletion_local_certified=local_certified,
            relocation_local_certified=False,
            swap_local_certified=False,
            global_optimal=False,
            lower_bound=lower_bound,
            absolute_bound_gap=absolute_gap,
            relative_bound_gap=relative_gap,
            trace=tuple(trace),
            unknown_evaluation_count=statuses.count(FeasibilityStatus.UNKNOWN),
            infeasible_evaluation_count=statuses.count(FeasibilityStatus.INFEASIBLE),
            solution_mode="SCALABLE",
            selection_reason=self.selection_reason,
            minimality_proven=False,
            unresolved_lower_cost_states=unresolved_lower,
            configuration_evaluation_budget=self.maximum_configuration_evaluations,
            total_evaluator_call_budget=(
                self.maximum_total_evaluator_calls
                or self.maximum_configuration_evaluations
            ),
            time_budget_seconds=self.time_budget_seconds,
            candidate_width=self.candidate_width,
            locally_minimal=local_certified,
            maximum_restarts=self.maximum_restarts,
            maximum_vns_neighborhood=self.maximum_vns_neighborhood,
        )


class ScalableReplicaOptimizer(MultiStartReplicaVNSOptimizer):
    """Backward-compatible public name for the scalable replica heuristic."""

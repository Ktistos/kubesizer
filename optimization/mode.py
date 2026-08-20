import math
from dataclasses import dataclass
from enum import Enum


class OptimizationMode(str, Enum):
    AUTO = "AUTO"
    EXACT = "EXACT"
    SCALABLE = "SCALABLE"
    # User-facing compatibility name. It is an alias for the scalable
    # heuristic; it does not imply a certified approximation ratio.
    APPROXIMATE = "SCALABLE"


class WorkflowOptimizationMode(str, Enum):
    AUTO = "AUTO"
    EXACT = "EXACT"
    SCALABLE = "SCALABLE"
    HYBRID = "HYBRID"


@dataclass(frozen=True)
class OptimizationModes:
    """Independent search-mode policy for replica sizing and placement.

    Each optimizer still receives only a mode it can execute.  A workflow is
    described as HYBRID when the two modes differ, whether they were selected
    explicitly or resolved independently from AUTO.
    """

    mode: WorkflowOptimizationMode = WorkflowOptimizationMode.AUTO
    replica: OptimizationMode | None = None
    placement: OptimizationMode | None = None

    def __post_init__(self):
        workflow_mode = normalize_workflow_optimization_mode(self.mode)
        object.__setattr__(self, "mode", workflow_mode)
        if workflow_mode == WorkflowOptimizationMode.HYBRID:
            if self.replica is None or self.placement is None:
                raise ValueError(
                    "HYBRID mode requires explicit replica and placement modes."
                )
            replica = normalize_optimization_mode(self.replica)
            placement = normalize_optimization_mode(self.placement)
            if OptimizationMode.AUTO in {replica, placement}:
                raise ValueError(
                    "HYBRID replica and placement modes must be EXACT or SCALABLE."
                )
            if replica == placement:
                raise ValueError(
                    "HYBRID mode requires replica and placement modes to differ."
                )
        else:
            if self.replica is not None or self.placement is not None:
                raise ValueError(
                    "Per-optimizer modes may only be specified in HYBRID mode."
                )
            replica = OptimizationMode(workflow_mode.value)
            placement = replica
        object.__setattr__(self, "replica", replica)
        object.__setattr__(self, "placement", placement)

    @classmethod
    def uniform(cls, mode):
        return cls(mode=normalize_workflow_optimization_mode(mode))

    @property
    def workflow_mode(self):
        return self.mode.value


class ReplicaEvaluationMode(str, Enum):
    """Choose deterministic analytical means or statistical p95 simulation."""

    ANALYTICAL = "ANALYTICAL"
    SIMULATION = "SIMULATION"


def normalize_optimization_mode(value):
    if isinstance(value, OptimizationMode):
        return value
    if str(value).upper() == "APPROXIMATE":
        return OptimizationMode.SCALABLE
    try:
        return OptimizationMode(str(value).upper())
    except ValueError as error:
        choices = "AUTO, EXACT, SCALABLE (or APPROXIMATE)"
        raise ValueError(f"optimization mode must be one of: {choices}.") from error


def normalize_workflow_optimization_mode(value):
    if isinstance(value, WorkflowOptimizationMode):
        return value
    if isinstance(value, OptimizationMode):
        return WorkflowOptimizationMode(value.value)
    if str(value).upper() == "APPROXIMATE":
        return WorkflowOptimizationMode.SCALABLE
    try:
        return WorkflowOptimizationMode(str(value).upper())
    except ValueError as error:
        choices = "AUTO, EXACT, SCALABLE, HYBRID (or APPROXIMATE)"
        raise ValueError(
            f"workflow optimization mode must be one of: {choices}."
        ) from error


def optimization_workflow_mode(replica_mode, placement_mode):
    """Describe the combined result of two independently selected modes."""
    replica_mode = normalize_optimization_mode(replica_mode)
    placement_mode = normalize_optimization_mode(placement_mode)
    if replica_mode == placement_mode:
        return replica_mode.value
    return "HYBRID"


def normalize_replica_evaluation_mode(value):
    if isinstance(value, ReplicaEvaluationMode):
        return value
    try:
        return ReplicaEvaluationMode(str(value).upper())
    except ValueError as error:
        choices = ", ".join(item.value for item in ReplicaEvaluationMode)
        raise ValueError(
            f"replica evaluation mode must be one of: {choices}."
        ) from error


def _positive_integer(name, value, *, allow_none=False):
    if allow_none and value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer.")


def _positive_duration(name, value):
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive.")


@dataclass(frozen=True)
class ScalableReplicaSearchOptions:
    maximum_configuration_evaluations: int = 64
    maximum_total_evaluator_calls: int | None = None
    maximum_simulation_replications: int | None = None
    time_budget_seconds: float = 300.0
    candidate_width: int = 8
    restricted_candidate_fraction: float = 0.10
    maximum_restarts: int = 4
    maximum_no_improvement: int = 3
    maximum_vns_neighborhood: int = 3
    vns_attempts_per_neighborhood: int = 2
    random_seed: int = 0
    removal_order: str = "cost"
    archive_equal_cost: bool = False
    collect_trace: bool = False

    def __post_init__(self):
        for name in (
            "maximum_configuration_evaluations",
            "candidate_width",
            "maximum_restarts",
            "maximum_no_improvement",
            "maximum_vns_neighborhood",
            "vns_attempts_per_neighborhood",
        ):
            _positive_integer(name, getattr(self, name))
        _positive_integer(
            "maximum_total_evaluator_calls",
            self.maximum_total_evaluator_calls,
            allow_none=True,
        )
        _positive_integer(
            "maximum_simulation_replications",
            self.maximum_simulation_replications,
            allow_none=True,
        )
        _positive_duration("time_budget_seconds", self.time_budget_seconds)
        if not 0 <= self.restricted_candidate_fraction <= 1:
            raise ValueError(
                "restricted_candidate_fraction must lie between zero and one."
            )
        if self.maximum_vns_neighborhood > 3:
            raise ValueError(
                "maximum_vns_neighborhood may be at most three by default."
            )
        if self.removal_order not in {"cost", "random"}:
            raise ValueError("removal_order must be 'cost' or 'random'.")
        if isinstance(self.random_seed, bool) or not isinstance(self.random_seed, int):
            raise ValueError("random_seed must be an integer.")


@dataclass(frozen=True)
class ScalablePlacementSearchOptions:
    beam_width: int = 32
    diversity_per_signature: int = 2
    candidate_distributions_per_service: int = 64
    maximum_partial_states: int = 100_000
    maximum_complete_evaluations: int = 10_000
    maximum_total_evaluator_calls: int | None = None
    maximum_simulation_replications: int | None = None
    maximum_local_iterations: int = 100
    time_budget_seconds: float = 30.0
    maximum_restarts: int = 4
    maximum_no_improvement: int = 3
    random_seed: int = 0
    lns_neighborhood_sizes: tuple[int, ...] = (3, 4, 5)
    lns_subset_attempts: int = 3
    lns_subproblem_budget: int = 1_000
    collect_trace: bool = False

    def __post_init__(self):
        for name in (
            "beam_width",
            "diversity_per_signature",
            "candidate_distributions_per_service",
            "maximum_partial_states",
            "maximum_complete_evaluations",
            "maximum_local_iterations",
            "maximum_restarts",
            "maximum_no_improvement",
            "lns_subset_attempts",
            "lns_subproblem_budget",
        ):
            _positive_integer(name, getattr(self, name))
        _positive_integer(
            "maximum_total_evaluator_calls",
            self.maximum_total_evaluator_calls,
            allow_none=True,
        )
        _positive_integer(
            "maximum_simulation_replications",
            self.maximum_simulation_replications,
            allow_none=True,
        )
        _positive_duration("time_budget_seconds", self.time_budget_seconds)
        if not self.lns_neighborhood_sizes or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in self.lns_neighborhood_sizes
        ):
            raise ValueError("lns_neighborhood_sizes must contain positive integers.")
        if isinstance(self.random_seed, bool) or not isinstance(self.random_seed, int):
            raise ValueError("random_seed must be an integer.")


# Exact replica search can invoke statistically controlled replications for
# every state. AUTO therefore uses a deliberately conservative hard ceiling.
DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT = 10_000
DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT = 1_000_000

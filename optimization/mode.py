from dataclasses import dataclass
from enum import Enum


class OptimizationMode(str, Enum):
    AUTO = "AUTO"
    EXACT = "EXACT"
    SCALABLE = "SCALABLE"


def normalize_optimization_mode(value):
    if isinstance(value, OptimizationMode):
        return value
    try:
        return OptimizationMode(str(value).upper())
    except ValueError as error:
        choices = ", ".join(item.value for item in OptimizationMode)
        raise ValueError(f"optimization mode must be one of: {choices}.") from error


@dataclass(frozen=True)
class ScalableReplicaSearchOptions:
    maximum_configuration_evaluations: int = 64
    time_budget_seconds: float = 300.0
    candidate_width: int = 8

    def __post_init__(self):
        if (
            isinstance(self.maximum_configuration_evaluations, bool)
            or not isinstance(self.maximum_configuration_evaluations, int)
            or self.maximum_configuration_evaluations <= 0
        ):
            raise ValueError("maximum_configuration_evaluations must be positive.")
        if self.time_budget_seconds <= 0:
            raise ValueError("time_budget_seconds must be positive.")
        if (
            isinstance(self.candidate_width, bool)
            or not isinstance(self.candidate_width, int)
            or self.candidate_width <= 0
        ):
            raise ValueError("candidate_width must be positive.")


@dataclass(frozen=True)
class ScalablePlacementSearchOptions:
    beam_width: int = 32
    candidate_distributions_per_service: int = 64
    maximum_partial_states: int = 100_000
    maximum_complete_evaluations: int = 10_000
    maximum_local_iterations: int = 100
    time_budget_seconds: float = 30.0

    def __post_init__(self):
        integer_options = {
            "beam_width": self.beam_width,
            "candidate_distributions_per_service": (
                self.candidate_distributions_per_service
            ),
            "maximum_partial_states": self.maximum_partial_states,
            "maximum_complete_evaluations": self.maximum_complete_evaluations,
            "maximum_local_iterations": self.maximum_local_iterations,
        }
        for name, value in integer_options.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise ValueError(f"{name} must be positive.")
        if self.time_budget_seconds <= 0:
            raise ValueError("time_budget_seconds must be positive.")


# Exact replica search is valuable for a proof, but every visited state can run
# dozens of statistically controlled native replications. Keep AUTO conservative.
DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT = 10_000
DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT = 1_000_000

"""Exact replica optimization over analytical mean-latency evidence."""

from .base import GraphSearchProblem, OptimizationProblem, Optimizer
from .exceptions import NoFeasibleConfigurationError
from .feasibility import FeasibilityStatus, TerminationReason
from .replica_sizing import (
    AnalyticalClassification,
    AnalyticalReplicaSizingEvaluation,
    AnalyticalReplicaSizingProblem,
)
from .result import OptimizationResult
from .selection import optimize_replica_configuration
from .uniform_cost import ReplicaOptimizationResult, UniformCostReplicaOptimizer

__all__ = [
    "AnalyticalClassification",
    "AnalyticalReplicaSizingEvaluation",
    "AnalyticalReplicaSizingProblem",
    "FeasibilityStatus",
    "GraphSearchProblem",
    "NoFeasibleConfigurationError",
    "OptimizationProblem",
    "OptimizationResult",
    "Optimizer",
    "ReplicaOptimizationResult",
    "TerminationReason",
    "UniformCostReplicaOptimizer",
    "optimize_replica_configuration",
]

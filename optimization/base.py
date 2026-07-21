from abc import ABC, abstractmethod

from .result import OptimizationResult


class Optimizer(ABC):
    """Search algorithm independent of any domain-specific model."""

    @abstractmethod
    def optimize(self, problem) -> OptimizationResult:
        """Find and return an optimal feasible configuration."""


class OptimizationProblem(ABC):
    """Minimal contract shared by optimization problem definitions."""

    @abstractmethod
    def evaluate(self, configuration):
        """Return the metrics associated with a configuration."""

    @abstractmethod
    def is_feasible(self, configuration):
        """Return whether a configuration satisfies every constraint."""


class GraphSearchProblem(OptimizationProblem):
    """Optimization problem that exposes a graph-search state space."""

    @abstractmethod
    def initial_state(self):
        """Return the state from which graph search starts."""

    @abstractmethod
    def neighbors(self, state):
        """Return states reachable in one search step."""

    @abstractmethod
    def step_cost(self, current, neighbor):
        """Return the actual incremental cost of a state transition."""

    @abstractmethod
    def heuristic(self, state):
        """Return a lower bound on remaining path cost."""

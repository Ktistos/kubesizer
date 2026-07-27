from collections.abc import Mapping
from dataclasses import dataclass

from .base import OptimizationProblem


@dataclass(frozen=True)
class NodePlacementEvaluation:
    """Validation result for a replica-to-node placement."""

    configuration: tuple[tuple[str, str], ...]
    missing_replicas: tuple[str, ...]
    unknown_replicas: tuple[str, ...]
    unknown_nodes: tuple[str, ...]
    used_node_count: int

    @property
    def feasible(self):
        return not (
            self.missing_replicas
            or self.unknown_replicas
            or self.unknown_nodes
        )


class NodePlacementProblem(OptimizationProblem):
    """Sample problem that assigns each replica identifier to one node.

    A configuration is a mapping such as {"api-0": "node-a"}. This sample
    validates that every required replica is assigned to a known node and
    reports the number of nodes used as a simple packing cost. Resource
    requests, affinity rules, and network latency can be added as constraints
    when those inputs are modeled.
    """

    def __init__(self, cluster):
        self.__cluster = cluster
        
    def evaluate(self, configuration):
        """Return validation details and the simple node-packing cost."""
        pass

    def is_feasible(self, configuration):
        """Return whether every required replica has a valid node assignment."""
        return self.evaluate(configuration).feasible

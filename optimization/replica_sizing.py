import math
from dataclasses import dataclass

from metrics import Metrics

from .base import GraphSearchProblem
from .exceptions import RequestClassMetricsUnavailableError


def zero_heuristic(configuration):
    """Admissible heuristic that reduces A* to uniform-cost search."""
    return 0


class AnalyticalStabilityHeuristic:
    """Lower bound on replicas still needed for strict analytical stability."""

    def __init__(self, arrival_rates, service_rates):
        if len(arrival_rates) != len(service_rates):
            raise ValueError("Arrival and service rate vectors must have equal length.")
        if any(service_rate <= 0 for service_rate in service_rates):
            raise ValueError("Service rates must be greater than zero.")

        self.__required_replicas = tuple(
            math.floor(arrival_rate / service_rate) + 1
            for arrival_rate, service_rate in zip(arrival_rates, service_rates)
        )

    @classmethod
    def from_system(cls, system):
        services = tuple(system.get_services().values())
        service_rates = tuple(service.get_mu() for service in services)
        return cls(system.get_effective_arrival_rates(), service_rates)

    def __call__(self, configuration):
        if len(configuration) != len(self.__required_replicas):
            raise ValueError("Configuration size does not match the model.")
        return sum(
            max(0, required - replicas)
            for required, replicas in zip(self.__required_replicas, configuration)
        )


@dataclass(frozen=True)
class ReplicaSizingEvaluation:
    """Constraint results and metrics for one replica configuration."""

    configuration: tuple[int, ...]
    feasible: bool
    cost: float
    tie_breaker: float
    analytical_metrics: Metrics
    simulation_metrics: Metrics | None

    def ranking_key(self):
        """Return the lexicographic feasibility, cost, and latency ordering."""
        return (
            0 if self.feasible else 1,
            self.cost,
            self.tie_breaker,
        )


class ReplicaSizingProblem(GraphSearchProblem):
    """Bounded replica-sizing problem with cached model evaluations.

    End-to-end p95 values must be supplied through
    ``Metrics.get_request_class_p95_latencies``. Current per-service p95 values
    are intentionally not substituted for request-class measurements.
    """

    def __init__(
        self,
        system,
        simulation_duration,
        request_class_slos,
        max_replicas,
        heuristic=None,
    ):
        self.__system = system
        self.__simulation_duration = simulation_duration
        self.__request_class_slos = dict(request_class_slos)
        self.__service_count = len(system.get_services())
        self.__initial_state = tuple(1 for _ in range(self.__service_count))
        self.__max_replicas = self.__normalize_max_replicas(max_replicas)
        self.__heuristic = (
            heuristic
            if heuristic is not None
            else AnalyticalStabilityHeuristic.from_system(system)
        )

        if simulation_duration <= 0:
            raise ValueError("simulation_duration must be greater than zero.")
        if any(slo <= 0 for slo in self.__request_class_slos.values()):
            raise ValueError("Request-class SLOs must be greater than zero.")

        self.__analytical_cache = {}
        self.__simulation_cache = {}
        self.__evaluation_cache = {}

    def __normalize_max_replicas(self, max_replicas):
        if isinstance(max_replicas, int):
            normalized = tuple(max_replicas for _ in range(self.__service_count))
        else:
            normalized = tuple(max_replicas)

        if len(normalized) != self.__service_count:
            raise ValueError("max_replicas must provide one bound per service.")
        if any(limit < 1 for limit in normalized):
            raise ValueError("Every maximum replica bound must be at least one.")
        return normalized

    def __normalize_configuration(self, configuration):
        configuration = tuple(configuration)
        if len(configuration) != self.__service_count:
            raise ValueError("Configuration size does not match the system.")
        if any(not isinstance(value, int) or value < 1 for value in configuration):
            raise ValueError("Replica counts must be positive integers.")
        if any(
            value > limit
            for value, limit in zip(configuration, self.__max_replicas)
        ):
            raise ValueError("Configuration exceeds the replica search bounds.")
        return configuration

    def initial_state(self):
        return self.__initial_state

    def neighbors(self, state):
        state = self.__normalize_configuration(state)
        neighbors = []
        for service_id, replicas in enumerate(state):
            if replicas >= self.__max_replicas[service_id]:
                continue
            neighbor = list(state)
            neighbor[service_id] += 1
            neighbors.append(tuple(neighbor))
        return tuple(neighbors)

    def step_cost(self, current, neighbor):
        current = self.__normalize_configuration(current)
        neighbor = self.__normalize_configuration(neighbor)
        differences = tuple(
            neighbor_value - current_value
            for current_value, neighbor_value in zip(current, neighbor)
        )
        if differences.count(1) != 1 or any(
            value not in (0, 1) for value in differences
        ):
            raise ValueError("A replica-sizing step must add exactly one replica.")
        return 1

    def heuristic(self, state):
        state = self.__normalize_configuration(state)
        return self.__heuristic(state)

    def evaluate(self, configuration):
        configuration = self.__normalize_configuration(configuration)
        if configuration in self.__evaluation_cache:
            return self.__evaluation_cache[configuration]

        self.__system.configure_system_replicas(configuration)
        analytical_metrics = self.__analytical_cache.get(configuration)
        if analytical_metrics is None:
            analytical_metrics = self.__system.solve()
            self.__analytical_cache[configuration] = analytical_metrics

        if not analytical_metrics.get_stability():
            evaluation = ReplicaSizingEvaluation(
                configuration=configuration,
                feasible=False,
                cost=math.inf,
                tie_breaker=math.inf,
                analytical_metrics=analytical_metrics,
                simulation_metrics=None,
            )
            self.__evaluation_cache[configuration] = evaluation
            return evaluation

        simulation_metrics = self.__simulation_cache.get(configuration)
        if simulation_metrics is None:
            simulation_metrics = self.__system.simulate(
                self.__simulation_duration,
                analytical_metrics=analytical_metrics,
            )
            self.__simulation_cache[configuration] = simulation_metrics

        request_class_p95 = simulation_metrics.get_request_class_p95_latencies()
        missing_classes = self.__request_class_slos.keys() - request_class_p95.keys()
        if missing_classes:
            missing = ", ".join(sorted(missing_classes))
            raise RequestClassMetricsUnavailableError(
                "Simulation did not provide end-to-end p95 latency for request "
                f"classes: {missing}."
            )

        feasible = all(
            request_class_p95[request_class] <= slo
            for request_class, slo in self.__request_class_slos.items()
        )
        cost = sum(configuration) if feasible else math.inf
        tie_breaker = (
            max(
                request_class_p95[request_class] / slo
                for request_class, slo in self.__request_class_slos.items()
            )
            if feasible and self.__request_class_slos
            else (0.0 if feasible else math.inf)
        )

        evaluation = ReplicaSizingEvaluation(
            configuration=configuration,
            feasible=feasible,
            cost=cost,
            tie_breaker=tie_breaker,
            analytical_metrics=analytical_metrics,
            simulation_metrics=simulation_metrics,
        )
        self.__evaluation_cache[configuration] = evaluation
        return evaluation

    def is_feasible(self, configuration):
        return self.evaluate(configuration).feasible

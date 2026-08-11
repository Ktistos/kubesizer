import math
from dataclasses import dataclass

from metrics import (
    Metrics,
    ReliabilityStatus,
    RequestClassP95Reliability,
    evaluate_request_class_p95_reliability,
)

from .base import GraphSearchProblem
from .exceptions import (
    RequestClassMetricsUnavailableError,
    StatisticalEvaluationError,
)
from .simulation_evaluation import (
    CalibratedConfigurationEvaluator,
    ConfigurationEvaluation,
    SimulationClassification,
)


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
    reliability: RequestClassP95Reliability | None = None
    classification: SimulationClassification | None = None
    configuration_evaluation: ConfigurationEvaluation | None = None

    @property
    def reliability_status(self):
        if self.classification == SimulationClassification.STATISTICALLY_FEASIBLE:
            return ReliabilityStatus.RELIABLY_FEASIBLE
        if self.classification in (
            SimulationClassification.INCONCLUSIVE,
            SimulationClassification.CONTINUE_SAMPLING,
        ):
            return ReliabilityStatus.UNCERTAIN
        if self.reliability is None:
            return ReliabilityStatus.RELIABLY_INFEASIBLE
        return self.reliability.status

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
    are intentionally not substituted for request-class measurements. When a
    warm-up is provided, the system continues without resetting at its end;
    ``simulation_duration`` defines the measurement-arrival window. Each
    configuration uses a fixed batch of independent native replications and is
    feasible only when every simultaneous p95 upper confidence bound passes.
    """

    def __init__(
        self,
        system,
        simulation_duration,
        request_class_slos,
        max_replicas,
        heuristic=None,
        warmup_duration=0.0,
        initial_configuration=None,
        replication_count=30,
        simulation_workers=1,
        evaluation_seed_start=300_000,
        confidence_level=0.95,
    ):
        self.__system = system
        self.__measurement_duration = float(simulation_duration)
        self.__warmup_duration = float(warmup_duration)
        self.__request_class_slos = dict(request_class_slos)
        self.__replication_count = replication_count
        self.__simulation_workers = simulation_workers
        self.__evaluation_seed_start = evaluation_seed_start
        self.__confidence_level = float(confidence_level)
        self.__service_count = len(system.get_services())
        self.__max_replicas = self.__normalize_max_replicas(max_replicas)
        self.__initial_state = self.__normalize_configuration(
            tuple(1 for _ in range(self.__service_count))
            if initial_configuration is None
            else initial_configuration
        )
        self.__heuristic = (
            heuristic
            if heuristic is not None
            else AnalyticalStabilityHeuristic.from_system(system)
        )

        if (
            not math.isfinite(self.__measurement_duration)
            or self.__measurement_duration <= 0
        ):
            raise ValueError("simulation_duration must be greater than zero.")
        if not math.isfinite(self.__warmup_duration) or self.__warmup_duration < 0:
            raise ValueError("warmup_duration must be finite and non-negative.")
        if any(slo <= 0 for slo in self.__request_class_slos.values()):
            raise ValueError("Request-class SLOs must be greater than zero.")
        if (
            isinstance(self.__replication_count, bool)
            or not isinstance(self.__replication_count, int)
            or self.__replication_count < 2
        ):
            raise ValueError("replication_count must be an integer of at least two.")
        if (
            isinstance(self.__simulation_workers, bool)
            or not isinstance(self.__simulation_workers, int)
            or self.__simulation_workers <= 0
        ):
            raise ValueError("simulation_workers must be a positive integer.")
        if (
            isinstance(self.__evaluation_seed_start, bool)
            or not isinstance(self.__evaluation_seed_start, int)
            or self.__evaluation_seed_start < 0
        ):
            raise ValueError("evaluation_seed_start must be non-negative.")
        if not 0.0 < self.__confidence_level < 1.0:
            raise ValueError("confidence_level must lie strictly between zero and one.")
        configuration_count = math.prod(self.__max_replicas)
        largest_seed = (
            self.__evaluation_seed_start
            + configuration_count * self.__replication_count
            - 1
        )
        if largest_seed > 2**63 - 1:
            raise ValueError("Evaluation seed range exceeds signed 64-bit integers.")

        self.__analytical_cache = {}
        self.__replication_cache = {}
        self.__simulation_cache = {}
        self.__reliability_cache = {}
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

    def evaluation_seeds(self, configuration):
        """Return the deterministic disjoint seed block for a configuration."""
        configuration = self.__normalize_configuration(configuration)
        configuration_index = 0
        for replicas, radix in zip(configuration, self.__max_replicas):
            configuration_index = configuration_index * radix + (replicas - 1)
        first_seed = (
            self.__evaluation_seed_start
            + configuration_index * self.__replication_count
        )
        return tuple(
            first_seed + offset for offset in range(self.__replication_count)
        )

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
        reliability = self.__reliability_cache.get(configuration)
        if simulation_metrics is None or reliability is None:
            replications = self.__replication_cache.get(configuration)
            if replications is None:
                replications = self.__system.simulate_measurement_replications(
                    warmup_duration=self.__warmup_duration,
                    measurement_duration=self.__measurement_duration,
                    seeds=self.evaluation_seeds(configuration),
                    workers=self.__simulation_workers,
                )
                self.__replication_cache[configuration] = tuple(replications)
                replications = self.__replication_cache[configuration]
            missing_classes = {
                request_class
                for request_class in self.__request_class_slos
                if any(
                    request_class not in replication.request_class_p95_latencies
                    for replication in replications
                )
            }
            if missing_classes:
                missing = ", ".join(sorted(missing_classes))
                raise RequestClassMetricsUnavailableError(
                    "Simulation did not provide end-to-end p95 latency for "
                    f"request classes in every replication: {missing}."
                )
            try:
                reliability = evaluate_request_class_p95_reliability(
                    replications,
                    self.__request_class_slos,
                    confidence_level=self.__confidence_level,
                    adjust_for_multiple_classes=True,
                )
            except ValueError as error:
                if "measured no requests" in str(error) or "has no p95" in str(error):
                    raise RequestClassMetricsUnavailableError(str(error)) from error
                raise
            request_class_mean_p95 = {
                request_class: interval.mean_p95
                for request_class, interval
                in reliability.request_class_intervals.items()
            }
            simulation_metrics = Metrics(
                (),
                (),
                True,
                request_class_p95_latencies=request_class_mean_p95,
            )
            self.__simulation_cache[configuration] = simulation_metrics
            self.__reliability_cache[configuration] = reliability

        feasible = reliability.status == ReliabilityStatus.RELIABLY_FEASIBLE
        cost = sum(configuration) if feasible else math.inf
        tie_breaker = (
            max(
                interval.upper_bound / interval.slo
                for interval in reliability.request_class_intervals.values()
            )
            if feasible and reliability.request_class_intervals
            else (0.0 if feasible else math.inf)
        )

        evaluation = ReplicaSizingEvaluation(
            configuration=configuration,
            feasible=feasible,
            cost=cost,
            tie_breaker=tie_breaker,
            analytical_metrics=analytical_metrics,
            simulation_metrics=simulation_metrics,
            reliability=reliability,
        )
        self.__evaluation_cache[configuration] = evaluation
        return evaluation

    def is_feasible(self, configuration):
        return self.evaluate(configuration).feasible


class CalibratedReplicaSizingProblem(GraphSearchProblem):
    """Bounded graph search driven exclusively by calibrated simulation results."""

    def __init__(
        self,
        system,
        calibration_profile,
        *,
        heuristic=None,
        simulation_workers=1,
        evaluator=None,
    ):
        self.__system = system
        self.__profile = calibration_profile
        self.__service_count = len(system.get_services())
        self.__maximum = tuple(calibration_profile.maximum_replica_configuration)
        self.__initial = tuple(calibration_profile.minimum_replica_configuration)
        if len(self.__maximum) != self.__service_count:
            raise ValueError("Calibration profile does not match service count.")
        self.__heuristic = (
            heuristic
            if heuristic is not None
            else AnalyticalStabilityHeuristic.from_system(system)
        )
        self.__evaluator = evaluator or CalibratedConfigurationEvaluator(
            system,
            calibration_profile,
            workers=simulation_workers,
        )
        self.__evaluation_history = {}

    @property
    def configuration_evaluations(self):
        """Return run evidence; entries are never reused as simulation cache."""
        return dict(self.__evaluation_history)

    def __normalize(self, configuration):
        configuration = tuple(configuration)
        if (
            len(configuration) != self.__service_count
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
                for value in configuration
            )
            or any(
                value > maximum
                for value, maximum in zip(configuration, self.__maximum)
            )
        ):
            raise ValueError("Configuration is outside calibrated replica bounds.")
        return configuration

    def initial_state(self):
        return self.__initial

    def neighbors(self, state):
        state = self.__normalize(state)
        values = []
        for service_id, replicas in enumerate(state):
            if replicas >= self.__maximum[service_id]:
                continue
            neighbor = list(state)
            neighbor[service_id] += 1
            values.append(tuple(neighbor))
        return tuple(values)

    def step_cost(self, current, neighbor):
        current = self.__normalize(current)
        neighbor = self.__normalize(neighbor)
        differences = tuple(
            right - left for left, right in zip(current, neighbor)
        )
        if differences.count(1) != 1 or any(value not in (0, 1) for value in differences):
            raise ValueError("A replica-sizing step must add exactly one replica.")
        return 1

    def heuristic(self, state):
        return self.__heuristic(self.__normalize(state))

    def evaluate(self, configuration):
        configuration = self.__normalize(configuration)
        statistical = self.__evaluator.evaluate(configuration)
        self.__system.configure_system_replicas(configuration)
        analytical = self.__system.solve()
        intervals = statistical.intervals
        simulation_metrics = (
            Metrics(
                (),
                (),
                analytical.get_stability(),
                request_class_p95_latencies={
                    request_class: interval.p95_estimate
                    for request_class, interval in intervals.items()
                },
            )
            if intervals
            else None
        )
        feasible = (
            statistical.classification
            == SimulationClassification.STATISTICALLY_FEASIBLE
        )
        evaluation = ReplicaSizingEvaluation(
            configuration=configuration,
            feasible=feasible,
            cost=sum(configuration) if feasible else math.inf,
            tie_breaker=(
                max(
                    interval.upper_bound / interval.slo
                    for interval in intervals.values()
                )
                if feasible and intervals
                else math.inf
            ),
            analytical_metrics=analytical,
            simulation_metrics=simulation_metrics,
            classification=statistical.classification,
            configuration_evaluation=statistical,
        )
        self.__evaluation_history[configuration] = evaluation
        if statistical.classification in (
            SimulationClassification.SIMULATION_FAILURE,
            SimulationClassification.CALIBRATION_FAILURE,
        ):
            raise StatisticalEvaluationError(
                statistical.failure_reason
                or f"Configuration evaluation failed: {statistical.classification.value}",
                evaluation,
            )
        return evaluation

    def evaluate_and_check(self, configuration):
        evaluation = self.evaluate(configuration)
        return evaluation, evaluation.feasible

    def is_feasible(self, configuration):
        return self.evaluate(configuration).feasible

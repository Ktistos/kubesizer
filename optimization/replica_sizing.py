import math
from dataclasses import dataclass
from enum import Enum

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

    def __init__(self, arrival_rates, service_rates, replica_costs=None):
        if len(arrival_rates) != len(service_rates):
            raise ValueError("Arrival and service rate vectors must have equal length.")
        if any(service_rate <= 0 for service_rate in service_rates):
            raise ValueError("Service rates must be greater than zero.")

        required_replicas = tuple(
            math.floor(arrival_rate / service_rate) + 1
            for arrival_rate, service_rate in zip(arrival_rates, service_rates)
        )
        self.__initialize(required_replicas, replica_costs)

    def __initialize(self, required_replicas, replica_costs):
        if not required_replicas or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in required_replicas
        ):
            raise ValueError("Required replicas must be positive integers.")
        self.__required_replicas = tuple(required_replicas)
        self.__replica_costs = (
            (1.0,) * len(self.__required_replicas)
            if replica_costs is None
            else tuple(float(value) for value in replica_costs)
        )
        if len(self.__replica_costs) != len(self.__required_replicas) or any(
            not math.isfinite(value) or value <= 0 for value in self.__replica_costs
        ):
            raise ValueError(
                "replica_costs must contain one finite positive value per service."
            )

    @classmethod
    def from_required_replicas(cls, required_replicas, replica_costs=None):
        """Build the lower bound from an already solved robust requirement."""
        instance = cls.__new__(cls)
        instance.__initialize(tuple(required_replicas), replica_costs)
        return instance

    @classmethod
    def from_system(cls, system, replica_costs=None):
        stable_method = getattr(system, "get_minimum_stable_replicas", None)
        if stable_method is not None:
            return cls.from_required_replicas(
                stable_method(),
                replica_costs=replica_costs,
            )
        services = tuple(system.get_services().values())
        service_rates = tuple(service.get_mu() for service in services)
        return cls(
            system.get_effective_arrival_rates(),
            service_rates,
            replica_costs=replica_costs,
        )

    def __call__(self, configuration):
        if len(configuration) != len(self.__required_replicas):
            raise ValueError("Configuration size does not match the model.")
        return sum(
            max(0, required - replicas) * cost
            for required, replicas, cost in zip(
                self.__required_replicas,
                configuration,
                self.__replica_costs,
            )
        )


class AnalyticalClassification(str, Enum):
    """Deterministic classifications from the configured analytical model."""

    ANALYTICALLY_FEASIBLE = "ANALYTICALLY_FEASIBLE"
    ANALYTICALLY_INFEASIBLE = "ANALYTICALLY_INFEASIBLE"
    ANALYTICALLY_UNSTABLE = "ANALYTICALLY_UNSTABLE"
    STRUCTURALLY_INFEASIBLE = "STRUCTURALLY_INFEASIBLE"


@dataclass(frozen=True)
class AnalyticalReplicaSizingEvaluation:
    """Mean-latency evidence for an analytical-only replica evaluation.

    The values are scenario-specific analytical expectations. They are not
    p95 latency estimates and have no statistical confidence guarantee.
    """

    configuration: tuple[int, ...]
    feasible: bool
    cost: float
    tie_breaker: float
    analytical_metrics: Metrics
    classification: AnalyticalClassification
    scenario_request_class_mean_latencies: tuple[
        tuple[str, tuple[tuple[str, float], ...]], ...
    ]
    request_class_mean_latency_limits: tuple[tuple[str, float], ...]
    evaluation_basis: str = "ANALYTICAL_MEAN_LATENCY"

    @property
    def scenario_means(self):
        return {
            scenario: dict(values)
            for scenario, values in self.scenario_request_class_mean_latencies
        }

    @property
    def limits(self):
        return dict(self.request_class_mean_latency_limits)

    def ranking_key(self):
        return (0 if self.feasible else 1, self.cost, self.tie_breaker)


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


class AnalyticalReplicaSizingProblem(GraphSearchProblem):
    """Bounded replica sizing using analytical mean-latency constraints only.

    Exact/scalable describes search coverage. It does not change the queueing
    formulas selected by the system, and neither search mode turns analytical
    mean latency into a p95 or a statistical feasibility claim.
    """

    def __init__(
        self,
        system,
        request_class_mean_latency_limits,
        max_replicas,
        *,
        replica_costs=None,
        structural_feasibility=None,
    ):
        self.__system = system
        self.__service_count = len(system.get_services())
        self.__request_classes = tuple(system.get_request_classes())
        self.__limits = self.__normalize_limits(
            request_class_mean_latency_limits
        )
        self.__stable = self.__normalize_configuration(
            tuple(system.get_minimum_stable_replicas()),
            maximum=None,
        )
        self.__maximum = self.__normalize_maximum(max_replicas)
        if any(
            maximum < minimum
            for minimum, maximum in zip(self.__stable, self.__maximum)
        ):
            raise ValueError(
                "Maximum replica bounds cannot be below the analytically "
                "stable configuration."
            )
        self.__replica_costs = self.__normalize_replica_costs(replica_costs)
        self.__structural_feasibility = structural_feasibility
        self.__evidence_cache = {}
        self.__evaluation_cache = {}

    def __normalize_limits(self, values):
        limits = dict(values)
        expected = set(self.__request_classes)
        actual = set(limits)
        if actual != expected:
            missing = ", ".join(sorted(expected - actual)) or "none"
            unexpected = ", ".join(sorted(actual - expected)) or "none"
            raise ValueError(
                "Analytical mean-latency limits must exactly match request "
                f"classes (missing: {missing}; unexpected: {unexpected})."
            )
        normalized = {name: float(limits[name]) for name in self.__request_classes}
        if any(
            not math.isfinite(value) or value <= 0
            for value in normalized.values()
        ):
            raise ValueError(
                "Analytical request-class mean-latency limits must be finite "
                "and positive."
            )
        return normalized

    def __normalize_maximum(self, value):
        if isinstance(value, int) and not isinstance(value, bool):
            values = (value,) * self.__service_count
        else:
            values = tuple(value)
        if len(values) != self.__service_count or any(
            isinstance(item, bool) or not isinstance(item, int) or item <= 0
            for item in values
        ):
            raise ValueError(
                "max_replicas must define one positive integer per service."
            )
        return values

    def __normalize_replica_costs(self, values):
        if values is None:
            normalized = (1.0,) * self.__service_count
        elif isinstance(values, (int, float)) and not isinstance(values, bool):
            normalized = (float(values),) * self.__service_count
        else:
            normalized = tuple(float(value) for value in values)
        if len(normalized) != self.__service_count or any(
            not math.isfinite(value) or value <= 0 for value in normalized
        ):
            raise ValueError(
                "replica_costs must define one finite positive cost per service."
            )
        return normalized

    def __normalize_configuration(self, configuration, *, maximum=True):
        values = tuple(configuration)
        if len(values) != self.__service_count or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in values
        ):
            raise ValueError(
                "Configuration must define a positive integer per service."
            )
        if maximum is not None and any(
            value > limit for value, limit in zip(values, self.__maximum)
        ):
            raise ValueError("Configuration exceeds the replica search bounds.")
        return values

    @property
    def configuration_evaluations(self):
        return dict(self.__evaluation_cache)

    def initial_state(self):
        return self.__stable

    def stable_configuration(self):
        return self.__stable

    def maximum_configuration(self):
        return self.__maximum

    def configuration_count(self):
        return math.prod(
            maximum - minimum + 1
            for minimum, maximum in zip(self.__stable, self.__maximum)
        )

    def replica_costs(self):
        return self.__replica_costs

    def configuration_cost(self, configuration):
        configuration = self.__normalize_configuration(configuration)
        return sum(
            count * cost
            for count, cost in zip(configuration, self.__replica_costs)
        )

    def candidate_is_structurally_feasible(self, configuration):
        configuration = self.__normalize_configuration(configuration)
        return (
            True
            if self.__structural_feasibility is None
            else bool(self.__structural_feasibility(configuration))
        )

    def neighbors(self, state):
        state = self.__normalize_configuration(state)
        neighbors = []
        for service_id, replicas in enumerate(state):
            if replicas >= self.__maximum[service_id]:
                continue
            candidate = list(state)
            candidate[service_id] += 1
            neighbors.append(tuple(candidate))
        return tuple(neighbors)

    def step_cost(self, current, neighbor):
        current = self.__normalize_configuration(current)
        neighbor = self.__normalize_configuration(neighbor)
        differences = tuple(
            right - left for left, right in zip(current, neighbor)
        )
        if differences.count(1) != 1 or any(
            difference not in (0, 1) for difference in differences
        ):
            raise ValueError("A replica-sizing step must add exactly one replica.")
        return self.__replica_costs[differences.index(1)]

    def heuristic(self, state):
        self.__normalize_configuration(state)
        return 0.0

    def _scenario_means(self):
        method = getattr(
            self.__system,
            "get_request_class_mean_latencies_by_scenario",
            None,
        )
        if method is not None:
            values = method()
        else:
            scenario_method = getattr(self.__system, "get_scenario_name", None)
            scenario = scenario_method() if scenario_method is not None else None
            values = {
                scenario or "default": (
                    self.__system.get_request_class_mean_latencies()
                )
            }
        return tuple(
            (
                str(scenario),
                tuple(
                    (request_class, float(class_values[request_class]))
                    for request_class in self.__request_classes
                ),
            )
            for scenario, class_values in values.items()
        )

    def _evidence(self, configuration):
        configuration = self.__normalize_configuration(configuration)
        cached = self.__evidence_cache.get(configuration)
        if cached is not None:
            return cached
        self.__system.configure_system_replicas(configuration)
        analytical = self.__system.solve()
        scenario_means = self._scenario_means() if analytical.get_stability() else ()
        evidence = (analytical, scenario_means)
        self.__evidence_cache[configuration] = evidence
        return evidence

    def _worst_normalized_latency(self, scenario_means):
        return max(
            mean / self.__limits[request_class]
            for _, class_values in scenario_means
            for request_class, mean in class_values
        )

    def search_tie_breaker(self, configuration):
        analytical, scenario_means = self._evidence(configuration)
        if not analytical.get_stability() or not scenario_means:
            raise ValueError(
                "Analytical optimization search states must be strictly stable."
            )
        value = self._worst_normalized_latency(scenario_means)
        if not math.isfinite(value) or value < 0:
            raise ValueError("Analytical mean-latency ranking must be finite.")
        return value

    def rank_replica_addition(self, current, candidate):
        added_cost = self.configuration_cost(candidate) - self.configuration_cost(
            current
        )
        return (
            self.search_tie_breaker(current)
            - self.search_tie_breaker(candidate)
        ) / added_cost

    def evaluate(self, configuration):
        configuration = self.__normalize_configuration(configuration)
        cached = self.__evaluation_cache.get(configuration)
        if cached is not None:
            return cached

        analytical, scenario_means = self._evidence(configuration)
        if not analytical.get_stability():
            classification = AnalyticalClassification.ANALYTICALLY_UNSTABLE
            feasible = False
            tie_breaker = math.inf
        elif not self.candidate_is_structurally_feasible(configuration):
            classification = AnalyticalClassification.STRUCTURALLY_INFEASIBLE
            feasible = False
            tie_breaker = self._worst_normalized_latency(scenario_means)
        else:
            tie_breaker = self._worst_normalized_latency(scenario_means)
            feasible = tie_breaker <= 1.0
            classification = (
                AnalyticalClassification.ANALYTICALLY_FEASIBLE
                if feasible
                else AnalyticalClassification.ANALYTICALLY_INFEASIBLE
            )

        evaluation = AnalyticalReplicaSizingEvaluation(
            configuration=configuration,
            feasible=feasible,
            cost=(
                self.configuration_cost(configuration) if feasible else math.inf
            ),
            tie_breaker=tie_breaker,
            analytical_metrics=analytical,
            classification=classification,
            scenario_request_class_mean_latencies=scenario_means,
            request_class_mean_latency_limits=tuple(self.__limits.items()),
        )
        self.__evaluation_cache[configuration] = evaluation
        return evaluation

    def evaluate_and_check(self, configuration):
        evaluation = self.evaluate(configuration)
        return evaluation, evaluation.feasible

    def is_feasible(self, configuration):
        return self.evaluate(configuration).feasible


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
        replica_costs=None,
        structural_feasibility=None,
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
        self.__replica_costs = self.__normalize_replica_costs(replica_costs)
        self.__structural_feasibility = structural_feasibility
        stable_method = getattr(system, "get_minimum_stable_replicas", None)
        stable_configuration = (
            stable_method()
            if stable_method is not None
            else tuple(
                math.floor(arrival_rate / service.get_mu()) + 1
                for arrival_rate, service in zip(
                    system.get_effective_arrival_rates(),
                    system.get_services().values(),
                )
            )
        )
        self.__stable_configuration = self.__normalize_configuration(
            stable_configuration
        )
        self.__initial_state = self.__normalize_configuration(
            tuple(1 for _ in range(self.__service_count))
            if initial_configuration is None
            else initial_configuration
        )
        self.__heuristic = (
            heuristic
            if heuristic is not None
            else AnalyticalStabilityHeuristic.from_system(
                system, replica_costs=self.__replica_costs
            )
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
        self.__search_order_cache = {}

    def __normalize_replica_costs(self, replica_costs):
        if replica_costs is None:
            values = (1.0,) * self.__service_count
        elif isinstance(replica_costs, (int, float)) and not isinstance(
            replica_costs, bool
        ):
            values = (float(replica_costs),) * self.__service_count
        else:
            values = tuple(float(value) for value in replica_costs)
        if len(values) != self.__service_count or any(
            not math.isfinite(value) or value <= 0 for value in values
        ):
            raise ValueError(
                "replica_costs must define one finite positive cost per service."
            )
        return values

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
            value > limit for value, limit in zip(configuration, self.__max_replicas)
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
        return tuple(first_seed + offset for offset in range(self.__replication_count))

    def initial_state(self):
        return self.__initial_state

    def stable_configuration(self):
        return self.__stable_configuration

    def configuration_count(self):
        return math.prod(self.__max_replicas)

    def maximum_configuration(self):
        return self.__max_replicas

    def replica_costs(self):
        return self.__replica_costs

    def configuration_cost(self, configuration):
        configuration = self.__normalize_configuration(configuration)
        return sum(
            count * cost for count, cost in zip(configuration, self.__replica_costs)
        )

    def candidate_is_structurally_feasible(self, configuration):
        configuration = self.__normalize_configuration(configuration)
        return (
            True
            if self.__structural_feasibility is None
            else bool(self.__structural_feasibility(configuration))
        )

    def search_tie_breaker(self, configuration):
        configuration = self.__normalize_configuration(configuration)
        cached = self.__search_order_cache.get(configuration)
        if cached is not None:
            return cached
        self.__system.configure_system_replicas(configuration)
        analytical = self.__analytical_cache.get(configuration)
        if analytical is None:
            analytical = self.__system.solve()
            self.__analytical_cache[configuration] = analytical
        utilizations = tuple(analytical.get_mean_utilizations())
        value = max((float(item) for item in utilizations), default=math.inf)
        if math.isnan(value):
            raise ValueError("Analytical utilization ranking returned NaN.")
        self.__search_order_cache[configuration] = value
        return value

    def rank_replica_addition(self, current, candidate):
        added_cost = self.configuration_cost(candidate) - self.configuration_cost(
            current
        )
        return (
            self.search_tie_breaker(current) - self.search_tie_breaker(candidate)
        ) / added_cost

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
        service_id = differences.index(1)
        return self.__replica_costs[service_id]

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

        if not self.candidate_is_structurally_feasible(configuration):
            evaluation = ReplicaSizingEvaluation(
                configuration=configuration,
                feasible=False,
                cost=math.inf,
                tie_breaker=math.inf,
                analytical_metrics=analytical_metrics,
                simulation_metrics=None,
                classification=(SimulationClassification.STRUCTURALLY_INFEASIBLE),
            )
            self.__evaluation_cache[configuration] = evaluation
            return evaluation

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
                for request_class, interval in reliability.request_class_intervals.items()
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
        cost = self.configuration_cost(configuration) if feasible else math.inf
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
        replica_costs=None,
        structural_feasibility=None,
        progress_callback=None,
    ):
        self.__system = system
        self.__profile = calibration_profile
        self.__service_count = len(system.get_services())
        self.__maximum = tuple(calibration_profile.maximum_replica_configuration)
        self.__initial = tuple(calibration_profile.minimum_replica_configuration)
        self.__replica_costs = self.__normalize_replica_costs(replica_costs)
        self.__structural_feasibility = structural_feasibility
        if progress_callback is not None and not callable(progress_callback):
            raise TypeError("progress_callback must be callable or None.")
        self.__progress_callback = progress_callback
        if len(self.__maximum) != self.__service_count:
            raise ValueError("Calibration profile does not match service count.")
        # The calibrated anchor is already strictly stable, so stability adds
        # no remaining-cost information. Keep a zero heuristic only for callers
        # that explicitly inject the legacy A* optimizer.
        self.__heuristic = heuristic if heuristic is not None else zero_heuristic
        self.__evaluator = evaluator or CalibratedConfigurationEvaluator(
            system,
            calibration_profile,
            workers=simulation_workers,
            progress_callback=progress_callback,
        )
        self.__evaluation_history = {}
        self.__search_order_cache = {}

    def __normalize_replica_costs(self, replica_costs):
        if replica_costs is None:
            values = (1.0,) * self.__service_count
        elif isinstance(replica_costs, (int, float)) and not isinstance(
            replica_costs, bool
        ):
            values = (float(replica_costs),) * self.__service_count
        else:
            values = tuple(float(value) for value in replica_costs)
        if len(values) != self.__service_count or any(
            not math.isfinite(value) or value <= 0 for value in values
        ):
            raise ValueError(
                "replica_costs must define one finite positive cost per service."
            )
        return values

    @property
    def configuration_evaluations(self):
        """Return run evidence; entries are never reused as simulation cache."""
        return dict(self.__evaluation_history)

    def __report_evaluation(self, configuration, evaluation):
        if self.__progress_callback is None:
            return
        classification = getattr(evaluation, "classification", None)
        self.__progress_callback(
            "replica_configuration_evaluated",
            {
                "configuration": tuple(configuration),
                "completed_configurations": len(self.__evaluation_history),
                "search_space_configurations": self.configuration_count(),
                "classification": getattr(classification, "value", classification),
            },
        )

    def __normalize(self, configuration):
        configuration = tuple(configuration)
        if (
            len(configuration) != self.__service_count
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in configuration
            )
            or any(
                value > maximum for value, maximum in zip(configuration, self.__maximum)
            )
        ):
            raise ValueError("Configuration is outside calibrated replica bounds.")
        return configuration

    def initial_state(self):
        return self.__initial

    def stable_configuration(self):
        return self.__initial

    def configuration_count(self):
        return math.prod(
            maximum - minimum + 1
            for minimum, maximum in zip(self.__initial, self.__maximum)
        )

    def maximum_configuration(self):
        return self.__maximum

    def replica_costs(self):
        return self.__replica_costs

    def configuration_cost(self, configuration):
        configuration = self.__normalize(configuration)
        return sum(
            count * cost for count, cost in zip(configuration, self.__replica_costs)
        )

    def candidate_is_structurally_feasible(self, configuration):
        configuration = self.__normalize(configuration)
        return (
            True
            if self.__structural_feasibility is None
            else bool(self.__structural_feasibility(configuration))
        )

    def rank_replica_addition(self, current, candidate):
        added_cost = self.configuration_cost(candidate) - self.configuration_cost(
            current
        )
        return (
            self.search_tie_breaker(current) - self.search_tie_breaker(candidate)
        ) / added_cost

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
        differences = tuple(right - left for left, right in zip(current, neighbor))
        if differences.count(1) != 1 or any(
            value not in (0, 1) for value in differences
        ):
            raise ValueError("A replica-sizing step must add exactly one replica.")
        service_id = differences.index(1)
        return self.__replica_costs[service_id]

    def heuristic(self, state):
        """Retain compatibility for callers that explicitly inject A*."""
        return self.__heuristic(self.__normalize(state))

    def search_tie_breaker(self, configuration):
        """Order equal-cost states by lower analytical bottleneck utilization."""
        configuration = self.__normalize(configuration)
        cached = self.__search_order_cache.get(configuration)
        if cached is not None:
            return cached
        self.__system.configure_system_replicas(configuration)
        analytical = self.__system.solve()
        utilizations = tuple(analytical.get_mean_utilizations())
        if not analytical.get_stability() or not utilizations:
            raise ValueError(
                "Calibrated uniform-cost search requires stable bounded states."
            )
        ordering = max(float(value) for value in utilizations)
        if not math.isfinite(ordering) or ordering < 0:
            raise ValueError("Analytical utilization ordering must be finite.")
        self.__search_order_cache[configuration] = ordering
        return ordering

    def evaluate(self, configuration):
        configuration = self.__normalize(configuration)
        if not self.candidate_is_structurally_feasible(configuration):
            self.__system.configure_system_replicas(configuration)
            analytical = self.__system.solve()
            evaluation = ReplicaSizingEvaluation(
                configuration=configuration,
                feasible=False,
                cost=math.inf,
                tie_breaker=math.inf,
                analytical_metrics=analytical,
                simulation_metrics=None,
                classification=(SimulationClassification.STRUCTURALLY_INFEASIBLE),
            )
            self.__evaluation_history[configuration] = evaluation
            self.__report_evaluation(configuration, evaluation)
            return evaluation
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
            cost=self.configuration_cost(configuration) if feasible else math.inf,
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
        self.__report_evaluation(configuration, evaluation)
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

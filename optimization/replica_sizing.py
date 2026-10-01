import math
from dataclasses import dataclass
from enum import Enum

from metrics import Metrics

from .base import GraphSearchProblem


class AnalyticalClassification(str, Enum):
    """Deterministic classifications from the configured analytical model."""

    ANALYTICALLY_FEASIBLE = "ANALYTICALLY_FEASIBLE"
    ANALYTICALLY_INFEASIBLE = "ANALYTICALLY_INFEASIBLE"
    ANALYTICALLY_UNSTABLE = "ANALYTICALLY_UNSTABLE"
    STRUCTURALLY_INFEASIBLE = "STRUCTURALLY_INFEASIBLE"


@dataclass(frozen=True)
class AnalyticalReplicaSizingEvaluation:
    """Mean-latency evidence for an analytical-only replica evaluation.

    The values are analytical expectations. They are not p95 latency estimates
    and have no statistical confidence guarantee.
    """

    configuration: tuple[int, ...]
    feasible: bool
    cost: float
    tie_breaker: float
    analytical_metrics: Metrics
    classification: AnalyticalClassification
    request_class_mean_latencies: tuple[tuple[str, float], ...]
    request_class_mean_latency_limits: tuple[tuple[str, float], ...]
    evaluation_basis: str = "ANALYTICAL_MEAN_LATENCY"

    @property
    def means(self):
        return dict(self.request_class_mean_latencies)

    @property
    def limits(self):
        return dict(self.request_class_mean_latency_limits)

    def ranking_key(self):
        return (0 if self.feasible else 1, self.cost, self.tie_breaker)


def _positive_integer_tuple(values, size, message):
    values = tuple(values)
    if len(values) != size or any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in values
    ):
        raise ValueError(message)
    return values


def _maximum_replicas(value, service_count):
    values = (value,) * service_count if isinstance(value, int) else value
    return _positive_integer_tuple(
        values,
        service_count,
        "max_replicas must define one positive integer per service.",
    )


def _replica_costs(values, service_count):
    if values is None:
        result = (1.0,) * service_count
    elif isinstance(values, (int, float)) and not isinstance(values, bool):
        result = (float(values),) * service_count
    else:
        result = tuple(float(value) for value in values)
    if len(result) != service_count or any(
        not math.isfinite(value) or value <= 0 for value in result
    ):
        raise ValueError(
            "replica_costs must define one finite positive cost per service."
        )
    return result


class _BoundedReplicaSizingProblem(GraphSearchProblem):
    """Exact-search geometry for analytical evidence."""

    def __init__(
        self,
        system,
        initial,
        maximum,
        *,
        replica_costs,
        structural_feasibility,
        progress_callback,
        invalid_configuration_message,
        exceeded_configuration_message,
    ):
        if progress_callback is not None and not callable(progress_callback):
            raise TypeError("progress_callback must be callable or None.")
        self._system = system
        self._service_count = len(system.get_services())
        self._invalid_configuration_message = invalid_configuration_message
        self._exceeded_configuration_message = exceeded_configuration_message
        self._initial = _positive_integer_tuple(
            initial,
            self._service_count,
            invalid_configuration_message,
        )
        self._maximum = _positive_integer_tuple(
            maximum,
            self._service_count,
            exceeded_configuration_message,
        )
        self._replica_costs = _replica_costs(replica_costs, self._service_count)
        self._structural_feasibility = structural_feasibility
        self._progress_callback = progress_callback
        self._evaluation_history = {}

    @property
    def configuration_evaluations(self):
        return dict(self._evaluation_history)

    def _normalize(self, configuration):
        values = _positive_integer_tuple(
            configuration,
            self._service_count,
            self._invalid_configuration_message,
        )
        if any(value > limit for value, limit in zip(values, self._maximum)):
            raise ValueError(self._exceeded_configuration_message)
        return values

    def _record_evaluation(self, configuration, evaluation):
        self._evaluation_history[configuration] = evaluation
        if self._progress_callback is not None:
            classification = getattr(evaluation, "classification", None)
            self._progress_callback(
                "replica_configuration_evaluated",
                {
                    "configuration": configuration,
                    "completed_configurations": len(self._evaluation_history),
                    "search_space_configurations": self.configuration_count(),
                    "classification": getattr(classification, "value", classification),
                },
            )
        return evaluation

    def initial_state(self):
        return self._initial

    def stable_configuration(self):
        return self._initial

    def maximum_configuration(self):
        return self._maximum

    def configuration_count(self):
        return math.prod(
            maximum - minimum + 1
            for minimum, maximum in zip(self._initial, self._maximum)
        )

    def replica_costs(self):
        return self._replica_costs

    def configuration_cost(self, configuration):
        return sum(
            count * cost
            for count, cost in zip(self._normalize(configuration), self._replica_costs)
        )

    def candidate_is_structurally_feasible(self, configuration):
        configuration = self._normalize(configuration)
        return self._structural_feasibility is None or bool(
            self._structural_feasibility(configuration)
        )

    def neighbors(self, state):
        state = self._normalize(state)
        result = []
        for service_id, replicas in enumerate(state):
            if replicas < self._maximum[service_id]:
                candidate = list(state)
                candidate[service_id] += 1
                result.append(tuple(candidate))
        return tuple(result)

    def step_cost(self, current, neighbor):
        current = self._normalize(current)
        neighbor = self._normalize(neighbor)
        differences = tuple(right - left for left, right in zip(current, neighbor))
        if differences.count(1) != 1 or any(
            value not in (0, 1) for value in differences
        ):
            raise ValueError("A replica-sizing step must add exactly one replica.")
        return self._replica_costs[differences.index(1)]

    def evaluate_and_check(self, configuration):
        evaluation = self.evaluate(configuration)
        return evaluation, evaluation.feasible

    def is_feasible(self, configuration):
        return self.evaluate(configuration).feasible


class AnalyticalReplicaSizingProblem(_BoundedReplicaSizingProblem):
    """Bounded replica sizing using stability and optional mean constraints.

    Exact search does not change the queueing formulas selected by the system
    or turn analytical mean latency into a p95/statistical feasibility claim.
    ``minimum_configuration`` can raise the hard lower bound above the
    analytical vector.
    """

    def __init__(
        self,
        system,
        request_class_mean_latency_limits,
        max_replicas,
        *,
        minimum_configuration=None,
        replica_costs=None,
        structural_feasibility=None,
        progress_callback=None,
    ):
        service_count = len(system.get_services())
        self.__request_classes = tuple(system.get_request_classes())
        self.__limits = self.__normalize_limits(request_class_mean_latency_limits)
        analytical_stable = _positive_integer_tuple(
            tuple(system.get_minimum_stable_replicas()),
            service_count,
            "Configuration must define a positive integer per service.",
        )
        initial = (
            analytical_stable
            if minimum_configuration is None
            else _positive_integer_tuple(
                minimum_configuration,
                service_count,
                "Configuration must define a positive integer per service.",
            )
        )
        if any(
            supplied < analytical
            for supplied, analytical in zip(initial, analytical_stable)
        ):
            raise ValueError(
                "minimum_configuration cannot be below analytical stability."
            )
        maximum = _maximum_replicas(max_replicas, service_count)
        if any(high < low for low, high in zip(initial, maximum)):
            raise ValueError(
                "Maximum replica bounds cannot be below the required stable "
                "configuration."
            )
        super().__init__(
            system,
            initial,
            maximum,
            replica_costs=replica_costs,
            structural_feasibility=structural_feasibility,
            progress_callback=progress_callback,
            invalid_configuration_message=(
                "Configuration must define a positive integer per service."
            ),
            exceeded_configuration_message=(
                "Configuration exceeds the replica search bounds."
            ),
        )
        self.__evidence_cache = {}

    def __normalize_limits(self, values):
        limits = dict(values)
        expected = set(self.__request_classes)
        actual = set(limits)
        if not actual <= expected:
            unexpected = ", ".join(sorted(actual - expected)) or "none"
            raise ValueError(
                "Analytical mean-latency limits must be a subset of "
                f"request classes (unexpected: {unexpected})."
            )
        normalized = {
            name: float(limits[name])
            for name in self.__request_classes
            if name in limits
        }
        if any(not math.isfinite(value) or value <= 0 for value in normalized.values()):
            raise ValueError(
                "Analytical request-class mean-latency limits must be finite "
                "and positive."
            )
        return normalized

    def _request_class_means(self):
        values = self._system.get_request_class_mean_latencies()
        return tuple(
            (request_class, float(values[request_class]))
            for request_class in self.__request_classes
        )

    def _evidence(self, configuration):
        configuration = self._normalize(configuration)
        cached = self.__evidence_cache.get(configuration)
        if cached is not None:
            return cached
        self._system.configure_system_replicas(configuration)
        analytical = self._system.solve()
        class_means = self._request_class_means() if analytical.get_stability() else ()
        evidence = (analytical, class_means)
        self.__evidence_cache[configuration] = evidence
        return evidence

    def _worst_normalized_latency(self, class_means):
        if not self.__limits:
            return 0.0
        return max(
            mean / self.__limits[request_class]
            for request_class, mean in class_means
            if request_class in self.__limits
        )

    def search_tie_breaker(self, configuration):
        analytical, class_means = self._evidence(configuration)
        if not analytical.get_stability() or not class_means:
            raise ValueError(
                "Analytical optimization search states must be strictly stable."
            )
        value = self._worst_normalized_latency(class_means)
        if not math.isfinite(value) or value < 0:
            raise ValueError("Analytical mean-latency ranking must be finite.")
        return value

    def evaluate(self, configuration):
        configuration = self._normalize(configuration)
        cached = self._evaluation_history.get(configuration)
        if cached is not None:
            return cached

        analytical, class_means = self._evidence(configuration)
        if not analytical.get_stability():
            classification = AnalyticalClassification.ANALYTICALLY_UNSTABLE
            feasible = False
            tie_breaker = math.inf
        elif not self.candidate_is_structurally_feasible(configuration):
            classification = AnalyticalClassification.STRUCTURALLY_INFEASIBLE
            feasible = False
            tie_breaker = self._worst_normalized_latency(class_means)
        else:
            tie_breaker = self._worst_normalized_latency(class_means)
            feasible = tie_breaker <= 1.0
            classification = (
                AnalyticalClassification.ANALYTICALLY_FEASIBLE
                if feasible
                else AnalyticalClassification.ANALYTICALLY_INFEASIBLE
            )

        evaluation = AnalyticalReplicaSizingEvaluation(
            configuration=configuration,
            feasible=feasible,
            cost=(self.configuration_cost(configuration) if feasible else math.inf),
            tie_breaker=tie_breaker,
            analytical_metrics=analytical,
            classification=classification,
            request_class_mean_latencies=class_means,
            request_class_mean_latency_limits=tuple(self.__limits.items()),
            evaluation_basis=(
                "ANALYTICAL_MEAN_LATENCY" if self.__limits else "ANALYTICAL_STABILITY"
            ),
        )
        return self._record_evaluation(configuration, evaluation)

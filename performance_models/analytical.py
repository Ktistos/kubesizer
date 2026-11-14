import math
from enum import Enum


class AnalyticalQueueingModel(str, Enum):
    """Queueing model selected for one service."""

    MMC = "M/M/c"
    MG1 = "M/G/1 Pollaczek-Khinchine exact mean"
    MGC_PS = "M/G/c processor-sharing insensitive mean"
    MGC = "M/G/c variability-adjusted Erlang-C approximation"
    GMC = "G/M/c variability-adjusted Erlang-C approximation"
    GGC = "G/G/c variability-adjusted Erlang-C approximation"
    UNSUPPORTED = "unsupported analytical execution/admission/dispatch semantics"

    @property
    def exact(self):
        return self in {
            AnalyticalQueueingModel.MMC,
            AnalyticalQueueingModel.MG1,
            AnalyticalQueueingModel.MGC_PS,
        }


def select_queueing_model(*, poisson_arrivals, exponential_service, replicas):
    if poisson_arrivals and exponential_service:
        return AnalyticalQueueingModel.MMC
    if poisson_arrivals:
        if replicas == 1:
            return AnalyticalQueueingModel.MG1
        return AnalyticalQueueingModel.MGC
    if exponential_service:
        return AnalyticalQueueingModel.GMC
    return AnalyticalQueueingModel.GGC


def _validate_queue(service_arrival_rate, per_replica_service_rate, replicas):
    if (
        not math.isfinite(service_arrival_rate)
        or service_arrival_rate < 0.0
    ):
        raise ValueError("service_arrival_rate must be finite and non-negative.")
    if (
        not math.isfinite(per_replica_service_rate)
        or per_replica_service_rate <= 0.0
    ):
        raise ValueError("per_replica_service_rate must be finite and positive.")
    if isinstance(replicas, bool) or not isinstance(replicas, int) or replicas <= 0:
        raise ValueError("replicas must be a positive integer.")


def erlang_c_waiting_probability(
    service_arrival_rate,
    per_replica_service_rate,
    replicas,
):
    """Return the exact M/M/c probability that an arrival must wait."""
    _validate_queue(service_arrival_rate, per_replica_service_rate, replicas)
    if service_arrival_rate == 0.0:
        return 0.0
    offered_load = service_arrival_rate / per_replica_service_rate
    utilization = offered_load / replicas
    if utilization >= 1.0:
        return 1.0

    term = 1.0
    finite_sum = 1.0
    for population in range(1, replicas):
        term *= offered_load / population
        finite_sum += term
    final_term = term * offered_load / replicas / (1.0 - utilization)
    return final_term / (finite_sum + final_term)


def mmc_mean_response_time(
    service_arrival_rate,
    per_replica_service_rate,
    replicas,
):
    """Return exact stationary M/M/c mean response time."""
    _validate_queue(service_arrival_rate, per_replica_service_rate, replicas)
    if service_arrival_rate >= replicas * per_replica_service_rate:
        return math.inf
    probability_wait = erlang_c_waiting_probability(
        service_arrival_rate,
        per_replica_service_rate,
        replicas,
    )
    mean_wait = probability_wait / (
        replicas * per_replica_service_rate - service_arrival_rate
    )
    return 1.0 / per_replica_service_rate + mean_wait


def mg1_mean_response_time(
    service_arrival_rate,
    service_rate,
    *,
    service_scv,
):
    """Return exact stationary M/G/1 mean response time from two moments."""
    _validate_queue(service_arrival_rate, service_rate, 1)
    if not math.isfinite(service_scv) or service_scv < 0.0:
        raise ValueError("service_scv must be finite and non-negative.")
    utilization = service_arrival_rate / service_rate
    if utilization >= 1.0:
        return math.inf
    mean_service = 1.0 / service_rate
    second_moment = (service_scv + 1.0) * mean_service * mean_service
    mean_wait = service_arrival_rate * second_moment / (
        2.0 * (1.0 - utilization)
    )
    return mean_service + mean_wait


def variability_adjusted_mean_response_time(
    service_arrival_rate,
    per_replica_service_rate,
    replicas,
    *,
    arrival_scv,
    service_scv,
):
    """Approximate G/G/c response time using an Erlang-C variability factor.

    The queueing delay from the corresponding M/M/c queue is multiplied by
    ``(arrival_scv + service_scv) / 2``. It reduces to exact M/M/c when both
    SCVs equal one and to Kingman's waiting-time approximation when ``c=1``.
    """
    _validate_queue(service_arrival_rate, per_replica_service_rate, replicas)
    for name, value in (("arrival_scv", arrival_scv), ("service_scv", service_scv)):
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"{name} must be finite and non-negative.")
    if service_arrival_rate >= replicas * per_replica_service_rate:
        return math.inf
    probability_wait = erlang_c_waiting_probability(
        service_arrival_rate,
        per_replica_service_rate,
        replicas,
    )
    mmc_wait = probability_wait / (
        replicas * per_replica_service_rate - service_arrival_rate
    )
    variability_factor = (arrival_scv + service_scv) / 2.0
    return 1.0 / per_replica_service_rate + variability_factor * mmc_wait


def combine_independent_stream_scvs(streams):
    """Approximate the SCV of independent superposed renewal streams.

    ``streams`` contains ``(rate, scv)`` pairs. Zero-rate streams are ignored.
    The approximation preserves Poisson input and moves toward one as more
    independent streams are superposed.
    """
    positive = []
    for rate, scv in streams:
        if not math.isfinite(rate) or rate < 0.0:
            raise ValueError("Stream rates must be finite and non-negative.")
        if not math.isfinite(scv) or scv < 0.0:
            raise ValueError("Stream SCVs must be finite and non-negative.")
        if rate > 0.0:
            positive.append((rate, scv))
    if not positive:
        return 1.0
    total_rate = sum(rate for rate, _ in positive)
    combined = 1.0 + sum(
        (rate / total_rate) ** 2 * (scv - 1.0)
        for rate, scv in positive
    )
    return max(0.0, combined)


def thinned_stream_scv(source_scv, routing_probability):
    """Approximate SCV after independent Bernoulli routing."""
    if not math.isfinite(source_scv) or source_scv < 0.0:
        raise ValueError("source_scv must be finite and non-negative.")
    if (
        not math.isfinite(routing_probability)
        or routing_probability < 0.0
        or routing_probability > 1.0
    ):
        raise ValueError("routing_probability must lie between zero and one.")
    return 1.0 + routing_probability * (source_scv - 1.0)


def approximate_departure_scv(
    arrival_scv,
    service_scv,
    utilization,
    replicas,
):
    """Approximate GI/G/c departure SCV for network variability propagation."""
    for name, value in (
        ("arrival_scv", arrival_scv),
        ("service_scv", service_scv),
        ("utilization", utilization),
    ):
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"{name} must be finite and non-negative.")
    if isinstance(replicas, bool) or not isinstance(replicas, int) or replicas <= 0:
        raise ValueError("replicas must be a positive integer.")
    bounded_utilization = min(1.0, utilization)
    squared = bounded_utilization * bounded_utilization
    estimate = (
        1.0
        + (1.0 - squared) * (arrival_scv - 1.0)
        + squared / math.sqrt(replicas) * (service_scv - 1.0)
    )
    return max(0.0, estimate)

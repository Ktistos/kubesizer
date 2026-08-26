"""Translate sufficiently detailed evidence into current native-simulator inputs."""

from __future__ import annotations

import copy
from itertools import pairwise, product

from performance_models import QueueingNetworkType, System

from .models import SimulationWorkloadVariant


def _thaw(value):
    if isinstance(value, tuple):
        if all(
            isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
            for item in value
        ):
            return {key: _thaw(item) for key, item in value}
        return [_thaw(item) for item in value]
    return value


def _rescaled_interarrivals(samples, target_rate):
    intervals = tuple(right - left for left, right in pairwise(samples))
    if not intervals:
        return ()
    observed_mean = sum(intervals) / len(intervals)
    scale = (1.0 / target_rate) / observed_mean
    return tuple(interval * scale for interval in intervals)


def _arrival_specification(inputs, request_class, target_rate):
    timestamps = tuple(
        item.timestamp
        for item in inputs.measurements.external_arrival_samples
        if item.request_class == request_class
    )
    samples = _rescaled_interarrivals(timestamps, target_rate)
    if samples:
        return {
            "distribution": "empirical_interarrival",
            "samples": list(samples),
            "sampling": "iid_with_replacement",
        }, "empirical interarrivals rescaled to the target rate"
    return {
        "distribution": "exponential",
        "rate": target_rate,
    }, "exponential arrivals because raw arrival timestamps were unavailable"


def _service_time_specification(inputs, characterization, deployment_service):
    evidence = characterization.service_map[deployment_service.name]
    # CPU-seconds/request divided by the CPU capacity assigned to one replica
    # is the reserved-capacity execution time used by the processor-sharing kernel.
    # Unlike the former fixed-slot conversion, it remains valid when the number
    # of simultaneous runnable requests changes during a visit.
    safe_window_demand = evidence.cpu_demand_safe_seconds
    if safe_window_demand is None or safe_window_demand <= 0.0:
        raise ValueError(
            f"Service {deployment_service.name!r} has no positive aligned "
            "CPU-demand windows."
        )
    return {
        "execution_model": "processor_sharing",
        "distribution": "deterministic",
        "mean": safe_window_demand / deployment_service.cpu_capacity_cores,
    }


def _flow_maps(characterization):
    visits = {
        request_class: dict(values)
        for request_class, values in characterization.request_class_visits
    }
    calls = {
        request_class: {source: dict(targets) for source, targets in sources}
        for request_class, sources in characterization.request_class_calls
    }
    return visits, calls


def _request_class_specification(request_class, calls, mode, concurrency_weight):
    specification = {"concurrency_weight": concurrency_weight}
    specification["calls"] = copy.deepcopy(calls)
    specification["call_execution"] = mode
    if request_class.fixed_latency_overhead_seconds > 0.0:
        specification["fixed_latency_overhead_seconds"] = (
            request_class.fixed_latency_overhead_seconds
        )
    if request_class.end_to_end_timeout_seconds > 0.0:
        specification["end_to_end_timeout_seconds"] = (
            request_class.end_to_end_timeout_seconds
        )
    if request_class.latency_slo is not None:
        specification["end_to_end_latency_slos"] = _thaw(request_class.latency_slo)
    if request_class.maximum_terminal_failure_rate is not None:
        specification["maximum_terminal_failure_rate"] = (
            request_class.maximum_terminal_failure_rate
        )
    return specification


def build_simulation_workload_variants(
    inputs,
    characterization,
    *,
    maximum_ambiguity_variants=64,
):
    """Build executable workload variants without inventing call semantics.

    Aggregate edge counters reveal call multiplicity but not whether sibling
    calls are sequential or parallel. When the target marks execution as
    uncertain, or calls were inferred rather than declared, two explicit
    sensitivity variants are returned.
    """
    if characterization.input_fingerprint != inputs.fingerprint:
        raise ValueError("Characterization belongs to a different evidence package.")
    visits_by_class, calls_by_class = _flow_maps(characterization)
    measured_class_rates = {
        item.request_class: item.observed_arrival_rate_rps
        for item in characterization.external_request_classes
    }
    missing_rates = [
        request_class.name
        for request_class in inputs.workload.request_classes
        if measured_class_rates.get(request_class.name) is None
        or measured_class_rates[request_class.name] <= 0.0
    ]
    if missing_rates:
        raise ValueError(
            "Simulation requires a positive measured external arrival rate for "
            f"every request class; missing {missing_rates}."
        )
    total_measured_rate = sum(measured_class_rates.values())
    unresolved = []
    class_calls = {}
    declared_modes = {}
    for request_class in inputs.workload.request_classes:
        calls = request_class.call_graph or calls_by_class.get(request_class.name)
        visits = visits_by_class.get(request_class.name, {})
        if calls is None:
            reachable = set(visits)
            entries = set(request_class.entry_services)
            if reachable and reachable <= entries:
                calls = {}
            else:
                raise ValueError(
                    f"Request class {request_class.name!r} has mean visits but no "
                    "executable call graph. Supply calls or class-attributed edges."
                )
        class_calls[request_class.name] = calls
        if request_class.call_graph:
            declared_modes[request_class.name] = request_class.call_execution
        else:
            declared_modes[request_class.name] = "uncertain"
        if declared_modes[request_class.name] == "uncertain":
            unresolved.append(request_class.name)

    if (
        isinstance(maximum_ambiguity_variants, bool)
        or not isinstance(maximum_ambiguity_variants, int)
        or maximum_ambiguity_variants <= 0
    ):
        raise ValueError("maximum_ambiguity_variants must be a positive integer.")
    required_variants = 2 ** len(unresolved)
    if required_variants > maximum_ambiguity_variants:
        raise ValueError(
            f"Call ordering has {required_variants} ambiguity variants, exceeding "
            f"the configured maximum of {maximum_ambiguity_variants}. Declare "
            "sequential/parallel semantics for more request classes or raise the "
            "explicit limit."
        )
    mode_variants = (
        (("declared", {}),)
        if not unresolved
        else tuple(
            (
                "-".join(
                    f"{request_class}-{mode}"
                    for request_class, mode in zip(unresolved, modes)
                ),
                dict(zip(unresolved, modes)),
            )
            for modes in product(("sequential", "parallel"), repeat=len(unresolved))
        )
    )
    variants = []
    for variant_name, uncertain_modes in mode_variants:
        assumptions = []
        request_classes = {}
        entry_services = {}
        for request_class in inputs.workload.request_classes:
            mode = declared_modes[request_class.name]
            if mode == "uncertain":
                mode = uncertain_modes[request_class.name]
                assumptions.append(f"{request_class.name} sibling calls execute {mode}")
            request_classes[request_class.name] = _request_class_specification(
                request_class,
                class_calls[request_class.name],
                mode,
                measured_class_rates[request_class.name] / total_measured_rate,
            )
            class_rate = measured_class_rates[request_class.name]
            streams = {}
            for service, entry_weight in request_class.entry_service_weights:
                rate = class_rate * entry_weight
                arrival, assumption = _arrival_specification(
                    inputs,
                    request_class.name,
                    rate,
                )
                streams[service] = arrival
                if assumption not in assumptions:
                    assumptions.append(assumption)
            entry_services[request_class.name] = streams
        service_times = {
            service.name: _service_time_specification(
                inputs,
                characterization,
                service,
            )
            for service in inputs.deployment.services
        }
        assumptions.append(
            "the p90 of aligned U/X windows is used as deterministic CPU work; "
            "per-request service-demand tails are not observable from counters"
        )
        assumptions.append(
            "CPU demand is converted to reserved-capacity execution time and shared "
            "dynamically across concurrent work"
        )
        variants.append(
            SimulationWorkloadVariant(
                name=variant_name,
                workload={
                    "request_classes": request_classes,
                    "traffic_model": {
                        "entry_services": entry_services,
                        "service_times": service_times,
                    },
                },
                assumptions=tuple(dict.fromkeys(assumptions)),
            )
        )
    return tuple(variants)


def build_characterized_system_variants(
    inputs,
    characterization,
    *,
    recommendation=None,
    name="characterized-deployment",
    maximum_ambiguity_variants=64,
):
    """Construct current ``System`` objects for every ambiguity variant."""
    results = []
    for variant in build_simulation_workload_variants(
        inputs,
        characterization,
        maximum_ambiguity_variants=maximum_ambiguity_variants,
    ):
        system = System(name, variant.workload, QueueingNetworkType.OPEN)
        if recommendation is not None:
            order = tuple(
                service.get_name()
                for service in sorted(
                    system.get_services().values(),
                    key=lambda item: item.get_id(),
                )
            )
            system.configure_system_replicas(recommendation.configuration(order))
        results.append((variant, system))
    return tuple(results)

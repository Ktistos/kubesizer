"""Translate sufficiently detailed evidence into current native-simulator inputs."""

from __future__ import annotations

import copy
from itertools import pairwise, product

from performance_models import (
    QueueingNetworkType,
    System,
    duration_distribution_to_workload_mapping,
)

from .models import SimulationWorkloadVariant, UncertaintyDimension


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


def _timestamps_from_interarrivals(interarrivals):
    timestamps = [0.0]
    for interval in interarrivals:
        timestamps.append(timestamps[-1] + interval)
    return tuple(timestamps)


def _arrival_specification(
    inputs,
    request_class,
    target_rate,
    *,
    trace_attribution_is_unambiguous,
):
    timestamps = tuple(
        item.timestamp
        for item in inputs.measurements.external_arrival_samples
        if item.request_class == request_class
    )
    samples = _rescaled_interarrivals(timestamps, target_rate)
    if samples and trace_attribution_is_unambiguous:
        raw_interarrivals = tuple(right - left for left, right in pairwise(timestamps))
        time_scale = sum(samples) / sum(raw_interarrivals)
        window_duration = min(
            row.duration for row in inputs.measurements.service_intervals
        )
        return {
            "distribution": "arrival_trace",
            "timestamps": list(_timestamps_from_interarrivals(samples)),
            "sampling": "block_bootstrap",
            "block_duration": window_duration * time_scale,
        }, "arrival ordering is preserved with a moving trace-block bootstrap"
    return {
        "distribution": "exponential",
        "rate": target_rate,
    }, "exponential arrivals because raw arrival timestamps were unavailable"


def _service_time_specification(
    characterization,
    deployment_service,
    *,
    joint_service_demand_means,
):
    evidence = characterization.service_map[deployment_service.name]
    # CPU-seconds/request divided by the CPU capacity assigned to one replica
    # is the reserved-capacity execution time used by the processor-sharing kernel.
    # Unlike the former fixed-slot conversion, it remains valid when the number
    # of simultaneous runnable requests changes during a visit.
    window_demand = (
        joint_service_demand_means[deployment_service.name]
        if joint_service_demand_means is not None
        else evidence.cpu_demand_safe_seconds
    )
    if window_demand is None or window_demand <= 0.0:
        raise ValueError(
            f"Service {deployment_service.name!r} has no positive aligned "
            "CPU-demand windows."
        )
    return {
        "execution_model": "processor_sharing",
        "distribution": "deterministic",
        "mean": window_demand / deployment_service.cpu_capacity_cores,
    }


def _joint_input_uncertainty(characterization, *, block_length):
    windows = characterization.aligned_input_windows
    if len(windows) < 2:
        return None, None
    if (
        isinstance(block_length, bool)
        or not isinstance(block_length, int)
        or block_length <= 0
    ):
        raise ValueError("input_uncertainty_block_length must be a positive integer.")
    block_length = min(block_length, len(windows))
    request_class_names = tuple(
        item.request_class for item in characterization.external_request_classes
    )
    service_names = tuple(item.service for item in characterization.services)
    external_means = {
        name: sum(dict(window.request_class_arrival_rates)[name] for window in windows)
        / len(windows)
        for name in request_class_names
    }
    service_means = {
        name: sum(dict(window.service_cpu_demands)[name] for window in windows)
        / len(windows)
        for name in service_names
    }
    if any(value is None or value <= 0.0 for value in external_means.values()):
        return None, None
    if any(value is None or value <= 0.0 for value in service_means.values()):
        return None, None
    epochs = []
    for window in windows:
        arrivals = dict(window.request_class_arrival_rates)
        demands = dict(window.service_cpu_demands)
        epochs.append(
            {
                "arrival_rate_multipliers": {
                    name: arrivals[name] / external_means[name]
                    for name in external_means
                },
                "service_time_multipliers": {
                    name: demands[name] / service_means[name] for name in service_means
                },
            }
        )
    return (
        {
            "sampling": "moving_block_bootstrap",
            "block_length": block_length,
            "epochs": epochs,
        },
        service_means,
    )


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
    if request_class.latency_components:
        specification["latency_boundary"] = request_class.latency_boundary
        specification["latency_components"] = {
            name: duration_distribution_to_workload_mapping(component)
            for name, component in request_class.latency_components
        }
    elif request_class.latency_overhead_distribution is not None:
        specification["latency_overhead"] = duration_distribution_to_workload_mapping(
            request_class.latency_overhead_distribution
        )
    elif request_class.fixed_latency_overhead_seconds > 0.0:
        specification["fixed_latency_overhead_seconds"] = (
            request_class.fixed_latency_overhead_seconds
        )
    elif request_class.latency_boundary != "unspecified":
        specification["latency_boundary"] = request_class.latency_boundary
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


def _communication_latency_specification(specification):
    if specification is None:
        return None
    return {
        "same_node_one_way": duration_distribution_to_workload_mapping(
            specification.same_node_one_way
        ),
        "cross_node_one_way": duration_distribution_to_workload_mapping(
            specification.cross_node_one_way
        ),
        "unplaced_cross_node_probability": (
            specification.unplaced_cross_node_probability
        ),
    }


def build_simulation_workload_variants(
    inputs,
    characterization,
    *,
    maximum_ambiguity_variants=64,
    input_uncertainty_block_length=2,
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
        item.request_class: (
            item.observed_arrival_rate_rps * inputs.workload.maximum_load_multiplier
            if item.observed_arrival_rate_rps is not None
            else None
        )
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
    service_blocking_times = dict(inputs.workload.service_blocking_times)
    joint_uncertainty, joint_service_demand_means = _joint_input_uncertainty(
        characterization,
        block_length=input_uncertainty_block_length,
    )
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
        uncertainty_dimensions = [
            UncertaintyDimension(
                kind="simulation",
                name="event_process",
                treatment="independent_replications",
            )
        ]
        request_classes = {}
        entry_services = {}
        for request_class in inputs.workload.request_classes:
            mode = declared_modes[request_class.name]
            if mode == "uncertain":
                mode = uncertain_modes[request_class.name]
                assumptions.append(f"{request_class.name} sibling calls execute {mode}")
                uncertainty_dimensions.append(
                    UncertaintyDimension(
                        kind="structural",
                        name=f"{request_class.name}.call_execution",
                        treatment="explicit_sensitivity_variant",
                        values=(mode,),
                    )
                )
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
                    trace_attribution_is_unambiguous=(
                        len(request_class.entry_service_weights) == 1
                    ),
                )
                streams[service] = arrival
                if assumption not in assumptions:
                    assumptions.append(assumption)
            entry_services[request_class.name] = streams
        service_times = {
            service.name: {
                **_service_time_specification(
                    characterization,
                    service,
                    joint_service_demand_means=joint_service_demand_means,
                ),
                **(
                    {
                        "concurrency_limit_per_replica": (
                            service.concurrency_limit_per_replica
                        )
                    }
                    if service.concurrency_limit_per_replica is not None
                    else {}
                ),
                **(
                    {
                        "non_cpu_blocking_time": (
                            duration_distribution_to_workload_mapping(
                                service_blocking_times[service.name]
                            )
                        )
                    }
                    if service.name in service_blocking_times
                    else {}
                ),
            }
            for service in inputs.workload.services
        }
        if joint_uncertainty is None:
            assumptions.append(
                "the p90 of aligned U/X windows is used as deterministic CPU work; "
                "per-request service-demand tails are not observable from counters"
            )
        else:
            assumptions.append(
                "aligned arrival-rate and U/X windows are sampled jointly as "
                "moving blocks once per replication"
            )
            uncertainty_dimensions.append(
                UncertaintyDimension(
                    kind="input",
                    name="aligned_measurement_windows",
                    treatment="moving_block_bootstrap",
                    values=(
                        len(characterization.aligned_input_windows),
                        joint_uncertainty["block_length"],
                    ),
                )
            )
        if inputs.workload.load_forecast_multipliers != (1.0,):
            assumptions.append(
                "replica certification uses the declared maximum load multiplier"
            )
            uncertainty_dimensions.append(
                UncertaintyDimension(
                    kind="forecast",
                    name="external_arrival_rate",
                    treatment="upper_envelope_certification",
                    values=inputs.workload.load_forecast_multipliers,
                )
            )
        assumptions.append(
            "CPU demand is converted to reserved-capacity execution time and shared "
            "dynamically across concurrent work"
        )
        uncertainty_dimensions.append(
            UncertaintyDimension(
                kind="model_form",
                name="cpu_demand_to_response_time",
                treatment="processor_sharing_sensitivity_assumption",
            )
        )
        variants.append(
            SimulationWorkloadVariant(
                name=variant_name,
                workload={
                    "request_classes": request_classes,
                    "traffic_model": {
                        "entry_services": entry_services,
                        "service_times": service_times,
                        **(
                            {
                                "communication_latency": (
                                    _communication_latency_specification(
                                        inputs.workload.communication_latency
                                    )
                                )
                            }
                            if inputs.workload.communication_latency is not None
                            else {}
                        ),
                        **(
                            {"input_uncertainty": joint_uncertainty}
                            if joint_uncertainty is not None
                            else {}
                        ),
                    },
                },
                assumptions=tuple(dict.fromkeys(assumptions)),
                uncertainty_dimensions=tuple(uncertainty_dimensions),
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
    input_uncertainty_block_length=2,
):
    """Construct current ``System`` objects for every ambiguity variant."""
    results = []
    for variant in build_simulation_workload_variants(
        inputs,
        characterization,
        maximum_ambiguity_variants=maximum_ambiguity_variants,
        input_uncertainty_block_length=input_uncertainty_block_length,
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

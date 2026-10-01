"""Translate portable measurements into analytical queueing inputs."""

from __future__ import annotations
import copy
from itertools import pairwise, product
from performance_models import (
    QueueingNetworkType,
    System,
    duration_distribution_to_workload_mapping,
)
from .models import (
    MINIMUM_ARRIVAL_TRACE_INTERVALS,
    AnalyticalWorkloadVariant,
    UncertaintyDimension,
)


def _thaw(value):
    if isinstance(value, tuple):
        if all(
            (
                isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
                for item in value
            )
        ):
            return {key: _thaw(item) for key, item in value}
        return [_thaw(item) for item in value]
    return value


def _rescaled_interarrivals(samples, target_rate):
    intervals = tuple((right - left for left, right in pairwise(samples)))
    if not intervals:
        return ()
    observed_mean = sum(intervals) / len(intervals)
    if observed_mean <= 0.0:
        return ()
    scale = 1.0 / target_rate / observed_mean
    return tuple((interval * scale for interval in intervals))


def _timestamps_from_interarrivals(interarrivals):
    timestamps = [0.0]
    for interval in interarrivals:
        timestamps.append(timestamps[-1] + interval)
    return tuple(timestamps)


def _arrival_specification(
    inputs,
    request_class,
    entry_service,
    target_rate,
    *,
    trace_attribution_is_unambiguous,
    require_trace,
):
    class_samples = tuple(
        (
            item
            for item in inputs.measurements.external_arrival_samples
            if item.request_class == request_class
        )
    )
    if trace_attribution_is_unambiguous:
        timestamps = tuple(
            sorted(
                (
                    item.timestamp
                    for item in class_samples
                    if item.entry_service in (None, entry_service)
                )
            )
        )
    else:
        timestamps = tuple(
            (
                item.timestamp
                for item in class_samples
                if item.entry_service == entry_service
            )
        )
    samples = _rescaled_interarrivals(timestamps, target_rate)
    trace_has_minimum_evidence = len(samples) >= MINIMUM_ARRIVAL_TRACE_INTERVALS
    if samples and (trace_has_minimum_evidence or not require_trace):
        raw_interarrivals = tuple(
            (right - left for left, right in pairwise(timestamps))
        )
        time_scale = sum(samples) / sum(raw_interarrivals)
        measurement_window_duration = min(
            (row.duration for row in inputs.measurements.service_intervals)
        )
        source_block_duration = min(
            measurement_window_duration, timestamps[-1] - timestamps[0]
        )
        mean = sum(raw_interarrivals) / len(raw_interarrivals)
        variance = sum(
            ((interval - mean) ** 2 for interval in raw_interarrivals)
        ) / len(raw_interarrivals)
        scv = variance / (mean * mean)
        tied_arrivals = sum((interval == 0.0 for interval in raw_interarrivals))
        return (
            {
                "distribution": "arrival_trace",
                "timestamps": list(_timestamps_from_interarrivals(samples)),
                "sampling": "block_bootstrap",
                "block_duration": source_block_duration * time_scale,
            },
            f"the analytical model uses arrival-rate and interarrival-SCV moments from {request_class}/{entry_service} ({len(timestamps)} starts, interarrival SCV {scv:.3g}, {tied_arrivals} tied gaps)",
        )
    if require_trace:
        if not class_samples:
            reason = "no complete raw arrival trace was supplied"
        elif not timestamps:
            reason = "raw arrival timestamps are not attributable to this entry service"
        elif len(timestamps) < 2 or timestamps[-1] <= timestamps[0]:
            reason = "the attributed arrival trace has no positive time span"
        else:
            reason = f"the attributed trace contains only {len(samples)} interarrival gaps; at least {MINIMUM_ARRIVAL_TRACE_INTERVALS} are required"
        raise ValueError(
            f"Cannot certify latency for request class {request_class!r}: {reason} ({entry_service!r}). Supply request-start timestamps from the declared latency boundary."
        )
    return (
        {"distribution": "exponential", "rate": target_rate},
        "exponential arrivals because raw arrival timestamps were unavailable",
    )


def _service_time_specification(
    characterization, deployment_service, *, joint_service_demand_means
):
    evidence = characterization.service_map[deployment_service.name]
    window_demand = (
        joint_service_demand_means[deployment_service.name]
        if joint_service_demand_means is not None
        else evidence.cpu_demand_safe_seconds
    )
    if window_demand is None or window_demand <= 0.0:
        raise ValueError(
            f"Service {deployment_service.name!r} has no positive aligned CPU-demand windows."
        )
    return {
        "execution_model": "processor_sharing",
        "distribution": "deterministic",
        "mean": window_demand / deployment_service.cpu_capacity_cores,
    }


def _joint_input_uncertainty(characterization, *, block_length):
    windows = characterization.aligned_input_windows
    if len(windows) < 2:
        return (None, None)
    if (
        isinstance(block_length, bool)
        or not isinstance(block_length, int)
        or block_length <= 0
    ):
        raise ValueError("input_uncertainty_block_length must be a positive integer.")
    block_length = min(block_length, len(windows))
    request_class_names = tuple(
        (item.request_class for item in characterization.external_request_classes)
    )
    service_names = tuple((item.service for item in characterization.services))
    external_means = {
        name: sum(
            (dict(window.request_class_arrival_rates)[name] for window in windows)
        )
        / len(windows)
        for name in request_class_names
    }
    service_means = {
        name: sum((dict(window.service_cpu_demands)[name] for window in windows))
        / len(windows)
        for name in service_names
    }
    if any((value is None or value <= 0.0 for value in external_means.values())):
        return (None, None)
    if any((value is None or value <= 0.0 for value in service_means.values())):
        return (None, None)
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
    return (visits, calls)


def _request_class_specification(
    request_class, calls, mode, concurrency_weight, *, inferred_latency_components=None
):
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
    elif inferred_latency_components:
        specification["latency_boundary"] = request_class.latency_boundary
        specification["latency_components"] = copy.deepcopy(inferred_latency_components)
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
        "cross_node_probability": specification.cross_node_probability,
    }


def _build_characterized_workload_variants(
    inputs,
    characterization,
    *,
    maximum_ambiguity_variants=64,
    input_uncertainty_block_length=2,
):
    """Build workload variants without inventing call semantics.

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
    external_characterizations = {
        item.request_class: item for item in characterization.external_request_classes
    }
    missing_rates = [
        request_class.name
        for request_class in inputs.workload.request_classes
        if measured_class_rates.get(request_class.name) is None
        or measured_class_rates[request_class.name] <= 0.0
    ]
    if missing_rates:
        raise ValueError(
            f"Characterization requires a positive measured external arrival rate for every request class; missing {missing_rates}."
        )
    total_measured_rate = sum(measured_class_rates.values())
    joint_uncertainty, joint_service_demand_means = _joint_input_uncertainty(
        characterization, block_length=input_uncertainty_block_length
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
                    f"Request class {request_class.name!r} has mean visits but no executable call graph. Supply calls or class-attributed edges."
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
            f"Call ordering has {required_variants} ambiguity variants, exceeding the configured maximum of {maximum_ambiguity_variants}. Declare sequential/parallel semantics for more request classes or raise the explicit limit."
        )
    mode_variants = (
        (("declared", {}),)
        if not unresolved
        else tuple(
            (
                (
                    "-".join(
                        (
                            f"{request_class}-{mode}"
                            for request_class, mode in zip(unresolved, modes)
                        )
                    ),
                    dict(zip(unresolved, modes)),
                )
                for modes in product(("sequential", "parallel"), repeat=len(unresolved))
            )
        )
    )
    variants = []
    for variant_name, uncertain_modes in mode_variants:
        assumptions = []
        uncertainty_dimensions = []
        model_issues = []
        request_classes = {}
        entry_services = {}
        resolved_modes = {}
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
            resolved_modes[request_class.name] = mode
            inferred_latency_components = None
            boundary_latency = external_characterizations[
                request_class.name
            ].boundary_latency
            if boundary_latency.status == "inferred":
                inferred_latency_components = {
                    boundary_latency.component_name: {
                        "distribution": "deterministic",
                        "mean": boundary_latency.mean_seconds,
                    }
                }
                assumptions.append(
                    f"{request_class.name} client-to-entry boundary latency is the stable request-count-weighted difference between aligned external and entry-service means ({len(boundary_latency.window_mean_seconds)} of {boundary_latency.candidate_window_count} candidate windows retained)"
                )
                uncertainty_dimensions.append(
                    UncertaintyDimension(
                        kind="input",
                        name=f"{request_class.name}.client_to_entry_mean",
                        treatment="aligned_window_stability_gated_mean_fit",
                        values=boundary_latency.window_mean_seconds,
                    )
                )
            elif boundary_latency.status == "negligible":
                assumptions.append(
                    f"{request_class.name} client-to-entry boundary latency is omitted because its aligned mean difference is negligible"
                )
            elif boundary_latency.status == "unavailable":
                issue = f"{request_class.name} external-client boundary latency cannot be characterized: {boundary_latency.reason}"
                assumptions.append(issue)
                model_issues.append(issue)
            request_classes[request_class.name] = _request_class_specification(
                request_class,
                class_calls[request_class.name],
                mode,
                measured_class_rates[request_class.name] / total_measured_rate,
                inferred_latency_components=inferred_latency_components,
            )
            class_rate = measured_class_rates[request_class.name]
            streams = {}
            for service, entry_weight in request_class.entry_service_weights:
                rate = class_rate * entry_weight
                arrival, assumption = _arrival_specification(
                    inputs,
                    request_class.name,
                    service,
                    rate,
                    trace_attribution_is_unambiguous=len(
                        request_class.entry_service_weights
                    )
                    == 1,
                    require_trace=request_class.latency_slo is not None,
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
                        "concurrency_limit_per_replica": service.concurrency_limit_per_replica
                    }
                    if service.concurrency_limit_per_replica is not None
                    else {}
                ),
            }
            for service in inputs.workload.services
        }
        assumptions.append(
            "non-CPU blocking variability is excluded from the analytical model because the baseline does not model blocking-related finite concurrency"
        )
        if joint_uncertainty is None:
            assumptions.append(
                "the p90 of aligned U/X windows is used as deterministic CPU work; per-request service-demand tails are not observable from counters"
            )
        else:
            assumptions.append(
                "aligned arrival-rate and U/X windows are pooled into fixed analytical mean inputs"
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
            "CPU demand is converted to reserved-capacity execution time and shared dynamically across concurrent work"
        )
        uncertainty_dimensions.append(
            UncertaintyDimension(
                kind="model_form",
                name="cpu_demand_to_response_time",
                treatment="processor_sharing_sensitivity_assumption",
            )
        )
        variants.append(
            AnalyticalWorkloadVariant(
                name=variant_name,
                workload={
                    "request_classes": request_classes,
                    "traffic_model": {
                        "entry_services": entry_services,
                        "service_times": service_times,
                        **(
                            {
                                "communication_latency": _communication_latency_specification(
                                    inputs.workload.communication_latency
                                )
                            }
                            if inputs.workload.communication_latency is not None
                            else {}
                        ),
                        **{},
                    },
                },
                assumptions=tuple(dict.fromkeys(assumptions)),
                uncertainty_dimensions=tuple(uncertainty_dimensions),
                model_issues=tuple(dict.fromkeys(model_issues)),
            )
        )
    return tuple(variants)


def build_analytical_workload_variants(
    inputs,
    characterization,
    *,
    maximum_ambiguity_variants=64,
    input_uncertainty_block_length=2,
):
    """Build the retained CPU queueing model for analytical evaluation."""
    return _build_characterized_workload_variants(
        inputs,
        characterization,
        maximum_ambiguity_variants=maximum_ambiguity_variants,
        input_uncertainty_block_length=input_uncertainty_block_length,
    )


def build_characterized_system_variants(
    inputs,
    characterization,
    *,
    recommendation=None,
    name="characterized-deployment",
    maximum_ambiguity_variants=64,
    input_uncertainty_block_length=2,
):
    """Build analytical systems from the same characterized measurements."""
    unavailable = [
        (item.request_class, item.boundary_latency.reason)
        for item in characterization.external_request_classes
        if item.boundary_latency.status == "unavailable"
    ]
    if unavailable:
        raise ValueError(
            "Cannot construct a latency model with an incomplete external-client boundary: "
            + "; ".join(f"{name}: {reason}" for name, reason in unavailable)
        )
    variants = build_analytical_workload_variants(
        inputs,
        characterization,
        maximum_ambiguity_variants=maximum_ambiguity_variants,
        input_uncertainty_block_length=input_uncertainty_block_length,
    )
    results = []
    for variant in variants:
        system = System(name, variant.workload, QueueingNetworkType.OPEN)
        if recommendation is not None:
            order = tuple(
                service.get_name()
                for service in sorted(
                    system.get_services().values(), key=lambda item: item.get_id()
                )
            )
            system.configure_system_replicas(recommendation.configuration(order))
        results.append((variant, system))
    return tuple(results)

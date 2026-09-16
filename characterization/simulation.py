"""Translate sufficiently detailed evidence into current native-simulator inputs."""

from __future__ import annotations

import copy
from itertools import pairwise, product

from performance_models import (
    QueueingNetworkType,
    System,
    duration_distribution_to_workload_mapping,
)

from .latency import (
    estimate_low_load_service_blocking_time,
    fit_leaf_blocking_variability,
)
from .models import SimulationWorkloadVariant, UncertaintyDimension


MAXIMUM_BLOCKING_FIT_CPU_UTILIZATION = 0.20


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
        "cross_node_probability": specification.cross_node_probability,
    }


def _communication_round_trip_mean(specification):
    if specification is None:
        return 0.0
    one_way = (
        (1.0 - specification.cross_node_probability)
        * specification.same_node_one_way.mean
        + specification.cross_node_probability * specification.cross_node_one_way.mean
    )
    return 2.0 * one_way


def _response_aligned_cpu_execution_mean(inputs, service, cpu_capacity_cores):
    """Return pooled CPU work/request for the same rows as the response mean."""
    rows = tuple(
        row
        for row in inputs.measurements.service_intervals
        if row.service == service and row.response_duration_sum_seconds is not None
    )
    completed_requests = sum(row.request_count for row in rows)
    if completed_requests <= 0.0:
        return None
    cpu_demand = sum(row.cpu_seconds for row in rows) / completed_requests
    return cpu_demand / cpu_capacity_cores


def _downstream_response_mean(
    source,
    *,
    class_calls,
    class_modes,
    visits_by_class,
    observed_class_rates,
    service_response_means,
    communication_round_trip_mean,
):
    weighted_durations = []
    for request_class, calls_by_source in class_calls.items():
        calls = calls_by_source.get(source, {})
        if not calls:
            continue
        if class_modes[request_class] == "parallel":
            return (
                None,
                "parallel downstream duration cannot be identified from child "
                "means alone",
            )
        if any(
            service_response_means.get(target) is None
            for target, mean_calls in calls.items()
            if float(mean_calls) > 0.0
        ):
            return None, "a downstream response mean is unavailable"
        durations = tuple(
            float(mean_calls)
            * (service_response_means[target] + communication_round_trip_mean)
            for target, mean_calls in calls.items()
            if float(mean_calls) > 0.0
        )
        if not durations:
            continue
        duration = sum(durations)
        source_visits = visits_by_class.get(request_class, {}).get(source, 0.0)
        weight = observed_class_rates[request_class] * source_visits
        if weight > 0.0:
            weighted_durations.append((weight, duration))
    if not weighted_durations:
        return 0.0, None
    total_weight = sum(weight for weight, _duration in weighted_durations)
    return (
        sum(weight * duration for weight, duration in weighted_durations)
        / total_weight,
        None,
    )


def _fit_service_blocking_times(
    inputs,
    characterization,
    *,
    class_calls,
    class_modes,
    visits_by_class,
):
    """Return the one supported measurement-derived blocking model."""
    explicit = dict(inputs.workload.service_blocking_times)
    fitted = {
        service: duration_distribution_to_workload_mapping(specification)
        for service, specification in explicit.items()
    }
    assumptions = []
    uncertainty_dimensions = []
    model_issues = []
    observed_class_rates = {
        item.request_class: float(item.observed_arrival_rate_rps or 0.0)
        for item in characterization.external_request_classes
    }
    response_means = {
        service.service: service.latency.mean_seconds
        for service in characterization.services
    }
    round_trip_mean = _communication_round_trip_mean(
        inputs.workload.communication_latency
    )

    for deployment_service in inputs.workload.services:
        name = deployment_service.name
        if name in fitted:
            assumptions.append(
                f"{name} uses its explicitly supplied blocking distribution"
            )
            continue
        evidence = characterization.service_map[name]
        observed_mean = response_means[name]
        observed_median = evidence.latency.p50_seconds
        cpu_samples = evidence.observed_cpu_usage_cores_per_replica
        cpu_mean = _response_aligned_cpu_execution_mean(
            inputs,
            name,
            deployment_service.cpu_capacity_cores,
        )
        if observed_mean is None or not cpu_samples or cpu_mean is None:
            issue = (
                f"{name} blocking cannot be fitted because aligned response or "
                "CPU evidence is unavailable"
            )
            assumptions.append(issue)
            model_issues.append(issue)
            continue
        maximum_cpu_utilization = max(cpu_samples) / (
            deployment_service.cpu_capacity_cores
        )
        if maximum_cpu_utilization > MAXIMUM_BLOCKING_FIT_CPU_UTILIZATION:
            issue = (
                f"{name} blocking cannot be fitted because characterization CPU "
                f"utilization exceeds {MAXIMUM_BLOCKING_FIT_CPU_UTILIZATION:.0%}"
            )
            assumptions.append(issue)
            model_issues.append(issue)
            continue

        downstream, downstream_issue = _downstream_response_mean(
            name,
            class_calls=class_calls,
            class_modes=class_modes,
            visits_by_class=visits_by_class,
            observed_class_rates=observed_class_rates,
            service_response_means=response_means,
            communication_round_trip_mean=round_trip_mean,
        )
        if downstream_issue is not None:
            issue = f"{name} blocking cannot be fitted because {downstream_issue}"
            assumptions.append(issue)
            model_issues.append(issue)
            continue
        try:
            residual = estimate_low_load_service_blocking_time(
                observed_response_mean_seconds=observed_mean,
                cpu_execution_mean_seconds=cpu_mean,
                downstream_response_mean_seconds=downstream,
                communication_round_trip_mean_seconds=0.0,
                maximum_observed_cpu_utilization=maximum_cpu_utilization,
                maximum_allowed_cpu_utilization=(MAXIMUM_BLOCKING_FIT_CPU_UTILIZATION),
            )
        except ValueError as error:
            if "boundaries are incompatible" not in str(error):
                raise
            issue = (
                f"{name} blocking cannot be fitted because its latency boundaries "
                "cannot be separated reliably"
            )
            assumptions.append(issue)
            model_issues.append(issue)
            continue
        if residual is None:
            assumptions.append(
                f"{name} blocking is omitted because its residual is negligible"
            )
            continue
        if deployment_service.concurrency_limit_per_replica is None:
            raise ValueError(
                f"Fitted blocking for service {name!r} requires "
                f"services.{name}.concurrency_limit_per_replica."
            )

        is_leaf = not any(
            calls_by_source.get(name) for calls_by_source in class_calls.values()
        )
        blocking = residual
        treatment = "deterministic_nonleaf_residual"
        if is_leaf and observed_median is not None:
            blocking = fit_leaf_blocking_variability(
                blocking_mean_seconds=residual["mean"],
                observed_response_median_seconds=observed_median,
                accounted_median_seconds=cpu_mean,
            )
            treatment = (
                "lognormal_mean_p50_fit"
                if blocking["distribution"] == "lognormal"
                else "deterministic_non_skewed_residual"
            )
        elif is_leaf:
            treatment = "deterministic_missing_p50_residual"
        fitted[name] = blocking
        assumptions.append(f"{name} blocking uses {treatment}")
        uncertainty_dimensions.append(
            UncertaintyDimension(
                kind="model_form",
                name=f"{name}.blocking_distribution",
                treatment=treatment,
            )
        )
    return (
        fitted,
        tuple(assumptions),
        tuple(uncertainty_dimensions),
        tuple(model_issues),
    )


def _build_characterized_workload_variants(
    inputs,
    characterization,
    *,
    include_fitted_blocking,
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
    missing_rates = [
        request_class.name
        for request_class in inputs.workload.request_classes
        if measured_class_rates.get(request_class.name) is None
        or measured_class_rates[request_class.name] <= 0.0
    ]
    if missing_rates:
        raise ValueError(
            "Characterization requires a positive measured external arrival rate for "
            f"every request class; missing {missing_rates}."
        )
    total_measured_rate = sum(measured_class_rates.values())
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
        uncertainty_dimensions = []
        model_issues = []
        if include_fitted_blocking:
            uncertainty_dimensions.append(
                UncertaintyDimension(
                    kind="simulation",
                    name="event_process",
                    treatment="independent_replications",
                )
            )
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
            }
            for service in inputs.workload.services
        }
        if include_fitted_blocking:
            (
                blocking_times,
                blocking_assumptions,
                blocking_uncertainty,
                blocking_issues,
            ) = _fit_service_blocking_times(
                inputs,
                characterization,
                class_calls=class_calls,
                class_modes=resolved_modes,
                visits_by_class=visits_by_class,
            )
            for service_name, blocking_time in blocking_times.items():
                service_times[service_name]["non_cpu_blocking_time"] = blocking_time
            assumptions.extend(blocking_assumptions)
            uncertainty_dimensions.extend(blocking_uncertainty)
            model_issues.extend(blocking_issues)
        else:
            assumptions.append(
                "non-CPU blocking variability is excluded from the analytical "
                "model because its finite-concurrency semantics require simulation"
            )
        if joint_uncertainty is None:
            assumptions.append(
                "the p90 of aligned U/X windows is used as deterministic CPU work; "
                "per-request service-demand tails are not observable from counters"
            )
        elif include_fitted_blocking:
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
        else:
            assumptions.append(
                "aligned arrival-rate and U/X windows are pooled into fixed "
                "analytical mean inputs"
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
                            if include_fitted_blocking and joint_uncertainty is not None
                            else {}
                        ),
                    },
                },
                assumptions=tuple(dict.fromkeys(assumptions)),
                uncertainty_dimensions=tuple(uncertainty_dimensions),
                model_issues=tuple(dict.fromkeys(model_issues)),
            )
        )
    return tuple(variants)


def build_simulation_workload_variants(
    inputs,
    characterization,
    *,
    maximum_ambiguity_variants=64,
    input_uncertainty_block_length=2,
):
    """Build the sole supported fitted-variability simulation model."""
    return _build_characterized_workload_variants(
        inputs,
        characterization,
        include_fitted_blocking=True,
        maximum_ambiguity_variants=maximum_ambiguity_variants,
        input_uncertainty_block_length=input_uncertainty_block_length,
    )


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
        include_fitted_blocking=False,
        maximum_ambiguity_variants=maximum_ambiguity_variants,
        input_uncertainty_block_length=input_uncertainty_block_length,
    )


def build_characterized_system_variants(
    inputs,
    characterization,
    *,
    recommendation=None,
    name="characterized-deployment",
    evaluation_mode="simulation",
    maximum_ambiguity_variants=64,
    input_uncertainty_block_length=2,
):
    """Construct current ``System`` objects for every ambiguity variant."""
    if evaluation_mode not in {"analytical", "simulation"}:
        raise ValueError("evaluation_mode must be analytical or simulation.")
    builder = (
        build_simulation_workload_variants
        if evaluation_mode == "simulation"
        else build_analytical_workload_variants
    )
    variants = builder(
        inputs,
        characterization,
        maximum_ambiguity_variants=maximum_ambiguity_variants,
        input_uncertainty_block_length=input_uncertainty_block_length,
    )
    if evaluation_mode == "simulation":
        incomplete = tuple(
            (variant.name, issue)
            for variant in variants
            for issue in variant.model_issues
        )
        if incomplete:
            details = "; ".join(
                f"{variant_name}: {issue}" for variant_name, issue in incomplete
            )
            raise ValueError(
                "Cannot construct a production simulation from an incomplete "
                f"fitted-variability model: {details}. Supply explicit blocking "
                "evidence or collect an eligible low-load characterization."
            )
    results = []
    for variant in variants:
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

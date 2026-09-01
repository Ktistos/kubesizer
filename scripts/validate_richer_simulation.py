#!/usr/bin/env python3
"""Compare richer simulator semantics with retained deployment-family evidence.

The script never contacts the application cluster.  Characterization inputs
come only from the retained low-load run for each family; target observations
come from an independently produced sizing-study analysis.  Two enriched
models are evaluated alongside the existing workload:

``telemetry_cpu_blocking``
    CPU work is the measured application CPU-seconds/request.  The remaining
    low-load Istio service response is represented as non-CPU blocking after
    subtracting downstream response.  This is the portable model which can be
    built from ordinary Kubernetes/Istio/Prometheus evidence.

``telemetry_cpu_blocking_declared_bottleneck``
    Uses the same telemetry values, but assigns blocking to the narrow
    processing-pool limit declared by the synthetic application configuration.
    This tests the value of a DevOps-supplied concurrency limit; the value is
    not inferred from Prometheus counters.

``telemetry_blocking_fitted_variability``
    Additionally fits a lognormal blocking SCV for leaf services from the
    retained low-load Istio mean and median.  This uses extractable histogram
    telemetry, but remains a parametric approximation rather than direct
    per-request blocking observations.

``oracle_occupancy_blocking``
    The synthetic deployment's declared processing-occupancy distribution is
    retained and only the remaining low-load response residual becomes
    blocking.  This diagnoses model-form capability, but is not portable
    measurement evidence because the declared synthetic distribution is
    privileged ground truth.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import math
import sys
from pathlib import Path

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from metrics.cluster_bootstrap import (  # noqa: E402
    fixed_experiment_tail_alpha,
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
)
from performance_models import QueueingNetworkType, System  # noqa: E402


MODEL_BASELINE = "baseline_fixed_overhead"
MODEL_TELEMETRY = "telemetry_cpu_blocking"
MODEL_TELEMETRY_BOTTLENECK = "telemetry_cpu_blocking_declared_bottleneck"
MODEL_TELEMETRY_VARIABILITY = "telemetry_blocking_fitted_variability"
MODEL_ORACLE = "oracle_occupancy_blocking"
ANALYTICAL_MODEL_PREFIX = "analytical::"
MODEL_NAMES = (
    MODEL_BASELINE,
    MODEL_TELEMETRY,
    MODEL_TELEMETRY_BOTTLENECK,
    MODEL_TELEMETRY_VARIABILITY,
    MODEL_ORACLE,
)


def _load_yaml(path):
    with Path(path).open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_seed(*parts, base=61_000_000):
    digest = hashlib.sha256("\0".join(map(str, parts)).encode()).digest()
    return base + int.from_bytes(digest[:6], "big") % 1_000_000_000


def _characterization_directory(experiments_root, family):
    candidates = sorted(
        Path(experiments_root).glob(
            f"deployment-families-20260827-{family}-characterization*"
        )
    )
    complete = [
        path
        for path in candidates
        if (path / "measurement.yaml").is_file()
        and (path / "request_cohort.yaml").is_file()
        and (path / "config.yaml").is_file()
    ]
    if not complete:
        raise ValueError(f"No complete characterization exists for {family!r}.")
    return complete[-1]


def _client_means_by_request_class(config, cohort):
    class_summaries = cohort.get("request_classes", {})
    operations = cohort.get("operations", {})
    result = {}
    for request_class in config["load"]["request_classes"]:
        class_name = request_class["name"]
        class_summary = class_summaries.get(class_name, {})
        class_mean = class_summary.get("successful_latency_ms", {}).get("mean")
        if class_mean is not None:
            result[class_name] = float(class_mean) / 1000.0
            continue
        # Old artifacts exposed latency only by operation. The fallback is
        # still exact when every operation name belongs to only one class, but
        # is ambiguous when two classes reuse a name.
        if not _operation_names_identify_classes(config):
            raise ValueError(
                f"No class-specific client latency is available for {class_name!r}."
            )
        weighted_sum = 0.0
        total_count = 0
        for operation in request_class["operations"]:
            evidence = operations.get(operation["name"])
            if evidence is None:
                raise ValueError(
                    f"Missing operation evidence for {operation['name']!r}."
                )
            count = int(evidence["started_count"])
            weighted_sum += count * float(evidence["latency_ms"]["mean"]) / 1000.0
            total_count += count
        if total_count <= 0:
            raise ValueError(
                f"Request class {class_name!r} has no observations."
            )
        result[class_name] = weighted_sum / total_count
    return result


def _observed_class_rates(config, cohort):
    class_summaries = cohort.get("request_classes", {})
    operations = cohort.get("operations", {})
    duration = float(config["load"]["measurement_seconds"])
    result = {}
    for request_class in config["load"]["request_classes"]:
        class_name = request_class["name"]
        class_summary = class_summaries.get(class_name, {})
        if class_summary.get("started_count") is not None:
            result[class_name] = int(class_summary["started_count"]) / duration
            continue
        if not _operation_names_identify_classes(config):
            raise ValueError(
                f"No class-specific arrival count is available for {class_name!r}."
            )
        count = sum(
            int(operations[operation["name"]]["started_count"])
            for operation in request_class["operations"]
        )
        result[class_name] = count / duration
    return result


def _operation_names_identify_classes(config):
    owners = {}
    for request_class in config["load"]["request_classes"]:
        class_name = request_class["name"]
        for operation in request_class["operations"]:
            operation_name = operation["name"]
            previous = owners.setdefault(operation_name, class_name)
            if previous != class_name:
                return False
    return True


def _set_class_rates(workload, class_rates, interarrival_specs=None):
    total = sum(float(value) for value in class_rates.values())
    if total <= 0.0:
        raise ValueError("At least one class arrival rate must be positive.")
    for request_class, rate in class_rates.items():
        workload["request_classes"][request_class]["concurrency_weight"] = (
            float(rate) / total
        )
        entries = workload["traffic_model"]["entry_services"][request_class]
        original_rates = {
            service: float(specification.get("rate", 1.0))
            for service, specification in entries.items()
        }
        original_total = sum(original_rates.values())
        requested = (
            (interarrival_specs or {}).get(request_class)
            or {"distribution": "exponential"}
        )
        for service, original_rate in original_rates.items():
            specification = copy.deepcopy(requested)
            specification["rate"] = float(rate) * original_rate / original_total
            entries[service] = specification


def _queueing_model_document(workload):
    """Separate experiment/sizing metadata from the System input contract."""
    if not isinstance(workload, dict):
        raise TypeError("A workload must be a mapping.")
    required = {"request_classes", "traffic_model"}
    missing = required - set(workload)
    if missing:
        raise ValueError(f"Queueing workload is missing {sorted(missing)}.")
    unsupported = set(workload) - required - {"sizing_policy"}
    if unsupported:
        raise ValueError(
            "Queueing workload contains unsupported experiment metadata: "
            f"{sorted(unsupported)}."
        )
    return {name: copy.deepcopy(workload[name]) for name in sorted(required)}


def _service_response_means(measurement):
    return {
        service: float(evidence["istio_response_time_ms"]["mean"]) / 1000.0
        for service, evidence in measurement["services"].items()
    }


def _leaf_blocking_lognormal(blocking_mean, observed_median, accounted_median):
    """Fit lognormal variability from a low-load residual mean and median."""
    residual_median = max(0.0, float(observed_median) - float(accounted_median))
    if blocking_mean <= 0.0 or residual_median <= 0.0:
        return None
    if residual_median >= blocking_mean:
        return {"distribution": "deterministic", "mean": blocking_mean}
    sigma_squared = 2.0 * math.log(blocking_mean / residual_median)
    scv = min(100.0, math.expm1(sigma_squared))
    if scv <= 1e-9:
        return {"distribution": "deterministic", "mean": blocking_mean}
    return {
        "distribution": "lognormal",
        "mean": blocking_mean,
        "scv": scv,
    }


def _service_cpu_demands(measurement):
    result = {}
    utilizations = {}
    for service, evidence in measurement["services"].items():
        replicas = tuple(evidence["replicas"].values())
        demands = tuple(
            float(replica["cpu_service_demand_seconds_per_request"])
            for replica in replicas
            if replica.get("cpu_service_demand_seconds_per_request") is not None
        )
        if not demands:
            raise ValueError(f"No CPU demand is available for {service!r}.")
        result[service] = sum(demands) / len(demands)
        utilizations[service] = max(
            float(replica.get("cpu_usage_cores") or 0.0)
            / float(replica.get("cpu_capacity_cores") or 1.0)
            for replica in replicas
        )
    return result, utilizations


def _downstream_response_mean(
    workload,
    source,
    service_responses,
    class_rates,
):
    observations = []
    for request_class, specification in workload["request_classes"].items():
        calls = specification.get("calls", {}).get(source)
        if not calls:
            continue
        durations = [
            float(mean_calls) * service_responses[target]
            for target, mean_calls in calls.items()
            if float(mean_calls) > 0.0
        ]
        if not durations:
            continue
        if specification.get("call_execution", "parallel") == "sequential":
            duration = sum(durations)
        else:
            # Aggregate counters do not identify the distribution of a maximum.
            # The maximum child mean is an explicit low-load approximation.
            duration = max(durations)
        observations.append((class_rates[request_class], duration))
    if not observations:
        return 0.0
    total_weight = sum(weight for weight, _ in observations)
    return sum(weight * value for weight, value in observations) / total_weight


def _entry_response_mean(workload, request_class, service_responses):
    entries = workload["traffic_model"]["entry_services"][request_class]
    weights = {
        service: float(specification["rate"])
        for service, specification in entries.items()
    }
    total = sum(weights.values())
    return sum(
        weight * service_responses[service] for service, weight in weights.items()
    ) / total


def _deterministic_residual(observed, accounted, tolerance=0.001):
    if accounted > observed + tolerance:
        return None
    residual = max(0.0, observed - accounted)
    if residual <= tolerance:
        return None
    return {"distribution": "deterministic", "mean": residual}


def build_characterized_models(
    base_workload,
    measurement,
    cohort,
    config,
    *,
    blocking_concurrency_by_service=None,
):
    """Return baseline, telemetry, and oracle models plus diagnostics."""
    sizing_policy = copy.deepcopy(base_workload.get("sizing_policy"))
    base_workload = _queueing_model_document(base_workload)
    class_rates = _observed_class_rates(config, cohort)
    characterization_workload = copy.deepcopy(base_workload)
    _set_class_rates(characterization_workload, class_rates)
    service_responses = _service_response_means(measurement)
    cpu_demands, cpu_utilizations = _service_cpu_demands(measurement)
    overloaded = {
        service: utilization
        for service, utilization in cpu_utilizations.items()
        if utilization > 0.20
    }
    if overloaded:
        raise ValueError(
            "Blocking-time characterization requires maximum observed CPU "
            f"utilization at or below 0.20; observed {overloaded}."
        )
    client_means = _client_means_by_request_class(config, cohort)

    oracle_system = System(
        "oracle-characterization",
        characterization_workload,
        QueueingNetworkType.OPEN,
    )
    replica_configuration = tuple(
        int(measurement["services"][service.get_name()]["replica_count"])
        for service in sorted(
            oracle_system.get_services().values(),
            key=lambda item: item.get_id(),
        )
    )
    oracle_system.configure_system_replicas(replica_configuration)
    oracle_responses = {
        service.get_name(): float(
            oracle_system.solve().get_mean_latencies()[service.get_id()]
        )
        for service in oracle_system.get_services().values()
    }

    telemetry = copy.deepcopy(characterization_workload)
    oracle = copy.deepcopy(characterization_workload)
    for model in (telemetry, oracle):
        for request_class, specification in model["request_classes"].items():
            specification.pop("fixed_latency_overhead_seconds", None)
            specification.pop("latency_overhead", None)
            specification["latency_boundary"] = "external_client"
            boundary = max(
                0.0,
                client_means[request_class]
                - _entry_response_mean(
                    characterization_workload,
                    request_class,
                    service_responses,
                ),
            )
            specification["latency_components"] = (
                {
                    "client_to_entry": {
                        "distribution": "deterministic",
                        "mean": boundary,
                    }
                }
                if boundary > 0.0
                else {}
            )

    diagnostics = {
        "services": {},
        "request_classes": {},
        "sizing_policy_metadata": sizing_policy,
        "sizing_policy_applied_to_model_evaluation": False,
    }
    for request_class in telemetry["request_classes"]:
        diagnostics["request_classes"][request_class] = {
            "client_mean_seconds": client_means[request_class],
            "entry_response_mean_seconds": _entry_response_mean(
                characterization_workload,
                request_class,
                service_responses,
            ),
            "client_to_entry_mean_seconds": (
                telemetry["request_classes"][request_class]["latency_components"]
                .get("client_to_entry", {})
                .get("mean", 0.0)
            ),
        }

    for service, observed_response in service_responses.items():
        downstream = _downstream_response_mean(
            characterization_workload,
            service,
            service_responses,
            class_rates,
        )
        telemetry_block = _deterministic_residual(
            observed_response,
            cpu_demands[service] + downstream,
        )
        oracle_block = _deterministic_residual(
            observed_response,
            oracle_responses[service] + downstream,
        )

        original = characterization_workload["traffic_model"]["service_times"][
            service
        ]
        concurrency = original.get("concurrency_limit_per_replica")
        telemetry_specification = {
            "execution_model": "processor_sharing",
            "distribution": "deterministic",
            "mean": cpu_demands[service],
        }
        if concurrency is not None:
            telemetry_specification["concurrency_limit_per_replica"] = concurrency
        if telemetry_block is not None:
            telemetry_specification["non_cpu_blocking_time"] = telemetry_block
        telemetry["traffic_model"]["service_times"][service] = (
            telemetry_specification
        )
        if oracle_block is not None:
            oracle["traffic_model"]["service_times"][service][
                "non_cpu_blocking_time"
            ] = oracle_block

        diagnostics["services"][service] = {
            "observed_istio_response_mean_seconds": observed_response,
            "measured_cpu_demand_seconds_per_request": cpu_demands[service],
            "maximum_observed_cpu_utilization": cpu_utilizations[service],
            "modeled_downstream_response_mean_seconds": downstream,
            "oracle_processing_response_mean_seconds": oracle_responses[service],
            "telemetry_blocking_mean_seconds": (
                telemetry_block["mean"] if telemetry_block else 0.0
            ),
            "oracle_blocking_mean_seconds": (
                oracle_block["mean"] if oracle_block else 0.0
            ),
            "declared_processing_slots_per_replica": original.get(
                "processing_slots_per_replica", 1
            ),
            "declared_admission_concurrency_per_replica": concurrency,
        }
    telemetry_bottleneck = copy.deepcopy(telemetry)
    for service, specification in telemetry_bottleneck["traffic_model"][
        "service_times"
    ].items():
        original = characterization_workload["traffic_model"]["service_times"][
            service
        ]
        declared_blocking_concurrency = (blocking_concurrency_by_service or {}).get(
            service
        )
        specification["concurrency_limit_per_replica"] = int(
            declared_blocking_concurrency
            if declared_blocking_concurrency is not None
            else original.get("processing_slots_per_replica", 1)
        )
    telemetry_variability = copy.deepcopy(telemetry_bottleneck)
    for service, diagnostic in diagnostics["services"].items():
        if diagnostic["modeled_downstream_response_mean_seconds"] > 0.0:
            continue
        blocking_mean = diagnostic["telemetry_blocking_mean_seconds"]
        observed_median = (
            float(measurement["services"][service]["istio_response_time_ms"]["p50"])
            / 1000.0
        )
        fitted = _leaf_blocking_lognormal(
            blocking_mean,
            observed_median,
            diagnostic["measured_cpu_demand_seconds_per_request"],
        )
        if fitted is not None:
            telemetry_variability["traffic_model"]["service_times"][service][
                "non_cpu_blocking_time"
            ] = fitted
            diagnostic["fitted_leaf_blocking_distribution"] = fitted
    return {
        MODEL_BASELINE: copy.deepcopy(base_workload),
        MODEL_TELEMETRY: telemetry,
        MODEL_TELEMETRY_BOTTLENECK: telemetry_bottleneck,
        MODEL_TELEMETRY_VARIABILITY: telemetry_variability,
        MODEL_ORACLE: oracle,
    }, diagnostics


def _apply_target(model, class_rates, interarrival_specs):
    result = copy.deepcopy(model)
    _set_class_rates(result, class_rates, interarrival_specs)
    return result


def _configuration_key(configuration):
    return tuple(int(value) for value in configuration)


def _observed_configuration_evidence(point):
    return {
        _configuration_key(item["configuration"]): item
        for item in point["quality"]["configuration_evidence"]
    }


def _intervals_by_class(evidence):
    return {
        request_class: interval
        for request_class, interval in evidence.get("request_class_intervals", [])
    }


def _classification(intervals, slos):
    if intervals and all(
        intervals[request_class]["upper_bound"] <= slos[request_class]
        for request_class in slos
    ):
        return "FEASIBLE"
    if any(
        intervals[request_class]["lower_bound"] > slos[request_class]
        for request_class in slos
        if request_class in intervals
    ):
        return "INFEASIBLE"
    return "INCONCLUSIVE"


def _simulate_model(
    model,
    configuration,
    *,
    seeds,
    workers,
    warmup_duration,
    measurement_duration,
    bootstrap_resamples,
    bootstrap_seed,
):
    system = System("retained-validation", model, QueueingNetworkType.OPEN)
    system.configure_system_replicas(configuration)
    slos = {
        request_class: float(
            model["request_classes"][request_class]["end_to_end_latency_slos"][
                "mean_seconds"
            ]
        )
        for request_class in model["request_classes"]
    }
    if not system.solve().get_stability():
        return {
            "classification": "ANALYTICALLY_UNSTABLE",
            "evaluation_method": "NATIVE_EVENT_SIMULATION",
            "intervals": {},
            "statistics": {},
        }
    replications = system.simulate_measurement_replications(
        warmup_duration=warmup_duration,
        measurement_duration=measurement_duration,
        seeds=seeds,
        workers=workers,
        maximum_drain_time=10_000.0,
    )
    tail = fixed_experiment_tail_alpha(0.05, len(slos))
    intervals = {}
    p95_intervals = {}
    for offset, (request_class, slo) in enumerate(slos.items()):
        interval = replication_cluster_bootstrap_mean(
            tuple(
                replication.request_class_latencies[request_class]
                for replication in replications
            ),
            request_class=request_class,
            slo=slo,
            resample_count=bootstrap_resamples,
            seed=bootstrap_seed + offset,
            lower_tail_probability=tail,
            upper_tail_probability=1.0 - tail,
        )
        intervals[request_class] = {
            "estimate": float(interval.estimate),
            "lower_bound": float(interval.lower_bound),
            "upper_bound": float(interval.upper_bound),
            "eligible_request_count": interval.eligible_request_count,
        }
        p95_interval = replication_cluster_bootstrap_p95(
            tuple(
                replication.request_class_latencies[request_class]
                for replication in replications
            ),
            request_class=request_class,
            slo=slo,
            resample_count=bootstrap_resamples,
            seed=bootstrap_seed + 100_000 + offset,
            lower_tail_probability=tail,
            upper_tail_probability=1.0 - tail,
        )
        p95_intervals[request_class] = {
            "estimate": float(p95_interval.estimate),
            "lower_bound": float(p95_interval.lower_bound),
            "upper_bound": float(p95_interval.upper_bound),
            "eligible_request_count": p95_interval.eligible_request_count,
        }
    return {
        "classification": _classification(intervals, slos),
        "evaluation_method": "NATIVE_EVENT_SIMULATION",
        "intervals": intervals,
        "statistics": {
            request_class: {
                "mean": intervals[request_class],
                "p95": p95_intervals[request_class],
            }
            for request_class in intervals
        },
    }


def _solve_analytical_model(model, configuration):
    """Evaluate analytical mean latency without fabricating tail uncertainty."""

    system = System("retained-analytical-validation", model, QueueingNetworkType.OPEN)
    system.configure_system_replicas(configuration)
    slos = {
        request_class: float(
            model["request_classes"][request_class]["end_to_end_latency_slos"][
                "mean_seconds"
            ]
        )
        for request_class in model["request_classes"]
    }
    metrics = system.solve()
    if not metrics.get_stability():
        return {
            "classification": "ANALYTICALLY_UNSTABLE",
            "evaluation_method": "ANALYTICAL_MEAN_POINT_ESTIMATE",
            "intervals": {},
            "statistics": {},
        }
    estimates = system.get_request_class_mean_latencies()
    intervals = {
        request_class: {
            "estimate": float(estimate),
            "lower_bound": None,
            "upper_bound": None,
            "eligible_request_count": None,
        }
        for request_class, estimate in estimates.items()
    }
    return {
        "classification": (
            "FEASIBLE"
            if all(intervals[name]["estimate"] <= slos[name] for name in slos)
            else "INFEASIBLE"
        ),
        "evaluation_method": "ANALYTICAL_MEAN_POINT_ESTIMATE",
        "intervals": intervals,
        "statistics": {
            request_class: {"mean": interval}
            for request_class, interval in intervals.items()
        },
        "uncertainty_limitation": (
            "Analytical means are point estimates; no p95 or model-error "
            "confidence interval is implied."
        ),
    }


def _empty_summary():
    return {
        model: {
            "comparison_count": 0,
            "absolute_error_sum_seconds": 0.0,
            "relative_error_sum": 0.0,
            "improved_over_baseline_count": 0,
            "decision_count": 0,
            "decision_agreement_count": 0,
            "unsafe_false_feasible_count": 0,
            "conservative_false_infeasible_count": 0,
        }
        for model in MODEL_NAMES
    }


def summarize_results(results, *, accepted_only=False, target_validation_only=False):
    selected = [
        result
        for result in results
        if (not accepted_only or result["evidence_gate_passed"])
        and (
            not target_validation_only
            or result["load_point"] == "target_validation"
        )
    ]
    summary = _empty_summary()
    for result in selected:
        observed = result["observed_intervals"]
        observed_classification = result["observed_classification"]
        baseline_errors = {
            request_class: abs(
                result["models"][MODEL_BASELINE]["intervals"][request_class][
                    "estimate"
                ]
                - interval["estimate"]
            )
            for request_class, interval in observed.items()
            if request_class
            in result["models"][MODEL_BASELINE].get("intervals", {})
        }
        for model_name, model in result["models"].items():
            target = summary[model_name]
            for request_class, observed_interval in observed.items():
                predicted = model.get("intervals", {}).get(request_class)
                if predicted is None:
                    continue
                error = abs(predicted["estimate"] - observed_interval["estimate"])
                target["comparison_count"] += 1
                target["absolute_error_sum_seconds"] += error
                target["relative_error_sum"] += error / observed_interval["estimate"]
                if model_name != MODEL_BASELINE and error < baseline_errors.get(
                    request_class, math.inf
                ):
                    target["improved_over_baseline_count"] += 1
            raw_classification = model["classification"]
            classification = (
                "INFEASIBLE"
                if raw_classification == "ANALYTICALLY_UNSTABLE"
                else raw_classification
            )
            if classification in {"FEASIBLE", "INFEASIBLE", "INCONCLUSIVE"}:
                target["decision_count"] += 1
                target["decision_agreement_count"] += int(
                    classification == observed_classification
                )
                target["unsafe_false_feasible_count"] += int(
                    classification == "FEASIBLE"
                    and observed_classification == "INFEASIBLE"
                )
                target["conservative_false_infeasible_count"] += int(
                    classification == "INFEASIBLE"
                    and observed_classification == "FEASIBLE"
                )
    for values in summary.values():
        count = values.pop("comparison_count")
        error_sum = values.pop("absolute_error_sum_seconds")
        relative_sum = values.pop("relative_error_sum")
        values["latency_comparison_count"] = count
        values["mean_absolute_error_seconds"] = error_sum / count if count else None
        values["mean_absolute_percentage_error"] = (
            relative_sum / count if count else None
        )
        decisions = values["decision_count"]
        values["decision_agreement_rate"] = (
            values["decision_agreement_count"] / decisions if decisions else None
        )
    return {
        "load_point_count": len(
            {(result["family"], result["load_point"]) for result in selected}
        ),
        "configuration_count": len(selected),
        "models": summary,
    }


def summarize_certified_sizing(results):
    grouped = {}
    for result in results:
        if not result.get("empirical_minimum_certified"):
            continue
        grouped.setdefault((result["family"], result["load_point"]), []).append(
            result
        )
    models = {
        model: {
            "recommendation_count": 0,
            "exact_configuration_match_count": 0,
            "replica_cost_match_count": 0,
            "underprovisioned_count": 0,
            "conservative_count": 0,
            "no_feasible_recommendation_count": 0,
        }
        for model in MODEL_NAMES
    }
    points = []
    for (family, load_point), configurations in sorted(grouped.items()):
        empirical = tuple(configurations[0]["empirical_minimum_configuration"])
        point = {
            "family": family,
            "load_point": load_point,
            "empirical_minimum_configuration": list(empirical),
            "models": {},
        }
        for model_name in MODEL_NAMES:
            feasible = [
                tuple(item["configuration"])
                for item in configurations
                if item["models"][model_name]["classification"] == "FEASIBLE"
            ]
            recommendation = (
                min(feasible, key=lambda value: (sum(value), value))
                if feasible
                else None
            )
            point["models"][model_name] = (
                None if recommendation is None else list(recommendation)
            )
            summary = models[model_name]
            if recommendation is None:
                summary["no_feasible_recommendation_count"] += 1
                continue
            summary["recommendation_count"] += 1
            summary["exact_configuration_match_count"] += int(
                recommendation == empirical
            )
            summary["replica_cost_match_count"] += int(
                sum(recommendation) == sum(empirical)
            )
            summary["underprovisioned_count"] += int(
                sum(recommendation) < sum(empirical)
            )
            summary["conservative_count"] += int(
                sum(recommendation) > sum(empirical)
            )
        points.append(point)
    return {
        "certified_empirical_minimum_point_count": len(points),
        "models": models,
        "points": points,
    }


def run_validation(
    *,
    analysis_path,
    analysis_origin=None,
    load_tests_root,
    point_selection,
    replications,
    workers,
    bootstrap_resamples,
):
    analysis = _load_yaml(analysis_path)
    load_tests_root = Path(load_tests_root)
    experiments_root = load_tests_root / "experiments"
    family_root = load_tests_root / "synthetic" / "deployment_families"
    selected_points = []
    for point in analysis["results"]:
        if point_selection == "target-validation" and point["load_point"] != (
            "target_validation"
        ):
            continue
        if point_selection == "evidence-passed" and not point["evidence_gate"][
            "passed"
        ]:
            continue
        selected_points.append(point)

    family_models = {}
    characterization_diagnostics = {}
    characterization_sources = {}
    for family in sorted({point["family"] for point in selected_points}):
        directory = _characterization_directory(experiments_root, family)
        workload_path = family_root / f"workload-{family}.yaml"
        measurement_path = directory / "measurement.yaml"
        cohort_path = directory / "request_cohort.yaml"
        config_path = directory / "config.yaml"
        models, diagnostics = build_characterized_models(
            _load_yaml(workload_path),
            _load_yaml(measurement_path),
            _load_yaml(cohort_path),
            _load_yaml(config_path),
        )
        family_models[family] = models
        characterization_diagnostics[family] = diagnostics
        characterization_sources[family] = {
            "directory": str(directory),
            "workload_sha256": _sha256(workload_path),
            "measurement_sha256": _sha256(measurement_path),
            "request_cohort_sha256": _sha256(cohort_path),
            "config_sha256": _sha256(config_path),
        }

    results = []
    for point in selected_points:
        family = point["family"]
        observed_by_configuration = _observed_configuration_evidence(point)
        for configuration, observed_evidence in observed_by_configuration.items():
            observed_intervals = {
                request_class: {
                    "estimate": float(interval["p95_estimate"]),
                    "lower_bound": float(interval["lower_bound"]),
                    "upper_bound": float(interval["upper_bound"]),
                }
                for request_class, interval in _intervals_by_class(
                    observed_evidence
                ).items()
            }
            if not observed_intervals:
                continue
            seeds = tuple(
                _stable_seed(family, point["load_point"], configuration) + offset
                for offset in range(replications)
            )
            model_results = {}
            for model_name, model in family_models[family].items():
                target_model = _apply_target(
                    model,
                    point["class_arrival_rates"],
                    point["class_interarrivals"],
                )
                model_results[model_name] = _simulate_model(
                    target_model,
                    configuration,
                    seeds=seeds,
                    workers=workers,
                    warmup_duration=60.0,
                    measurement_duration=120.0,
                    bootstrap_resamples=bootstrap_resamples,
                    bootstrap_seed=_stable_seed(
                        "bootstrap",
                        family,
                        point["load_point"],
                        configuration,
                    ),
                )
            results.append(
                {
                    "family": family,
                    "load_point": point["load_point"],
                    "configuration": list(configuration),
                    "evidence_gate_passed": bool(point["evidence_gate"]["passed"]),
                    "observed_classification": observed_evidence["status"],
                    "empirical_minimum_configuration": point["quality"].get(
                        "empirical_minimum_configuration"
                    ),
                    "empirical_minimum_certified": bool(
                        point["quality"].get(
                            "minimum_within_declared_space_certified"
                        )
                    ),
                    "observed_intervals": observed_intervals,
                    "simulation_seeds": list(seeds),
                    "models": model_results,
                }
            )
    return {
        "method": "retained-low-load-characterization-to-heldout-native-simulation",
        "analysis_source": {
            "path": str(Path(analysis_path).resolve()),
            "origin": analysis_origin,
            "sha256": _sha256(analysis_path),
            "plan_fingerprint": analysis.get("plan_fingerprint"),
        },
        "simulation_design": {
            "replications": replications,
            "warmup_duration_seconds": 60.0,
            "measurement_duration_seconds": 120.0,
            "common_random_numbers_across_models": True,
            "bootstrap_resample_count": bootstrap_resamples,
            "latency_statistic": "pooled mean with replication-cluster bootstrap",
        },
        "model_semantics": {
            MODEL_BASELINE: (
                "existing synthetic workload with privileged processing-occupancy "
                "distribution and fitted fixed request overhead"
            ),
            MODEL_TELEMETRY: (
                "measured CPU demand as processor-sharing work plus deterministic "
                "low-load Istio residual as non-CPU blocking; blocking retains "
                "the declared admission-concurrency slot"
            ),
            MODEL_TELEMETRY_BOTTLENECK: (
                "the telemetry CPU-plus-blocking model with the synthetic "
                "application's declared narrow processing-pool size used as the "
                "blocking concurrency limit"
            ),
            MODEL_TELEMETRY_VARIABILITY: (
                "declared-bottleneck telemetry model with leaf blocking "
                "lognormal SCV fitted from low-load Istio mean and p50"
            ),
            MODEL_ORACLE: (
                "privileged synthetic processing-occupancy distribution plus only "
                "the remaining low-load Istio residual as blocking"
            ),
        },
        "limitations": [
            "service blocking tails are not identifiable and are deterministic",
            "parallel child maximum is approximated by the maximum child mean",
            "predicted intervals include simulation randomness but not characterization uncertainty",
            "telemetry cannot identify a hidden serial non-CPU processing semaphore",
            "declared-bottleneck results require application/deployment concurrency configuration which counters alone do not reveal",
            "fitted blocking variability assumes a lognormal leaf residual and does not subtract independently aggregated percentiles",
            "oracle_occupancy_blocking uses synthetic ground truth unavailable on a general deployment",
        ],
        "characterization_sources": characterization_sources,
        "characterization_diagnostics": characterization_diagnostics,
        "results": results,
        "summaries": {
            "all_selected": summarize_results(results),
            "evidence_gate_passed": summarize_results(results, accepted_only=True),
            "target_validation": summarize_results(
                results,
                target_validation_only=True,
            ),
            "certified_sizing": summarize_certified_sizing(results),
        },
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis", required=True, type=Path)
    parser.add_argument(
        "--analysis-origin",
        help="Stable provenance URI for a locally copied analysis artifact",
    )
    parser.add_argument("--load-tests-root", required=True, type=Path)
    parser.add_argument(
        "--points",
        choices=("target-validation", "evidence-passed", "all"),
        default="target-validation",
    )
    parser.add_argument("--replications", type=int, default=30)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--bootstrap-resamples", type=int, default=20_000)
    parser.add_argument("--out", type=Path)
    arguments = parser.parse_args(argv)
    result = run_validation(
        analysis_path=arguments.analysis,
        analysis_origin=arguments.analysis_origin,
        load_tests_root=arguments.load_tests_root,
        point_selection=arguments.points,
        replications=arguments.replications,
        workers=arguments.workers,
        bootstrap_resamples=arguments.bootstrap_resamples,
    )
    if arguments.out is not None:
        arguments.out.parent.mkdir(parents=True, exist_ok=True)
        arguments.out.write_text(
            yaml.safe_dump(result, sort_keys=False),
            encoding="utf-8",
        )
    print(yaml.safe_dump(result["summaries"], sort_keys=False), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

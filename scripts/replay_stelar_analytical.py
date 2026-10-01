#!/usr/bin/env python3
"""Read-only retrospective replay; see docs/STELAR_REPLAY_PROTOCOL.md."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from performance_models import QueueingNetworkType, System
from performance_models.layered import LayeredAnalyticalSystem


def read_yaml(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _series(window, key, label):
    result = defaultdict(float)
    for sample in window.get(key, []):
        value = float(sample["value"])
        if math.isfinite(value):
            result[sample["labels"][label]] += value
    return result


def characterize(root, operation):
    """Only low-load rate-4 files may enter this function."""
    counts, cpu_seconds, response_sums, edge_counts = (
        defaultdict(float) for _ in range(4)
    )
    response_counts = defaultdict(float)
    client_sum, client_count, gaps = 0.0, 0, []
    provenance = {}
    for repetition in (1, 2):
        directory = root / operation / "rate-4" / f"rep-{repetition}"
        measurement = read_yaml(directory / "measurement.yaml")
        cohort = read_yaml(directory / "request_cohort.yaml")
        config = read_yaml(directory / "config.yaml")
        duration = float(config["measurement"]["stable_window_seconds"])
        endpoints = measurement["raw"]["stable_window_end_times"]
        if any(
            not math.isclose(b - a, duration, abs_tol=0.02)
            for a, b in zip(endpoints, endpoints[1:])
        ):
            raise ValueError(
                "Characterization windows must be common and non-overlapping."
            )
        if cohort["censored_count"] or cohort["failure_count"]:
            raise ValueError("Characterization must be complete and failure-free.")
        pod_to_service = {
            pod: service
            for service, details in measurement["services"].items()
            for pod in details["replicas"]
        }
        for window in measurement["raw"]["stable_window_samples"]:
            throughput = _series(window, "pod_throughput", "pod")
            application_cpu = _series(window, "cpu_usage", "pod")
            rates = _series(window, "service_rates", "destination_workload")
            means = _series(window, "service_duration_mean", "destination_workload")
            for pod, rate in throughput.items():
                if rate > 0:
                    if pod not in application_cpu:
                        raise ValueError(f"Missing matched application CPU for {pod}.")
                    service = pod_to_service[pod]
                    counts[service] += rate * duration
                    cpu_seconds[service] += application_cpu[pod] * duration
            for service, rate in rates.items():
                response_counts[service] += rate * duration
                response_sums[service] += (
                    rate * duration * means.get(service, 0.0) / 1000
                )
            for sample in window["edge_rates"]:
                labels = sample["labels"]
                edge_counts[
                    labels["source_workload"], labels["destination_workload"]
                ] += (float(sample["value"]) * duration)
        n = int(cohort["started_count"])
        client_sum += n * float(cohort["latency_ms"]["mean"]) / 1000
        client_count += n
        trace = directory / f"request_trace.{operation}.csv"
        with trace.open(newline="", encoding="utf-8") as stream:
            timestamps = sorted(
                float(row["started_epoch"])
                for row in csv.DictReader(stream)
                if float(cohort["measurement_start_epoch"])
                <= float(row["started_epoch"])
                < float(cohort["measurement_end_epoch"])
            )
        gaps.extend(b - a for a, b in zip(timestamps, timestamps[1:]))
        for filename in (
            "measurement.yaml",
            "request_cohort.yaml",
            "config.yaml",
            trace.name,
        ):
            path = directory / filename
            provenance[str(path.relative_to(root))] = _hash(path)
    cpu = {s: cpu_seconds[s] / count for s, count in counts.items() if count > 0}
    observed = {s: response_sums[s] / response_counts[s] for s in cpu}
    calls = defaultdict(dict)
    for (source, target), count in edge_counts.items():
        if count > 0 and source in cpu and target in cpu:
            calls[source][target] = count / counts[source]
    residual = client_sum / client_count - observed["stelarapi"]
    if residual < 0:
        raise ValueError(
            "Negative client-to-entry residual; boundary is not separable."
        )
    gap_mean = sum(gaps) / len(gaps)
    arrival_scv = sum((gap - gap_mean) ** 2 for gap in gaps) / len(gaps) / gap_mean**2
    services, issues = {}, []
    worker_sensitivity = {"ckan": 2, "stelarapi": 4}
    for service, demand in cpu.items():
        block = (
            observed[service]
            - demand
            - sum(
                count * observed[target]
                for target, count in calls.get(service, {}).items()
            )
        )
        specification = {
            "distribution": "deterministic",
            "mean": demand,
            "execution_model": "processor_sharing",
            "replica_queue_model": "independent_random",
        }
        if service in worker_sensitivity:
            specification["concurrency_limit_per_replica"] = worker_sensitivity[service]
        if block > 0:
            specification["non_cpu_blocking_time"] = {
                "distribution": "deterministic",
                "mean": block,
            }
        else:
            issues.append(
                f"{service}: blocking residual {block:.6g}s is nonpositive/unseparable and omitted"
            )
        services[service] = specification
    workload = {
        "request_classes": {
            operation: {
                "concurrency_weight": 1.0,
                "calls": dict(calls),
                "call_execution": "sequential",
                "latency_boundary": "external_client",
                "latency_components": {
                    "client_to_entry": {
                        "distribution": "deterministic",
                        "mean": residual,
                    }
                },
                "end_to_end_latency_slos": {"mean_seconds": 0.5},
            }
        },
        "traffic_model": {
            "entry_services": {
                operation: {
                    "stelarapi": {
                        "distribution": "gamma",
                        "rate": 4.0,
                        "scv": arrival_scv,
                    }
                }
            },
            "service_times": services,
        },
    }
    return workload, {
        "hashes": provenance,
        "issues": issues,
        "capacity_basis": "one-core-normalized sensitivity; CPU request not recorded",
        "worker_pool_basis": "historical declared-count sensitivity, not telemetry inference",
        "cpu_demand_seconds": cpu,
        "observed_lowload_response_seconds": observed,
        "client_to_entry_mean_seconds": residual,
        "arrival_scv": arrival_scv,
    }


def _finite(value):
    return value if math.isfinite(value) else None


def evaluate(workload, directory, operation):
    # Target telemetry is accessed only for replica counts; all demands,
    # variability, residuals and call graphs remain from characterization.
    target = read_yaml(directory / "measurement.yaml")
    cohort = read_yaml(directory / "request_cohort.yaml")
    model = copy.deepcopy(workload)
    rate = float(cohort["scheduled_rate_rps"])
    model["traffic_model"]["entry_services"][operation]["stelarapi"]["rate"] = rate
    system = System("stelar-replay", model, QueueingNetworkType.OPEN)
    configuration = tuple(
        int(target["services"][s.get_name()]["replica_count"])
        for s in sorted(system.get_services().values(), key=lambda s: s.get_id())
    )
    system.configure_system_replicas(configuration)
    baseline = system.get_request_class_mean_latencies()[operation]
    layered = LayeredAnalyticalSystem(system)
    enriched = layered.get_request_class_mean_latencies()[operation]
    unknown = copy.deepcopy(model)
    for spec in unknown["traffic_model"]["service_times"].values():
        spec.pop("concurrency_limit_per_replica", None)
    unknown_system = System("unknown-pools", unknown, QueueingNetworkType.OPEN)
    unknown_system.configure_system_replicas(configuration)
    unknown_mean = LayeredAnalyticalSystem(
        unknown_system
    ).get_request_class_mean_latencies()[operation]
    observed = float(cohort["latency_ms"]["mean"]) / 1000
    failure_rate = float(cohort["failure_count"]) / int(cohort["completed_count"])
    censored = int(cohort["censored_count"])
    healthy = failure_rate < 0.01 and censored == 0
    unsafe = not healthy or observed > 0.5
    return {
        "operation": operation,
        "run": str(directory),
        "offered_rate_rps": rate,
        "configuration": {
            s.get_name(): configuration[s.get_id()]
            for s in system.get_services().values()
        },
        "observed_mean_seconds": observed,
        "failure_rate": failure_rate,
        "censored_count": censored,
        "mean_accuracy_eligible": healthy,
        "observed_unsafe": unsafe,
        "baseline_strict_incompatibilities": list(
            system.get_strict_analytical_incompatibilities()
        ),
        "predictions": {
            "baseline_diagnostic": _finite(baseline),
            "experimental_layered": _finite(enriched),
            "layered_unknown_pool_sensitivity": _finite(unknown_mean),
        },
        "model_false_feasible": {
            "baseline_diagnostic": baseline <= 0.5 and unsafe,
            "experimental_layered": layered.solve().get_stability()
            and enriched <= 0.5
            and unsafe,
        },
        "layered_stable": bool(layered.solve().get_stability()),
        "layered_details": layered.get_layered_model_details(),
        "hashes": {
            f: _hash(directory / f) for f in ("measurement.yaml", "request_cohort.yaml")
        },
    }


def _json_safe(value):
    if isinstance(value, float):
        return _finite(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = args.data_root.resolve()
    models, characterizations = {}, {}
    # Complete and freeze characterization objects before any target scoring.
    for operation in ("dataset_search", "organizations"):
        models[operation], characterizations[operation] = characterize(root, operation)
    rows = [
        evaluate(
            models[operation],
            root / operation / f"rate-{rate}" / f"rep-{rep}",
            operation,
        )
        for operation in models
        for rate in (32, 56)
        for rep in (1, 2)
    ]
    summary = {}
    for method in rows[0]["predictions"]:
        eligible = [r for r in rows if r["mean_accuracy_eligible"]]
        finite = [r for r in eligible if r["predictions"][method] is not None]
        summary[method] = {
            "healthy_cases": len(eligible),
            "finite_predictions": len(finite),
            "mean_absolute_percentage_error": (
                sum(
                    abs(r["predictions"][method] - r["observed_mean_seconds"])
                    / r["observed_mean_seconds"]
                    for r in finite
                )
                / len(finite)
                if finite
                else None
            ),
            "false_feasible_all_runs": (
                sum(r["model_false_feasible"].get(method, False) for r in rows)
                if method != "layered_unknown_pool_sensitivity"
                else None
            ),
        }
    result = _json_safe(
        {
            "status": "RETROSPECTIVE_EXPERIMENTAL_COMPARISON",
            "implementation_commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
            ).strip(),
            "protocol": "docs/STELAR_REPLAY_PROTOCOL.md",
            "models": models,
            "characterizations": characterizations,
            "summary": summary,
            "runs": rows,
        }
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

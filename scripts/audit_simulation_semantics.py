"""Offline, read-only-to-source audit of a frozen prediction replay.

This is a diagnostic, not a sizing entry point or a fitted replacement model.
It never labels finite cohort completion as proof of stationary operation.
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
from pathlib import Path


METHODS = ("analytical_layered", "simulation_fitted")


def accuracy_comparison(rows):
    """Use the intersection for accuracy; retain each method's full coverage."""
    groups = {}
    for row in rows:
        if row["method"] not in METHODS or row["primary_latency_eligible"] != "True":
            continue
        group = groups.setdefault((row["run"], row["request_class"]), {})
        if row["method"] in group:
            raise ValueError("Duplicate run/class/method observation")
        group[row["method"]] = row

    def available(group, method):
        row = group.get(method, {})
        if row.get("prediction_available") != "True":
            return False
        observed, predicted = float(row["observed_successful_mean_ms"]), float(
            row["predicted_mean_ms"]
        )
        if not (
            math.isfinite(observed)
            and observed > 0
            and math.isfinite(predicted)
            and predicted >= 0
        ):
            raise ValueError("Available prediction has invalid latency")
        return True

    common = [g for g in groups.values() if all(available(g, m) for m in METHODS)]
    for group in common:
        if len({float(group[m]["observed_successful_mean_ms"]) for m in METHODS}) != 1:
            raise ValueError("Methods disagree on the observed latency")
    scores = {}
    for method in METHODS:
        errors = [
            100
            * abs(
                float(g[method]["predicted_mean_ms"])
                / float(g[method]["observed_successful_mean_ms"])
                - 1
            )
            for g in common
        ]
        scores[method] = {
            "finite_observations": sum(available(g, method) for g in groups.values()),
            "matched_mape_percent": statistics.fmean(errors) if errors else None,
            "matched_median_ape_percent": statistics.median(errors) if errors else None,
        }
    return {
        "eligible_observations": len(groups),
        "matched_observations": len(common),
        "methods": scores,
    }


def trend_summary(early_late_pairs, *, relative_tolerance=0.10):
    """Screen independent-replication contrasts; buckets are NOT IID samples.

    A three-SE lower margin above 10% of the mean flags material upward drift.
    A two-sided three-SE interval wholly inside that band finds no material
    drift. Everything else is inconclusive. This is a diagnostic heuristic,
    not a stationarity proof, familywise test, or confidence bound for an SLO.
    """
    if not math.isfinite(relative_tolerance) or relative_tolerance <= 0:
        raise ValueError("relative_tolerance must be positive and finite")
    pairs = list(early_late_pairs)
    if any(
        len(pair) != 2 or any(not math.isfinite(x) or x < 0 for x in pair)
        for pair in pairs
    ):
        raise ValueError("Contrasts must be finite, non-negative pairs")
    if len(pairs) < 3:
        return {
            "status": "inconclusive",
            "reason": "Need at least three independent replications",
        }
    early, late = (statistics.fmean(p[i] for p in pairs) for i in (0, 1))
    differences = [b - a for a, b in pairs]
    delta = statistics.fmean(differences)
    se = statistics.stdev(differences) / math.sqrt(len(differences))
    threshold = relative_tolerance * (early + late) / 2
    if delta - 3 * se > threshold:
        status = "growth_detected"
    elif abs(delta) + 3 * se <= threshold:
        status = "no_material_drift_detected"
    else:
        status = "inconclusive"
    return {
        "status": status,
        "early_mean": early,
        "late_mean": late,
        "late_minus_early": delta,
        "contrast_standard_error": se,
        "practical_threshold": threshold,
        "replications": len(pairs),
    }


def cohort_summary(records):
    """Never turn successful cohort drain into a stable=True assertion."""
    if not records:
        raise ValueError("No replications")
    classes = set(records[0].request_class_latencies)
    pairs = {c: [] for c in classes}
    means = {c: [] for c in classes}
    for record in records:
        if set(record.request_class_latencies) != classes:
            raise ValueError("Inconsistent request classes")
        if record.tagged_requests_in_flight_at_end or any(
            record.failed_measured_request_counts.values()
        ):
            return {"status": "incomplete_or_failed", "classes": {}}
        for c in classes:
            arrivals, latencies = (
                record.request_class_arrival_times[c],
                record.request_class_latencies[c],
            )
            if (
                len(arrivals) != len(latencies)
                or len(latencies) != record.measured_request_counts[c]
            ):
                raise ValueError("Mismatched cohort arrays/counts")
            begin, duration = record.warmup_duration, record.measurement_duration
            if any(
                not math.isfinite(t) or not begin <= t < begin + duration
                for t in arrivals
            ):
                raise ValueError("Arrival outside measurement cohort")
            if any(not math.isfinite(v) or v < 0 for v in latencies):
                raise ValueError("Invalid latency")
            if latencies:
                means[c].append(statistics.fmean(latencies))
            halves = [
                [
                    v
                    for t, v in zip(arrivals, latencies)
                    if (t >= begin + duration / 2) == late
                ]
                for late in (False, True)
            ]
            if min(map(len, halves)) >= 30:
                pairs[c].append(tuple(map(statistics.fmean, halves)))
    details = {}
    for c in sorted(classes):
        trend = (
            trend_summary(pairs[c])
            if len(pairs[c]) == len(records)
            else {
                "status": "inconclusive",
                "reason": "Fewer than 30 arrivals per half in a replication",
            }
        )
        details[c] = {
            "finite_window_mean_ms": (
                1000 * statistics.fmean(means[c]) if means[c] else None
            ),
            "trend_seconds": trend,
        }
    statuses = {v["trend_seconds"]["status"] for v in details.values()}
    status = (
        "growth_detected"
        if "growth_detected" in statuses
        else (
            "no_material_drift_detected"
            if statuses == {"no_material_drift_detected"}
            else "inconclusive"
        )
    )
    return {
        "status": status,
        "classes": details,
        "mean_active_at_drain_end": statistics.fmean(
            r.active_requests_at_end for r in records
        ),
        "notice": "Finite-window evidence only; background arrivals continue during drain.",
    }


def state_summary(ensembles, warmup):
    names = ensembles[0].service_names
    first = round(warmup / ensembles[0].bucket_width)
    if not math.isclose(first * ensembles[0].bucket_width, warmup):
        raise ValueError("Warmup must align to state buckets")
    vectors = {"active_requests": [e.active_requests[first:] for e in ensembles]}
    for i, name in enumerate(names):
        vectors[f"queue:{name}"] = [
            e.queue_length_by_service[i][first:] for e in ensembles
        ]
    trends = {}
    for name, replicates in vectors.items():
        if any(len(values) < 4 or len(values) % 2 for values in replicates):
            raise ValueError("Need an even number of at least four post-warmup buckets")
        pairs = [
            (statistics.fmean(v[: len(v) // 2]), statistics.fmean(v[len(v) // 2 :]))
            for v in replicates
        ]
        trends[name] = trend_summary(pairs)
    return {
        "trends": trends,
        "bucket_width_seconds": ensembles[0].bucket_width,
        "mean_buckets": {
            name: [statistics.fmean(v) for v in zip(*replicates)]
            for name, replicates in vectors.items()
        },
        "arrival_rps_including_warmup": statistics.fmean(
            e.external_arrival_count / e.horizon for e in ensembles
        ),
        "completion_rps_including_warmup": statistics.fmean(
            e.completed_request_count / e.horizon for e in ensembles
        ),
        "cpu_busy_fraction_including_warmup": {
            name: statistics.fmean(
                statistics.fmean(e.busy_replicas_by_service[i]) for e in ensembles
            )
            / ensembles[0].replica_configuration[i]
            for i, name in enumerate(names)
        },
    }


def diagnostic_variants(job):
    """One-factor counterfactuals; NEVER recommendations or corrected models."""
    yield "frozen", copy.deepcopy(job["model"])
    for service in ("repeat-front", "identity"):
        model = copy.deepcopy(job["model"])
        model["traffic_model"]["service_times"][service][
            "concurrency_limit_per_replica"
        ] = 10000
        yield f"diagnostic_large_pool_{service}", model
        model = copy.deepcopy(job["model"])
        model["traffic_model"]["service_times"][service][
            "execution_model"
        ] = "fixed_slots"
        yield f"diagnostic_fcfs_{service}", model


def worker_holding_ms(pod, slots, window_seconds):
    """Use the same covered interval for occupancy and completed counters."""
    coverage = pod["coverage_fraction"]
    if not 0 < coverage <= 1 or pod["completed"] <= 0:
        raise ValueError("Worker holding estimate needs covered completion evidence")
    return (
        1000
        * pod["admitted_fraction"]
        * slots
        * window_seconds
        * coverage
        / pod["completed"]
    )


def inspect_system(system, backend, seeds, duration, drain):
    """Cohort trends and state trends use the same independent seeds."""
    try:
        records = backend.measurement_replications(
            system,
            seeds=seeds,
            warmup_duration=120,
            measurement_duration=duration,
            workers=2,
            maximum_drain_time=drain,
        )
        cohort = cohort_summary(records)
    except RuntimeError as error:
        cohort = {"status": "execution_failed", "reason": str(error)}
    ensembles = [
        backend.warmup_ensemble(
            system,
            seeds=(seed,),
            horizon=120 + duration,
            bucket_width=30,
            workers=1,
        )
        for seed in seeds
    ]
    state = state_summary(ensembles, 120)
    # A consistent active-population trend or latency trend suffices to flag
    # growth. Individual service queues are diagnostics, not multiple tests
    # that can silently turn every busy network into a rejection.
    statuses = {cohort["status"], state["trends"]["active_requests"]["status"]}
    status = (
        "growth_detected"
        if "growth_detected" in statuses
        else (
            "no_material_drift_detected"
            if statuses == {"no_material_drift_detected"}
            else "inconclusive"
        )
    )
    return {"status": status, "cohort": cohort, "state": state}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--sim-repo", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--screen-all",
        action="store_true",
        help="Screen all originally finite simulation jobs, without changing predictions",
    )
    args = parser.parse_args()
    snapshot, sim_repo, output = (
        p.resolve() for p in (args.snapshot, args.sim_repo, args.out)
    )
    if output == snapshot or snapshot in output.parents:
        parser.error("Output must be separate from the frozen snapshot")
    if output.exists():
        parser.error(
            "Output must be a new directory; existing audits are never overwritten"
        )
    if subprocess.check_output(
        ["git", "-C", str(sim_repo), "status", "--porcelain", "--untracked-files=no"],
        text=True,
    ).strip():
        parser.error("Use a clean simulator worktree to bind the audit to a revision")
    sys.path.insert(0, str(sim_repo))
    os.environ["KUBESIZER_CPP_BACKEND"] = str(
        sim_repo / "native_backend/cpp/build-release/kubesizer-simulator"
    )
    from native_backend import get_native_backend
    from performance_models import QueueingNetworkType, System
    import yaml

    bindings = {}
    bindings[str(Path(__file__).resolve())] = hashlib.sha256(
        Path(__file__).read_bytes()
    ).hexdigest()

    def read(path):
        raw = path.read_bytes()
        bindings[str(path)] = hashlib.sha256(raw).hexdigest()
        return (
            json.loads(raw)
            if path.suffix == ".json"
            else yaml.load(raw, Loader=yaml.CSafeLoader)
        )

    settings = read(snapshot / "prediction-jobs.json")
    freeze = read(snapshot / "freeze.json")
    original_predictions = read(snapshot / "simulation-predictions.json")
    scores_path = snapshot / "latency-scores.csv"
    bindings[str(scores_path)] = hashlib.sha256(scores_path.read_bytes()).hexdigest()
    with scores_path.open() as handle:
        comparison = accuracy_comparison(list(csv.DictReader(handle)))
    selected = next(
        (key, j)
        for key, j in settings["jobs"].items()
        if j["family"] == "repeated_calls"
        and j["load_point"] == "poisson_u080_c1_baseline"
    )
    seeds = settings["seeds"]  # All five original seeds; no seed selection.
    backend = get_native_backend()
    backend.ensure_available()
    bindings[str(backend.binary_path)] = hashlib.sha256(
        backend.binary_path.read_bytes()
    ).hexdigest()

    def make_system(name, model, job):
        system = System(name, model, QueueingNetworkType.OPEN)
        system.configure_system_replicas(
            tuple(
                job["configuration"][system.get_service_by_id(i).get_name()]
                for i in range(len(system.get_services()))
            )
        )
        return system

    diagnostics = []
    for name, model in diagnostic_variants(selected[1]):
        for duration in (300, 600, 1200) if name == "frozen" else (600,):
            print(f"Diagnostic {name}, measurement={duration}s", flush=True)
            system = make_system(name, model, selected[1])
            # Direct backend call deliberately permits testing overload. This
            # must never be confused with bypassing a recommendation gate.
            record = {
                "variant": name,
                "measurement_seconds": duration,
                "model_sha256": hashlib.sha256(
                    json.dumps(model, sort_keys=True).encode()
                ).hexdigest(),
                "necessary_capacity_passed": bool(system.solve().get_stability()),
            }
            record.update(inspect_system(system, backend, seeds, duration, 120))
            diagnostics.append(record)

    screening = {}
    if args.screen_all:
        for index, (key, job) in enumerate(settings["jobs"].items(), 1):
            if not original_predictions[key]["stable"]:
                screening[key] = {"status": "original_abstention"}
                continue
            print(
                f"Screen {index}/{len(settings['jobs'])}: {job['family']} {job['load_point']}",
                flush=True,
            )
            system = make_system("unchanged-frozen-screen", job["model"], job)
            screening[key] = inspect_system(system, backend, seeds, 600, 30)
            screening[key]["family"] = job["family"]
            screening[key]["load_point"] = job["load_point"]

    # Worker telemetry is scored AFTER prediction. It never modifies a model.
    worker_runs = []
    selected_runs = []
    for stage_name in ("characterization", "core"):
        stage = snapshot / "source/campaign/repeated_calls" / stage_name
        manifest = read(stage / "study_manifest.yaml")
        for run in manifest["runs"]:
            selected_run = (
                run["id"]
                in freeze["families"]["repeated_calls"][
                    "included_characterization_runs"
                ]
                if stage_name == "characterization"
                else "poisson_u080_c1_baseline" in run["id"]
            )
            if run["status"] == "COMPLETED" and selected_run:
                selected_runs.append((stage, run))
    for stage, run in selected_runs:
        path = stage / "runs" / run["id"]
        measurement = read(path / "measurement.yaml")
        if (
            bindings[str(path / "measurement.yaml")]
            != run["artifact_sha256"]["measurement.yaml"]
        ):
            raise ValueError("Measurement hash mismatch")
        evidence = read(path / "method-independent-evidence.yaml")
        receipt = read(stage / "evidence" / run["id"] / "receipt.yaml")
        # Receipt keys are absolute VM paths; bind by the exact run suffix.
        matches = [
            v
            for k, v in receipt["sha256"].items()
            if k.endswith(f"/runs/{run['id']}/method-independent-evidence.yaml")
        ]
        if matches != [bindings[str(path / "method-independent-evidence.yaml")]]:
            raise ValueError("Worker summary receipt mismatch")
        cpu, requests = {}, {}
        for window in measurement["raw"]["stable_window_samples"]:
            rates = {
                r["labels"]["pod"]: float(r["value"]) for r in window["pod_throughput"]
            }
            pod_service = {
                r["labels"]["pod"]: r["labels"]["destination_workload"]
                for r in window["pod_throughput"]
            }
            pod_cpu = {}
            for sample in window["cpu_usage"]:
                pod = sample["labels"]["pod"]
                pod_cpu[pod] = pod_cpu.get(pod, 0) + float(sample["value"])
            for pod, usage in pod_cpu.items():
                if pod not in rates or rates[pod] <= 0:
                    continue
                service = pod_service[pod]
                cpu[service] = cpu.get(service, 0) + usage
                requests[service] = requests.get(service, 0) + rates[pod]
        worker_runs.append(
            {
                "run": run["id"],
                "stage": stage.name,
                "evidence": evidence["workers"],
                "cpu_seconds_per_request_diagnostic_only": {
                    s: cpu[s] / requests[s] for s in cpu
                },
                "little_law_estimates_diagnostic_only": [
                    {
                        "service": pod["service"],
                        "holding_ms": worker_holding_ms(
                            pod,
                            selected[1]["model"]["traffic_model"]["service_times"][
                                pod["service"]
                            ]["concurrency_limit_per_replica"],
                            measurement["measurement"]["window_seconds"]
                            * measurement["measurement"]["stable_window_count"],
                        ),
                        "notice": "Finite-window occupancy/throughput estimate; not an exact per-request timing measurement or a model input.",
                    }
                    for pod in evidence["workers"]["pods"]
                ],
                "service_response_means_ms": {
                    s: v["istio_response_time_ms"]["mean"]
                    for s, v in measurement["services"].items()
                },
            }
        )

    result = {
        "purpose": "Retrospective diagnosis, not an accuracy-improved fitted model",
        "simulator_revision": subprocess.check_output(
            ["git", "-C", str(sim_repo), "rev-parse", "HEAD"], text=True
        ).strip(),
        "comparison": comparison,
        "case_key": selected[0],
        "seeds": seeds,
        "warmup_seconds": 120,
        "maximum_drain_seconds": 120,
        "diagnostics": diagnostics,
        "screening": screening,
        "worker_runs": worker_runs,
        "source_sha256": bindings,
    }
    # Check preservation of all bound source artifacts, including the binary.
    if any(
        hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest
        for path, digest in bindings.items()
    ):
        raise ValueError("An input changed during the audit")
    output.mkdir(parents=True)
    (output / "audit.json").write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n"
    )
    print(f"Saved {output / 'audit.json'}", flush=True)


if __name__ == "__main__":
    main()

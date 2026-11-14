"""Audit why a small prediction-error tail dominates mean absolute error.

This is a retrospective diagnostic.  It never rewrites frozen predictions and
does not treat removing nonstationary cases as an accuracy improvement.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


METHODS = ("analytical_layered", "simulation_fitted")
CELL_FIELDS = ("family", "configuration", "load_point", "request_class")


def _finite_positive(value, name):
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise ValueError(f"{name} must be finite and positive")
    return result


def nearest_rank(values, probability):
    values = sorted(float(value) for value in values)
    if not values or not 0.0 < probability <= 1.0:
        raise ValueError("nearest_rank needs values and a probability in (0, 1]")
    return values[max(0, math.ceil(probability * len(values)) - 1)]


def error_summary(values):
    values = tuple(float(value) for value in values)
    if not values:
        return {
            "count": 0,
            "mean_ape_percent": None,
            "median_ape_percent": None,
            "p90_ape_percent": None,
            "p95_ape_percent": None,
            "maximum_ape_percent": None,
        }
    if any(not math.isfinite(value) or value < 0.0 for value in values):
        raise ValueError("Errors must be finite and non-negative")
    return {
        "count": len(values),
        "mean_ape_percent": statistics.fmean(values),
        "median_ape_percent": statistics.median(values),
        "p90_ape_percent": nearest_rank(values, 0.90),
        "p95_ape_percent": nearest_rank(values, 0.95),
        "maximum_ape_percent": max(values),
    }


def read_score_rows(path):
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {
        "run",
        "method",
        "primary_latency_eligible",
        "prediction_available",
        "observed_successful_mean_ms",
        "predicted_mean_ms",
        *CELL_FIELDS,
    }
    if not rows or not required <= set(rows[0]):
        raise ValueError(f"{path} is not a compatible latency score table")
    return rows


def scored_rows(rows, method):
    if method not in METHODS:
        raise ValueError(f"Unknown method {method!r}")
    selected = []
    for row in rows:
        if (
            row["method"] != method
            or row["primary_latency_eligible"] != "True"
            or row["prediction_available"] != "True"
        ):
            continue
        observed = _finite_positive(
            row["observed_successful_mean_ms"], "observed mean"
        )
        predicted = _finite_positive(row["predicted_mean_ms"], "predicted mean")
        selected.append((row, observed, predicted))
    return selected


def aggregate_cells(rows, method, diagnostic_statuses=None):
    """Average independent deployment repetitions before scoring a cell.

    Each repetition remains equally weighted.  This is intentionally not a
    request-pooled latency mean; the score table does not contain the completed
    request counts needed for such pooling.
    """
    groups = defaultdict(list)
    for row, observed, predicted in scored_rows(rows, method):
        groups[tuple(row[field] for field in CELL_FIELDS)].append(
            (row, observed, predicted)
        )
    statuses = diagnostic_statuses or {}
    cells = []
    for key, observations in sorted(groups.items()):
        observed = statistics.fmean(item[1] for item in observations)
        predicted = statistics.fmean(item[2] for item in observations)
        predicted_range = max(item[2] for item in observations) - min(
            item[2] for item in observations
        )
        if predicted_range > max(1e-9, 1e-12 * predicted):
            raise ValueError(f"Frozen prediction changed across repetitions for {key}")
        signed = 100.0 * (predicted / observed - 1.0)
        identity = dict(zip(CELL_FIELDS, key, strict=True))
        identity.update(
            {
                "method": method,
                "repetition_count": len(observations),
                "observed_replication_mean_ms": observed,
                "predicted_mean_ms": predicted,
                "ape_percent": abs(signed),
                "signed_error_percent": signed,
                "diagnostic_status": statuses.get(
                    (identity["family"], identity["load_point"]),
                    "not_screened",
                ),
            }
        )
        cells.append(identity)
    return cells


def load_diagnostic_statuses(path):
    if path is None:
        return {}
    value = json.loads(path.read_text())
    statuses = {}
    for item in value.get("screening", {}).values():
        family, load = item.get("family"), item.get("load_point")
        if family is None or load is None:
            continue
        key = (family, load)
        status = item.get("status", "unknown")
        if key in statuses and statuses[key] != status:
            raise ValueError(f"Conflicting diagnostic statuses for {key}")
        statuses[key] = status
    return statuses


def trend_summary(pairs, relative_tolerance=0.10):
    """Screen early/late contrasts across independent replications."""
    pairs = tuple(pairs)
    if len(pairs) < 3:
        return {"status": "inconclusive", "reason": "fewer than 3 replications"}
    differences = tuple(late - early for early, late in pairs)
    early = statistics.fmean(pair[0] for pair in pairs)
    late = statistics.fmean(pair[1] for pair in pairs)
    delta = statistics.fmean(differences)
    standard_error = statistics.stdev(differences) / math.sqrt(len(differences))
    threshold = relative_tolerance * (early + late) / 2.0
    if delta - 3.0 * standard_error > threshold:
        status = "growth_detected"
    elif abs(delta) + 3.0 * standard_error <= threshold:
        status = "no_material_drift_detected"
    else:
        status = "inconclusive"
    return {
        "status": status,
        "early_mean_seconds": early,
        "late_mean_seconds": late,
        "late_minus_early_seconds": delta,
        "contrast_standard_error_seconds": standard_error,
        "practical_threshold_seconds": threshold,
        "replications": len(pairs),
    }


def _job_key(job):
    return (
        job["family"],
        job["load_point"],
        tuple(sorted(job["configuration"].items())),
    )


def select_tail_jobs(cells, jobs, count):
    selected = []
    selected_keys = set()
    lookup = {_job_key(job): (key, job) for key, job in jobs.items()}
    for cell in sorted(cells, key=lambda item: item["ape_percent"], reverse=True):
        if cell["diagnostic_status"] == "growth_detected":
            continue
        key = (
            cell["family"],
            cell["load_point"],
            tuple(),
        )
        matches = [
            value
            for job_key, value in lookup.items()
            if job_key[:2] == key[:2]
        ]
        if len(matches) != 1:
            raise ValueError(f"Expected one prediction job for {key[:2]}")
        job_id, job = matches[0]
        if job_id in selected_keys:
            continue
        selected.append((job_id, job, cell["ape_percent"]))
        selected_keys.add(job_id)
        if len(selected) == count:
            break
    return selected


def diagnose_tail_jobs(snapshot, sim_repo, cells, horizons, job_count, workers):
    if subprocess.check_output(
        ["git", "-C", str(sim_repo), "status", "--porcelain", "--untracked-files=no"],
        text=True,
    ).strip():
        raise ValueError("The diagnostic simulator worktree must be clean")
    settings = json.loads((snapshot / "prediction-jobs.json").read_text())
    jobs = settings["jobs"]
    selected = select_tail_jobs(cells, jobs, job_count)
    binary = sim_repo / "native_backend/cpp/build-release/kubesizer-simulator"
    if not binary.is_file():
        raise ValueError(f"Missing diagnostic simulator binary: {binary}")
    sys.path.insert(0, str(sim_repo))
    os.environ["KUBESIZER_CPP_BACKEND"] = str(binary)
    from native_backend import get_native_backend
    from performance_models import QueueingNetworkType, System

    backend = get_native_backend()
    backend.ensure_available()
    results = []
    for index, (job_id, job, original_ape) in enumerate(selected, start=1):
        print(
            f"Tail diagnostic {index}/{len(selected)}: "
            f"{job['family']} {job['load_point']}",
            flush=True,
        )
        system = System(f"tail-{job_id}", job["model"], QueueingNetworkType.OPEN)
        system.configure_system_replicas(
            tuple(
                job["configuration"][system.get_service_by_id(service_id).get_name()]
                for service_id in range(len(system.get_services()))
            )
        )
        record = {
            "job_id": job_id,
            "family": job["family"],
            "load_point": job["load_point"],
            "configuration": job["configuration"],
            "original_largest_cell_ape_percent": original_ape,
            "horizons": [],
        }
        for horizon in horizons:
            try:
                replications = backend.measurement_replications(
                    system,
                    seeds=tuple(settings["seeds"]),
                    warmup_duration=float(settings["warmup_seconds"]),
                    measurement_duration=float(horizon),
                    workers=workers,
                    maximum_drain_time=max(
                        120.0, float(settings["maximum_drain_seconds"])
                    ),
                )
            except Exception as error:
                record["horizons"].append(
                    {
                        "measurement_seconds": horizon,
                        "status": "execution_failed",
                        "reason": str(error),
                    }
                )
                continue
            classes = {}
            for request_class in sorted(replications[0].request_class_latencies):
                means = []
                pairs = []
                for replication in replications:
                    latencies = tuple(
                        replication.request_class_latencies[request_class]
                    )
                    arrivals = tuple(
                        replication.request_class_arrival_times[request_class]
                    )
                    if len(latencies) != len(arrivals):
                        raise ValueError("Latency and arrival arrays disagree")
                    if latencies:
                        means.append(statistics.fmean(latencies))
                    split = float(settings["warmup_seconds"]) + horizon / 2.0
                    early = tuple(
                        latency
                        for arrival, latency in zip(arrivals, latencies, strict=True)
                        if arrival < split
                    )
                    late = tuple(
                        latency
                        for arrival, latency in zip(arrivals, latencies, strict=True)
                        if arrival >= split
                    )
                    if len(early) >= 30 and len(late) >= 30:
                        pairs.append((statistics.fmean(early), statistics.fmean(late)))
                classes[request_class] = {
                    "finite_window_mean_ms": (
                        1000.0 * statistics.fmean(means) if means else None
                    ),
                    "successful_request_count": sum(
                        len(item.request_class_latencies[request_class])
                        for item in replications
                    ),
                    "trend": trend_summary(pairs),
                }
            record["horizons"].append(
                {
                    "measurement_seconds": horizon,
                    "status": "completed",
                    "classes": classes,
                    "mean_active_requests_at_end": statistics.fmean(
                        item.active_requests_at_end for item in replications
                    ),
                    "mean_tagged_requests_in_flight_at_end": statistics.fmean(
                        item.tagged_requests_in_flight_at_end for item in replications
                    ),
                    "mean_queue_lengths_at_end": [
                        statistics.fmean(values)
                        for values in zip(
                            *(item.queue_lengths_at_end for item in replications)
                        )
                    ],
                }
            )
        results.append(record)
    return {
        "simulator_revision": subprocess.check_output(
            ["git", "-C", str(sim_repo), "rev-parse", "HEAD"], text=True
        ).strip(),
        "simulator_binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "seeds": settings["seeds"],
        "warmup_seconds": settings["warmup_seconds"],
        "diagnostic_maximum_drain_seconds": max(
            120.0, float(settings["maximum_drain_seconds"])
        ),
        "jobs": results,
    }


def fmt(value):
    return "—" if value is None else f"{value:.2f}%"


def render_report(result):
    lines = [
        "# Mean-versus-median prediction-error audit",
        "",
        "Both statistics below evaluate **mean-latency predictions**. The median APE "
        "describes a typical experiment cell; mean APE is sensitive to a small error tail.",
        "",
        "Independent deployment repetitions are averaged before cell-level scoring. "
        "They are not pooled by request count because the score table lacks those counts.",
        "",
        "## Error summaries",
        "",
        "| Method | Population | n | Mean APE | Median APE | p95 APE | Maximum APE |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    labels = {
        "analytical_layered": "Enriched analytical",
        "simulation_fitted": "Fitted simulation",
    }
    for method in METHODS:
        item = result["methods"][method]
        for population, label in (
            ("run_level", "individual runs"),
            ("cell_level", "replication-mean cells"),
            ("without_detected_growth", "excluding detected growth"),
        ):
            score = item[population]
            lines.append(
                f"| {labels[method]} | {label} | {score['count']} | "
                f"{fmt(score['mean_ape_percent'])} | {fmt(score['median_ape_percent'])} | "
                f"{fmt(score['p95_ape_percent'])} | {fmt(score['maximum_ape_percent'])} |"
            )
    lines += [
        "",
        "Excluding a detected-growth cell changes the estimand and coverage; it is an "
        "abstention diagnostic, not an accuracy correction.",
        "",
        "## Fitted-simulation error tail",
        "",
        "| Family | Configuration/load | Observed mean | Predicted mean | APE | Drift screen | Error mass |",
        "|---|---|---:|---:|---:|---|---:|",
    ]
    for cell in result["methods"]["simulation_fitted"]["tail_cells"]:
        lines.append(
            f"| {cell['family']} | {cell['configuration']} / {cell['load_point']} | "
            f"{cell['observed_replication_mean_ms']:.2f} ms | "
            f"{cell['predicted_mean_ms']:.2f} ms | {cell['ape_percent']:.2f}% | "
            f"{cell['diagnostic_status']} | {cell['error_mass_percent']:.2f}% |"
        )
    diagnostics = result.get("tail_diagnostics")
    if diagnostics:
        lines += [
            "",
            "## Horizon sensitivity of the next error tier",
            "",
            "These reruns use the unchanged frozen model and original five seeds. They are "
            "diagnostics, not refitted predictions.",
            "",
            "| Family/load | Request class | 300 s | 600 s | 1,200 s | 1,200 s trend |",
            "|---|---|---:|---:|---:|---|",
        ]
        for job in diagnostics["jobs"]:
            class_names = sorted(
                {
                    name
                    for horizon in job["horizons"]
                    for name in horizon.get("classes", {})
                }
            )
            by_horizon = {
                item["measurement_seconds"]: item for item in job["horizons"]
            }
            for request_class in class_names:
                means = []
                for horizon in (300, 600, 1200):
                    item = by_horizon.get(horizon, {})
                    value = item.get("classes", {}).get(request_class, {}).get(
                        "finite_window_mean_ms"
                    )
                    means.append("—" if value is None else f"{value:.2f} ms")
                final = by_horizon.get(1200, {}).get("classes", {}).get(
                    request_class, {}
                )
                trend = final.get("trend", {}).get("status", "unavailable")
                lines.append(
                    f"| {job['family']} / {job['load_point']} | {request_class} | "
                    f"{' | '.join(means)} | {trend} |"
                )
    lines += [
        "",
        "## Interpretation",
        "",
        "1. Detected growth must be reported as modeled nonstationarity, not as a "
        "stationary finite mean.",
        "2. Horizon-stable large misses are model-semantic errors. Their service-resource "
        "boundaries, call overlap, finite concurrency, and class attribution need inspection.",
        "3. A revised model must be fitted on development data and evaluated on untouched "
        "holdout experiments. The currently frozen prospective campaign remains unchanged.",
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scores", type=Path, required=True)
    parser.add_argument("--semantics-audit", type=Path)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--sim-repo", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--tail-cells", type=int, default=15)
    parser.add_argument("--diagnose-jobs", type=int, default=0)
    parser.add_argument("--horizons", type=int, nargs="+", default=(300, 600, 1200))
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.out.exists():
        parser.error("--out must be a new directory")
    if args.tail_cells <= 0 or args.diagnose_jobs < 0 or args.workers <= 0:
        parser.error("counts and workers must be positive")
    if any(value <= 0 for value in args.horizons):
        parser.error("horizons must be positive")
    if bool(args.snapshot) != bool(args.sim_repo) or (
        args.diagnose_jobs and not args.snapshot
    ):
        parser.error("tail diagnosis requires both --snapshot and --sim-repo")

    rows = read_score_rows(args.scores)
    statuses = load_diagnostic_statuses(args.semantics_audit)
    result = {
        "purpose": "retrospective error-tail diagnosis, not a refitted model",
        "inputs": {
            "scores": str(args.scores.resolve()),
            "scores_sha256": hashlib.sha256(args.scores.read_bytes()).hexdigest(),
            "semantics_audit": (
                str(args.semantics_audit.resolve()) if args.semantics_audit else None
            ),
            "semantics_audit_sha256": (
                hashlib.sha256(args.semantics_audit.read_bytes()).hexdigest()
                if args.semantics_audit
                else None
            ),
        },
        "methods": {},
    }
    all_cells = {}
    for method in METHODS:
        run_errors = [
            abs(100.0 * (predicted / observed - 1.0))
            for _, observed, predicted in scored_rows(rows, method)
        ]
        cells = aggregate_cells(rows, method, statuses)
        all_cells[method] = cells
        stationary = [
            cell
            for cell in cells
            if cell["diagnostic_status"] != "growth_detected"
        ]
        tail = sorted(cells, key=lambda item: item["ape_percent"], reverse=True)[
            : args.tail_cells
        ]
        total_error = sum(cell["ape_percent"] for cell in cells)
        tail = [
            {
                **cell,
                "error_mass_percent": (
                    100.0 * cell["ape_percent"] / total_error
                    if total_error > 0.0
                    else 0.0
                ),
            }
            for cell in tail
        ]
        result["methods"][method] = {
            "run_level": error_summary(run_errors),
            "cell_level": error_summary(cell["ape_percent"] for cell in cells),
            "without_detected_growth": error_summary(
                cell["ape_percent"] for cell in stationary
            ),
            "detected_growth_cell_count": len(cells) - len(stationary),
            "tail_cells": tail,
        }
    if args.diagnose_jobs:
        result["tail_diagnostics"] = diagnose_tail_jobs(
            args.snapshot.resolve(),
            args.sim_repo.resolve(),
            all_cells["simulation_fitted"],
            tuple(sorted(set(args.horizons))),
            args.diagnose_jobs,
            args.workers,
        )

    args.out.mkdir(parents=True)
    with (args.out / "cell-scores.csv").open("w", newline="") as handle:
        fieldnames = [
            *CELL_FIELDS,
            "method",
            "repetition_count",
            "observed_replication_mean_ms",
            "predicted_mean_ms",
            "ape_percent",
            "signed_error_percent",
            "diagnostic_status",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for method in METHODS:
            writer.writerows(all_cells[method])
    (args.out / "summary.json").write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n"
    )
    (args.out / "REPORT.md").write_text(render_report(result) + "\n")
    print(f"Saved audit to {args.out}")


if __name__ == "__main__":
    main()

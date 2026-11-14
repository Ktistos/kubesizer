"""Local retrospective replay of the six reviewed prediction-safety fixes.

Never writes to the source snapshots. See the output PROTOCOL.md for the
predeclared comparison and limits; this is not a production sizing entrypoint.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml
from metrics import replication_cluster_bootstrap_mean
from native_backend.backend import (
    CppSimulationBackend,
    SIMULATOR_SEMANTICS_VERSION,
    get_native_backend,
)
from performance_models import QueueingNetworkType, System


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def source_bindings(out, snapshot):
    paths = [Path(__file__), out / "PROTOCOL.md", get_native_backend().binary_path]
    paths += list((out / "baseline").rglob("*.py"))
    paths += list((out / "baseline").rglob("*.cpp"))
    for folder in (
        "characterization",
        "performance_models",
        "native_backend",
        "metrics",
    ):
        paths += list((ROOT / folder).rglob("*.py"))
    paths += list(snapshot.rglob("*.json"))
    paths += list(snapshot.glob("*.csv"))
    paths += list((snapshot / "data").glob("*.csv"))
    paths += list(snapshot.glob("*.py"))
    return {str(p.resolve()): digest(p) for p in paths}


def assert_unchanged(bindings):
    for path, expected in bindings.items():
        if digest(path) != expected:
            raise ValueError(f"Bound source changed during replay: {path}")


def make_system(cls, job):
    system = cls("safety-fix-replay", job["model"], QueueingNetworkType.OPEN)
    system.configure_system_replicas(
        tuple(
            job["configuration"][system.get_service_by_id(i).get_name()]
            for i in range(len(system.get_services()))
        )
    )
    return system


def analytical_prediction(cls, job):
    system = make_system(cls, job)
    issues = system.get_strict_analytical_incompatibilities()
    capacity = bool(system.solve().get_stability())
    means = system.get_request_class_mean_latencies() if capacity else {}
    finite = bool(means) and all(math.isfinite(value) for value in means.values())
    supported = capacity and finite and not issues
    return {
        "available": supported,
        "means_ms": (
            {name: value * 1000 for name, value in means.items()} if supported else {}
        ),
        "capacity_gate": capacity,
        "strict_incompatibilities": list(issues),
        "diagnostic_means_ms": {
            name: value * 1000 if math.isfinite(value) else None
            for name, value in means.items()
        },
    }


def accuracy(rows, methods):
    eligible = [row for row in rows if row["eligible"]]
    matched = [row for row in eligible if all(row[m] is not None for m in methods)]

    def score(items, method):
        values = [row for row in items if row[method] is not None]
        errors = [100 * (row[method] / row["observed_ms"] - 1) for row in values]
        return {
            "finite": len(values),
            "mape_percent": statistics.fmean(map(abs, errors)) if errors else None,
            "median_ape_percent": (
                statistics.median(map(abs, errors)) if errors else None
            ),
            "mean_signed_error_percent": statistics.fmean(errors) if errors else None,
        }

    return {
        "eligible": len(eligible),
        "matched": len(matched),
        "full": {m: score(eligible, m) for m in methods},
        "matched_scores": {m: score(matched, m) for m in methods},
    }


def decision(prediction, limits, observed_bad):
    if not prediction["available"] or not all(
        c in prediction["means_ms"] for c in limits
    ):
        return "abstained"
    if any(prediction["means_ms"][c] > limits[c] for c in limits):
        return "predicted_infeasible"
    return "false_safe" if observed_bad else "accepted_safe"


def latest(out):
    snapshot = ROOT / "results/method-independent-v2-preliminary-20261019"
    destination = out / "latest-predictions.json"
    if destination.exists():
        raise ValueError(f"Refusing to overwrite {destination}")
    bindings = source_bindings(out, snapshot)
    freeze = json.loads((snapshot / "freeze.json").read_text())
    # Verify the already frozen characterization, declarations and manifests.
    for path, expected in freeze["bindings"].items():
        if digest(path) != expected:
            raise ValueError(f"Archived input hash mismatch: {path}")
        bindings[path] = expected
    settings = json.loads((snapshot / "prediction-jobs.json").read_text())
    original = json.loads((snapshot / "simulation-predictions.json").read_text())
    old = load_module(
        "performance_models._pre_fix_replay", out / "baseline/pre_fix_system.py"
    )
    runner = load_module(
        "safety_fixed_simulation_replay", snapshot / "simulate_frozen.py"
    )
    predictions = {}
    started = time.monotonic()
    for index, (key, job) in enumerate(settings["jobs"].items(), 1):
        before = original[key]
        try:
            after = runner.predict(job, settings)
        except (RuntimeError, ValueError) as error:
            # Match the archived runner: failed/censored runs abstain.
            after = {
                "stable": False,
                "means_ms": {},
                "reason": f"{type(error).__name__}: {error}",
            }
        predictions[key] = {
            "simulation_before": {
                "available": before["stable"],
                "means_ms": before["means_ms"],
            },
            "simulation_after": {
                "available": after["stable"],
                "means_ms": after["means_ms"],
                "reason": after["reason"],
            },
            "analytical_before": analytical_prediction(old.System, job),
            "analytical_after": analytical_prediction(System, job),
        }
        print(
            f"Latest {index}/{len(settings['jobs'])}: {job['family']} {job['load_point']}",
            flush=True,
        )
        save(destination, predictions)
    methods = tuple(next(iter(predictions.values())))
    lookup = {
        (j["family"], j["load_point"]): key for key, j in settings["jobs"].items()
    }
    if len(lookup) != len(predictions):
        raise ValueError("Ambiguous family/load mapping")
    rows = []
    for item in read_csv(snapshot / "latency-scores.csv"):
        if item["method"] != "simulation_fitted":
            continue
        key = lookup[item["family"], item["load_point"]]
        row = {
            "run": item["run"],
            "family": item["family"],
            "load_point": item["load_point"],
            "request_class": item["request_class"],
            "eligible": item["primary_latency_eligible"] == "True",
            "observed_ms": float(item["observed_successful_mean_ms"]),
        }
        row.update(
            {
                m: predictions[key][m]["means_ms"].get(item["request_class"])
                for m in methods
            }
        )
        rows.append(row)
    safety = []
    for cell in read_csv(snapshot / "data/safety-cells.csv"):
        if cell["method"] != "simulation_fitted":
            continue
        key = lookup[cell["family"], cell["load_point"]]
        limits = {
            c: spec["end_to_end_latency_slos"]["mean_seconds"] * 1000
            for c, spec in settings["jobs"][key]["model"]["request_classes"].items()
        }
        bad = cell["observed_bad"] == "True"
        safety.append(
            {
                "family": cell["family"],
                "load_point": cell["load_point"],
                "observed_bad": bad,
                **{m: decision(predictions[key][m], limits, bad) for m in methods},
            }
        )
    summary = {
        "jobs": len(predictions),
        "wall_seconds": time.monotonic() - started,
        "simulation": accuracy(rows, ("simulation_before", "simulation_after")),
        "analytical": accuracy(rows, ("analytical_before", "analytical_after")),
        "by_family": {
            family: {
                "simulation": accuracy(
                    [r for r in rows if r["family"] == family],
                    ("simulation_before", "simulation_after"),
                ),
                "analytical": accuracy(
                    [r for r in rows if r["family"] == family],
                    ("analytical_before", "analytical_after"),
                ),
            }
            for family in sorted({r["family"] for r in rows})
        },
        "safety": {m: dict(Counter(row[m] for row in safety)) for m in methods},
        "simulation_changed_availability": sum(
            p["simulation_before"]["available"] != p["simulation_after"]["available"]
            for p in predictions.values()
        ),
        "simulation_max_absolute_change_ms": max(
            abs(
                p["simulation_before"]["means_ms"][c]
                - p["simulation_after"]["means_ms"][c]
            )
            for p in predictions.values()
            if p["simulation_before"]["available"]
            and p["simulation_after"]["available"]
            for c in p["simulation_before"]["means_ms"]
        ),
        "simulator_semantics_version": SIMULATOR_SEMANTICS_VERSION,
        "notice": "Fixed-configuration, finite-window retrospective replay; not a stability proof or optimizer comparison.",
    }
    assert_unchanged(bindings)
    save(out / "latest-bindings.json", bindings)
    save(out / "latest-summary.json", summary)
    write_csv(out / "latest-accuracy.csv", rows)
    write_csv(out / "latest-safety.csv", safety)


def simulate_trace(system, backend, prediction, module):
    system.require_simulation_compatible_workload()
    if not system.solve().get_stability():
        return {"status": "ABSTAINED", "reason": "necessary capacity gate failed"}
    seed_base = 2_000_000_000 + int(
        hashlib.sha256(prediction["id"].encode()).hexdigest()[:7], 16
    )
    seeds = tuple(seed_base + 1000 + i for i in range(module.REPLICATION_COUNT))
    records = backend.measurement_replications(
        system,
        seeds=seeds,
        warmup_duration=module.WARMUP_SECONDS,
        measurement_duration=module.MEASUREMENT_SECONDS,
        workers=4,
        maximum_drain_time=10_000.0,
    )
    if any(
        r.tagged_requests_in_flight_at_end
        or any(r.failed_measured_request_counts.values())
        for r in records
    ):
        return {"status": "ABSTAINED", "reason": "incomplete or failed cohort"}
    cls = module.REQUEST_CLASS
    interval = replication_cluster_bootstrap_mean(
        tuple(r.request_class_latencies[cls] for r in records),
        request_class=cls,
        slo=module.SLO_SECONDS,
        resample_count=module.BOOTSTRAP_RESAMPLES,
        seed=seed_base + 1,
        lower_tail_probability=module.LOWER_TAIL,
        upper_tail_probability=module.UPPER_TAIL,
    )
    return {
        "status": module._classify(interval.lower_bound, interval.upper_bound),
        "estimate_ms": 1000 * interval.estimate,
        "lower_ms": 1000 * interval.lower_bound,
        "upper_ms": 1000 * interval.upper_bound,
        "generated_rps": sum(r.measured_request_counts[cls] for r in records)
        / (len(records) * module.MEASUREMENT_SECONDS),
        "effective_target_rps": prediction["target_rate_rps"]
        * statistics.fmean(r.input_arrival_rate_multipliers[cls] for r in records),
        "replication_mean_ms": [
            1000 * statistics.fmean(r.request_class_latencies[cls]) for r in records
        ],
    }


def burst(out):
    snapshot = ROOT / "results/burst-aware-replay-20260925"
    destination = out / "burst-predictions.json"
    if destination.exists():
        raise ValueError(f"Refusing to overwrite {destination}")
    module = load_module("safety_burst_replay", snapshot / "replay.py")
    old_backend = CppSimulationBackend(
        out / "baseline/native/build-release/kubesizer-simulator"
    )
    new_backend = get_native_backend()
    bindings = source_bindings(out, snapshot)
    bindings[str(old_backend.binary_path)] = digest(old_backend.binary_path)
    # Bind all archived evidence that reconstruction/scoring can read.
    for path in (snapshot / "source").rglob("*"):
        if path.is_file():
            bindings[str(path.resolve())] = digest(path)
    document = yaml.load(
        (module.FROZEN / "predictions.yaml").read_text(), Loader=yaml.CSafeLoader
    )
    selected = [
        p
        for p in document["predictions"]
        if p["family"] == "deep_chain_v2" and p["status"] == "ACCEPTED_RECOMMENDATION"
    ]
    results = {}
    models_dir = out / "burst-models"
    models_dir.mkdir(exist_ok=False)
    for i, prediction in enumerate(selected, 1):
        trace_path = (
            module._source_run(module.TRACES, prediction, 1)
            / f"request_trace.{module.REQUEST_CLASS}.csv"
        )
        try:
            variant, system, boundary = module._prepared_system(
                prediction, module._read_trace(trace_path)
            )
            save(models_dir / f"{prediction['id']}.json", variant.workload)
            before = simulate_trace(system, old_backend, prediction, module)
            after = simulate_trace(system, new_backend, prediction, module)
            residual_ms = 1000 * (boundary.mean_seconds or 0.0)
        except ValueError as error:
            before = after = {"status": "ABSTAINED", "reason": str(error)}
            residual_ms = None
        # Held-out observed latencies are read only after both predictions.
        observed = module._observed_interval(prediction)
        result = {
            "load_point": prediction["load_point"],
            "stage": prediction["stage"],
            "configuration": prediction["configuration"],
            "target_rps": prediction["target_rate_rps"],
            "residual_ms": residual_ms,
            "before": before,
            "after": after,
            **observed,
        }
        results[prediction["id"]] = result
        print(
            f"Burst {i}/{len(selected)}: {prediction['id']} before={before.get('estimate_ms')} after={after.get('estimate_ms')}",
            flush=True,
        )
        save(destination, results)
    rows = [
        {
            "cell_id": key,
            "family": "deep_chain_v2",
            "load_point": r["load_point"],
            "stage": r["stage"],
            "eligible": True,
            "observed_ms": r["observed_mean_seconds"] * 1000,
            "observed_status": r["observed_status"],
            "before": r["before"].get("estimate_ms"),
            "after": r["after"].get("estimate_ms"),
            "before_status": r["before"]["status"],
            "after_status": r["after"]["status"],
            "before_generated_rps": r["before"].get("generated_rps"),
            "after_generated_rps": r["after"].get("generated_rps"),
            "effective_target_rps": r["after"].get("effective_target_rps"),
        }
        for key, r in results.items()
    ]
    summary = {
        "all": accuracy(rows, ("before", "after")),
        "by_arrival_group": {
            group: accuracy(
                [
                    r
                    for r in rows
                    if (
                        r["stage"] == "utilization"
                        if group == "utilization"
                        else r["load_point"].endswith(group)
                    )
                ],
                ("before", "after"),
            )
            for group in ("utilization", "smooth", "bursty", "trace")
        },
        "safety": {
            m: {
                "classifications": dict(Counter(r[f"{m}_status"] for r in rows)),
                "false_safe": sum(
                    r[f"{m}_status"] == "FEASIBLE"
                    and r["observed_status"] == "INFEASIBLE"
                    for r in rows
                ),
                "accepted_observed_safe": sum(
                    r[f"{m}_status"] == "FEASIBLE"
                    and r["observed_status"] == "FEASIBLE"
                    for r in rows
                ),
            }
            for m in ("before", "after")
        },
        "notice": "Sampler-only controlled comparison using current fixed characterization; not the September release or a new independent test.",
    }
    assert_unchanged(bindings)
    save(out / "burst-bindings.json", bindings)
    save(out / "burst-summary.json", summary)
    write_csv(out / "burst-cells.csv", rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--phase", choices=("latest", "burst"), required=True)
    args = parser.parse_args()
    out = args.out.resolve()
    if not (out / "PROTOCOL.md").is_file():
        parser.error(
            "Prepare a separate output directory with the replay protocol first"
        )
    (latest if args.phase == "latest" else burst)(out)


if __name__ == "__main__":
    main()

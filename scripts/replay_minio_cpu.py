"""Compare fixed-layout MinIO latency baselines using retained r7 evidence."""

import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
from statistics import fmean
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from native_backend.backend import SIMULATOR_SEMANTICS_VERSION
from performance_models import QueueingNetworkType, System


MEMBERS = ("minio1", "minio2", "minio3", "minio4")


def read(path):
    return json.loads(path.read_bytes())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def verify_receipt(directory, receipt_name, expected_files):
    receipt = read(directory / receipt_name)
    if set(receipt) != set(expected_files):
        raise ValueError(f"Unexpected files in {directory / receipt_name}")
    for name, expected in receipt.items():
        if digest(directory / name) != expected:
            raise ValueError(f"Evidence hash mismatch for {directory / name}")
    return receipt


def run_evidence(root, name, rate, classes, seed):
    directory = root / "runs" / name
    files = ("summary.json", "requests.jsonl.gz", "snapshots.json")
    receipt = verify_receipt(directory, "receipt.json", files)
    summary = read(directory / "summary.json")
    if (
        summary["run_id"] != name
        or not math.isclose(summary["rate_rps"], rate)
        or set(summary["classes"]) != set(classes)
        or summary["seed"] != seed
        or summary["timed_out"]
        or summary["snapshot_errors"]
        or any(
            c["failures"] or c["completed"] == 0 for c in summary["classes"].values()
        )
    ):
        raise ValueError(f"Unexpected or incomplete evidence for {name}")
    return directory, summary, receipt


def cpu_windows(directory, summary):
    """Match each member's CPU counter interval to client completions."""
    with gzip.open(directory / "requests.jsonl.gz", "rt") as stream:
        requests = [json.loads(line) for line in stream]
    start = summary["started_epoch"]
    lower = start + summary["warmup_seconds"]
    upper = lower + summary["measurement_seconds"]
    completion_times = [
        start
        + row["scheduled_seconds"]
        + row["scheduling_delay_seconds"]
        + row["latency_seconds"]
        for row in requests
        if row["success"]
    ]
    snapshots = read(directory / "snapshots.json")["samples"]
    windows = {}
    for member in MEMBERS:
        samples = [
            sample["members"][member]
            for sample in snapshots
            if lower <= sample["members"][member]["epoch"] < upper
        ]
        if len(samples) < 2 or any(len(sample["processes"]) != 1 for sample in samples):
            raise ValueError(f"Missing CPU samples for {directory.name}/{member}")
        counters = [sample["processes"][0] for sample in samples]
        identities = {(counter["pid"], counter["start_ticks"]) for counter in counters}
        if len(identities) != 1:
            raise ValueError(f"Process identity changed in {directory.name}/{member}")
        first, last = samples[0]["epoch"], samples[-1]["epoch"]
        consumed = counters[-1]["cpu_seconds"] - counters[0]["cpu_seconds"]
        completed = sum(first <= stamp < last for stamp in completion_times)
        if consumed <= 0 or completed == 0 or last <= first:
            raise ValueError(f"Invalid CPU window in {directory.name}/{member}")
        windows[member] = {
            "elapsed_seconds": last - first,
            "cpu_seconds": consumed,
            "completed_requests": completed,
        }
    return windows


def deterministic(mean):
    return {"distribution": "deterministic", "mean": mean}


def workload(rate, shares, demands, residuals, slo):
    """One queue for the entry member, with a fixed ingress-boundary residual."""
    return {
        "request_classes": {
            cls: {
                "concurrency_weight": share,
                "routing": {},
                "latency_boundary": "external_client",
                **(
                    {
                        "latency_components": {
                            "unmodeled_residual": deterministic(residuals[cls])
                        }
                    }
                    if residuals
                    else {}
                ),
                "end_to_end_latency_slos": {"mean_seconds": slo},
            }
            for cls, share in shares.items()
        },
        "traffic_model": {
            "entry_services": {
                cls: {"minio1": {"distribution": "exponential", "rate": rate * share}}
                for cls, share in shares.items()
            },
            "service_times": {
                "minio1": {
                    "execution_model": "processor_sharing",
                    "replica_queue_model": "pooled",
                    **{cls: deterministic(demand) for cls, demand in demands.items()},
                }
            },
        },
        "replica_constraints": {"minio1": {"fixed": 1}},
    }


def pooled_ratio(rows, numerator, denominator):
    return sum(row[numerator] for row in rows) / sum(row[denominator] for row in rows)


def verify_layout(data, protocol):
    if protocol["physical_members"] != len(MEMBERS) or protocol["parity"] != "EC:2":
        raise ValueError("This replay covers the four-member EC:2 pilot")
    sets = {
        item["metadata"]["name"]: item["spec"]
        for item in read(data / "deployment-context.json")["items"]
        if item["kind"] == "StatefulSet"
    }
    if set(sets) != set(MEMBERS):
        raise ValueError("Unexpected MinIO deployment members")
    for member, spec in sets.items():
        container = next(
            c for c in spec["template"]["spec"]["containers"] if c["name"] == "minio"
        )
        if spec["replicas"] != 1 or str(container["resources"]["limits"]["cpu"]) != "1":
            raise ValueError(f"Expected one one-core MinIO container for {member}")


def replay(data, output):
    inputs = verify_receipt(
        data,
        "input-freeze-receipt.json",
        (
            "anchor-frozen.json",
            "target-order-frozen.json",
            "characterization-results.json",
            "pilot-results.json",
        ),
    )
    protocol = read(data / "protocol.json")
    verify_layout(data, protocol)
    anchor = read(data / "anchor-frozen.json")["anchor_rps"]
    shares = protocol["class_shares"]
    classes = tuple(protocol["classes"])
    slo = protocol["mean_slo_seconds"]
    cells = [
        (
            f"target-u{round(fraction * 100):02d}-rep{rep}",
            anchor * fraction,
            protocol["seed"] + 100 + i,
        )
        for i, (fraction, rep) in enumerate(read(data / "target-order-frozen.json"))
    ]
    low_rate = min(rate for _, rate, _ in cells)
    receipts, characterization, baseline_runs = {}, {}, []
    for i, cls in enumerate(classes):
        rows = []
        for rep in range(protocol["characterization_repetitions"]):
            name = f"characterization-{cls}-rep{rep}"
            seed = protocol["seed"] + i * protocol["characterization_repetitions"] + rep
            directory, summary, receipts[name] = run_evidence(
                data, name, protocol["characterization_rate_rps"], (cls,), seed
            )
            rows.append(cpu_windows(directory, summary)["minio1"])
        characterization[cls] = pooled_ratio(rows, "cpu_seconds", "completed_requests")

    low_cpu = []
    for name, rate, seed in cells:
        if rate != low_rate:
            continue
        directory, summary, receipts[name] = run_evidence(
            data, name, rate, classes, seed
        )
        if summary["latency_boundary"] != protocol["latency_boundary"]:
            raise ValueError("Baseline uses a different latency boundary")
        low_cpu.append(cpu_windows(directory, summary)["minio1"])
        baseline_runs.append(summary)
    baseline = {
        cls: sum(
            r["classes"][cls]["mean_seconds"] * r["classes"][cls]["completed"]
            for r in baseline_runs
        )
        / sum(r["classes"][cls]["completed"] for r in baseline_runs)
        for cls in classes
    }
    cpu_rate = pooled_ratio(low_cpu, "cpu_seconds", "elapsed_seconds")
    # Isolated runs provide class weights, while the ingress runs fix the
    # aggregate CPU work per request. The entry member has a one-core limit.
    scale = cpu_rate / (
        low_rate * sum(shares[c] * characterization[c] for c in classes)
    )
    demands = {c: characterization[c] * scale for c in classes}
    cpu_only = System(
        "minio-cpu-calibration",
        workload(low_rate, shares, demands, {}, slo),
        QueueingNetworkType.OPEN,
    ).get_request_class_mean_latencies()
    residuals = {c: baseline[c] - cpu_only[c] for c in classes}
    if any(r <= 0 for r in residuals.values()):
        raise ValueError(
            "Observed low-load latency cannot support the CPU decomposition"
        )

    output.mkdir(parents=True, exist_ok=False)
    predictions = {}
    for index, rate in enumerate(
        sorted({rate for _, rate, _ in cells if rate > low_rate})
    ):
        specification = workload(rate, shares, demands, residuals, slo)
        label = f"{rate:.1f}"
        write(output / f"workload-{label}.json", specification)
        system = System(f"minio-cpu-{label}", specification, QueueingNetworkType.OPEN)
        means = system.get_request_class_mean_latencies()
        seeds = tuple(range(411100 + index * 20, 411120 + index * 20))
        repeats = system.simulate_measurement_replications(
            warmup_duration=protocol["target_warmup_seconds"],
            measurement_duration=protocol["target_measurement_seconds"],
            maximum_drain_time=protocol["drain_seconds"],
            seeds=seeds,
            workers=2,
        )
        simulated = {
            cls: [fmean(rep.request_class_latencies[cls]) for rep in repeats]
            for cls in classes
        }
        predictions[label] = {
            "rate_rps": rate,
            "projected_entry_cpu_cores": rate
            * sum(shares[c] * demands[c] for c in classes),
            "mean_seconds": {
                "constant": baseline,
                "analytical_cpu": means,
                "simulated_cpu": {
                    cls: fmean(values) for cls, values in simulated.items()
                },
            },
            "simulation_seeds": seeds,
            "simulation_replication_means_seconds": simulated,
        }
    write(
        output / "predictions.json",
        {
            "scope": "retrospective fixed-layout mean-latency diagnostic",
            "input_freeze": inputs,
            "protocol_sha256": digest(data / "protocol.json"),
            "deployment_context_sha256": digest(data / "deployment-context.json"),
            "replay_script_sha256": digest(Path(__file__)),
            "training_run_bindings": receipts,
            "simulator_version": SIMULATOR_SEMANTICS_VERSION,
            "mean_slo_seconds": slo,
            "baseline_rate_rps": low_rate,
            "baseline_cpu_cores": cpu_rate,
            "isolated_cpu_seconds_per_completion": characterization,
            "cpu_demand_seconds": demands,
            "fixed_residual_seconds": residuals,
            "predictions": predictions,
        },
    )

    scores = []
    measured_cpu = {}
    scoring_receipts = {}
    # Higher-load summaries and counters are first read here.
    for name, rate, seed in cells:
        if rate == low_rate:
            continue
        directory, summary, scoring_receipts[name] = run_evidence(
            data, name, rate, classes, seed
        )
        if summary["latency_boundary"] != protocol["latency_boundary"]:
            raise ValueError("Scoring run uses a different latency boundary")
        windows = cpu_windows(directory, summary)
        measured_cpu[name] = {
            member: w["cpu_seconds"] / w["elapsed_seconds"]
            for member, w in windows.items()
        }
        for cls in classes:
            observed = summary["classes"][cls]["mean_seconds"]
            estimates = predictions[f"{rate:.1f}"]["mean_seconds"]
            scores.append(
                {
                    "run": name,
                    "rate_rps": rate,
                    "class": cls,
                    "observed_mean_seconds": observed,
                    "models": {
                        model: {
                            "mean_seconds": means[cls],
                            "ape_percent": abs(means[cls] / observed - 1) * 100,
                            "missed_slo_violation": means[cls] <= slo < observed,
                            "false_slo_violation": observed <= slo < means[cls],
                        }
                        for model, means in estimates.items()
                    },
                }
            )
    metrics = {}
    for label in ("overall", *predictions):
        selected = [
            s for s in scores if label == "overall" or f"{s['rate_rps']:.1f}" == label
        ]
        metrics[label] = {
            model: {
                "mape_percent": fmean(
                    s["models"][model]["ape_percent"] for s in selected
                ),
                "missed_slo_violations": sum(
                    s["models"][model]["missed_slo_violation"] for s in selected
                ),
                "false_slo_violations": sum(
                    s["models"][model]["false_slo_violation"] for s in selected
                ),
            }
            for model in selected[0]["models"]
        }
    write(
        output / "scores.json",
        {
            "prediction_sha256": digest(output / "predictions.json"),
            "scoring_run_bindings": scoring_receipts,
            "metrics": metrics,
            "measured_cpu_cores_by_run": measured_cpu,
            "scores": scores,
        },
    )
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps(replay(args.evidence, args.output), indent=2))

"""Retrospective pgp1 CPU-quota ablation; never changes the original evidence."""

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from native_backend.backend import PROTOCOL_VERSION, SIMULATOR_SEMANTICS_VERSION
from native_backend import get_native_backend
from performance_models import System, QueueingNetworkType
from scripts.run_postgres_pilot import MEMBERS, model


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def verify_evidence(data):
    bindings = 0
    receipts = [
        data / "prediction-receipt.json",
        *sorted((data / "runs").glob("*/receipt.json")),
    ]
    if len(receipts) != 21:
        raise ValueError(
            "Expected the complete pgp1 evidence: two characterization and 18 target runs"
        )
    for receipt in receipts:
        base = data if receipt.name == "prediction-receipt.json" else receipt.parent
        for name, expected in json.loads(receipt.read_text()).items():
            if digest(base / name) != expected:
                raise ValueError(f"Evidence receipt mismatch: {base / name}")
            bindings += 1
    return bindings


def replay(data, cpu_profile, output, *, replications=20, sensitivities=(1, 2, 4, 8)):
    if replications < 2:
        raise ValueError("At least two replications are required")
    verified = verify_evidence(data)
    parameters = json.loads(cpu_profile.read_text())
    for name, expected in parameters.get("source_sha256", {}).items():
        if digest(cpu_profile.parent / name) != expected:
            raise ValueError(f"CPU provenance mismatch: {name}")
    budgets = parameters["services"]
    if set(budgets) != set(MEMBERS):
        raise ValueError(
            "CPU declarations must cover the three measured physical members"
        )
    profile = json.loads((data / "characterization.json").read_text())
    original = json.loads((data / "predictions-frozen.json").read_text())
    # No target response is used to construct a model or select a parameter.
    cells = [r for r in original["predictions"] if r["variant"] == "enriched"]
    output.mkdir(parents=True, exist_ok=False)
    (output / "models").mkdir()
    write(output / "cpu-profile.json", parameters)
    source_files = [
        ROOT / "scripts/replay_postgres_cpu_bandwidth.py",
        ROOT / "scripts/run_postgres_pilot.py",
        ROOT / "native_backend/backend.py",
        ROOT / "native_backend/cpp/src/main.cpp",
        ROOT / "performance_models/system.py",
        ROOT / "performance_models/workload.py",
        ROOT / "performance_models/stages.py",
        get_native_backend().binary_path,
    ]
    frozen = dict(
        interpretation="retrospective diagnostic; targets already inspected; not new holdout evidence",
        protocol_version=PROTOCOL_VERSION,
        semantics_version=SIMULATOR_SEMANTICS_VERSION,
        verified_original_file_bindings=verified,
        original_prediction_receipt_sha256=digest(data / "prediction-receipt.json"),
        characterization_sha256=digest(data / "characterization.json"),
        cpu_profile_sha256=digest(cpu_profile),
        sources={str(p.relative_to(ROOT)): digest(p) for p in source_files},
        replications=replications,
        warmup_seconds=60,
        measurement_seconds=240,
        drain_seconds=90,
        sensitivity_parallelism=list(sensitivities),
        predictions=[],
    )
    variants = [
        "continuous-ps",
        "cpu-quota",
        *[f"quota-parallelism-{p}" for p in sensitivities],
    ]
    for cell_index, cell in enumerate(cells):
        for variant in variants:
            cpu = None
            if variant != "continuous-ps":
                cpu = {n: dict(v) for n, v in budgets.items()}
                if variant.startswith("quota-parallelism-"):
                    parallelism = int(variant.rsplit("-", 1)[1])
                    for value in cpu.values():
                        value["parallelism"] = parallelism
            specification = model(
                profile, cell["mode"], cell["rate_rps"], True, cpu_bandwidths=cpu
            )
            path = output / "models" / f"{cell['cell']}-{variant}.json"
            write(path, specification)
            system = System(cell["cell"], specification, QueueingNetworkType.OPEN)
            seeds = tuple(
                41_000_000 + 1000 * cell_index + i for i in range(replications)
            )
            repeats = system.simulate_measurement_replications(
                warmup_duration=60,
                measurement_duration=240,
                seeds=seeds,
                workers=2,
                maximum_drain_time=90,
            )
            frozen["predictions"].append(
                dict(
                    cell=cell["cell"],
                    variant=variant,
                    seeds=seeds,
                    model_sha256=digest(path),
                    classes={
                        c: dict(
                            mean_seconds=statistics.fmean(values),
                            replication_means_seconds=values,
                            simulation_mean_standard_error_seconds=statistics.stdev(
                                values
                            )
                            / len(values) ** 0.5,
                        )
                        for c in ("read", "write")
                        for values in [
                            [
                                statistics.fmean(r.request_class_latencies[c])
                                for r in repeats
                            ]
                        ]
                    },
                )
            )
        print(f"Replayed {cell['cell']}", flush=True)
    write(output / "predictions.json", frozen)
    write(
        output / "replay-receipt.json",
        {
            str(p.relative_to(output)): digest(p)
            for p in [
                output / "predictions.json",
                output / "cpu-profile.json",
                *sorted((output / "models").glob("*.json")),
            ]
        },
    )
    # Only now read the target outcomes for scoring the completed ablation.
    original_scores = json.loads((data / "scores.json").read_text())
    scores = []
    for row in original_scores:
        result = {
            k: row[k]
            for k in ("cell", "request_class", "observed_mean_seconds", "eligible")
        }
        result["original-enriched"] = row["enriched"]
        for variant in variants:
            prediction = next(
                p
                for p in frozen["predictions"]
                if p["cell"] == row["cell"] and p["variant"] == variant
            )
            estimate = prediction["classes"][row["request_class"]]["mean_seconds"]
            result[variant] = dict(
                mean_seconds=estimate,
                ape_percent=100 * abs(estimate / row["observed_mean_seconds"] - 1),
            )
        scores.append(result)
    write(output / "scores.json", scores)
    groups = {
        "overall": [r for r in scores if r["eligible"]],
        **{
            c: [r for r in scores if r["eligible"] and r["request_class"] == c]
            for c in ("read", "write")
        },
    }
    summary = {
        name: dict(
            count=len(rows),
            mape_percent={
                variant: statistics.fmean(r[variant]["ape_percent"] for r in rows)
                for variant in ["original-enriched", *variants]
            },
        )
        for name, rows in groups.items()
    }
    write(output / "summary.json", summary)
    lines = [
        "# PostgreSQL read replay: periodic CPU quota",
        "",
        "Retrospective diagnostic using the retained pgp1 targets. The hypothesis was developed after inspecting those targets. These results are not fresh holdout validation or a sizing safety claim. Original inputs, predictions, scores and eligibility remain unchanged.",
        "",
        "CPU means, local blocking, replay proxies, client boundary residuals, routing and acknowledgement policies are held fixed. The change is CPU execution: a measured periodic budget replaces continuously smoothed capacity. The quota variant uses the supplied runtime declaration; sensitivity variants are reported together rather than selecting the best error.",
        "",
        "Mean-latency MAPE gives equal weight to eligible request-class/load/mode cells; each observed cell pools its two physical repetitions. Simulation replications quantify Monte Carlo variation only.",
        "",
        "| Eligible cells | Original enriched | Replayed continuous PS | Periodic quota |",
        "|---|---:|---:|---:|",
    ]
    for name, group in summary.items():
        m = group["mape_percent"]
        lines.append(
            f"| {name} ({group['count']}) | {m['original-enriched']:.2f}% | {m['continuous-ps']:.2f}% | {m['cpu-quota']:.2f}% |"
        )
    lines += [
        "",
        "## Read predictions by cell",
        "",
        "| Cell | Observed ms | Continuous PS ms | Periodic quota ms | Eligible |",
        "|---|---:|---:|---:|---|",
    ]
    for row in scores:
        if row["request_class"] == "read":
            lines.append(
                f"| {row['cell']} | {1000*row['observed_mean_seconds']:.2f} | {1000*row['continuous-ps']['mean_seconds']:.2f} | {1000*row['cpu-quota']['mean_seconds']:.2f} | {row['eligible']} |"
            )
    lines += [
        "",
        "## Parallelism sensitivity",
        "",
        "| Parallelism | Eligible read MAPE |",
        "|---|---:|",
    ]
    for n in sensitivities:
        lines.append(
            f"| {n} | {summary['read']['mape_percent'][f'quota-parallelism-{n}']:.2f}% |"
        )
    lines += [
        "",
        "## Interpretation and unresolved limits",
        "",
        "The configured parallelism is a declared upper bound from the worker CPU count and backend concurrency, not an identified guarantee of available execution capacity. Historical per-process CPU affinity and instantaneous host contention were not retained. See cpu-profile.json for provenance. This is a periodic-budget abstraction, not a complete Linux scheduler: per-CPU quota silos, scheduling slices, hierarchical throttling, CPU-frequency variation and competing auxiliary processes are not reproduced.",
        "",
        "At the highest load the client rejected arrivals and failed to maintain the schedule. Successful read means of roughly 3.3 seconds remain severely underpredicted. Target per-query CPU work was also larger than the characterization mean. Periodic quota semantics alone do not resolve demand drift or establish near-capacity safety. Excluded targets remain visible above and are not added to eligible MAPE.",
        "",
        "The strict analytical evaluator abstains for periodic CPU quota. The next validation must freeze the quota model and runtime evidence before fresh targets, retain CPU affinity/quota/throttling and total competing CPU, verify a healthy load generator, and examine accuracy together with abstention and high-load coverage.",
        "",
    ]
    (output / "REPORT.md").write_text("\n".join(lines))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("--cpu-profile", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--replications", type=int, default=20)
    args = parser.parse_args()
    print(
        json.dumps(
            replay(
                args.data.resolve(),
                args.cpu_profile.resolve(),
                args.output.resolve(),
                replications=args.replications,
            ),
            indent=2,
        )
    )

"""Test an aggregate PUT-only volume queue on retained MinIO mixed-load runs."""

import argparse
from pathlib import Path
from statistics import fmean
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.replay_minio_cpu import (
    MEMBERS,
    QueueingNetworkType,
    System,
    deterministic,
    digest,
    read,
    workload as cpu_workload,
    write,
)


def volume_demands(audit):
    demands = {}
    for cls, size in (("put_64k", 65536), ("put_1m", 1048576)):
        blocks = [b for b in audit["measured_blocks"] if b["bytes"] == size]
        if len(blocks) != 2 or any(b["puts"] != 12 for b in blocks):
            raise ValueError(f"Expected two complete characterization blocks for {cls}")
        requests = sum(b["puts"] for b in blocks)
        demands[cls] = {
            member: sum(b["member_deltas"][member]["busy_ms"] for b in blocks)
            / (1000 * requests)
            for member in MEMBERS
        }
        if min(demands[cls].values()) <= 0:
            raise ValueError(f"Missing positive volume activity for {cls}")
    return demands


def volume_workload(rate, shares, cpu, residuals, slo, volumes):
    specification = cpu_workload(rate, shares, cpu, residuals, slo)
    specification["analytical_call_model"] = "response_bounds"
    resources = specification["traffic_model"]["service_times"]
    # Explicit stage demands supply all work. One sharing queue per logical
    # PVC is the hypothesis under test, with no GET volume work identified.
    resources["minio1"] = dict(
        execution_model="processor_sharing", replica_queue_model="pooled"
    )
    for member in MEMBERS:
        volume = f"volume_{member}"
        resources[volume] = dict(
            execution_model="processor_sharing", replica_queue_model="pooled"
        )
        specification["replica_constraints"][volume] = dict(fixed=1)
    for cls, config in specification["request_classes"].items():
        del config["routing"]
        stages = {"cpu": dict(resource="minio1", service_time=deterministic(cpu[cls]))}
        if cls in volumes:
            stages["cpu"]["next"] = "volume_visits"
            stages["volume_visits"] = dict(calls={member: 1 for member in MEMBERS})
            for member, demand in volumes[cls].items():
                stages[member] = dict(
                    resource=f"volume_{member}", service_time=deterministic(demand)
                )
        config["stage_workflow"] = dict(entries={"minio1": "cpu"}, stages=stages)
    return specification


def simulate(specification, seeds):
    model = System("minio-volume-replay", specification, QueueingNetworkType.OPEN)
    replications = model.simulate_measurement_replications(
        warmup_duration=60,
        measurement_duration=240,
        maximum_drain_time=90,
        seeds=seeds,
        workers=2,
    )
    return {
        cls: fmean(fmean(run.request_class_latencies[cls]) for run in replications)
        for cls in specification["request_classes"]
    }


def replay(cpu_dir, volume_audit, output):
    prediction_file = cpu_dir / "predictions.json"
    cpu_replay = read(prediction_file)
    demands = cpu_replay["cpu_demand_seconds"]
    slo, low_rate = cpu_replay["mean_slo_seconds"], cpu_replay["baseline_rate_rps"]
    low_means = next(iter(cpu_replay["predictions"].values()))["mean_seconds"][
        "constant"
    ]
    shares = {cls: 0.25 for cls in demands}
    volumes = volume_demands(read(volume_audit))
    low_seeds = tuple(range(811100, 811120))
    without_residual = simulate(
        volume_workload(low_rate, shares, demands, {}, slo, volumes), low_seeds
    )
    residuals = {
        cls: low_means[cls] - seconds for cls, seconds in without_residual.items()
    }
    if any(seconds <= 0 for seconds in residuals.values()):
        raise ValueError(
            "Measured low-load latencies cannot support the queue decomposition"
        )

    output.mkdir(parents=True, exist_ok=False)
    predictions = {}
    for label, base in cpu_replay["predictions"].items():
        rate, seeds = base["rate_rps"], base["simulation_seeds"]
        specification = volume_workload(rate, shares, demands, residuals, slo, volumes)
        write(output / f"workload-{label}.json", specification)
        model = System("minio-volume-bounds", specification, QueueingNetworkType.OPEN)
        analytical_reasons = model.get_strict_analytical_incompatibilities()
        predictions[label] = dict(
            simulated_mean_seconds=simulate(specification, seeds),
            analytical=(
                dict(status="abstained", reasons=analytical_reasons)
                if analytical_reasons
                else dict(
                    status="conditional_bounds",
                    mean_bounds_seconds=model.get_request_class_latency_bounds(),
                )
            ),
            projected_write_only_volume_utilization={
                m: rate * sum(shares[cls] * visit[m] for cls, visit in volumes.items())
                for m in MEMBERS
            },
            simulation_seeds=seeds,
        )
    prediction_path = output / "predictions.json"
    write(
        prediction_path,
        dict(
            scope="retrospective fixed-layout diagnostic, aggregate PUT volume visits; GET volume work omitted",
            inputs={
                path.name: digest(path) for path in (prediction_file, volume_audit)
            },
            replay_script_sha256=digest(Path(__file__)),
            per_put_volume_busy_seconds=volumes,
            low_rate_rps=low_rate,
            low_model_means_without_residual=without_residual,
            low_model_seeds=low_seeds,
            fixed_residual_seconds=residuals,
            predictions=predictions,
        ),
    )

    scores_path = cpu_dir / "scores.json"
    baseline = read(scores_path)
    if baseline["prediction_sha256"] != digest(prediction_file):
        raise ValueError(
            "The scoring observations are not bound to the retained CPU replay"
        )
    scores = []
    for row in baseline["scores"]:
        label = f"{row['rate_rps']:.1f}"
        cls, observed = row["class"], row["observed_mean_seconds"]
        predicted = predictions[label]["simulated_mean_seconds"][cls]
        scores.append(
            dict(
                run=row["run"],
                rate=label,
                request_class=cls,
                observed_mean_seconds=observed,
                cpu=row["models"]["simulated_cpu"],
                cpu_and_volume=dict(
                    mean_seconds=predicted,
                    ape_percent=100 * abs(predicted - observed) / observed,
                    missed_slo_violation=predicted <= slo < observed,
                    false_slo_violation=observed <= slo < predicted,
                ),
            )
        )
    metrics = {}
    for label in (*predictions, "overall"):
        group = [row for row in scores if label == "overall" or row["rate"] == label]
        metrics[label] = {
            name: dict(
                mape_percent=fmean(row[name]["ape_percent"] for row in group),
                missed_slo_violations=sum(
                    row[name]["missed_slo_violation"] for row in group
                ),
                false_slo_violations=sum(
                    row[name]["false_slo_violation"] for row in group
                ),
            )
            for name in ("cpu", "cpu_and_volume")
        }
    write(
        output / "scores.json",
        dict(
            prediction_sha256=digest(prediction_path),
            cpu_score_sha256=digest(scores_path),
            metrics=metrics,
            scores=scores,
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("cpu_dir", "volume_audit", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    replay(args.cpu_dir, args.volume_audit, args.output)

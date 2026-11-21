#!/usr/bin/env python3
"""Retrospectively compare continuous and periodic CPU on the MinIO A/B evidence."""

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
from statistics import fmean, stdev
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from native_backend import get_native_backend
from native_backend.backend import PROTOCOL_VERSION, SIMULATOR_SEMANTICS_VERSION
from performance_models import QueueingNetworkType, System
from scripts.replay_minio_cpu import deterministic, digest, write

from analyze_minio_cpu_intervention import analyze as analyze_intervention


METHODS = ("PUT", "GET")
PARALLELISM = (2, 4, 8, 16)
SEEDS = tuple(range(812000, 812040))


def retained(archive, name, receipt=None):
    with archive.extractfile(f"./{name}") as stream:
        raw = stream.read()
    if receipt is not None and hashlib.sha256(raw).hexdigest() != receipt[name]:
        raise ValueError(f"Evidence hash mismatch for {name}")
    return json.loads(gzip.decompress(raw) if name.endswith(".gz") else raw)


def characterize(archive):
    with tarfile.open(archive) as evidence:
        receipt = retained(evidence, "evidence-receipt.json")
        plan = retained(evidence, "protocol.json", receipt)["plan"]
        low_rate = min(cell["rate"] for cell in plan)
        windows = []
        mixed_latencies = {method: [] for method in METHODS}
        for i, cell in enumerate(plan):
            if cell["rate"] != low_rate:
                continue
            block = f"window-{i:02d}"
            rows = retained(evidence, f"{block}.json.gz", receipt)["rows"]
            if any(not row["success"] for row in rows):
                raise ValueError(f"Unexpected requests failed in {block}")
            before, after = (
                retained(evidence, f"snapshots/{block}-{phase}.json", receipt)
                for phase in ("before", "after")
            )
            if before["errors"] or after["errors"]:
                raise ValueError(f"Missing low-load CPU evidence in {block}")
            (first,), (last,) = (
                snapshot["members"]["minio1"]["processes"]
                for snapshot in (before, after)
            )
            if (first["pid"], first["start_ticks"]) != (
                last["pid"],
                last["start_ticks"],
            ):
                raise ValueError(f"Entry process restarted in {block}")
            cpu = last["cpu_seconds"] - first["cpu_seconds"]
            if cpu <= 0:
                raise ValueError(f"Entry CPU counter did not advance in {block}")
            requests = Counter(row["method"] for row in rows)
            windows.append(
                dict(
                    block=block,
                    mode=cell["mode"],
                    cpu_seconds=cpu,
                    counts={method: requests[method] for method in METHODS},
                )
            )
            if cell["mode"] == "mixed":
                for row in rows:
                    if row["tagged"]:
                        mixed_latencies[row["method"]].append(row["seconds"])

    only_put = [window for window in windows if window["mode"] == "write"]
    mixed = [window for window in windows if window["mode"] == "mixed"]
    if len(only_put) != 2 or len(mixed) != 2:
        raise ValueError(
            "Expected both complete repetitions at the lowest offered rate"
        )
    put_work = sum(w["cpu_seconds"] for w in only_put) / sum(
        w["counts"]["PUT"] for w in only_put
    )
    get_work = (
        sum(w["cpu_seconds"] for w in mixed)
        - put_work * sum(w["counts"]["PUT"] for w in mixed)
    ) / sum(w["counts"]["GET"] for w in mixed)
    work = dict(PUT=put_work, GET=get_work)
    low_cpu_load = low_rate * sum(work.values())
    if min(work.values()) <= 0 or not 0 < low_cpu_load < 1:
        raise ValueError(
            "Low-load evidence does not support the class CPU decomposition"
        )
    low_means = {method: fmean(mixed_latencies[method]) for method in METHODS}
    low_cpu_means = {method: work[method] / (1 - low_cpu_load) for method in METHODS}
    residuals = {
        method: low_means[method] - low_cpu_means[method] for method in METHODS
    }
    if min(residuals.values()) <= 0:
        raise ValueError("Low-load means cannot support a positive latency residual")
    return dict(
        low_rate_per_class=low_rate,
        verified_windows=windows,
        cpu_work_seconds_per_completion=work,
        low_observed_mean_seconds=low_means,
        low_cpu_model_mean_seconds=low_cpu_means,
        fixed_residual_seconds=residuals,
        low_measured_counts={m: len(mixed_latencies[m]) for m in METHODS},
    )


def treatments(archive):
    with tarfile.open(archive) as evidence:
        plan = retained(evidence, "campaign-plan.json")["plan"]
        settings = {}
        for arm in plan:
            name = arm["name"]
            receipt = retained(evidence, f"{name}/evidence-receipt.json")
            # Each arm receipt is rooted inside its own directory.
            receipt = {f"{name}/{path}": checksum for path, checksum in receipt.items()}
            config = retained(evidence, f"{name}/protocol.json", receipt)
            snapshot = retained(
                evidence, f"{name}/snapshots/window-00-before.json", receipt
            )
            cpu = snapshot["members"]["minio1"]["cgroup_cpu_stat"]
            cores = arm["entry_cores"]
            quota, period = cpu["quota_us"], cpu["period_us"]
            if quota != cores * period or arm["mode"] != "mixed":
                raise ValueError(f"Unsupported treatment in {name}")
            setting = dict(
                quota_seconds=quota / 1e6,
                period_seconds=period / 1e6,
                rate_per_class=arm["rate"],
                warmup=config["warmup_seconds"],
                measurement=config["measured_seconds"],
            )
            if cores in settings and settings[cores] != setting:
                raise ValueError("Treatments differ across repetitions")
            settings[cores] = setting
    if (
        set(settings) != {1, 2}
        or {arm["rep"] for arm in plan} != {0, 1}
        or len(plan) != 4
    ):
        raise ValueError("Expected the four preplanned CPU intervention arms")
    return settings


def workload(fit, setting, parallelism):
    quota, period = setting["quota_seconds"], setting["period_seconds"]
    cores = quota / period
    service = dict(
        execution_model="processor_sharing", replica_queue_model="independent_random"
    )
    service.update(
        {
            m: deterministic(work / cores)
            for m, work in fit["cpu_work_seconds_per_completion"].items()
        }
    )
    if parallelism is not None:
        service["cpu_bandwidth"] = dict(
            quota_seconds=quota, period_seconds=period, parallelism=parallelism
        )
    return dict(
        request_classes={
            m: dict(
                concurrency_weight=0.5,
                routing={},
                latency_boundary="external_client",
                latency_components={
                    "unmodeled_residual": deterministic(
                        fit["fixed_residual_seconds"][m]
                    )
                },
            )
            for m in METHODS
        },
        traffic_model=dict(
            entry_services={
                m: {
                    "minio1": dict(
                        distribution="exponential", rate=setting["rate_per_class"]
                    )
                }
                for m in METHODS
            },
            service_times={"minio1": service},
        ),
        replica_constraints={"minio1": dict(fixed=1)},
    )


def simulate(spec, setting):
    system = System("minio-cpu-quota-replay", spec, QueueingNetworkType.OPEN)
    runs = system.simulate_measurement_replications(
        warmup_duration=setting["warmup"],
        measurement_duration=setting["measurement"],
        maximum_drain_time=90,
        seeds=SEEDS,
        workers=2,
    )
    if any(run.tagged_requests_in_flight_at_end for run in runs):
        raise ValueError("A simulated cohort did not drain")
    results = {}
    for method in spec["request_classes"]:
        means = [1000 * fmean(run.request_class_latencies[method]) for run in runs]
        results[method] = dict(
            mean_ms=fmean(means),
            seed_means_ms=means,
            monte_carlo_standard_error_ms=stdev(means) / len(means) ** 0.5,
        )
    return results


def replay(sweep, intervention, output):
    fit, settings = characterize(sweep), treatments(intervention)
    output.mkdir(parents=True, exist_ok=False)
    predictions = {}
    for p in (None, *PARALLELISM):
        name = "continuous_ps" if p is None else f"periodic_quota_p{p}"
        predictions[name] = {}
        for cores, setting in settings.items():
            spec = workload(fit, setting, p)
            predictions[name][cores] = dict(
                workload=spec, classes=simulate(spec, setting)
            )
    write(
        output / "predictions.json",
        dict(
            semantics_version=SIMULATOR_SEMANTICS_VERSION,
            protocol_version=PROTOCOL_VERSION,
            executable_sha256=digest(get_native_backend().binary_path),
            replay_script_sha256=digest(Path(__file__)),
            characterization_archive_sha256=digest(sweep),
            intervention_archive_sha256=digest(intervention),
            fit=fit,
            settings=settings,
            seeds=SEEDS,
            parallelism_sensitivity=PARALLELISM,
            predictions=predictions,
        ),
    )

    observed = analyze_intervention(intervention)
    by_arm = {(a["rep"], a["entry_cores"]): a for a in observed["arms"]}
    variants = {}
    for name, model in predictions.items():
        pairs, cells = [], []
        for rep in (0, 1):
            gains = []
            for method in METHODS:
                predictions_ms = [
                    model[cores]["classes"][method]["mean_ms"] for cores in (1, 2)
                ]
                measured_ms = [
                    by_arm[rep, cores]["classes"][method]["mean_ms"] for cores in (1, 2)
                ]
                gain_ms = measured_ms[0] - measured_ms[1]
                expected_gain_ms = predictions_ms[0] - predictions_ms[1]
                gains.append(
                    dict(
                        method=method,
                        measured_gain_ms=gain_ms,
                        predicted_gain_ms=expected_gain_ms,
                        gain_absolute_error_ms=abs(gain_ms - expected_gain_ms),
                    )
                )
                for cores, estimate, outcome in zip(
                    (1, 2), predictions_ms, measured_ms
                ):
                    cells.append(
                        dict(
                            rep=rep,
                            entry_cores=cores,
                            method=method,
                            predicted_ms=estimate,
                            measured_ms=outcome,
                            absolute_error_ms=abs(estimate - outcome),
                            absolute_percentage_error=100
                            * abs(estimate - outcome)
                            / outcome,
                        )
                    )
            pairs.append(
                dict(
                    rep=rep,
                    methods=gains,
                    gain_mae_ms=fmean(row["gain_absolute_error_ms"] for row in gains),
                    latency_mae_ms=fmean(
                        row["absolute_error_ms"] for row in cells if row["rep"] == rep
                    ),
                )
            )
        variants[name] = dict(
            pairs=pairs,
            cells=cells,
            mape_percent=fmean(row["absolute_percentage_error"] for row in cells),
            latency_mae_ms=fmean(row["absolute_error_ms"] for row in cells),
            gain_mae_ms=fmean(row["gain_mae_ms"] for row in pairs),
        )

    baseline = variants["continuous_ps"]["pairs"]
    eligible = [
        name
        for name, model in variants.items()
        if name != "continuous_ps"
        and all(
            trial["gain_mae_ms"] < base["gain_mae_ms"]
            and trial["latency_mae_ms"] < base["latency_mae_ms"]
            for trial, base in zip(model["pairs"], baseline, strict=True)
        )
    ]
    write(
        output / "scores.json",
        dict(
            predictions_sha256=digest(output / "predictions.json"),
            gate="Reduce latency MAE and intervention-gain MAE in both matched pairs",
            eligible_quota_scenarios=eligible,
            variants=variants,
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("sweep", "intervention", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    replay(args.sweep, args.intervention, args.output)

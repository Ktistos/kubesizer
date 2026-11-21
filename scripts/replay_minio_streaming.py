#!/usr/bin/env python3
"""Replay a fixed-layout MinIO streaming correction from lower-rate traces."""

import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
from statistics import fmean
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from analyze_minio_cpu_intervention import analyze as analyze_intervention
from analyze_minio_sweep import EDGE_SECONDS, NS, analyze as analyze_sweep
from replay_minio_cpu_quota import (
    METHODS,
    SEEDS,
    retained,
    simulate,
    treatments,
    workload,
)
from scripts.replay_minio_cpu import digest, write
from native_backend import get_native_backend


VARIANTS = {
    "continuous_low_cpu": (None, False, False),
    "quota_low_cpu": (2, False, False),
    "quota_middle_cpu": (2, True, False),
    "quota_middle_cpu_streaming": (2, True, True),
}
CANDIDATE = "quota_middle_cpu_streaming"
BASELINE = "quota_low_cpu"


def pooled_work(windows):
    puts = [w for w in windows if w["mode"] == "write"]
    mixed = [w for w in windows if w["mode"] == "mixed"]
    if len(puts) != 2 or len(mixed) != 2:
        raise ValueError("Expected two complete windows per mode and rate")
    put = sum(w["cpu_seconds"] for w in puts) / sum(w["counts"]["PUT"] for w in puts)
    get = (
        sum(w["cpu_seconds"] for w in mixed)
        - put * sum(w["counts"]["PUT"] for w in mixed)
    ) / sum(w["counts"]["GET"] for w in mixed)
    if min(put, get) <= 0:
        raise ValueError("Characterization produced nonpositive class CPU work")
    return dict(PUT=put, GET=get)


def coordinator_spans(archive, receipt):
    with archive.extractfile("./probe-minio1.jsonl.gz") as stream:
        probe_bytes = stream.read()
    if hashlib.sha256(probe_bytes).hexdigest() != receipt["probe-minio1.jsonl.gz"]:
        raise ValueError("Coordinator probe evidence failed receipt verification")
    by_put = defaultdict(list)
    for line in gzip.decompress(probe_bytes).splitlines():
        probe = json.loads(line)
        by_put[probe["temporary_path"]].append(probe)
    spans = []
    for probes in by_put.values():
        sent = [p for p in probes if p["side"] == "send"]
        if (
            len(probes) != 4
            or {p["peer"] for p in sent}
            != {"minio2:9000", "minio3:9000", "minio4:9000"}
            or any(not p["success"] for p in probes)
        ):
            raise ValueError("An incomplete PUT fragment fanout was retained")
        # All times in the envelope were recorded on the coordinator.
        first = min(p["start_ns"] for p in probes)
        last = max(p["start_ns"] + p["elapsed_ns"] for p in probes)
        spans.append((first, last))
    return spans


def window_phase_samples(response, protocol, spans):
    start = response["started_epoch_ns"]
    lower = start + (protocol["warmup_seconds"] + EDGE_SECONDS) * NS
    upper = (
        start
        + (protocol["warmup_seconds"] + protocol["measured_seconds"] - EDGE_SECONDS)
        * NS
    )
    return {
        "PUT": [
            (last - first) / NS
            for first, last in spans
            if lower <= first and last < upper
        ],
        "GET": [
            (row["last_byte_epoch_ns"] - row["headers_epoch_ns"]) / NS
            for row in response["rows"]
            if row["method"] == "GET"
            and lower <= row["headers_epoch_ns"]
            and row["last_byte_epoch_ns"] < upper
        ],
    }


def characterize(sweep):
    with tarfile.open(sweep) as archive:
        receipt = retained(archive, "evidence-receipt.json")
        protocol = retained(archive, "protocol.json", receipt)
        rates = sorted({window["rate"] for window in protocol["plan"]})
        if len(rates) != 3:
            raise ValueError("Expected two characterization rates and one scoring rate")
        low, middle, upper = rates
        spans = coordinator_spans(archive, receipt)

        windows, tagged, phases = [], defaultdict(list), defaultdict(list)
        for index, window in enumerate(protocol["plan"]):
            rate, mode = window["rate"], window["mode"]
            if rate == upper:
                continue
            block = f"window-{index:02d}"
            response = retained(archive, f"{block}.json.gz", receipt)
            before, after = [
                retained(archive, f"snapshots/{block}-{edge}.json", receipt)
                for edge in ("before", "after")
            ]
            (first,), (last,) = [
                s["members"]["minio1"]["processes"] for s in (before, after)
            ]
            if (first["pid"], first["start_ticks"]) != (
                last["pid"],
                last["start_ticks"],
            ):
                raise ValueError("The entry process restarted during characterization")
            if (
                before["errors"]
                or after["errors"]
                or any(not r["success"] for r in response["rows"])
            ):
                raise ValueError(f"Characterization is incomplete in {block}")
            counts = Counter(r["method"] for r in response["rows"])
            windows.append(
                dict(
                    block=block,
                    rate=rate,
                    mode=mode,
                    cpu_seconds=last["cpu_seconds"] - first["cpu_seconds"],
                    counts=dict(counts),
                )
            )
            for method, samples in window_phase_samples(
                response, protocol, spans
            ).items():
                phases[rate, mode, method].extend(samples)
            for row in response["rows"]:
                if row["tagged"]:
                    tagged[rate, mode, row["method"]].append(row["seconds"])

    characterization = {}
    for rate in (low, middle):
        characterization[rate] = dict(
            cpu_work_seconds=pooled_work([w for w in windows if w["rate"] == rate]),
            modes={
                mode: {
                    method: dict(
                        observed_mean_seconds=fmean(tagged[rate, mode, method]),
                        tagged_samples=len(tagged[rate, mode, method]),
                        phase_elapsed_mean_seconds=fmean(phases[rate, mode, method]),
                        phase_samples=len(phases[rate, mode, method]),
                    )
                    for method in (METHODS if mode == "mixed" else ("PUT",))
                }
                for mode in ("write", "mixed")
            },
        )
    return (low, middle, upper), characterization, windows


def model_at(characterization, rates, setting, mode, variant):
    low, middle, _ = rates
    p, middle_cpu, streaming = VARIANTS[variant]
    cpu = characterization[middle if middle_cpu else low]["cpu_work_seconds"]
    methods = METHODS if mode == "mixed" else ("PUT",)
    low_load = low * sum(cpu[method] for method in methods)
    if not 0 < low_load < 1:
        raise ValueError("The low-load CPU anchor is unstable")
    low_traffic = characterization[low]["modes"][mode]
    middle_traffic = characterization[middle]["modes"][mode]
    residuals = {}
    increments = {}
    for method in methods:
        low_ps = cpu[method] / (1 - low_load)
        slope = (
            middle_traffic[method]["phase_elapsed_mean_seconds"]
            - low_traffic[method]["phase_elapsed_mean_seconds"]
        ) / (middle - low)
        increment = slope * (setting["rate_per_class"] - low) if streaming else 0
        residuals[method] = (
            low_traffic[method]["observed_mean_seconds"] - low_ps + increment
        )
        increments[method] = increment
    if min(residuals.values()) <= 0:
        raise ValueError("The rate-specific CPU and residuals are unsupported")
    if mode == "write":
        # Reuse the same single-queue implementation for the one-class control.
        residuals["GET"] = residuals["PUT"]
    spec = workload(
        dict(cpu_work_seconds_per_completion=cpu, fixed_residual_seconds=residuals),
        setting,
        p,
    )
    if mode == "write":
        del spec["request_classes"]["GET"]
        spec["request_classes"]["PUT"]["concurrency_weight"] = 1.0
        del spec["traffic_model"]["entry_services"]["GET"]
        del spec["traffic_model"]["service_times"]["minio1"]["GET"]
    return spec, increments


def observations(sweep, intervention, rate):
    high = analyze_sweep(sweep)["windows"]
    arms = analyze_intervention(intervention)["arms"]
    cases = [
        dict(
            dataset="sweep", rep=w["rep"], mode=w["mode"], cores=1, classes=w["classes"]
        )
        for w in high
        if w["rate"] == rate
    ]
    cases.extend(
        dict(
            dataset="cpu_ab",
            rep=a["rep"],
            mode="mixed",
            cores=a["entry_cores"],
            classes=a["classes"],
        )
        for a in arms
    )
    if len(cases) != 8:
        raise ValueError("Expected all eight high-rate comparison windows")
    return cases


def diagnose_upper_phases(sweep, rate, low_fit, forecast):
    with tarfile.open(sweep) as archive:
        receipt = retained(archive, "evidence-receipt.json")
        protocol = retained(archive, "protocol.json", receipt)
        spans = coordinator_spans(archive, receipt)
        samples = defaultdict(list)
        for i, cell in enumerate(protocol["plan"]):
            if cell["rate"] == rate:
                response = retained(archive, f"window-{i:02d}.json.gz", receipt)
                for method, values in window_phase_samples(
                    response, protocol, spans
                ).items():
                    samples[cell["mode"], method].extend(values)
    return [
        dict(
            mode=mode,
            method=method,
            samples=len(values),
            measured_phase_mean_ms=1000 * fmean(values),
            extrapolated_phase_mean_ms=1000
            * (
                low_fit["modes"][mode][method]["phase_elapsed_mean_seconds"]
                + forecast[f"{mode}-1cpu"]["phase_increment_seconds"][method]
            ),
        )
        for (mode, method), values in samples.items()
        if values
    ]


def score(predictions, cases):
    variants = {}
    for name, estimates in predictions.items():
        rows = [
            dict(
                dataset=c["dataset"],
                rep=c["rep"],
                mode=c["mode"],
                cores=c["cores"],
                method=m,
                observed_ms=value["mean_ms"],
                predicted_ms=estimates[f"{c['mode']}-{c['cores']}cpu"]["classes"][m][
                    "mean_ms"
                ],
            )
            for c in cases
            for m, value in c["classes"].items()
        ]
        by_pair = {
            (r["rep"], r["cores"], r["method"]): r
            for r in rows
            if r["dataset"] == "cpu_ab"
        }
        pairs = []
        for rep in (0, 1):
            gains = []
            for method in METHODS:
                one, two = (by_pair[rep, c, method] for c in (1, 2))
                gains.append(
                    dict(
                        method=method,
                        observed_gain_ms=one["observed_ms"] - two["observed_ms"],
                        predicted_gain_ms=one["predicted_ms"] - two["predicted_ms"],
                    )
                )
            pairs.append(
                dict(
                    rep=rep,
                    gains=gains,
                    gain_mae_ms=fmean(
                        abs(g["observed_gain_ms"] - g["predicted_gain_ms"])
                        for g in gains
                    ),
                    latency_mae_ms=fmean(
                        abs(r["observed_ms"] - r["predicted_ms"])
                        for r in by_pair.values()
                        if r["rep"] == rep
                    ),
                )
            )
        metrics = {}
        for data in ("cpu_ab", "sweep"):
            group = [r for r in rows if r["dataset"] == data]
            metrics[data] = dict(
                latency_mae_ms=fmean(
                    abs(r["observed_ms"] - r["predicted_ms"]) for r in group
                ),
                mape_percent=fmean(
                    100 * abs(r["observed_ms"] - r["predicted_ms"]) / r["observed_ms"]
                    for r in group
                ),
            )
        variants[name] = dict(rows=rows, pairs=pairs, metrics=metrics)
    candidate, baseline = variants[CANDIDATE], variants[BASELINE]
    passed = candidate["metrics"]["sweep"]["latency_mae_ms"] < baseline["metrics"][
        "sweep"
    ]["latency_mae_ms"] and all(
        c["gain_mae_ms"] < b["gain_mae_ms"]
        and c["latency_mae_ms"] < b["latency_mae_ms"]
        for c, b in zip(candidate["pairs"], baseline["pairs"], strict=True)
    )
    return dict(
        variants=variants,
        candidate=CANDIDATE,
        baseline=BASELINE,
        eligible_for_fresh_fixed_layout_test=passed,
    )


def replay(sweep, intervention, output):
    rates, characterization, windows = characterize(sweep)
    settings = treatments(intervention)
    if any(setting["rate_per_class"] != rates[-1] for setting in settings.values()):
        raise ValueError("The highest sweep rate and matched CPU experiments differ")
    output.mkdir(parents=True, exist_ok=False)
    predictions = {}
    for variant in VARIANTS:
        variants = {}
        for mode, cores in (("write", 1), ("mixed", 1), ("mixed", 2)):
            setting = settings[cores]
            spec, increments = model_at(characterization, rates, setting, mode, variant)
            variants[f"{mode}-{cores}cpu"] = dict(
                workload=spec,
                phase_increment_seconds=increments,
                classes=simulate(spec, setting),
            )
        predictions[variant] = variants
    path = output / "predictions.json"
    write(
        path,
        dict(
            sweep_sha256=digest(sweep),
            intervention_sha256=digest(intervention),
            replay_sha256=digest(Path(__file__)),
            native_binary_sha256=digest(get_native_backend().binary_path),
            seeds=SEEDS,
            rates=rates,
            settings=settings,
            characterization=characterization,
            cpu_windows=windows,
            predictions=predictions,
        ),
    )
    scored = score(predictions, observations(sweep, intervention, rates[-1]))
    scored["upper_rate_phase_diagnostics"] = diagnose_upper_phases(
        sweep, rates[-1], characterization[rates[0]], predictions[CANDIDATE]
    )
    write(output / "scores.json", dict(predictions_sha256=digest(path), **scored))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("sweep", "intervention", "output"):
        parser.add_argument(arg, type=Path)
    args = parser.parse_args()
    replay(args.sweep, args.intervention, args.output)

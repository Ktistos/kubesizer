#!/usr/bin/env python3
"""Characterize a MinIO read/write mix and reserve an intermediate mix for validation."""

import argparse
from copy import deepcopy
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import signal
from statistics import fmean
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from analyze_minio_sweep import MEMBERS, analyze
from scripts import characterize_minio_fragments as fragments
from scripts import characterize_minio_sweep as sweep
from scripts import run_minio_pilot as pilot


PUT_RATE = 10.8
TRAIN_RATES = (0.0, 3.6, 10.8)
HOLDOUT_RATE = 7.2
FIELDS = ("put_ms", "put_fragment_ms", "get_ms", "get_body_ms")


def design(rates, cores, seed):
    return [
        dict(
            rate=PUT_RATE,
            get_rate=reads,
            mode="mixed" if reads else "write",
            entry_cores=cores,
            rep=rep,
            seed=seed + rep,
        )
        for rep, order in enumerate((rates, rates[::-1]))
        for reads in order
    ]


def observe(folder, cores, *, retain_successful_only=False):
    with tempfile.TemporaryDirectory() as directory:
        archive = Path(directory) / "arm.tar.gz"
        with tarfile.open(archive, "w:gz") as stream:
            stream.add(folder, arcname=".")
        windows = analyze(
            archive,
            include_phase_spans=True,
            skip_failed_windows=retain_successful_only,
        )["windows"]
    for window in windows:
        if (
            window["incomplete_fragments"]
            or window["telemetry_errors"]
            or window["total_failures"]
        ):
            raise ValueError(
                f"Unusable fragment, request, or hardware data in {window['block']}"
            )
        for edge in ("before", "after"):
            cpu = json.loads(
                (folder / "snapshots" / f"{window['block']}-{edge}.json").read_text()
            )
            for member in MEMBERS:
                stats = cpu["members"][member]["cgroup_cpu_stat"]
                expected = cores if member == "minio1" else 1
                if stats["quota_us"] != expected * stats["period_us"]:
                    raise ValueError(
                        f"Actual cgroup CPU quota disagrees with plan on {member}"
                    )
    return windows


def measures(window):
    return {
        "put_ms": window["classes"]["PUT"]["mean_ms"],
        "put_fragment_ms": window["phase_spans"]["put_ms"],
        "get_ms": window["classes"].get("GET", {}).get("mean_ms"),
        "get_body_ms": window["phase_spans"]["get_body_ms"],
    }


def fit(windows):
    means = {}
    for rate in TRAIN_RATES:
        group = [measures(w) for w in windows if w["get_rate"] == rate]
        if len(group) != 2:
            raise ValueError("Both characterization replicates are required")
        means[rate] = {
            name: (
                fmean(row[name] for row in group)
                if group[0][name] is not None
                else None
            )
            for name in FIELDS
        }

    def between(field, low, high):
        fraction = (HOLDOUT_RATE - low) / (high - low)
        return means[low][field] + fraction * (means[high][field] - means[low][field])

    candidate = {field: between(field, 3.6, 10.8) for field in FIELDS}
    endpoints = {
        **candidate,
        **{field: between(field, 0.0, 10.8) for field in ("put_ms", "put_fragment_ms")},
    }
    return means, dict(piecewise=candidate, endpoints=endpoints)


def score(windows, predictions):
    rows = [
        dict(
            rep=w["rep"],
            get_rate=w["get_rate"],
            measured=measures(w),
            errors={
                variant: {
                    name: value - w_measures[name] for name, value in estimate.items()
                }
                for variant, estimate in predictions.items()
            },
        )
        for w in windows
        for w_measures in [measures(w)]
    ]
    mae = {
        variant: {
            name: fmean(abs(row["errors"][variant][name]) for row in rows)
            for name in estimate
        }
        for variant, estimate in predictions.items()
    }
    return dict(rows=rows, mean_absolute_error_ms=mae)


def prepare_transfer(replay, destination):
    from replay_minio_cpu_quota import SEEDS, simulate

    record = json.loads(replay.read_text())
    means = {}
    specs = {}
    for cores in (1, 2):
        spec = deepcopy(
            record["predictions"]["quota_middle_cpu_streaming"][f"mixed-{cores}cpu"][
                "workload"
            ]
        )
        spec["traffic_model"]["entry_services"]["GET"]["minio1"]["rate"] = HOLDOUT_RATE
        for method, rate in (("GET", HOLDOUT_RATE), ("PUT", PUT_RATE)):
            spec["request_classes"][method]["concurrency_weight"] = rate / (
                HOLDOUT_RATE + PUT_RATE
            )
        specs[cores] = spec
        means[cores] = {
            method: val["mean_ms"]
            for method, val in simulate(spec, record["settings"][str(cores)]).items()
        }
    pilot.write(
        destination,
        dict(
            source_sha256=pilot.digest(replay),
            put_rate=PUT_RATE,
            get_rate=HOLDOUT_RATE,
            cpu_difference_ms={m: means[1][m] - means[2][m] for m in ("PUT", "GET")},
            cpu_prediction_ms=means,
            cpu_models=specs,
            seeds=SEEDS,
        ),
    )


def verify_campaign(archive):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        with tarfile.open(archive) as stream:
            stream.extractall(root, filter="data")

        def read(name):
            return json.loads((root / name).read_text())

        status = read("campaign-status.json")
        if status["status"] != "CAPTURED":
            raise ValueError("Expected a completed mixture campaign")
        plan = read("campaign-plan.json")
        offset_path = root / "transfer-offset.json"
        if pilot.digest(offset_path) != plan["cpu_transfer_offset_sha256"]:
            raise ValueError("Frozen CPU offset differs from the plan")
        frozen = read("predictions-frozen.json")
        hash_ = pilot.digest(root / "predictions-frozen.json")
        if read("freeze-receipt.json")["predictions-frozen.json"] != hash_:
            raise ValueError("Prediction freeze receipt does not match the forecast")
        if (
            pilot.digest(root / "training-2cpu/evidence-receipt.json")
            != frozen["training_receipt_sha256"]
        ):
            raise ValueError("Training evidence differs from the prediction provenance")
        samples = observe(root / "training-2cpu", 2)
        if "retained_training_receipt_sha256" in frozen:
            retained = root / "retained-training-2cpu"
            if (
                pilot.digest(retained / "evidence-receipt.json")
                != frozen["retained_training_receipt_sha256"]
            ):
                raise ValueError(
                    "Retained characterization evidence differs from the freeze receipt"
                )
            samples = observe(retained, 2, retain_successful_only=True) + samples
        fit_means, predictions = fit(samples)
        if (
            json.loads(json.dumps(fit_means)) != frozen["characterization_by_get_rate"]
            or predictions != frozen["prediction_2cpu"]
        ):
            raise ValueError(
                "Forecast could not be rebuilt from archived training evidence"
            )
        stages = {"training-2cpu": samples}
        for arm in status["completed"]:
            name = arm["name"]
            if name == "training-2cpu":
                continue
            stamp = read(f"{name}/status.json")["started_at"]
            if datetime.fromisoformat(stamp) < datetime.fromisoformat(
                frozen["frozen_at"]
            ):
                raise ValueError(
                    "A validation stage started before predictions were frozen"
                )
            cores = 2 if name == "holdout-2cpu" else 1
            held = observe(root / name, cores)
            if len(held) != 2 or any(w["get_rate"] != HOLDOUT_RATE for w in held):
                raise ValueError(
                    "The held-out observations differ from the reserved mix"
                )
            recorded = read(f"{name}-scores.json")
            expected = score(held, frozen[f"prediction_{cores}cpu"])
            if recorded["frozen_sha256"] != hash_ or any(
                recorded[key] != value for key, value in expected.items()
            ):
                raise ValueError("Saved validation scores differ from the replay")
            stages[name] = held
        return dict(
            frozen_predictions_sha256=hash_,
            campaign_status=status,
            reserved_scores=read("holdout-2cpu-scores.json"),
            transfer_scores=(
                read("holdout-1cpu-scores.json") if "holdout-1cpu" in stages else None
            ),
            windows=stages,
        )


def restore(output):
    path = output / "campaign-status.json"
    if path.exists():
        status = json.loads(path.read_text())
        current = status.get("current")
        if current:
            os.environ["KUBESIZER_ENTRY_CPU_LIMIT"] = str(current["entry_cores"])
            fragments.restore(output / current["name"])


def run(output, deps, binary, digest, offset_file, *, resume_training=None):
    output.mkdir(parents=True, exist_ok=False)
    offset = json.loads(offset_file.read_text())
    if (offset["put_rate"], offset["get_rate"]) != (PUT_RATE, HOLDOUT_RATE):
        raise ValueError("CPU transfer settings differ from the reserved mixture")
    train = design(TRAIN_RATES, 2, 26112520)
    held = design((HOLDOUT_RATE,), 2, 26112540)
    transfer = design((HOLDOUT_RATE,), 1, 26112540)
    pilot.write(
        output / "campaign-plan.json",
        dict(
            training=train,
            reserved_2cpu=held,
            conditional_1cpu=transfer,
            model="piecewise linear response in GET rate at fixed PUT rate",
            comparator="linear response between 0 and 10.8 GET/s for PUT latency and span",
            transfer_check="run if 2-CPU PUT latency and fragment-span MAE both improve over the comparator",
            cpu_transfer_offset_sha256=pilot.digest(offset_file),
        ),
    )
    pilot.write(output / "transfer-offset.json", offset)
    status = dict(status="RUNNING", started_at=pilot.now(), current=None, completed=[])
    pilot.write(output / "campaign-status.json", status)
    retained = []
    recovery = train
    retained_hash = {}

    def collect(name, cores, plan, *, prepared, forecast_hash=None):
        os.environ["KUBESIZER_ENTRY_CPU_LIMIT"] = str(cores)
        status["current"] = dict(
            name=name, entry_cores=cores, frozen_forecast_sha256=forecast_hash
        )
        pilot.write(output / "campaign-status.json", status)
        folder = output / name
        arm = sweep.run(
            folder,
            deps,
            binary,
            digest,
            plan=plan,
            use_preloaded_bucket=prepared,
            require_space=resume_training is not None,
        )
        status["completed"].append(dict(name=name, outcome=arm["status"]))
        if arm["status"] != "CAPTURED":
            status.update(status="STOPPED_AT_GUARD", completed_at=pilot.now())
            return None
        return observe(folder, cores)

    try:
        if resume_training is not None:
            earlier = json.loads((resume_training / "campaign-status.json").read_text())
            earlier_plan = json.loads(
                (resume_training / "campaign-plan.json").read_text()
            )
            if (
                earlier["status"] != "STOPPED_AT_GUARD"
                or (resume_training / "predictions-frozen.json").exists()
                or earlier_plan["training"] != train
                or earlier_plan["reserved_2cpu"] != held
            ):
                raise ValueError(
                    "The earlier campaign is not an unopened, stopped characterization"
                )
            retained_root = output / "retained-training-2cpu"
            shutil.copytree(resume_training / "training-2cpu", retained_root)
            retained = observe(retained_root, 2, retain_successful_only=True)
            recovery = [
                cell
                for cell in train
                if not any(all(w[k] == v for k, v in cell.items()) for w in retained)
            ]
            if len(retained) != 5 or len(recovery) != 1:
                raise ValueError(
                    "Expected five complete characterization windows and one retry"
                )
            retained_hash = dict(
                retained_training_receipt_sha256=pilot.digest(
                    retained_root / "evidence-receipt.json"
                )
            )
        baseline = collect(
            "training-2cpu", 2, recovery, prepared=resume_training is not None
        )
        if baseline is None:
            return
        means, forecast = fit(retained + baseline)
        frozen = output / "predictions-frozen.json"
        transfer_predictions = dict(
            unchanged_2cpu=forecast["piecewise"],
            cpu_adjusted={
                **forecast["piecewise"],
                **{
                    f"{m.lower()}_ms": forecast["piecewise"][f"{m.lower()}_ms"]
                    + offset["cpu_difference_ms"][m]
                    for m in ("PUT", "GET")
                },
            },
        )
        pilot.write(
            frozen,
            dict(
                frozen_at=pilot.now(),
                held_out_get_rate=HOLDOUT_RATE,
                training_receipt_sha256=pilot.digest(
                    output / "training-2cpu/evidence-receipt.json"
                ),
                characterization_by_get_rate=means,
                prediction_2cpu=forecast,
                prediction_1cpu=transfer_predictions,
                **retained_hash,
            ),
        )
        forecast_hash = pilot.digest(frozen)
        pilot.write(output / "freeze-receipt.json", {frozen.name: forecast_hash})
        observations = collect(
            "holdout-2cpu", 2, held, prepared=True, forecast_hash=forecast_hash
        )
        if observations is None:
            return
        if pilot.digest(frozen) != forecast_hash:
            raise ValueError("Frozen predictions changed after the holdout started")
        scores = score(observations, forecast)
        errors = scores["mean_absolute_error_ms"]
        eligible = all(
            errors["piecewise"][m] < errors["endpoints"][m]
            for m in ("put_ms", "put_fragment_ms")
        )
        pilot.write(
            output / "holdout-2cpu-scores.json",
            dict(
                frozen_sha256=forecast_hash,
                eligible_for_cpu_transfer=eligible,
                **scores,
            ),
        )
        status["eligible_for_cpu_transfer"] = eligible
        if eligible:
            result = collect(
                "holdout-1cpu", 1, transfer, prepared=True, forecast_hash=forecast_hash
            )
            if result is None:
                return
            if pilot.digest(frozen) != forecast_hash:
                raise ValueError(
                    "Frozen predictions changed after the transfer check started"
                )
            pilot.write(
                output / "holdout-1cpu-scores.json",
                dict(
                    frozen_sha256=forecast_hash, **score(result, transfer_predictions)
                ),
            )
        status.update(status="CAPTURED", completed_at=pilot.now())
    except BaseException as error:
        status.update(
            status="FAILED",
            error=f"{type(error).__name__}: {error}",
            failed_at=pilot.now(),
        )
        raise
    finally:
        pilot.write(output / "campaign-status.json", status)
        restore(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "output",
        "deps",
        "binary",
        "transfer-offset",
        "prepare-transfer",
        "verify",
        "resume-training",
    ):
        parser.add_argument(f"--{name}", type=Path)
    parser.add_argument("--digest")
    parser.add_argument("--finish", action="store_true")
    args = parser.parse_args()
    if args.verify:
        print(json.dumps(verify_campaign(args.verify), indent=2, allow_nan=False))
    elif args.prepare_transfer:
        if args.transfer_offset is None:
            parser.error("--prepare-transfer requires --transfer-offset")
        prepare_transfer(args.prepare_transfer, args.transfer_offset)
    elif args.finish:
        restore(args.output.resolve())
    else:
        if None in (
            args.output,
            args.deps,
            args.binary,
            args.digest,
            args.transfer_offset,
        ):
            parser.error(
                "--output, --deps, --binary, --digest and --transfer-offset are required"
            )

        def interrupt(*_):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupt)
        run(
            args.output.resolve(),
            args.deps.resolve(),
            args.binary.resolve(),
            args.digest,
            args.transfer_offset,
            resume_training=args.resume_training,
        )

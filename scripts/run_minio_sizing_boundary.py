"""Locate a MinIO SLO boundary and test frozen sizing choices on fresh pairs."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import gzip
import json
import math
from pathlib import Path
import shutil
import signal
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import run_minio_sizing as sizing

lab = sizing.lab
pilot = sizing.pilot
PROTOCOL = {
    "candidate_members": [4, 8],
    "hardware": "same four workers and Longhorn volumes as mis1-r4",
    "baseline_total_rate": 24,
    "sweep_total_rates": [28, 32, 36, 40],
    "sweep_seed": 26112021,
    "target_seed": 26112081,
    "target_pairs": 4,
    "warmup_seconds": 15,
    "sweep_measurement_seconds": 45,
    "target_measurement_seconds": 75,
    "storage_headroom_wait_seconds": 360,
    "mean_slo_seconds": sizing.PLAN["mean_slo_seconds"],
    "failure_rate_limit": sizing.PLAN["failure_rate_limit"],
    "schedule_p95_limit_ms": sizing.PLAN["schedule_p95_limit_ms"],
    "max_cohort_drain_seconds": sizing.PLAN["max_cohort_drain_seconds"],
    "selection": "Test the last apparent pass, the first apparent miss, and their midpoint. "
                 "With no apparent miss, test the two highest valid rates and their midpoint. "
                 "An invalid client run stops the sweep without establishing an SLO miss.",
    "estimation": "Keep the completed mis1-r4 aggregate characterization. The new sweep selects rates only.",
    "storage": "Check per-volume free space before each window for all scheduled writes on a single set.",
}
CONFIRMATION = {
    "candidate_members": PROTOCOL["candidate_members"],
    "hardware": PROTOCOL["hardware"],
    "total_rates": [30, 32],
    "target_seed": 26113081,
    "precheck_seed": 26113010,
    "precheck_measurement_seconds": 45,
    "target_pairs": PROTOCOL["target_pairs"],
    "warmup_seconds": PROTOCOL["warmup_seconds"],
    "target_measurement_seconds": PROTOCOL["target_measurement_seconds"],
    "max_in_flight": 128,
    "observer_limits": {"cpu": "1", "memory": "384Mi"},
    "mean_slo_seconds": PROTOCOL["mean_slo_seconds"],
    "estimation": "Use the original frozen recommendations without recomputing any model.",
    "storage": PROTOCOL["storage"],
}


def read(path):
    return json.loads(path.read_text())


def pilot_condition(row):
    if not row["client_valid"]:
        return "CLIENT_INVALID"
    for name, slo in PROTOCOL["mean_slo_seconds"].items():
        sample = row["classes"][name]
        if (sample["successes"] / sample["offered"] < 1 - PROTOCOL["failure_rate_limit"]
                or sample["mean_ms"] is None or sample["mean_ms"] > slo * 1000
                or row["drain_seconds"] > PROTOCOL["max_cohort_drain_seconds"]):
            return "APPARENT_MISS"
    return "APPARENT_PASS"


def select_rates(exploration):
    passed = [PROTOCOL["baseline_total_rate"]]
    for row in exploration:
        if row["condition"] == "APPARENT_MISS":
            lower, upper = passed[-1], row["rate"] * 2
            return [lower, (lower + upper) / 2, upper]
        if row["condition"] == "CLIENT_INVALID":
            break
        passed.append(row["rate"] * 2)
    if len(passed) < 2:
        raise ValueError("The sweep did not produce a valid rate above baseline")
    lower, upper = passed[-2:]
    return [lower, (lower + upper) / 2, upper]


def target_plan(total_rates):
    return {
        **sizing.PLAN,
        "warmup_seconds": PROTOCOL["warmup_seconds"],
        "measurement_seconds": PROTOCOL["target_measurement_seconds"],
        "storage_headroom_wait_seconds": PROTOCOL["storage_headroom_wait_seconds"],
        "seed": PROTOCOL["target_seed"],
        "target_pairs": PROTOCOL["target_pairs"],
        "target_equal_per_class_rates": [rate / 2 for rate in total_rates],
    }


def bind_prior(output, source, binary_digest):
    status = read(source / "status.json")
    if status["status"] != "COMPLETED" or status["completed"] != status["total"]:
        raise ValueError("The baseline trial must have completed")
    if not read(source / "template-restoration.json")["stock_entrypoints_restored"]:
        raise ValueError("The baseline trial has no restoration receipt")
    sizing.verify(source, "prediction-receipt.json")
    for size in lab.LAYOUTS:
        if read(source / f"instrumented-binaries-{size}.json") != dict.fromkeys(lab.members(size), binary_digest):
            raise ValueError(f"Layout {size} must reuse the measured MinIO binary")
        updated = set()
        for row in read(source / "target-results.json"):
            if row["layout"] != size:
                continue
            window = source / "windows" / row["id"]
            sizing.verify(window, "receipt.json")
            with gzip.open(window / "requests.json.gz", "rt") as stream:
                updated.update(r["key"] for r in json.load(stream)["rows"] if r["method"] == "PUT" and r["success"])
        expected = {f"size-{sizing.paths.SIZE}/put-{n:03d}" for n in range(sizing.sweep.PUT_KEYS)}
        if updated != expected:
            raise ValueError(f"Earlier writes did not establish all expected PUT payloads for layout {size}")
        shutil.copyfile(source / f"layout-{size}.yaml", output / f"layout-{size}.yaml")
    shutil.copyfile(source / "profile-aggregate.json", output / "profile-aggregate.json")
    pilot.write(output / "prior-evidence.json", {
        "source": str(source),
        "prediction_receipt_sha256": pilot.digest(source / "prediction-receipt.json"),
        "all_put_keys_updated": sorted(expected),
        "binary_digest": binary_digest,
    })
    return sorted(expected)


def freeze(output, plan, exploratory):
    pilot.write(output / "target-plan.json", plan)
    order = sizing.target_order(plan)
    pilot.write(output / "target-order.json", order)
    profile = read(output / "profile-aggregate.json")
    predictions = {}
    for rate in plan["target_equal_per_class_rates"]:
        try:
            predictions[f"{rate:g}"] = sizing.cli(output, "aggregate", rate, profile, None)
        except (ValueError, subprocess.TimeoutExpired) as error:
            predictions[f"{rate:g}"] = {"abstained": str(error)}
    pilot.write(output / "predictions-frozen.json", {"frozen_at": pilot.now(), "arms": {"aggregate": predictions}})
    sizing.seal(output, "prediction-receipt.json", [
        *(output / name for name in (
            "protocol.json", "source-receipt.json", "prior-evidence.json", "profile-aggregate.json",
            "exploration.json", "target-plan.json", "target-order.json", "predictions-frozen.json",
        )),
        *(output / "windows" / row["id"] / "receipt.json" for row in exploratory),
        *sorted(file for file in (output / "models").rglob("*") if file.is_file()),
    ])
    return order


def bind_confirmation(output, source):
    status = read(source / "status.json")
    if status["status"] != "COMPLETED" or status["completed"] != status["total"]:
        raise ValueError("The earlier boundary trial must have completed")
    sizing.verify(source, "prediction-receipt.json")
    if (source / "profile-aggregate.json").read_bytes() != (output / "profile-aggregate.json").read_bytes():
        raise ValueError("The frozen forecasts were trained on a different aggregate profile")
    plan = target_plan(CONFIRMATION["total_rates"])
    plan["seed"] = CONFIRMATION["target_seed"]
    order = sizing.target_order(plan)
    earlier_seeds = {job["seed"] for job in read(source / "target-order.json")}
    if earlier_seeds.intersection(job["seed"] for job in order):
        raise ValueError("Confirmation targets reuse an earlier holdout seed")
    source_forecast = source / "predictions-frozen.json"
    prior_arms = read(source_forecast)["arms"]["aggregate"]
    if any(f"{rate:g}" not in prior_arms for rate in plan["target_equal_per_class_rates"]):
        raise ValueError("A confirmation rate has no earlier frozen forecast")
    shutil.copyfile(source_forecast, output / source_forecast.name)
    pilot.write(output / "target-plan.json", plan)
    pilot.write(output / "target-order.json", order)
    pilot.write(output / "retained-forecasts.json", {
        "source": str(source),
        "prediction_receipt_sha256": pilot.digest(source / "prediction-receipt.json"),
        "predictions_sha256": pilot.digest(source_forecast),
        "frozen_at": read(source_forecast)["frozen_at"],
    })
    sizing.seal(output, "prediction-receipt.json", [
        output / name for name in (
            "protocol.json", "source-receipt.json", "prior-evidence.json", "profile-aggregate.json",
            "retained-forecasts.json", "target-plan.json", "target-order.json", source_forecast.name,
        )
    ])
    return plan, order


def check_delivery(output, job, plan):
    window = output / "windows" / job["id"]
    with gzip.open(window / "requests.json.gz", "rt") as stream:
        rows = json.load(stream)["rows"]
    warmup = plan["warmup_seconds"]
    horizon = warmup + plan["measurement_seconds"]
    arrivals = sorted([
        *sizing.sweep.arrivals(job["rate"], "PUT", job["seed"], horizon),
        *sizing.sweep.arrivals(job["rate"], "GET", job["seed"] + 1_000_000, horizon),
    ])
    if len(rows) != len(arrivals) or any(
        (row["method"], row["key"]) != (method, key)
        or not math.isclose(row["scheduled_seconds"], timestamp, rel_tol=0, abs_tol=1e-9)
        or row["tagged"] != (row["scheduled_seconds"] >= warmup)
        for row, (timestamp, method, key) in zip(rows, arrivals)
    ):
        raise ValueError(f"Captured traffic does not match the declared schedule for {job['id']}")
    started = [row for row in rows if "sdk_start_epoch_ns" in row]
    events = []
    for row in started:
        events.extend([(row["sdk_start_epoch_ns"], 1), (row["sdk_finish_epoch_ns"], -1)])
    active = peak = 0
    for _, change in sorted(events):
        active += change
        peak = max(peak, active)
    summary = {
        "id": job["id"],
        "scheduled": len(rows),
        "started": len(started),
        "measured_scheduled": sum(row["tagged"] for row in rows),
        "measured_started": sum(row["tagged"] for row in started),
        "peak_in_flight": peak,
    }
    return summary


def explore(output, client, mark):
    exploratory = []
    sweep_plan = {**PROTOCOL, "measurement_seconds": PROTOCOL["sweep_measurement_seconds"]}
    for index, rate in enumerate(PROTOCOL["sweep_total_rates"]):
        job = {"id": f"explore-{rate}", "layout": 4, "rate": rate / 2,
               "mode": "mixed", "seed": PROTOCOL["sweep_seed"] + index}
        mark("EXPLORING", current=job["id"])
        row = sizing.run_window(output, 4, job, client, plan=sweep_plan)
        row["condition"] = pilot_condition(row)
        exploratory.append(row)
        pilot.write(output / "exploration.json", exploratory)
        if row["condition"] != "APPARENT_PASS":
            break
    rates = select_rates(exploratory)
    plan = target_plan(rates)
    mark("FREEZING", current=None, target_total_rates=rates)
    return plan, freeze(output, plan, exploratory)


def run(output, source, deps, binary, binary_digest, *, confirm_from=None):
    output.mkdir(parents=True, exist_ok=False)
    confirm = confirm_from is not None
    pilot.write(output / "protocol.json", CONFIRMATION if confirm else PROTOCOL)
    sizing.seal(sizing.ROOT, output / "source-receipt.json", [
        Path(__file__).resolve(),
        *(sizing.ROOT / name for name in (
            "scripts/run_minio_sizing.py", "scripts/minio_sizing_lab.py",
            "scripts/characterize_minio_sweep.py", "scripts/characterize_minio_paths.py",
            "kubesizer_cli.py", "native_backend/backend.py", "metrics/time_weighted_buckets.py",
            "native_backend/cpp/build-release/kubesizer-simulator",
        )),
    ])
    status = {"status": "STARTING", "started_at": pilot.now(), "completed": 0}

    def mark(stage, **details):
        status.update(status=stage, updated_at=pilot.now(), **details)
        pilot.write(output / "status.json", status)

    mark("STARTING")
    try:
        lab.assert_isolated()
        keys = bind_prior(output, source, binary_digest)
        client_limits = CONFIRMATION["observer_limits"] if confirm else None
        client_cap = CONFIRMATION["max_in_flight"] if confirm else sizing.sweep.MAX_IN_FLIGHT
        if confirm:
            plan, order = bind_confirmation(output, confirm_from)
            mark("STARTING", target_total_rates=CONFIRMATION["total_rates"])
        pilot.write(output / "resources-owned.json", {"used_layouts": list(lab.LAYOUTS), "prior": str(source)})
        lab.start(4, output, instrumented=binary, digest=binary_digest, client_limits=client_limits)
        with ExitStack() as sessions:
            client_args = {"preloaded": True, "updated_keys": keys, "max_in_flight": client_cap}
            clients = {4: sessions.enter_context(sizing.client_session(output, 4, deps, **client_args))}
            if not confirm:
                plan, order = explore(output, clients[4], mark)
            mark("PROVISIONING_ALTERNATIVE")
            lab.start(8, output, instrumented=binary, digest=binary_digest, client_limits=client_limits)
            clients[8] = sessions.enter_context(sizing.client_session(output, 8, deps, **client_args))
            if confirm:
                precheck_plan = {**plan, "measurement_seconds": CONFIRMATION["precheck_measurement_seconds"]}
                for size in lab.LAYOUTS:
                    job = {"id": f"precheck-layout{size}", "layout": size,
                           "rate": max(plan["target_equal_per_class_rates"]),
                           "mode": "mixed", "seed": CONFIRMATION["precheck_seed"]}
                    mark("CLIENT_PRECHECK", current=job["id"])
                    row = sizing.run_window(output, size, job, clients[size], plan=precheck_plan)
                    delivery = check_delivery(output, job, precheck_plan)
                    pilot.write(output / f"client-precheck-{size}.json", {"summary": row, "delivery": delivery})
                    if not row["client_valid"] or delivery["started"] != delivery["scheduled"]:
                        raise RuntimeError(f"The resized client could not deliver the precheck on layout {size}")
            results = []
            deliveries = []
            for job in order:
                sizing.verify(output, "prediction-receipt.json")
                mark("HOLDOUT", current=job["id"], completed=len(results), total=len(order))
                results.append(sizing.run_window(output, job["layout"], job, clients[job["layout"]], plan=plan))
                pilot.write(output / "target-results.json", results)
                if confirm:
                    deliveries.append(check_delivery(output, job, plan))
                    pilot.write(output / "client-delivery.json", deliveries)
                sizing.scores(output, results, plan=plan)
        mark("COMPLETED", current=None, completed=len(results), completed_at=pilot.now())
    except BaseException as error:
        mark("FAILED", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        sizing.finish(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--prior", type=Path)
    parser.add_argument("--confirm-from", type=Path)
    parser.add_argument("--deps", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--binary-digest")
    parser.add_argument("--finish", action="store_true")
    parser.add_argument("--score-only", action="store_true")
    args = parser.parse_args()
    if args.finish:
        sizing.finish(args.output)
    elif args.score_only:
        sizing.scores(args.output, read(args.output / "target-results.json"), plan=read(args.output / "target-plan.json"))
    else:
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        run(args.output.resolve(), args.prior.resolve(), args.deps.resolve(), args.binary.resolve(),
            args.binary_digest, confirm_from=args.confirm_from.resolve() if args.confirm_from else None)

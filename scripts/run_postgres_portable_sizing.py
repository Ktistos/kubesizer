"""Compare container-level and process-level characterization on the same PostgreSQL targets."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
from datetime import datetime
import gzip
import json
import math
from pathlib import Path
import random
import signal
import statistics
import subprocess
import sys
import time

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import run_postgres_pilot as pg
from scripts import run_postgres_sizing as trial
from scripts.audit_postgres_sizing import audit_run

RESOURCES = trial.PilotResources("pgs2", "sizing-gateway-pgs2", "sizing-source-pgs2")
COUNTER_FIELD = "aggregate_container_cpu"
CHARACTERIZATION = (
    ("read-only", 12.0, 1.0),
    ("write-only", 20.0, 0.0),
    ("mixed-0", 12.0, 0.75),
    ("mixed-1", 12.0, 0.75),
)
PLAN = {
    **trial.PLAN,
    "trial": RESOURCES.name,
    "seed": 26111961,
    "target_rates_rps": [20.0, 28.0, 34.0],
    "warmup_seconds": 25,
    "measurement_seconds": 65,
    "characterization_measurement_seconds": 95,
    "characterization_conditions": [
        {"run_id": name, "rate_rps": rate, "read_share": read_share}
        for name, rate, read_share in CHARACTERIZATION
    ],
    "idle_seconds": 80,
    "minimum_cpu_window_seconds": 45,
    "input_arms": {
        "aggregate": "database container cgroup cumulative CPU, idle-subtracted on isolated class runs",
        "process": "PostgreSQL backend and replay-process cumulative CPU from the same isolated runs",
    },
    "telemetry_transport": "container cgroup cpu.stat (or cpuacct.usage), because lab Prometheus access timed out",
    "shared_measurements": "client and gateway-to-SQL request times in the two low-load mixed runs",
    "model_replay_delay_stage": False,
    "target_order": "three matched blocks with alternating layout order; randomized rate order shared by layouts",
    "physical_evidence": "same stationarity proxies, decision and class SLOs as pgs1",
}
for unused in ("characterization_rps", "characterization_runs", "target_rate_formula",
               "maximum_target_rps", "witness_samples", "portable_telemetry_only"):
    PLAN.pop(unused, None)

# The sampler uses only this container's own CPU accounting. The stock
# PostgreSQL entrypoint stays the foreground process and receives its normal args.
SAMPLER = r"""
(
    while :; do
        if [ -r /sys/fs/cgroup/cpu.stat ]; then
            usec=$(awk '$1 == "usage_usec" {print $2}' /sys/fs/cgroup/cpu.stat)
        elif [ -r /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            read -r ns < /sys/fs/cgroup/cpuacct/cpuacct.usage
            usec=$((ns / 1000))
        else
            exit 0
        fi
        if [ -n "$usec" ]; then
            printf '%s,%s\n' "$(date +%s.%N)" "$usec" > /pgs2-meter/next
            mv /pgs2-meter/next /pgs2-meter/cpu
        fi
        sleep 3
    done
) &
exec docker-entrypoint.sh "$@"
"""


def replace_template(name, template, *, preview=False):
    arguments = ["patch", "statefulset", name, "--type=json", "-p", json.dumps([
        {"op": "replace", "path": "/spec/template", "value": template},
    ])]
    if preview:
        arguments.append("--dry-run=server")
    trial.kube(*arguments)


def prepare_members(output):
    originals = {
        member: trial.get_resource("statefulset", member)["spec"]["template"]
        for member in pg.MEMBERS
    }
    pg.write(output / "original-member-templates.json", originals)
    changed = copy.deepcopy(originals)
    for name, template in changed.items():
        spec = template["spec"]
        containers = {container["name"]: container for container in spec["containers"]}
        database, observer = containers["database"], containers["observer"]
        if database.get("command") or not database["args"] or database["args"][0] != "postgres":
            raise ValueError(f"{name} differs from the original PostgreSQL entrypoint assumptions")
        database["command"] = ["/bin/sh", "-c", SAMPLER, "pgs2-database"]
        database["volumeMounts"].append({"name": "pgs2-meter", "mountPath": "/pgs2-meter"})
        observer["volumeMounts"].append(
            {"name": "pgs2-meter", "mountPath": "/pgs2-meter", "readOnly": True}
        )
        observer.setdefault("env", []).append(
            {"name": "PGPILOT_AGGREGATE_PATH", "value": "/pgs2-meter/cpu"}
        )
        source = next(volume for volume in spec["volumes"] if volume["name"] == "source")
        source["configMap"]["name"] = RESOURCES.source
        spec["volumes"].append({"name": "pgs2-meter", "emptyDir": {}})
        replace_template(name, template, preview=True)
    pg.write(output / "instrumented-member-templates.json", changed)
    for name, template in changed.items():
        replace_template(name, template)


def finish(output):
    trial.cleanup(output, RESOURCES)
    original = output / "original-member-templates.json"
    restoration = output / "member-template-restoration.json"
    if not original.is_file() or restoration.exists():
        return
    for member, template in json.loads(original.read_text()).items():
        resource = trial.get_resource("statefulset", member)
        if resource["spec"]["replicas"] or resource["metadata"]["labels"]["kubesizer.campaign"] != "pgp1":
            raise ValueError(f"Unable to restore {member} while active or under different ownership")
        if resource["spec"]["template"] != template:
            replace_template(member, template)
        if trial.get_resource("statefulset", member)["spec"]["template"] != template:
            raise RuntimeError(f"Pod template restoration did not reconcile for {member}")
    pg.write(restoration, {"restored": list(pg.MEMBERS), "completed_at": pg.now()})


def collect_idle(output):
    pg.api("/configure", {"mode": "async"})
    start = time.time()
    stop = time.monotonic() + PLAN["idle_seconds"]
    snapshots = []
    while time.monotonic() < stop:
        snapshots.append(pg.api("/snapshot", timeout=8))
        time.sleep(3)
    record = {"start_epoch": start, "end_epoch": time.time(), "snapshots": snapshots}
    pg.write(output / "idle-window.json", record)
    return record


def collect(output, run_id, layout, rate, seed, *, characterization=False, read_share=0.75):
    return pg.collect(
        output, run_id, "async", rate, seed,
        warmup=PLAN["warmup_seconds"],
        measurement=PLAN["characterization_measurement_seconds" if characterization else "measurement_seconds"],
        client_address=trial.ADDRESS, expected_standbys=len(trial.LAYOUTS[layout]),
        read_share=read_share,
    )


def read_run(output, run):
    location = output / "runs" / run["run_id"]
    trial.verify_receipt(location, "receipt.json")
    evidence = trial.audit_run(output, run, "one")
    if (not evidence["client_valid"] or not evidence["evidence_valid"] or not evidence["drained"]
        or any(value["failures"] for value in run["classes"].values())):
        raise ValueError(f"Unhealthy characterization run {run['run_id']}")
    with gzip.open(location / "events.json.gz", "rt") as stream:
        events = json.load(stream)
    if events["lost"]:
        raise ValueError(f"Incomplete gateway trace {run['run_id']}")
    start = run["started_epoch"] + run["warmup_seconds"]
    return {
        "start_epoch": start, "end_epoch": start + run["measurement_seconds"],
        "snapshots": json.loads((location / "snapshots.json").read_text()),
        "events": events["events"], "folder": location,
    }


def cpu_window(window, member, arm, *, category="query"):
    series = []
    for snapshot in window["snapshots"]:
        if member not in snapshot.get("members", {}):
            continue
        observed = snapshot["members"][member]
        if arm == "aggregate":
            sample = observed.get(COUNTER_FIELD)
            if not sample:
                continue
            epoch = sample["epoch"]
            value = sample["usage_seconds"]
        else:
            epoch, value = observed["epoch"], snapshot
        if window["start_epoch"] <= epoch <= window["end_epoch"]:
            if not series or epoch > series[-1][0]:
                series.append((epoch, value))
    if len(series) < 4:
        raise ValueError(f"Too few {arm} CPU samples for {member}")
    left, right = series[0][0], series[-1][0]
    elapsed = right - left
    if elapsed < PLAN["minimum_cpu_window_seconds"]:
        raise ValueError(f"The retained {arm} CPU interval is too short for {member}")
    if arm == "aggregate":
        if any(b[1] < a[1] for a, b in zip(series, series[1:])):
            raise ValueError(f"Container CPU counter reset for {member}")
        seconds = series[-1][1] - series[0][1]
    else:
        seconds = pg.cpu_deltas(series[0][1], series[-1][1])[member][category]
    counts = {
        c: sum(left <= row["epoch"] < right and row["request_class"] == c
               for row in window.get("events", []) if row["type"] == "response")
        for c in ("read", "write")
    }
    return dict(start_epoch=left, end_epoch=right, samples=len(series),
                seconds=seconds, elapsed_seconds=elapsed, completed=counts)


def shared_latency(windows):
    profile = {
        "local_samples_seconds": {"primary": [], "standby1": []},
        "residual_seconds": {"read": [], "write": []},
    }
    for window in windows:
        with gzip.open(window["folder"] / "requests.jsonl.gz", "rt") as stream:
            for line in stream:
                row = json.loads(line)
                if not (row["tagged"] and row["success"]):
                    continue
                service_time = row["server_response_seconds"]
                profile["local_samples_seconds"][row["member"]].append(service_time)
                profile["residual_seconds"][row["request_class"]].append(
                    max(0.0, row["latency_seconds"] - service_time)
                )
    if not all(profile["local_samples_seconds"].values()) or not all(profile["residual_seconds"].values()):
        raise ValueError("Both mixed-window request classes need healthy latency evidence")
    return profile


def measure_arm(arm, windows, idle, latencies):
    demands, evidence = {}, {}
    roles = (
        ("read", "read-only", "standby1", "query"),
        ("write", "write-only", "primary", "query"),
        ("replay", "write-only", "standby1", "replay"),
    )
    for demand, run, member, category in roles:
        sample = cpu_window(windows[run], member, arm, category=category)
        idle_cpu = 0
        if arm == "aggregate":
            idle_sample = cpu_window(idle, member, arm)
            idle_cpu = idle_sample["seconds"] / idle_sample["elapsed_seconds"]
            sample["idle_sample"] = idle_sample
        seconds = sample["seconds"] - idle_cpu * sample["elapsed_seconds"]
        completed = sample["completed"]["read" if demand == "read" else "write"]
        if not completed or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError(f"{arm} evidence did not identify positive incremental CPU for {demand}")
        demands[demand] = seconds / completed
        evidence[demand] = {**sample, "incremental_cpu_seconds": seconds}
    return {
        **latencies,
        "cpu_demands_seconds": {"primary": demands["write"], "standby1": demands["read"]},
        "replay_cpu_demands_seconds": {"standby1": demands["replay"]},
        "cpu_measurement_windows": evidence,
    }


def target_order():
    generator = random.Random(PLAN["seed"])
    layouts = list(trial.LAYOUTS)
    generator.shuffle(layouts)
    order = []
    for pair in range(PLAN["target_pairs"]):
        rates = list(PLAN["target_rates_rps"])
        generator.shuffle(rates)
        order.extend([
            {"pair": pair, "layout": layout, "rate_rps": rate,
             "seed": PLAN["seed"] + 100 + pair * len(rates) + rates.index(rate)}
            for layout in layouts for rate in rates
        ])
        layouts.reverse()
    return order


def forecast(output, arm, rate, profile):
    folder = output / "candidate-models" / arm / f"r{rate:g}"
    folder.mkdir(parents=True)
    transferred = copy.deepcopy(profile)
    for field in ("cpu_demands_seconds", "replay_cpu_demands_seconds", "local_samples_seconds"):
        transferred[field]["standby2"] = transferred[field]["standby1"]
    candidates = []
    for layout, weights in trial.LAYOUTS.items():
        declaration = pg.model(
            profile if layout == "one" else transferred, "async", rate, True,
            read_weights=weights, include_replay_proxy=False,
        )
        workload = f"workload-{layout}.yaml"
        (folder / workload).write_text(yaml.safe_dump(declaration, sort_keys=False))
        candidates.append({
            "name": layout, "mechanism": f"{len(weights)} read-serving standbys with local commits",
            "workload": workload, "base_cost": PLAN["costs_in_database_members"][layout],
            "elastic_replica_costs": {},
            "evidence_sources": [f"../../../profile-{arm}.json", "../../../protocol.json"],
        })
    candidate_file = folder / "candidates.json"
    pg.write(candidate_file, {"version": 1, "candidates": candidates})
    invocation = [sys.executable, str(ROOT / "kubesizer_cli.py"), "--stateful-candidates",
                  str(candidate_file), "--replica-evaluation", "simulation",
                  "--workers", "2", "--json", "--quiet"]
    try:
        result = subprocess.run(invocation, capture_output=True, text=True, cwd=ROOT, timeout=300)
        return {"exit_code": result.returncode, "response": json.loads(result.stdout)}
    except subprocess.TimeoutExpired:
        return {"response": {"ok": False, "error": {"type": "RuntimeLimit", "message": "300 s CLI timeout"}}}


def freeze(output, records, idle):
    windows = {record["run_id"]: read_run(output, record) for record in records}
    latencies = shared_latency([windows["mixed-0"], windows["mixed-1"]])
    responses, jobs = {}, []
    for arm in PLAN["input_arms"]:
        try:
            profile = measure_arm(arm, windows, idle, latencies)
        except ValueError as error:
            responses[arm] = {
                "status": "ABSTAINED_INPUT",
                "reason": str(error),
            }
            continue
        pg.write(output / f"profile-{arm}.json", profile)
        responses[arm] = {"status": "HAS_PROFILE", "rates": {}}
        jobs.extend((arm, rate, profile) for rate in PLAN["target_rates_rps"])
    with ThreadPoolExecutor(max_workers=2) as workers:
        for (arm, rate, _), response in zip(
            jobs, workers.map(lambda job: forecast(output, *job), jobs), strict=True,
        ):
            responses[arm]["rates"][f"{rate:g}"] = response
    order = target_order()
    pg.write(output / "target-order.json", order)
    pg.write(output / "predictions-frozen.json", {
        "frozen_at": pg.now(), "target_outcomes_used": False, "arms": responses,
    })
    files = [
        *(output / name for name in (
            "protocol.json", "source-receipt.json", "idle-window.json", "characterization-runs.json",
            "target-order.json", "predictions-frozen.json",
        )),
        *(output / "runs" / run["run_id"] / "receipt.json" for run in records),
        *sorted(output.glob("profile-*.json")),
        *sorted(path for path in (output / "candidate-models").rglob("*") if path.is_file()),
    ]
    pg.write(output / "prediction-receipt.json", {
        str(path.relative_to(output)): pg.digest(path) for path in files
    })
    return order


def score(output, runs):
    trial.verify_receipt(output, "prediction-receipt.json")
    forecasts = json.loads((output / "predictions-frozen.json").read_text())
    freeze_epoch = datetime.fromisoformat(forecasts["frozen_at"]).timestamp()
    details = []
    for rate in PLAN["target_rates_rps"]:
        members = [run for run in runs if run["rate_rps"] == rate]
        layouts = trial.score_layouts(output, members)
        eligible = [layout for layout, result in layouts.items() if result["observed"] == "FEASIBLE_IN_PILOT"]
        minimum = min(eligible, key=PLAN["costs_in_database_members"].__getitem__, default=None)
        counts = {}
        for arm, result in forecasts["arms"].items():
            cli = result.get("rates", {}).get(f"{rate:g}", {}).get("response", {})
            selected = cli.get("result", {}).get("stateful_planning", {}).get("selected")
            counts[arm] = {
                "selected": selected,
                "observed_selected": layouts[selected]["observed"] if selected else "ABSTAINED",
                "cheaper_observed_feasible": bool(
                    selected and minimum and PLAN["costs_in_database_members"][minimum]
                    < PLAN["costs_in_database_members"][selected]
                ),
            }
        pairs = []
        for pair in range(PLAN["target_pairs"]):
            paired = [r for r in members if r["pair"] == pair]
            if len(paired) != 2:
                continue
            if paired[0]["seed"] != paired[1]["seed"]:
                raise ValueError("Matched layout pair used different arrival seeds")
            audits = [audit_run(output / "runs" / r["run_id"], r, freeze_epoch) for r in paired]
            observed = [item[1] for item in audits]
            left, right = observed
            same_schedule = left.keys() == right.keys() and all(
                row["request_class"] == right[index]["request_class"]
                and abs(row.get("scheduled_seconds", 0) - right[index].get("scheduled_seconds", 0)) <= 1e-6
                for index, row in left.items()
            )
            pairs.append({"pair": pair, "schedule_verified": same_schedule, "runs": [item[0] for item in audits]})
        details.append({
            "target_rps": rate, "physical": layouts, "model_decisions": counts,
            "lowest_measured_feasible_layout": minimum, "paired_audits": pairs,
        })
    pg.write(output / "scores.json", details)


def run(output):
    output.mkdir(parents=True, exist_ok=False)
    pg.write(output / "protocol.json", PLAN)
    sources = [Path(__file__).resolve(), ROOT / "scripts/audit_postgres_sizing.py",
               ROOT / "scripts/run_postgres_sizing.py", ROOT / "scripts/run_postgres_pilot.py",
               ROOT / "scripts/postgres_pilot_agent.py", ROOT / "calibration/warmup_parameters.py",
               ROOT / "performance_models/system.py", ROOT / "kubesizer_cli.py",
               ROOT / "native_backend/cpp/build-release/kubesizer-simulator"]
    pg.write(output / "source-receipt.json", {str(path.relative_to(ROOT)): pg.digest(path) for path in sources})
    status = {"status": "PROVISIONING", "started_at": pg.now()}
    pg.write(output / "status.json", status)
    try:
        trial.wait_pods_absent("kubesizer.sizing")
        trial.provision(output, RESOURCES, prepare_members=prepare_members)
        status.update(status="CHARACTERIZING", stage_started_at=pg.now())
        pg.write(output / "status.json", status)
        with trial.active_layout(output, "one", RESOURCES):
            idle = collect_idle(output)
            observations = []
            for index, (name, rate, read_share) in enumerate(CHARACTERIZATION):
                status.update(characterization_run=name, stage_started_at=pg.now())
                pg.write(output / "status.json", status)
                observations.append(collect(
                    output, name, "one", rate, PLAN["seed"] + index,
                    characterization=True, read_share=read_share,
                ))
            pg.write(output / "characterization-runs.json", observations)
            status.update(status="FREEZING", stage_started_at=pg.now())
            pg.write(output / "status.json", status)
            order = freeze(output, observations, idle)
        targets = []
        width = len(PLAN["target_rates_rps"])
        for index in range(0, len(order), width):
            block = order[index:index + width]
            layout = block[0]["layout"]
            pg.write(output / "status.json", {
                **status, "status": "SWITCHING_LAYOUT", "current_layout": layout,
                "completed": len(targets), "total": len(order), "stage_started_at": pg.now(),
            })
            with trial.active_layout(output, layout, RESOURCES):
                collect_targets(output, block, targets, status, len(order))
        status.update(status="COMPLETED", completed=len(targets), total=len(order), completed_at=pg.now())
    except BaseException as error:
        status.update(status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=pg.now())
        raise
    finally:
        pg.write(output / "status.json", status)
        finish(output)


def collect_targets(output, block, targets, status, total):
    for item in block:
        trial.verify_receipt(output, "prediction-receipt.json")
        rate, layout, pair = item["rate_rps"], item["layout"], item["pair"]
        run_id = f"r{rate:g}-{layout}-pair{pair}"
        status.update(status="TARGETS", stage_started_at=pg.now(), current=run_id,
                      completed=len(targets), total=total)
        pg.write(output / "status.json", status)
        record = collect(output, run_id, layout, rate, item["seed"])
        targets.append({**record, **item})
        pg.write(output / "target-results.json", targets)
        score(output, targets)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--finish", action="store_true")
    parser.add_argument("--score-only", action="store_true")
    args = parser.parse_args()
    if args.finish:
        finish(args.output.resolve())
    elif args.score_only:
        score(args.output.resolve(), json.loads((args.output / "target-results.json").read_text()))
    else:
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        run(args.output.resolve())

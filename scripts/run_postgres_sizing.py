#!/usr/bin/env python3
"""Prospective one-versus-two read-standby trial in the isolated pgp1 namespace."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
from dataclasses import dataclass
import gzip
import json
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

NAME = "pgs1"
GATEWAY = "sizing-gateway-pgs1"
SOURCE = "sizing-source-pgs1"
ADDRESS = ("127.0.0.1", 29290)


@dataclass(frozen=True)
class PilotResources:
    name: str
    gateway: str
    source: str
    gateway_environment: tuple[tuple[str, str], ...] = ()


DEFAULT_RESOURCES = PilotResources(NAME, GATEWAY, SOURCE)
LAYOUTS = {
    "one": {"standby1": 1.0},
    "two": {"standby1": 0.5, "standby2": 0.5},
}
PLAN = {
    "trial": NAME,
    "namespace": pg.NAMESPACE,
    "costs_in_database_members": {"one": 2, "two": 3},
    "read_routing": LAYOUTS,
    "writes": "same primary, synchronous_commit=local in both arms",
    "consistency": "eventual reads, no read-your-writes guarantee",
    "measurement_boundary": "VM client to isolated gateway through loopback kubectl port-forward",
    "mean_slo_seconds": pg.PLAN["mean_slos_seconds"],
    "read_share": 0.75,
    "characterization_layout": "one",
    "characterization_rps": 12,
    "characterization_runs": 2,
    "characterization_measurement_seconds": 120,
    "target_rate_formula": "min(42, 0.90/(0.75*standby_read_cpu + 0.25*standby_replay_cpu))",
    "maximum_target_rps": 42,
    "warmup_seconds": 30,
    "measurement_seconds": 90,
    "target_pairs": 3,
    "pair_order": "random first layout, alternating order across pairs",
    "seed": 26111933,
    "witness_samples": 80,
    "maximum_schedule_p95_seconds": 0.05,
    "maximum_failure_rate": 0.01,
    "maximum_sampled_replay_backlog_bytes": 1_048_576,
    "interval": "two-sided Student t over independent physical run means, 95% per class",
    "transfer_assumption": "second standby has the observed first standby's per-request CPU, local delay and replay proxy",
    "portable_telemetry_only": False,
}


def kube(*args, timeout=180):
    return pg.kube("-n", pg.NAMESPACE, *args, timeout=timeout)


def get_resource(kind, name):
    value = kube("get", kind, name, "--ignore-not-found", "-o", "json")
    return json.loads(value) if value.strip() else None


def deploy_documents(output, resources=DEFAULT_RESOURCES):
    original = get_resource("deployment", "gateway")
    template = copy.deepcopy(original["spec"]["template"])
    labels = {"app": resources.gateway, "kubesizer.sizing": resources.name}
    template["metadata"] = {"labels": labels}
    for volume in template["spec"]["volumes"]:
        if volume.get("name") == "source":
            volume["configMap"]["name"] = resources.source
    template["spec"]["containers"][0]["env"].extend(
        {"name": name, "value": value}
        for name, value in (("PGPILOT_READ_LAYOUT", "one"), *resources.gateway_environment)
    )
    original_data = get_resource("configmap", "pilot-source")["data"]
    data = {
        **original_data,
        "postgres_pilot_agent.py": (ROOT / "scripts/postgres_pilot_agent.py").read_text(),
    }
    manifests = {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": resources.source, "namespace": pg.NAMESPACE, "labels": labels},
                "data": data,
            },
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": resources.gateway, "namespace": pg.NAMESPACE, "labels": labels},
                "spec": {
                    "replicas": 0,
                    "selector": {"matchLabels": labels},
                    "strategy": {"type": "Recreate"},
                    "template": template,
                },
            },
        ],
    }
    path = output / "gateway-declaration.yaml"
    path.write_text(yaml.safe_dump_all(manifests["items"], sort_keys=False))
    kube("create", "--dry-run=server", "-f", str(path))
    kube("create", "-f", str(path))


def wait_pods_absent(selector):
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        pods = json.loads(kube("get", "pods", "-l", selector, "-o", "json"))["items"]
        if not pods:
            return
        time.sleep(3)
    raise RuntimeError(f"Pods for {selector} did not terminate")


def cleanup(output, resources=DEFAULT_RESOURCES):
    marker = output / "cluster-ownership.json"
    if not marker.is_file() or (output / "cleanup.json").is_file():
        return
    # This receipt is written only after verifying that all three pilot members
    # and the old gateway are idle and that this trial's gateway name is unused.
    workloads = [("deployment", resources.gateway, "kubesizer.sizing", resources.name)]
    workloads.extend(("statefulset", n, "kubesizer.campaign", "pgp1") for n in pg.MEMBERS)
    for kind, name, label, owner in workloads:
        resource = get_resource(kind, name)
        if resource is None:
            continue
        if resource["metadata"].get("labels", {}).get(label) != owner:
            raise RuntimeError(f"Unexpected owner for {kind}/{name}")
        kube("scale", kind, name, "--replicas=0")
    for _, name, _, _ in workloads:
        wait_pods_absent(f"app={name}")
    pg.write(output / "cleanup.json", {"completed_at": pg.now(), "pilot_pods_running": 0})


def provision(output, resources=DEFAULT_RESOURCES, *, prepare_members=None):
    comparison = subprocess.run(
        ["systemctl", "is-active", pg.common.COMPARISON],
        capture_output=True, text=True,
    ).stdout.strip()
    if comparison != "inactive":
        raise RuntimeError(f"Comparison service is {comparison!r}, expected inactive")
    for kind, names in (("statefulset", pg.MEMBERS), ("deployment", ("gateway",))):
        for name in names:
            resource = get_resource(kind, name)
            if (resource is None
                or resource["metadata"].get("labels", {}).get("kubesizer.campaign") != "pgp1"
                or resource["spec"]["replicas"] != 0):
                raise RuntimeError(f"Unexpected or active pgp1 resource {kind}/{name}")
    if get_resource("deployment", resources.gateway) or get_resource("configmap", resources.source):
        raise RuntimeError("This trial requires an unused isolated gateway and source name")
    pg.write(output / "cluster-ownership.json", {"namespace": pg.NAMESPACE, "claimed_at": pg.now()})
    deploy_documents(output, resources)
    if prepare_members is not None:
        prepare_members(output)
    kube("scale", "statefulset", "primary", "standby1", "--replicas=1")
    for member in ("primary", "standby1"):
        kube("rollout", "status", f"statefulset/{member}", "--timeout=300s", timeout=310)


@contextmanager
def active_layout(output, layout, resources=DEFAULT_RESOURCES):
    gateway = resources.gateway
    kube("scale", "deploy", gateway, "--replicas=0")
    wait_pods_absent(f"app={gateway}")
    standby_count = len(LAYOUTS[layout])
    kube("scale", "statefulset", "standby2", f"--replicas={standby_count-1}")
    if standby_count == 2:
        kube("rollout", "status", "statefulset/standby2", "--timeout=240s", timeout=250)
    else:
        wait_pods_absent("app=standby2")
    kube("set", "env", f"deployment/{gateway}", f"PGPILOT_READ_LAYOUT={layout}")
    kube("scale", "deploy", gateway, "--replicas=1")
    kube("rollout", "status", f"deployment/{gateway}", "--timeout=240s", timeout=250)
    with (output / "port-forward.log").open("a") as log:
        forward = subprocess.Popen(
            ["kubectl", "-n", pg.NAMESPACE, "port-forward", f"deployment/{gateway}",
             "29290:8080", "29291:9091", "--address=127.0.0.1"],
            stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 90
            while True:
                try:
                    snapshot = pg.api("/snapshot", timeout=5)
                except OSError:
                    snapshot = {}
                replication = snapshot.get("replication", [])
                if (snapshot.get("read_layout") == layout
                    and {row[0] for row in replication} == set(LAYOUTS[layout])
                    and all(row[1] == "streaming" and row[3] == 0 for row in replication)):
                    break
                if time.monotonic() >= deadline or forward.poll() is not None:
                    raise RuntimeError(f"Expected streaming layout is not ready: {layout}")
                time.sleep(2)
            yield
        finally:
            forward.terminate()
            try:
                forward.wait(timeout=15)
            except subprocess.TimeoutExpired:
                forward.kill()
                forward.wait()


def record(output, name, layout, rate, seed, measurement):
    return pg.collect(
        output, name, "async", rate, seed, warmup=PLAN["warmup_seconds"],
        measurement=measurement, client_address=ADDRESS,
        expected_standbys=len(LAYOUTS[layout]),
    )


def freeze(output, profile):
    cpu = profile["cpu_demands_seconds"]["standby1"]
    replay = profile["replay_cpu_demands_seconds"]["standby1"]
    rate = min(PLAN["maximum_target_rps"], 0.90 / (0.75 * cpu + 0.25 * replay))
    profile_two = copy.deepcopy(profile)
    for field in ("cpu_demands_seconds", "replay_cpu_demands_seconds", "local_samples_seconds"):
        profile_two[field]["standby2"] = profile_two[field]["standby1"]
    for witness in profile_two["replay_witness"]["samples"]:
        witness["standbys"]["standby2"] = witness["standbys"]["standby1"]
    pg.write(output / "transferred-characterization.json", profile_two)

    candidates, schedules = [], []
    pair = list(LAYOUTS)
    random.Random(PLAN["seed"]).shuffle(pair)
    for index in range(PLAN["target_pairs"]):
        schedules.extend({"layout": n, "pair": index, "seed": PLAN["seed"] + 100 + index} for n in pair)
        pair.reverse()
    for name, weights in LAYOUTS.items():
        workload = f"workload-{name}.yaml"
        declaration = pg.model(
            profile if name == "one" else profile_two, "async", rate, True, read_weights=weights,
        )
        (output / workload).write_text(yaml.safe_dump(declaration, sort_keys=False))
        candidates.append({
            "name": name,
            "mechanism": f"local durable commits with {len(weights)} read-serving streaming standbys",
            "workload": workload,
            "base_cost": PLAN["costs_in_database_members"][name],
            "elastic_replica_costs": {},
            "evidence_sources": ["protocol.json", "characterization.json"],
        })
    pg.write(output / "candidates.json", {"version": 1, "candidates": candidates})
    pg.write(output / "target-order.json", schedules)
    forecasts = {}
    for mode in ("analytical", "simulation"):
        result = subprocess.run(
            [sys.executable, str(ROOT / "kubesizer_cli.py"), "--stateful-candidates",
             str(output / "candidates.json"), "--replica-evaluation", mode,
             "--workers", "3", "--json", "--quiet"],
            capture_output=True, text=True, cwd=ROOT, timeout=360,
        )
        forecasts[mode] = {"exit_code": result.returncode, "response": json.loads(result.stdout)}
    receipt_files = [
        "protocol.json", "source-receipt.json", "characterization.json",
        "transferred-characterization.json", "candidates.json", "target-order.json",
        "workload-one.yaml", "workload-two.yaml", "predictions-frozen.json",
    ]
    pg.write(output / "predictions-frozen.json", {
        "frozen_at": pg.now(), "target_rate_rps": rate,
        "target_outcomes_used": False, "forecasts": forecasts,
    })
    pg.write(output / "prediction-receipt.json", {name: pg.digest(output / name) for name in receipt_files})
    return rate, schedules


def verify_receipt(folder, receipt):
    for name, expected in json.loads((folder / receipt).read_text()).items():
        if pg.digest(folder / name) != expected:
            raise ValueError(f"Evidence changed: {folder / name}")


def audit_run(output, run, layout):
    directory = output / "runs" / run["run_id"]
    verify_receipt(directory, "receipt.json")
    with gzip.open(directory / "requests.jsonl.gz", "rt") as stream:
        rows = [row for line in stream if (row := json.loads(line))["tagged"]]
    snapshots = json.loads((directory / "snapshots.json").read_text())
    usable = [row for row in snapshots if "replication" in row]
    scheduling = sorted(row["scheduling_delay_seconds"] for row in rows)
    p95 = scheduling[int(0.95 * (len(scheduling) - 1))] if scheduling else None
    backlog = [member[3] for row in usable for member in row["replication"] if member[3] is not None]
    routing = {n: 0 for n in LAYOUTS[layout]}
    for row in rows:
        if row["request_class"] == "read" and row["success"]:
            routing[row["member"]] += 1
    client_valid = bool(rows) and p95 <= PLAN["maximum_schedule_p95_seconds"] and not any(
        row.get("error") == "client_capacity_rejected" for row in rows
    )
    expected = set(LAYOUTS[layout])
    evidence_valid = bool(backlog) and len(usable) == len(snapshots) and all(
        row["read_layout"] == layout and row["mode"] == "async"
        and {member[0] for member in row["replication"]} == expected
        and all(member[1] == "streaming" and member[3] is not None for member in row["replication"])
        for row in usable
    )
    return dict(
        run_id=run["run_id"], client_valid=client_valid, evidence_valid=evidence_valid,
        scheduling_p95_seconds=p95, drained=not run["timed_out"],
        maximum_sampled_replay_backlog_bytes=max(backlog, default=None), read_completions=routing,
    )


def score_layouts(output, runs):
    results = {}
    for layout in LAYOUTS:
        members = [item for item in runs if item["layout"] == layout]
        audits = [audit_run(output, item, layout) for item in members]
        outcomes = {}
        for c, slo in PLAN["mean_slo_seconds"].items():
            means = [item["classes"][c]["mean_seconds"] for item in members]
            failures = [item["classes"][c]["failures"] / item["classes"][c]["count"]
                        for item in members if item["classes"][c]["count"]]
            point = lower = upper = None
            if len(means) == 3 and all(value is not None for value in means):
                point = statistics.fmean(means)
                half_width = 4.30265273 * statistics.stdev(means) / (len(means) ** 0.5)
                lower, upper = max(0.0, point - half_width), point + half_width
            if len(members) != PLAN["target_pairs"] or not all(
                a["client_valid"] and a["evidence_valid"] for a in audits
            ):
                observed = "INCONCLUSIVE_EVIDENCE"
            elif max(failures, default=0) > PLAN["maximum_failure_rate"] or (
                lower is not None and lower > slo
            ):
                observed = "INFEASIBLE"
            elif upper is not None and upper <= slo and all(
                a["drained"]
                and a["maximum_sampled_replay_backlog_bytes"] <= PLAN["maximum_sampled_replay_backlog_bytes"]
                for a in audits
            ):
                observed = "FEASIBLE_IN_PILOT"
            else:
                observed = "INCONCLUSIVE"
            outcomes[c] = dict(
                observed=observed, mean_seconds=point, lower_seconds=lower, upper_seconds=upper,
                run_means_seconds=means, run_failure_rates=failures,
            )
        statuses = {item["observed"] for item in outcomes.values()}
        summary = (
            "INFEASIBLE" if "INFEASIBLE" in statuses else
            "FEASIBLE_IN_PILOT" if statuses == {"FEASIBLE_IN_PILOT"} else "INCONCLUSIVE"
        )
        results[layout] = dict(observed=summary, classes=outcomes, audits=audits)
    return results


def score(output, runs):
    verify_receipt(output, "prediction-receipt.json")
    pg.write(output / "scores.json", score_layouts(output, runs))


def run(output):
    output.mkdir(parents=True, exist_ok=False)
    pg.write(output / "protocol.json", PLAN)
    sources = [
        Path(__file__),
        *(ROOT / "scripts" / name for name in (
            "run_postgres_pilot.py", "postgres_pilot_agent.py", "run_stateful_pilot.py",
        )),
        ROOT / "kubesizer_cli.py", ROOT / "stateful_planning.py",
        ROOT / "native_backend/cpp/build-release/kubesizer-simulator",
    ]
    pg.write(output / "source-receipt.json", {str(path.relative_to(ROOT)): pg.digest(path) for path in sources})
    status = {"status": "PROVISIONING", "started_at": pg.now()}
    pg.write(output / "status.json", status)
    try:
        provision(output)
        status.update(status="CHARACTERIZING", stage_started_at=pg.now())
        pg.write(output / "status.json", status)
        with active_layout(output, "one"):
            runs = [
                record(output, f"characterization-{i}", "one", PLAN["characterization_rps"],
                       PLAN["seed"] + i, PLAN["characterization_measurement_seconds"])
                for i in range(PLAN["characterization_runs"])
            ]
            witness = pg.api("/witness", {"count": PLAN["witness_samples"]}, timeout=90)
            pg.write(output / "replay-witness.json", witness)
        for observation in runs:
            verify_receipt(output / "runs" / observation["run_id"], "receipt.json")
        profile = pg.characterize(output, runs, witness, members=pg.MEMBERS[:2])
        status.update(status="FREEZING", stage_started_at=pg.now())
        pg.write(output / "status.json", status)
        rate, schedule = freeze(output, profile)
        targets = []
        for item in schedule:
            verify_receipt(output, "prediction-receipt.json")
            layout, pair = item["layout"], item["pair"]
            status.update(
                status="TARGETS", stage_started_at=pg.now(), completed=len(targets),
                total=len(schedule), target_rps=rate, current_layout=layout, pair=pair,
            )
            pg.write(output / "status.json", status)
            with active_layout(output, layout):
                result = record(output, f"{layout}-pair{pair}", layout, rate, item["seed"],
                                PLAN["measurement_seconds"])
            targets.append({**result, **item})
            pg.write(output / "target-results.json", targets)
            score(output, targets)
        status.update(status="COMPLETED", completed=len(targets), completed_at=pg.now())
    except BaseException as error:
        status.update(status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=pg.now())
        raise
    finally:
        pg.write(output / "status.json", status)
        cleanup(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--finish", action="store_true")
    parser.add_argument("--score-only", action="store_true")
    args = parser.parse_args()
    if args.finish:
        cleanup(args.output.resolve())
    elif args.score_only:
        score(args.output.resolve(), json.loads((args.output / "target-results.json").read_text()))
    else:
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        run(args.output.resolve())

"""Freeze and score two MinIO sizing treatments on matched real S3 traffic."""

from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import datetime
import gzip
import json
import math
from pathlib import Path
import random
import shutil
import signal
import statistics
import subprocess
import sys
import time

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import characterize_minio_fragments as fragments
from scripts import characterize_minio_mixed as mixed
from scripts import characterize_minio_paths as paths
from scripts import characterize_minio_put as serial
from scripts import characterize_minio_sweep as sweep
from scripts import minio_sizing_lab as lab
from scripts import run_minio_pilot as pilot

CLASSES = ("GET", "PUT")
ARMS = ("aggregate", "fragment")
PLAN = {
    "campaign": lab.CAMPAIGN,
    "candidate_active_members_and_drives": [4, 8],
    "erasure_set_drives": 4,
    "storage_class_parity": 2,
    "pods_per_worker_and_layout": {"4": 1, "8": 2},
    "entry_selection": "key suffix modulo active members, independent of MinIO placement",
    "object_bytes": paths.SIZE,
    "get_keys": sweep.GET_KEYS,
    "put_keys": sweep.PUT_KEYS,
    "warmup_seconds": 12,
    "measurement_seconds": 45,
    "idle_seconds": 45,
    "characterization_class_rate": 3.0,
    "characterization_pairs": 2,
    "target_equal_per_class_rates": [4.0, 8.0, 12.0],
    "target_pairs": 3,
    "seed": 26111991,
    "mean_slo_seconds": {"GET": 0.250, "PUT": 0.400},
    "failure_rate_limit": 0.01,
    "schedule_p95_limit_ms": 20,
    "max_cohort_drain_seconds": 30,
    "minimum_fragment_group_coverage": 0.80,
    "input_arms": {
        "aggregate": "idle-corrected CPU and per-drive block busy-time, with measured client latency",
        "fragment": "same inputs with a receiver write-and-flush phase distribution in place of aggregate PUT I/O",
    },
    "model": "CPU sharing per member then an effective per-erasure-set storage queue, with measured low-load latency residual",
    "transfer": "retain one-operation demand and latency residual, balance work equally across equivalent sets",
    "capacity_scope": "fixed CPU/drive provisioning and a balanced 1 MiB GET/PUT workload",
}
COUNTERS = r"""
for file in /sys/fs/cgroup/cpuacct/cpuacct.usage /sys/fs/cgroup/cpu,cpuacct/cpuacct.usage /sys/fs/cgroup/cpu/cpuacct.usage; do
    if [ -r "$file" ]; then
        printf 'cpuacct_usage_ns %s\n' "$(cat "$file")"
        break
    fi
done
""" + serial.READ_MEMBER_COUNTERS + r"""
printf '\nNETDEV\n'
cat /proc/net/dev
printf '\nFREE\n'
df -Pk /data
"""


def response_window(request):
    return float(request["warmup"]) + float(request["measurement"])


def worker():
    from minio import Minio

    params = json.loads(sys.stdin.readline())
    content = pilot.payload(paths.SIZE)

    class Entries:
        def __init__(self, pool):
            self.clients = [
                Minio(addr, access_key=params["access_key"], secret_key=params["secret_key"],
                      secure=False, http_client=pool)
                for addr in params["endpoints"]
            ]

        def for_key(self, key):
            return self.clients[int(key.rsplit("-", 1)[-1]) % len(self.clients)]

        def get_object(self, bucket, key, *args, **kwargs):
            return self.for_key(key).get_object(bucket, key, *args, **kwargs)

        def put_object(self, bucket, key, *args, **kwargs):
            return self.for_key(key).put_object(bucket, key, *args, **kwargs)

    max_in_flight = params.get("max_in_flight", sweep.MAX_IN_FLIGHT)
    client = mixed.measured_client(lambda http_client: Entries(http_client), maxsize=max_in_flight)
    bucket = params["bucket"]
    print(json.dumps({"ready": True}), flush=True)
    for line in sys.stdin:
        request = json.loads(line)
        try:
            if request.get("prepare"):
                entries = client[0]
                head = entries.clients[0]
                reuse = request.get("reuse_preload", False)
                if head.bucket_exists(bucket) != reuse:
                    raise ValueError("The experiment bucket does not match the declared preload state")
                baseline = bytes([content[0] ^ 0xFF]) + content[1:]
                keys = [
                    (f"size-{paths.SIZE}/{method}-{n:03d}", baseline if method == "put" else content)
                    for method, amount in (("put", sweep.PUT_KEYS), ("get", sweep.GET_KEYS))
                    for n in range(amount)
                ]
                updated = set(request.get("updated_put_keys", []))
                keys = [(key, content if key in updated else data) for key, data in keys]
                with ThreadPoolExecutor(max_workers=4) as tasks:
                    if not reuse:
                        head.make_bucket(bucket)
                        list(tasks.map(lambda item: serial.put(entries, bucket, *item), keys))
                    if {item.object_name for item in head.list_objects(bucket, recursive=True)} != {key for key, _ in keys}:
                        raise ValueError("Seeded dataset contains unexpected or missing keys")
                    list(tasks.map(lambda item: serial.verify(entries, bucket, *item), keys))
                result = {"prepared": len(keys)}
            else:
                result = sweep.measure(client, content, bucket, max_in_flight=max_in_flight, **request)
                for row in result["rows"]:
                    row["entry_index"] = int(row["key"].rsplit("-", 1)[-1]) % len(params["endpoints"])
            print(json.dumps(result), flush=True)
        except Exception as error:
            print(json.dumps({"error": type(error).__name__, "code": getattr(error, "code", None)}), flush=True)


@contextmanager
def client_session(folder, size, deps, *, preloaded=False, updated_keys=(), max_in_flight=sweep.MAX_IN_FLIGHT):
    command = (
        "kubectl", "-n", pilot.NAMESPACE, "exec", "-i",
        f"{lab.members(size)[3]}-0", "-c", "observer", "--",
    )
    paths.stage_worker(
        deps, exec_command=command,
        additional_scripts=(
            Path(__file__).name, "minio_sizing_lab.py", "characterize_minio_sweep.py",
            "characterize_minio_fragments.py",
        ),
    )
    directory = folder / f"layout-{size}-client"
    directory.mkdir()
    with paths.pod_session(
        directory, "", lab.BUCKET, script=Path(__file__).name,
        timeout=240, exec_command=command,
        extra_params={"endpoints": [f"{name}:9000" for name in lab.members(size)], "max_in_flight": max_in_flight},
    ) as exchange:
        preparation = {"prepare": True, "reuse_preload": preloaded, "updated_put_keys": list(updated_keys)}
        if exchange(preparation) != {"prepared": sweep.PUT_KEYS + sweep.GET_KEYS}:
            raise RuntimeError(f"Dataset was not fully seeded for layout {size}")
        yield exchange


def counter(name):
    before = time.time()
    raw = lab.kube("exec", f"{name}-0", "-c", "minio", "--", "sh", "-c", COUNTERS, timeout=20)
    epoch = (before + time.time()) / 2
    metrics, suffix = raw.split("\nNETDEV\n")
    network, free = suffix.split("\nFREE\n")
    cpu, volume = serial.member_counters(metrics)
    cpu_source = "cpu.stat"
    if "usage_usec" not in cpu and "cpuacct_usage_ns" in cpu:
        cpu["usage_usec"] = cpu["cpuacct_usage_ns"] / 1000
        cpu_source = "cpuacct.usage"
    if "usage_usec" not in cpu:
        raise ValueError(f"Container CPU usage counter is unavailable for {name} (found {sorted(cpu)})")
    if volume is None:
        raise ValueError(f"The mounted Longhorn volume has no block counters for {name}")
    rows = [line.split(":", 1) for line in network.splitlines() if ":" in line]
    interfaces = [tuple(map(int, values.split())) for iface, values in rows if iface.strip() != "lo"]
    if not interfaces:
        raise ValueError(f"No pod network counters for {name}")
    return {
        "epoch": epoch, "cpu": cpu, "cpu_source": cpu_source, "volume": volume,
        "network_rx_bytes": sum(row[0] for row in interfaces),
        "network_tx_bytes": sum(row[8] for row in interfaces),
        "volume_free_bytes": int(free.strip().splitlines()[-1].split()[3]) * 1024,
    }


def counters(size):
    names = lab.members(size)
    with ThreadPoolExecutor(max_workers=4) as pool:
        return dict(zip(names, pool.map(counter, names), strict=True))


def sample_delta(left, right):
    dt = right["epoch"] - left["epoch"]
    if dt <= 0 or left["volume"]["device"] != right["volume"]["device"]:
        raise ValueError("Invalid counter interval or volume identity")
    value = {
        "cpu": (right["cpu"]["usage_usec"] - left["cpu"]["usage_usec"]) / 1e6,
        "busy": (right["volume"]["busy_ms"] - left["volume"]["busy_ms"]) / 1000,
        "network_bytes": sum(right[f"network_{d}_bytes"] - left[f"network_{d}_bytes"] for d in ("rx", "tx")),
    }
    if min(value.values()) < 0:
        raise ValueError("A hardware counter decreased across its interval")
    return {**value, "elapsed_seconds": dt}


def idle_sample(output):
    first = counters(4)
    time.sleep(PLAN["idle_seconds"])
    last = counters(4)
    pilot.write(output / "idle-counters.json", {"before": first, "after": last})
    return {member: sample_delta(first[member], last[member]) for member in first}


def request(rate, seed, mode="mixed", *, plan=PLAN):
    return {
        "rate": rate, "seed": seed, "mode": mode,
        "warmup": plan["warmup_seconds"], "measurement": plan["measurement_seconds"],
    }


def run_window(folder, size, job, client, *, plan=PLAN):
    directory = folder / "windows" / job["id"]
    directory.mkdir(parents=True, exist_ok=False)
    specification = request(job["rate"], job["seed"], job["mode"], plan=plan)
    writes = sum(1 for _ in sweep.arrivals(job["rate"], "PUT", job["seed"], response_window(specification)))
    required = (writes if job["mode"] != "read" else 0) * (paths.SIZE // 2 + 8192) + 64 * 2**20
    deadline = time.monotonic() + plan.get("storage_headroom_wait_seconds", 180)
    while True:
        before = counters(size)
        if min(row["volume_free_bytes"] for row in before.values()) >= required:
            break
        if time.monotonic() > deadline:
            pilot.write(directory / "storage-guard.json", {"required_free_bytes": required, "observed": before})
            raise RuntimeError(f"Insufficient volume headroom for {job['id']}")
        time.sleep(10)
    measured = client(specification)
    with gzip.open(directory / "requests.json.gz", "wt") as stream:
        json.dump(measured, stream)
    after = counters(size)
    pilot.write(directory / "counters.json", {"before": before, "after": after})
    row = sweep.summarize_window(job, measured)
    row["target_started_ns"] = measured["started_epoch_ns"]
    row["drain_seconds"] = max(0.0, (measured["ended_epoch_ns"] - measured["started_epoch_ns"]) / 1e9
                               - response_window(specification))
    row["client_valid"] = (
        row["scheduling_delay_p95_ms"] is not None
        and row["scheduling_delay_p95_ms"] <= plan["schedule_p95_limit_ms"]
        and all(record.get("error_type") not in ("client_capacity_rejected", "concurrent_key_rejected")
                for record in measured["rows"])
    )
    pilot.write(directory / "summary.json", row)
    seal(directory, "receipt.json")
    return row


def seal(folder, name, files=None):
    if files is None:
        files = [item for item in folder.iterdir() if item.is_file() and item.name != name]
    pilot.write(folder / name, {str(item.relative_to(folder)): pilot.digest(item) for item in files})


def verify(folder, name):
    for relative, expected in json.loads((folder / name).read_text()).items():
        if pilot.digest(folder / relative) != expected:
            raise ValueError(f"Evidence hash mismatch in {relative}")


def capture_fragments(output):
    folder = output / "characterization-fragments"
    folder.mkdir()
    for member in lab.members(4):
        logs = lab.kube("logs", f"{member}-0", "-c", "minio", timeout=35)
        with gzip.open(folder / f"{member}.jsonl.gz", "wt") as stream:
            for line in logs.splitlines():
                if line.startswith("KUBESIZER_FRAGMENT "):
                    raw = json.loads(line.split(" ", 1)[1])
                    if raw["side"] == "receive":
                        stream.write(json.dumps(raw) + "\n")
    seal(folder, "receipt.json")
    return folder


def fit(folder, training, idle, extra):
    profile = {}
    for cls in CLASSES:
        mode = {"GET": "read", "PUT": "write"}[cls]
        windows = [row for row in training if row["mode"] == mode]
        details, total_requests = [], 0
        for row in windows:
            path = folder / "windows" / row["id"]
            verify(path, "receipt.json")
            if not row["client_valid"] or row["total_failures"] or row["drain_seconds"] > PLAN["max_cohort_drain_seconds"]:
                raise ValueError(f"Unusable single-class evidence in {row['id']}")
            sample = json.loads((path / "counters.json").read_text())
            with gzip.open(path / "requests.json.gz", "rt") as stream:
                all_requests = json.load(stream)["rows"]
            total_requests += len(all_requests)
            for name in lab.members(4):
                raw = sample_delta(sample["before"][name], sample["after"][name])
                elapsed = raw["elapsed_seconds"]
                details.append({key: raw[key] - idle[name][key] / idle[name]["elapsed_seconds"] * elapsed
                                for key in ("cpu", "busy", "network_bytes")})
        if len(windows) != PLAN["characterization_pairs"] or not total_requests:
            raise ValueError("Both characterization repetitions are required")
        cpu = sum(d["cpu"] for d in details) / total_requests
        busy = max(0.0, sum(d["busy"] for d in details) / (4 * total_requests))
        if not math.isfinite(cpu) or cpu <= 0:
            raise ValueError(f"Container counters did not identify positive {cls} CPU demand")
        profile[cls] = {
            "per_request_cpu_seconds": cpu, "per_drive_busy_seconds": busy,
            "network_bytes_per_request": sum(d["network_bytes"] for d in details) / total_requests,
            "observed_mean_seconds": statistics.fmean(row["classes"][cls]["mean_ms"] / 1000 for row in windows),
            "total_requests_in_counter_windows": total_requests,
        }
    pilot.write(folder / "profile-aggregate.json", profile)
    bursts = phase_samples(extra, training)
    return profile, bursts


def phase_samples(folder, training):
    verify(folder, "receipt.json")
    tagged = defaultdict(list)
    for file in sorted(folder.glob("*.jsonl.gz")):
        with gzip.open(file, "rt") as stream:
            tagged[file.stem].extend(json.loads(line) for line in stream)
    samples, counts = [], []
    for row in training:
        if row["mode"] != "write":
            continue
        low = row["target_started_ns"] + PLAN["warmup_seconds"] * 1_000_000_000
        high = low + PLAN["measurement_seconds"] * 1_000_000_000
        groups = defaultdict(list)
        for member, records in tagged.items():
            for record in records:
                if record["success"] and paths.SIZE // 2 <= record["size"] < paths.SIZE // 2 + 4096:
                    groups[record["temporary_path"]].append((member, record))
        count = 0
        for group in groups.values():
            if len(group) != 4 or len({name for name, _ in group}) != 4:
                continue
            anchor = min(record["start_ns"] for _, record in group)
            if low <= anchor < high:
                span = max(record["write_ns"] + record["flush_ns"] for _, record in group)
                if span > 0:
                    count += 1
                    samples.append(span / 1e9)
        counts.append({"window": row["id"], "complete_groups": count, "tagged_puts": row["classes"]["PUT"]["offered"]})
    return {"samples_seconds": samples, "counts": counts}


def workload(size, per_class_rate, profile, phase=None):
    def deterministic(mean):
        return {"distribution": "deterministic", "mean": mean}

    specs = {
        "cpu": {"execution_model": "processor_sharing", "replica_queue_model": "independent_random"},
        "stripe_io": {"processing_slots_per_replica": 1, "replica_queue_model": "independent_random"},
    }
    classes, entries = {}, {}
    for cls, sample in profile.items():
        cpu = sample["per_request_cpu_seconds"]
        disk = sample["per_drive_busy_seconds"]
        disk_spec = deterministic(disk)
        if cls == "PUT" and phase is not None:
            disk = statistics.fmean(phase)
            disk_spec = pilot.common.empirical(phase)
        residual = sample["observed_mean_seconds"] - cpu - disk
        if residual <= 0:
            raise ValueError(f"Low-load client mean cannot support this {cls} stage decomposition")
        classes[cls] = {
            "concurrency_weight": 0.5,
            "calls": {"cpu": {"stripe_io": 1}} if disk > 0 else {},
            "call_execution": "sequential",
            "latency_boundary": "external_client",
            "latency_components": {"unmodeled_residual": deterministic(residual)},
            "end_to_end_latency_slos": {"mean_seconds": PLAN["mean_slo_seconds"][cls]},
        }
        entries[cls] = {"cpu": {"distribution": "exponential", "rate": per_class_rate}}
        specs["cpu"][cls] = deterministic(cpu)
        if disk > 0:
            specs["stripe_io"][cls] = disk_spec
    constraints = {"cpu": {"fixed": size}}
    if any(key in specs["stripe_io"] for key in CLASSES):
        constraints["stripe_io"] = {"fixed": size // 4}
    else:
        del specs["stripe_io"]
    return {"request_classes": classes, "replica_constraints": constraints,
            "traffic_model": {"entry_services": entries, "service_times": specs}}


def cli(folder, name, rate, profile, phase):
    destination = folder / "models" / name / f"r{rate:g}"
    destination.mkdir(parents=True)
    choices = []
    for size in lab.LAYOUTS:
        file = f"layout-{size}.yaml"
        (destination / file).write_text(yaml.safe_dump(workload(size, rate, profile, phase), sort_keys=False))
        choices.append({
            "name": str(size), "mechanism": f"one pool with {size//4} four-drive EC:2 sets",
            "base_cost": size, "elastic_replica_costs": {}, "workload": file,
            "evidence_sources": ["../../../protocol.json", "../../../profile-aggregate.json"]
            + (["../../../profile-fragment.json"] if phase is not None else []),
        })
    candidates = destination / "candidates.json"
    pilot.write(candidates, {"version": 1, "candidates": choices})
    invoked = [sys.executable, str(ROOT / "kubesizer_cli.py"), "--stateful-candidates", str(candidates),
               "--replica-evaluation", "simulation", "--workers", "2", "--json", "--quiet"]
    result = subprocess.run(invoked, cwd=ROOT, capture_output=True, text=True, timeout=240)
    return {"response": json.loads(result.stdout), "exit_code": result.returncode}


def target_order(plan=PLAN):
    rng = random.Random(plan["seed"])
    order = []
    for pair in range(plan["target_pairs"]):
        rates = list(plan["target_equal_per_class_rates"])
        rng.shuffle(rates)
        for index, rate in enumerate(rates):
            layouts = list(lab.LAYOUTS)
            if (pair + index) % 2:
                layouts.reverse()
            for size in layouts:
                order.append({
                    "id": f"r{rate:g}-layout{size}-pair{pair}", "layout": size, "rate": rate,
                    "pair": pair, "mode": "mixed", "seed": plan["seed"] + 100 + pair * 10 + index,
                })
    return order


def freeze(output, training, idle, folder):
    try:
        profile, evidence = fit(output, training, idle, folder)
    except ValueError as error:
        profile = None
        evidence = {"abstained": str(error)}
    pilot.write(output / "profile-fragment.json", evidence)
    predictions = {}
    for name in ARMS:
        samples = None
        if profile is None:
            predictions[name] = {"abstained": evidence["abstained"]}
            continue
        if name == "fragment":
            if not all(row["complete_groups"] / row["tagged_puts"] >= PLAN["minimum_fragment_group_coverage"]
                       for row in evidence["counts"]):
                predictions[name] = {"abstained": "Insufficient complete four-receiver PUT groups"}
                continue
            samples = evidence["samples_seconds"]
        predictions[name] = {}
        for rate in PLAN["target_equal_per_class_rates"]:
            try:
                predictions[name][f"{rate:g}"] = cli(output, name, rate, profile, samples)
            except (ValueError, subprocess.TimeoutExpired) as error:
                predictions[name][f"{rate:g}"] = {"abstained": str(error)}
    order = target_order()
    pilot.write(output / "target-order.json", order)
    pilot.write(output / "predictions-frozen.json", {"frozen_at": pilot.now(), "arms": predictions})
    candidates = [
        output / "protocol.json", output / "source-receipt.json", output / "target-order.json",
        output / "predictions-frozen.json", output / "profile-fragment.json", output / "idle-counters.json",
        output / "characterization.json", folder / "receipt.json",
        *(output / "windows" / row["id"] / "receipt.json" for row in training),
        *sorted(output.glob("profile-aggregate.json")),
        *sorted(item for item in (output / "models").rglob("*") if item.is_file()),
    ]
    seal(output, "prediction-receipt.json", candidates)
    return order


def scores(output, results, *, plan=PLAN):
    from metrics import student_t_quantile

    verify(output, "prediction-receipt.json")
    prediction = json.loads((output / "predictions-frozen.json").read_text())
    frozen = datetime.fromisoformat(prediction["frozen_at"]).timestamp()
    stored_order = json.loads((output / "target-order.json").read_text())
    if len(results) > len(stored_order) or any(
        row["target_started_ns"] / 1e9 <= frozen or any(row[key] != value for key, value in job.items())
        for row, job in zip(results, stored_order)
    ):
        raise ValueError("A physical target differs from the frozen order or precedes the freeze")
    for row in results:
        verify(output / "windows" / row["id"], "receipt.json")
    rows = []
    repetitions = plan["target_pairs"]
    t_critical = round(student_t_quantile(0.975, repetitions - 1), 8)
    for rate in plan["target_equal_per_class_rates"]:
        layouts = {}
        for size in lab.LAYOUTS:
            subset = [row for row in results if row["rate"] == rate and row["layout"] == size]
            classes = {}
            for cls, slo in plan["mean_slo_seconds"].items():
                means = [row["classes"].get(cls, {}).get("mean_ms") for row in subset]
                failure_rates = [
                    1 - cls_result["successes"] / cls_result["offered"]
                    for row in subset if (cls_result := row["classes"].get(cls)) and cls_result["offered"]
                ]
                central = lower = upper = None
                status = "INCONCLUSIVE"
                if len(subset) == repetitions and all(r["client_valid"] for r in subset):
                    if all(value is not None for value in means):
                        central = statistics.fmean(means)
                        width = t_critical * statistics.stdev(means) / math.sqrt(len(means))
                        lower, upper = max(0, central - width), central + width
                    if max(failure_rates, default=0) > plan["failure_rate_limit"] or (lower is not None and lower > slo * 1000):
                        status = "INFEASIBLE"
                    elif upper is not None and upper <= slo * 1000 and all(r["drain_seconds"] <= plan["max_cohort_drain_seconds"] for r in subset):
                        status = "FEASIBLE_IN_PILOT"
                classes[cls] = {"observed": status, "mean_ms": central, "upper_ms": upper, "lower_ms": lower,
                                "run_means_ms": means, "failure_rates": failure_rates}
            statuses = {row["observed"] for row in classes.values()}
            disposition = ("INFEASIBLE" if "INFEASIBLE" in statuses else "FEASIBLE_IN_PILOT"
                           if statuses == {"FEASIBLE_IN_PILOT"} else "INCONCLUSIVE")
            layouts[str(size)] = {"observed": disposition, "classes": classes}
        best = next((name for name, row in layouts.items() if row["observed"] == "FEASIBLE_IN_PILOT"), None)
        decisions = {}
        for arm, value in prediction["arms"].items():
            available = value.get(f"{rate:g}", {}).get("response", {})
            selected = available.get("result", {}).get("stateful_planning", {}).get("selected")
            decisions[arm] = {"selected": selected, "observed": layouts[selected]["observed"] if selected else "ABSTAINED"}
        rows.append({"per_class_rps": rate, "layouts": layouts, "lowest_observed_feasible_layout": best,
                     "decisions": decisions})
    pilot.write(output / "scores.json", rows)


def finish(output):
    lab.finish(output)
    restoration = output / "template-restoration.json"
    if not (output / "cleanup.json").is_file() or restoration.exists():
        return
    for size in lab.LAYOUTS:
        source = output / f"layout-{size}.yaml"
        for row in yaml.safe_load_all(source.read_text()):
            if row["kind"] != "StatefulSet":
                continue
            member = row["metadata"]["name"]
            existing = lab.owned(member)
            if existing is None or existing["spec"]["replicas"] != 0:
                raise RuntimeError(f"MinIO template cannot be safely restored: {member}")
            template = row["spec"]["template"]
            lab.kube("patch", "statefulset", member, "--type=json", "-p",
                     json.dumps([{"op": "replace", "path": "/spec/template", "value": template}]))
            installed = lab.owned(member)["spec"]["template"]["spec"]
            containers = installed["containers"]
            if any(value.get("command") for value in containers if value["name"] == "minio"):
                raise RuntimeError(f"Instrumented entrypoint remains in {member}")
    pilot.write(restoration, {"stock_entrypoints_restored": True, "completed_at": pilot.now()})


def written_keys(windows, captures):
    result = set()
    for row in captures:
        if row["mode"] == "write":
            with gzip.open(windows / row["id"] / "requests.json.gz", "rt") as stream:
                result.update(record["key"] for record in json.load(stream)["rows"] if record["success"])
    return sorted(result)


def retain_characterization(output, source):
    previous = json.loads((source / "status.json").read_text())
    if (previous["status"] != "FAILED" or "Server not initialized yet" not in previous["error"]
        or (source / "target-results.json").exists() or not (source / "template-restoration.json").is_file()):
        raise ValueError("The retained study must have stopped at alternative readiness before any targets")
    verify(source, "prediction-receipt.json")
    if json.loads((source / "protocol.json").read_text()) != PLAN:
        raise ValueError("The retained characterization used a different protocol")
    if json.loads((source / "target-order.json").read_text()) != target_order():
        raise ValueError("The retained target schedule differs")
    captures = json.loads((source / "characterization.json").read_text())
    windows = output / "windows"
    windows.mkdir()
    for row in captures:
        original = source / "windows" / row["id"]
        verify(original, "receipt.json")
        shutil.copytree(original, windows / row["id"])
    updated = written_keys(windows, captures)
    raw = json.loads((source / "idle-counters.json").read_text())
    idle = {name: sample_delta(raw["before"][name], raw["after"][name]) for name in lab.members(4)}
    for name in ("characterization.json", "idle-counters.json"):
        shutil.copyfile(source / name, output / name)
    phase = output / "characterization-fragments"
    shutil.copytree(source / phase.name, phase)
    verify(phase, "receipt.json")
    pilot.write(output / "retained-characterization.json", {
        "source": str(source), "source_freeze_receipt": pilot.digest(source / "prediction-receipt.json"),
        "updated_put_keys": updated, "target_results_existed": False,
    })
    return captures, idle, phase, updated


def run(output, preflight, deps, binary, binary_digest, *, recovery=None, resume=None):
    output.mkdir(parents=True, exist_ok=False)
    pilot.write(output / "protocol.json", PLAN)
    seal(ROOT, output / "source-receipt.json", [
        Path(__file__).resolve(), ROOT / "scripts/minio_sizing_lab.py",
        ROOT / "scripts/characterize_minio_sweep.py", ROOT / "scripts/characterize_minio_paths.py",
        ROOT / "kubesizer_cli.py", ROOT / "native_backend/backend.py",
        ROOT / "metrics/time_weighted_buckets.py", ROOT / "calibration/workflow.py",
        ROOT / "native_backend/cpp/build-release/kubesizer-simulator",
    ])
    status = {"status": "PROVISIONING", "started_at": pilot.now()}
    pilot.write(output / "status.json", status)
    try:
        lab.assert_isolated()
        if recovery is not None:
            previous = json.loads((recovery / "status.json").read_text())
            if (previous["status"] != "FAILED" or "awk: command not found" not in previous["error"]
                or (recovery / "windows").exists() or not (recovery / "template-restoration.json").is_file()):
                raise ValueError("The previous attempt is ineligible for preload-only reuse")
            pilot.write(output / "recovery-preload.json", {
                "prior_attempt": str(recovery), "prior_status_sha256": pilot.digest(recovery / "status.json"),
                "all_keys_verified_before_measurement": True,
            })
        if (json.loads((preflight / "status.json").read_text())["status"] != "COMPLETED"
            or not (preflight / "cleanup.json").is_file()):
            raise ValueError("Both storage layouts must pass the isolated preflight")
        for size in lab.LAYOUTS:
            for name in lab.members(size):
                if lab.owned(name) is None:
                    raise ValueError(f"The preflight resources are missing for {name}")
            for name in (f"layout-{size}.yaml", f"layout-{size}-verified.json"):
                shutil.copyfile(preflight / name, output / name)
        pilot.write(output / "resources-owned.json", {"used_layouts": list(lab.LAYOUTS), "preflight": str(preflight)})
        retained = retain_characterization(output, resume) if resume is not None else None
        updated = retained[3] if retained is not None else ()
        lab.start(4, output, instrumented=binary, digest=binary_digest)
        with ExitStack() as sessions:
            clients = {4: sessions.enter_context(client_session(
                output, 4, deps, preloaded=recovery is not None or retained is not None,
                updated_keys=updated,
            ))}
            if retained is None:
                status.update(status="CHARACTERIZING", stage_started_at=pilot.now())
                pilot.write(output / "status.json", status)
                idle = idle_sample(output)
                training = []
                for pair in range(PLAN["characterization_pairs"]):
                    modes = ("read", "write") if pair == 0 else ("write", "read")
                    for mode in modes:
                        job = {"id": f"characterize-{mode}-{pair}", "layout": 4, "mode": mode,
                               "rate": PLAN["characterization_class_rate"], "seed": PLAN["seed"] + 10 * pair}
                        status.update(current=job["id"], stage_started_at=pilot.now())
                        pilot.write(output / "status.json", status)
                        training.append(run_window(output, 4, job, clients[4]))
                pilot.write(output / "characterization.json", training)
                extra = capture_fragments(output)
                updated = written_keys(output / "windows", training)
            else:
                training, idle, extra, updated = retained
            status.update(status="FREEZING", stage_started_at=pilot.now())
            pilot.write(output / "status.json", status)
            order = freeze(output, training, idle, extra)
            status.update(status="PROVISIONING_ALTERNATIVE", stage_started_at=pilot.now())
            pilot.write(output / "status.json", status)
            lab.start(8, output, instrumented=binary, digest=binary_digest)
            clients[8] = sessions.enter_context(client_session(output, 8, deps, updated_keys=updated))
            results = []
            for job in order:
                verify(output, "prediction-receipt.json")
                status.update(status="TARGETS", current=job["id"], completed=len(results), total=len(order),
                              stage_started_at=pilot.now())
                pilot.write(output / "status.json", status)
                results.append(run_window(output, job["layout"], job, clients[job["layout"]]))
                pilot.write(output / "target-results.json", results)
                scores(output, results)
        status.update(status="COMPLETED", completed=len(results), completed_at=pilot.now())
    except BaseException as error:
        status.update(status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=pilot.now())
        raise
    finally:
        pilot.write(output / "status.json", status)
        finish(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--preflight", type=Path)
    parser.add_argument("--deps", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--binary-digest")
    parser.add_argument("--recover-preload-from", type=Path)
    parser.add_argument("--resume-frozen-from", type=Path)
    parser.add_argument("--finish", action="store_true")
    parser.add_argument("--score-only", action="store_true")
    args = parser.parse_args()
    if args.worker:
        worker()
    elif args.finish:
        finish(args.output.resolve())
    elif args.score_only:
        scores(args.output.resolve(), json.loads((args.output / "target-results.json").read_text()))
    else:
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        run(args.output.resolve(), args.preflight.resolve(), args.deps.resolve(),
            args.binary.resolve(), args.binary_digest, recovery=args.recover_preload_from,
            resume=args.resume_frozen_from)

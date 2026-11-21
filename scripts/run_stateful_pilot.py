#!/usr/bin/env python3
"""Render, collect and score the isolated sfp1 stateful mechanism pilot.

This experimental harness deliberately uses measured local-worker occupancy.
It does not claim that generic CPU/HTTP counters identify database phases.
The ordinary Kubesizer recommendation workflow is unchanged.
"""

import argparse
import gzip
import hashlib
from http.client import HTTPConnection
import json
from pathlib import Path
import random
import signal
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.request import Request, urlopen

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
NAMESPACE = "kubesizer-stateful-pilot-sfp1"
COMPARISON = "kubesizer-simulator-comparison-targets.service"
MEMBERS = ("primary", "follower1", "follower2")
HOST = "ks-stateful-sfp1.83.212.80.252.nip.io"
INGRESS_IP = "83.212.80.252"
IMAGE = (
    "python:3.12.12-slim-bookworm@sha256:"
    "593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c"
)
PLAN = dict(
    campaign="sfp1",
    family="controlled_persistent_primary_two_followers",
    modes=["async", "quorum1", "all"],
    load_fractions=[0.25, 0.60, 0.90],
    repetitions=2,
    characterization_repetitions=2,
    characterization_rate_rps=4.0,
    warmup_seconds=60,
    measurement_seconds=240,
    drain_limit_seconds=90,
    cooldown_seconds=15,
    simulation_replications=20,
    seed=26111601,
    read_share=0.75,
    first_read_follower_probability=0.75,
    mean_slos_seconds={"read": 0.25, "write": 0.20},
    input_scope="explicit local-worker occupancy and RPC timing instrumentation",
    latency_boundary="external_client",
    portable_cpu_only_inference=False,
    general_database_reliability_claim=False,
)


def now():
    return datetime.now(timezone.utc).isoformat()


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def command(*args, timeout=180):
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args!r}: {result.stderr[-2000:]} {result.stdout[-1000:]}")
    return result.stdout


def kube(*args, timeout=180):
    return command("kubectl", "--request-timeout=120s", *args, timeout=timeout)


def http(url, data=None):
    request = Request(
        url,
        data=None if data is None else json.dumps(data).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=8) as response:
        return json.load(response)


def render(destination):
    destination.mkdir(parents=True, exist_ok=True)
    labels = {"kubesizer.campaign": "sfp1"}
    bootstrap = [
        dict(
            apiVersion="v1",
            kind="Namespace",
            metadata=dict(
                name=NAMESPACE, labels={**labels, "istio-injection": "enabled"}
            ),
        ),
        dict(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="Role",
            metadata=dict(name="pilot-runner", namespace=NAMESPACE),
            rules=[
                dict(
                    apiGroups=[""],
                    resources=[
                        "pods",
                        "pods/log",
                        "services",
                        "endpoints",
                        "persistentvolumeclaims",
                        "configmaps",
                    ],
                    verbs=["get", "list", "watch"],
                ),
                dict(apiGroups=[""], resources=["pods/portforward"], verbs=["create"]),
                dict(
                    apiGroups=["apps"],
                    resources=["statefulsets", "statefulsets/scale"],
                    verbs=["get", "list", "watch", "create", "patch", "update"],
                ),
                dict(
                    apiGroups=[""],
                    resources=["services", "configmaps"],
                    verbs=["create", "patch", "update"],
                ),
                dict(
                    apiGroups=["networking.k8s.io"],
                    resources=["ingresses"],
                    verbs=["get", "list", "create", "patch", "update"],
                ),
            ],
        ),
        dict(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="RoleBinding",
            metadata=dict(name="pilot-runner", namespace=NAMESPACE),
            subjects=[
                dict(
                    kind="ServiceAccount",
                    name="collector",
                    namespace="kubesizer-mi-control-m1003v1",
                )
            ],
            roleRef=dict(
                apiGroup="rbac.authorization.k8s.io", kind="Role", name="pilot-runner"
            ),
        ),
    ]
    workloads = [
        dict(
            apiVersion="v1",
            kind="ConfigMap",
            metadata=dict(name="worker-source", namespace=NAMESPACE, labels=labels),
            data={"server.py": (ROOT / "scripts/stateful_pilot_server.py").read_text()},
        )
    ]
    for name, node, means in zip(
        MEMBERS,
        ("skube07", "skube08", "skube09"),
        ((0.001, 0.002), (0.01, 0.02), (0.02, 0.04)),
    ):
        selector = {"app": name, **labels}
        workloads.append(
            dict(
                apiVersion="v1",
                kind="Service",
                metadata=dict(name=name, namespace=NAMESPACE, labels=labels),
                spec=dict(
                    clusterIP="None",
                    selector=selector,
                    ports=[
                        dict(name="http", port=8080, targetPort=8080),
                        dict(name="evidence", port=9091, targetPort=9091),
                    ],
                ),
            )
        )
        env = dict(
            SERVICE_NAME=name,
            DATABASE="/data/kv.sqlite",
            READ_CPU_SECONDS=str(means[0]),
            WRITE_CPU_SECONDS=str(means[1]),
        )
        if name == "primary":
            env["TARGETS"] = "http://follower1:8080/work,http://follower2:8080/work"
        container = dict(
            name="application",
            image=IMAGE,
            command=["python", "/experiment/server.py"],
            env=[dict(name=k, value=v) for k, v in env.items()],
            ports=[
                dict(name="http", containerPort=8080),
                dict(name="evidence", containerPort=9091),
            ],
            resources=dict(
                requests={"cpu": "1", "memory": "256Mi"},
                limits={"cpu": "1", "memory": "512Mi"},
            ),
            volumeMounts=[
                dict(name="source", mountPath="/experiment", readOnly=True),
                dict(name="data", mountPath="/data"),
            ],
            readinessProbe=dict(
                httpGet=dict(path="/health", port=8080),
                initialDelaySeconds=3,
                periodSeconds=3,
            ),
        )
        workloads.append(
            dict(
                apiVersion="apps/v1",
                kind="StatefulSet",
                metadata=dict(name=name, namespace=NAMESPACE, labels=labels),
                spec=dict(
                    serviceName=name,
                    replicas=1,
                    selector=dict(matchLabels=selector),
                    template=dict(
                        metadata=dict(
                            labels=selector,
                            annotations={
                                "traffic.sidecar.istio.io/excludeInboundPorts": "9091",
                                "sidecar.istio.io/proxyCPU": "100m",
                                "sidecar.istio.io/proxyMemory": "128Mi",
                            },
                        ),
                        spec=dict(
                            nodeSelector={"kubernetes.io/hostname": node},
                            containers=[container],
                            volumes=[
                                dict(
                                    name="source", configMap=dict(name="worker-source")
                                )
                            ],
                        ),
                    ),
                    volumeClaimTemplates=[
                        dict(
                            metadata=dict(name="data"),
                            spec=dict(
                                accessModes=["ReadWriteOnce"],
                                storageClassName="longhorn",
                                resources=dict(requests={"storage": "512Mi"}),
                            ),
                        )
                    ],
                ),
            )
        )
    workloads.append(
        dict(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            metadata=dict(
                name="primary",
                namespace=NAMESPACE,
                labels=labels,
                annotations={"nginx.ingress.kubernetes.io/proxy-read-timeout": "35"},
            ),
            spec=dict(
                ingressClassName="nginx",
                rules=[
                    dict(
                        host=HOST,
                        http=dict(
                            paths=[
                                dict(
                                    path="/work",
                                    pathType="Exact",
                                    backend=dict(
                                        service=dict(
                                            name="primary", port=dict(name="http")
                                        )
                                    ),
                                )
                            ]
                        ),
                    )
                ],
            ),
        )
    )
    for name, values in (("bootstrap.yaml", bootstrap), ("workload.yaml", workloads)):
        (destination / name).write_text(yaml.safe_dump_all(values, sort_keys=False))
    write(destination / "protocol.json", PLAN)
    write(
        destination / "bundle-receipt.json",
        {
            name: digest(destination / name)
            for name in ("bootstrap.yaml", "workload.yaml", "protocol.json")
        },
    )
    print(
        json.dumps(
            dict(
                rendered=str(destination),
                target_runs=18,
                nominal_measurement_minutes=18 * 5,
                provisioned_storage_mib=3 * 512,
            )
        ),
        flush=True,
    )


class Evidence:
    def __init__(self, output):
        self.output, self.processes = output, []
        self.urls = {
            name: f"http://127.0.0.1:{29191+i}" for i, name in enumerate(MEMBERS)
        }

    def __enter__(self):
        for i, name in enumerate(MEMBERS):
            log = (self.output / f"port-forward-{name}.log").open("w")
            process = subprocess.Popen(
                [
                    "kubectl",
                    "-n",
                    NAMESPACE,
                    "port-forward",
                    f"pod/{name}-0",
                    f"{29191+i}:9091",
                    "--address=127.0.0.1",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            self.processes.append((process, log))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                self.status()
                return self
            except OSError:
                time.sleep(1)
        self.__exit__(None, None, None)
        raise RuntimeError("Evidence port forwards did not become ready")

    def __exit__(self, *args):
        for process, log in self.processes:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            log.close()

    def status(self):
        return {name: http(url + "/status") for name, url in self.urls.items()}

    def idle(self, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = self.status()
            if all(
                not (
                    s["queued"]
                    or s["active"]
                    or s["counters"].get("pending_calls", 0)
                    or s["counters"].get("http_in_flight", 0)
                )
                for s in states.values()
            ):
                return
            time.sleep(1)
        raise RuntimeError(
            "Stateful work/backlog did not drain; next run cannot reset it"
        )


def collect(evidence, output, *, run_id, mode, rate, seed, warmup=60, measurement=240):
    directory = output / "runs" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    evidence.idle()
    for url in evidence.urls.values():
        http(url + "/reset", {"seed": seed})
    traces, snapshots = [], []
    lock = threading.Lock()
    capacity = threading.BoundedSemaphore(512)
    pending = [0]
    stop_monitor = threading.Event()
    started = time.monotonic()
    epoch_start = time.time()

    def monitor():
        while not stop_monitor.is_set():
            try:
                snapshots.append(
                    dict(
                        elapsed=time.monotonic() - started,
                        epoch=time.time(),
                        members=evidence.status(),
                    )
                )
            except Exception as error:
                snapshots.append(
                    dict(elapsed=time.monotonic() - started, error=str(error))
                )
            stop_monitor.wait(5)

    def send(index, scheduled, request_class, tagged):
        beginning = time.monotonic()
        row = dict(
            id=f"{run_id}:{index}",
            request_class=request_class,
            tagged=tagged,
            scheduled_seconds=scheduled - started,
            started_seconds=beginning - started,
            scheduling_delay_seconds=beginning - scheduled,
        )
        try:
            connection = HTTPConnection(INGRESS_IP, timeout=30)
            body = json.dumps(
                dict(
                    id=row["id"],
                    request_class=request_class,
                    key=index % 1024,
                    mode=mode,
                )
            ).encode()
            try:
                connection.request(
                    "POST",
                    "/work",
                    body=body,
                    headers={
                        "Host": HOST,
                        "Content-Type": "application/json",
                        "Connection": "close",
                    },
                )
                response = connection.getresponse()
                value = json.loads(response.read())
                row.update(
                    status=response.status,
                    success=response.status == 200 and value.get("success") is True,
                    server_response_seconds=value.get("response_seconds"),
                )
            finally:
                connection.close()
        except Exception as error:
            row.update(success=False, error=f"{type(error).__name__}: {error}")
        finally:
            row["latency_seconds"] = time.monotonic() - beginning
            with lock:
                traces.append(row)
                if tagged:
                    pending[0] -= 1
            capacity.release()

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    arrivals, classes = random.Random(seed), random.Random(seed + 1_000_000)
    next_time = started
    count = 0
    measurement_end = started + warmup + measurement
    timed_out = False
    try:
        with ThreadPoolExecutor(max_workers=128) as pool:
            while True:
                current = time.monotonic()
                with lock:
                    remaining = pending[0]
                if current >= measurement_end and not remaining:
                    break
                if current >= measurement_end + PLAN["drain_limit_seconds"]:
                    timed_out = True
                    break
                next_time += arrivals.expovariate(rate)
                time.sleep(max(0, next_time - time.monotonic()))
                if next_time >= measurement_end and not remaining:
                    break
                count += 1
                tagged = started + warmup <= next_time < measurement_end
                request_class = (
                    "read" if classes.random() < PLAN["read_share"] else "write"
                )
                with lock:
                    pending[0] += int(tagged)
                if capacity.acquire(blocking=False):
                    pool.submit(send, count, next_time, request_class, tagged)
                else:
                    with lock:
                        traces.append(
                            dict(
                                id=f"{run_id}:{count}",
                                request_class=request_class,
                                tagged=tagged,
                                scheduled_seconds=next_time - started,
                                started_seconds=None,
                                scheduling_delay_seconds=0,
                                latency_seconds=0,
                                success=False,
                                error="client_capacity_rejected",
                            )
                        )
                        pending[0] -= int(tagged)
        evidence.idle(timeout=PLAN["drain_limit_seconds"])
    finally:
        stop_monitor.set()
        thread.join(timeout=10)
        with gzip.open(directory / "requests.jsonl.gz", "wt") as stream:
            for row in sorted(traces, key=lambda x: x["scheduled_seconds"]):
                stream.write(json.dumps(row) + "\n")
        with gzip.open(directory / "events.json.gz", "wt") as stream:
            events = {
                name: http(url + "/events?since=0")
                for name, url in evidence.urls.items()
            }
            json.dump(events, stream)
        write(directory / "occupancy.json", snapshots)
    cohorts = [row for row in traces if row["tagged"]]
    result = dict(
        run_id=run_id,
        mode=mode,
        rate_rps=rate,
        seed=seed,
        started_epoch=epoch_start,
        warmup_seconds=warmup,
        measurement_seconds=measurement,
        timed_out=timed_out,
        cohort_count=len(cohorts),
        classes={},
    )
    for name in ("read", "write"):
        rows = [row for row in cohorts if row["request_class"] == name]
        successes = [row for row in rows if row["success"]]
        result["classes"][name] = dict(
            count=len(rows),
            completed=len(successes),
            failures=len(rows) - len(successes),
            mean_seconds=(
                statistics.fmean(r["latency_seconds"] for r in successes)
                if successes
                else None
            ),
            maximum_scheduling_delay_seconds=max(
                (r["scheduling_delay_seconds"] for r in rows), default=0
            ),
        )
    result["evidence_lost"] = any(item["lost"] for item in events.values())
    result["child_failures"] = sum(
        s["counters"].get("child_failures", 0) for s in evidence.status().values()
    )
    result["worker_occupancy"] = {}
    for member in MEMBERS:
        samples = [
            s
            for s in snapshots
            if warmup <= s["elapsed"] < warmup + measurement and "members" in s
        ]
        first = [
            s["members"][member]["queued"]
            for s in samples
            if s["elapsed"] < warmup + measurement / 2
        ]
        second = [
            s["members"][member]["queued"]
            for s in samples
            if s["elapsed"] >= warmup + measurement / 2
        ]
        result["worker_occupancy"][member] = dict(
            first_half_mean_queue=statistics.fmean(first) if first else None,
            second_half_mean_queue=statistics.fmean(second) if second else None,
            final_sample_queue=(
                samples[-1]["members"][member]["queued"] if samples else None
            ),
            sampled_queue_is_not_asymptotic_stability_proof=True,
        )
        if len(samples) >= 2:
            first_sample, last_sample = samples[0], samples[-1]
            elapsed = last_sample["elapsed"] - first_sample["elapsed"]
            for counter, label in (
                ("local_worker_seconds", "observed_worker_busy_fraction"),
                ("local_cpu_seconds", "observed_local_cpu_seconds_per_second"),
            ):
                delta = last_sample["members"][member]["counters"].get(
                    counter, 0
                ) - first_sample["members"][member]["counters"].get(counter, 0)
                result["worker_occupancy"][member][label] = delta / elapsed
    write(directory / "summary.json", result)
    write(
        directory / "receipt.json",
        {
            p.name: digest(p)
            for p in directory.iterdir()
            if p.is_file() and p.name != "receipt.json"
        },
    )
    print(
        json.dumps(
            dict(completed=run_id, mode=mode, rate_rps=rate, classes=result["classes"])
        ),
        flush=True,
    )
    return result


def characterize(output, runs):
    local = {name: {c: [] for c in ("read", "write")} for name in MEMBERS}
    transport, residual = [], {c: [] for c in ("read", "write")}
    for run in runs:
        directory = output / "runs" / run["run_id"]
        with gzip.open(directory / "requests.jsonl.gz", "rt") as stream:
            rows = [json.loads(line) for line in stream]
        tagged = {r["id"] for r in rows if r["tagged"]}
        for row in rows:
            if row["id"] in tagged and row["success"]:
                residual[row["request_class"]].append(
                    max(0, row["latency_seconds"] - row["server_response_seconds"])
                )
        with gzip.open(directory / "events.json.gz", "rt") as stream:
            events = json.load(stream)
        if run["evidence_lost"] or run["child_failures"] or run["timed_out"]:
            raise ValueError(
                "Characterization is incomplete or has propagation failures"
            )
        for name, value in events.items():
            for event in value["events"]:
                if event.get("request_id") not in tagged:
                    continue
                if event["type"] == "local":
                    local[name][event["request_class"]].append(event["local_seconds"])
                if event["type"] == "rpc" and event["success"]:
                    transport.append(event["transport_seconds"] / 2)
        if any(c["failures"] for c in run["classes"].values()):
            raise ValueError("Characterization contains client failures")
    if any(len(values) < 50 for item in local.values() for values in item.values()):
        raise ValueError("Fewer than 50 local service samples in a member/class")
    result = dict(
        local_samples_seconds=local,
        one_way_transport_seconds=transport,
        residual_seconds=residual,
        sources=[r["run_id"] for r in runs],
        characterization_only=True,
    )
    write(output / "characterization.json", result)
    return result


def empirical(values):
    return dict(
        distribution="empirical", samples=values, sampling="iid_with_replacement"
    )


def model(profile, mode, rate, *, enriched):
    result = dict(
        analytical_call_model="response_bounds",
        request_classes={},
        traffic_model=dict(entry_services={}, service_times={}),
        replica_constraints={name: {"fixed": 1} for name in MEMBERS},
    )
    for c, share in (("read", 0.75), ("write", 0.25)):
        result["request_classes"][c] = dict(
            concurrency_weight=share,
            calls={
                "primary": (
                    {"follower1": 0.75, "follower2": 0.25}
                    if c == "read"
                    else {"follower1": 1, "follower2": 1}
                )
            },
            call_execution="parallel",
            latency_boundary="external_client",
            latency_components={
                "client_to_entry": empirical(profile["residual_seconds"][c])
            },
            end_to_end_latency_slos={"mean_seconds": PLAN["mean_slos_seconds"][c]},
        )
        if enriched:
            policy = (
                {"execution": "choice"}
                if c == "read"
                else (
                    {"completion": "background"}
                    if mode == "async"
                    else (
                        {"completion": "quorum", "required": 1}
                        if mode == "quorum1"
                        else {"completion": "all"}
                    )
                )
            )
            result["request_classes"][c]["call_policies"] = {"primary": policy}
        result["traffic_model"]["entry_services"][c] = {
            "primary": {"distribution": "exponential", "rate": rate * share}
        }
    for name in MEMBERS:
        result["traffic_model"]["service_times"][name] = dict(
            processing_slots_per_replica=1,
            concurrency_limit_per_replica=512 if name == "primary" else 1,
            replica_queue_model="pooled",
            **{
                c: empirical(profile["local_samples_seconds"][name][c])
                for c in ("read", "write")
            },
        )
    duration = empirical(profile["one_way_transport_seconds"])
    result["traffic_model"]["communication_latency"] = dict(
        same_node_one_way=duration,
        cross_node_one_way=duration,
        cross_node_probability=1,
    )
    return result


def predict(output, profile):
    from performance_models import QueueingNetworkType, System
    from native_backend.backend import SIMULATOR_SEMANTICS_VERSION

    coefficients = {}
    for name in MEMBERS:
        read_visits = 1 if name == "primary" else 0.75 if name == "follower1" else 0.25
        coefficients[name] = 0.75 * read_visits * statistics.fmean(
            profile["local_samples_seconds"][name]["read"]
        ) + 0.25 * statistics.fmean(profile["local_samples_seconds"][name]["write"])
    capacity = 1 / max(coefficients.values())
    rows = []
    for mode in PLAN["modes"]:
        for fraction in PLAN["load_fractions"]:
            for enriched in (False, True):
                variant = "enriched" if enriched else "legacy-call-rules"
                frozen = model(profile, mode, capacity * fraction, enriched=enriched)
                cell = f"{mode}-u{round(fraction*100):02d}"
                path = output / "models" / f"{cell}-{variant}.json"
                write(path, frozen)
                network = System(cell, frozen, QueueingNetworkType.OPEN)
                row = dict(
                    cell=cell,
                    mode=mode,
                    load_fraction=fraction,
                    load_basis="maximum offered mean local-worker occupancy",
                    rate_rps=capacity * fraction,
                    variant=variant,
                    input_sha256=digest(path),
                    configuration=[1, 1, 1],
                )
                reasons = network.get_strict_analytical_incompatibilities()
                row["analytical"] = {"status": "ABSTAINED", "reasons": list(reasons)}
                if not reasons:
                    row["analytical"] = dict(
                        status="CONDITIONAL_BOUNDS",
                        mean_bounds=network.get_request_class_latency_bounds(),
                    )
                try:
                    replications = network.simulate_measurement_replications(
                        warmup_duration=PLAN["warmup_seconds"],
                        measurement_duration=PLAN["measurement_seconds"],
                        seeds=tuple(
                            7_000_000 + 1000 * len(rows) + i
                            for i in range(PLAN["simulation_replications"])
                        ),
                        workers=2,
                        maximum_drain_time=90,
                    )
                    row["simulation"] = dict(
                        status="PREDICTED",
                        classes={
                            c: dict(
                                mean_seconds=statistics.fmean(means),
                                replication_means_seconds=means,
                                approximate_mc95_half_width_seconds=(
                                    2.093 * statistics.stdev(means) / len(means) ** 0.5
                                ),
                            )
                            for c in ("read", "write")
                            for means in [
                                [
                                    statistics.fmean(r.request_class_latencies[c])
                                    for r in replications
                                ]
                            ]
                        },
                    )
                except (ValueError, RuntimeError) as error:
                    row["simulation"] = dict(status="ABSTAINED", reason=str(error))
                rows.append(row)
                print(
                    json.dumps(
                        dict(
                            frozen_prediction=cell,
                            variant=variant,
                            simulation=row["simulation"]["status"],
                        )
                    ),
                    flush=True,
                )
    value = dict(
        frozen_at=now(),
        target_outcomes_used=False,
        worker_capacity_reference_rps=capacity,
        work_coefficients=coefficients,
        semantics_version=SIMULATOR_SEMANTICS_VERSION,
        predictions=rows,
    )
    write(output / "predictions-frozen.json", value)
    write(
        output / "prediction-receipt.json",
        {
            str(p.relative_to(output)): digest(p)
            for p in [
                output / "characterization.json",
                output / "predictions-frozen.json",
                *sorted((output / "models").glob("*.json")),
            ]
        },
    )
    return value


def report(output, predictions, runs):
    lines = [
        "# Preliminary stateful mechanism pilot",
        "",
        "Controlled persistent application-level replication on Okeanos. This is not "
        "PostgreSQL/MinIO protocol validation or a general stateful sizing result.",
        "",
        "Models and predictions were frozen from characterization before target runs. "
        "Both variants use identical worker samples, transport and boundary residuals. "
        "The baseline removes only source call policies; it retains role-specific queues. "
        "Local-worker occupancy is additional explicit instrumentation. CPU, SQLite "
        "and storage waiting within one occupied FIFO worker are intentionally lumped.",
        "",
        "Load fractions refer to offered mean worker occupancy from characterization, "
        "not measured CPU utilization. Two physical repetitions per cell are exploratory. "
        "Simulation replications quantify Monte Carlo variation, not model error.",
        "",
        "Analytical abstentions are retained. Finite synchronous parent holding is "
        "unsupported by the strict analytical evaluator used here. Upper bounds, when "
        "available, are not scored as point-mean predictions.",
        "",
        "| Mode | Load fraction | Class | Measured mean ms | Legacy mean ms | New mean ms | Legacy APE % | New APE % | Eligible |",
        "|---|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    rows = []
    for mode in PLAN["modes"]:
        for fraction in PLAN["load_fractions"]:
            cell = f"{mode}-u{round(fraction*100):02d}"
            observations = [r for r in runs if r["cell"] == cell]
            for c in ("read", "write"):
                eligible = len(observations) == PLAN["repetitions"] and all(
                    not r["timed_out"]
                    and not r["evidence_lost"]
                    and not r["child_failures"]
                    and not r["classes"][c]["failures"]
                    and r["classes"][c]["maximum_scheduling_delay_seconds"] < 0.05
                    for r in observations
                )
                count = sum(r["classes"][c]["completed"] for r in observations)
                observed = (
                    sum(
                        r["classes"][c]["mean_seconds"] * r["classes"][c]["completed"]
                        for r in observations
                        if r["classes"][c]["mean_seconds"] is not None
                    )
                    / count
                    if count
                    else None
                )
                row = dict(
                    cell=cell,
                    request_class=c,
                    mode=mode,
                    load_fraction=fraction,
                    observed_mean_seconds=observed,
                    eligible=eligible,
                    physical_repetitions=len(observations),
                    observed_slo_violation=(
                        observed > PLAN["mean_slos_seconds"][c]
                        if observed is not None
                        else None
                    ),
                )
                for variant in ("legacy-call-rules", "enriched"):
                    prediction = next(
                        p
                        for p in predictions["predictions"]
                        if p["cell"] == cell and p["variant"] == variant
                    )
                    sim = prediction["simulation"]
                    estimate = sim.get("classes", {}).get(c, {}).get("mean_seconds")
                    row[variant] = dict(
                        mean_seconds=estimate,
                        ape_percent=(
                            100 * abs(estimate - observed) / observed
                            if estimate is not None and observed
                            else None
                        ),
                        analytical=prediction["analytical"],
                        pilot_point_mean_slo_pass=(
                            estimate <= PLAN["mean_slos_seconds"][c]
                            if estimate is not None
                            else None
                        ),
                        pilot_point_mean_unsafe=(
                            eligible
                            and estimate is not None
                            and observed is not None
                            and estimate <= PLAN["mean_slos_seconds"][c] < observed
                        ),
                    )
                rows.append(row)
                old, new = row["legacy-call-rules"], row["enriched"]

                def fmt(value, scale=1):
                    return f"{value*scale:.2f}" if value is not None else "abstain"

                lines.append(
                    f"| {mode} | {fraction} | {c} | {fmt(observed,1000)} | "
                    f"{fmt(old['mean_seconds'],1000)} | {fmt(new['mean_seconds'],1000)} | "
                    f"{fmt(old['ape_percent'])} | {fmt(new['ape_percent'])} | {eligible} |"
                )
    lines += [
        "",
        "Failures, cohort counts, scheduling delays and propagation failures "
        "are retained in every run's summary. Occupancy trajectories and raw "
        "request traces are retained; finite cohort completion alone does not "
        "establish steady-state stability. No production recommendation or "
        "statistical safety certificate is issued by this pilot.",
        "",
    ]
    (output / "REPORT.md").write_text("\n".join(lines))
    write(output / "scores.json", rows)


def wait_and_pause(output, *, comparison_root=None, startup_timeout=600):
    root = comparison_root or Path("/home/experiment/experiments/scv1/data")
    startup_deadline = time.monotonic() + startup_timeout
    while True:
        if command("systemctl", "is-active", COMPARISON).strip() != "active":
            raise RuntimeError("Expected the existing comparison campaign to be active")
        active, incomplete_manifest = [], False
        for path in root.glob("*/core*/study_manifest.yaml"):
            try:
                document = yaml.safe_load(path.read_text())
                for row in document["runs"]:
                    if row["status"] == "RUNNING":
                        active.append((path, row["id"]))
            except (OSError, yaml.YAMLError, TypeError, KeyError):
                # The comparison runner can be between manifest writes.
                incomplete_manifest = True
                break
        if len(active) > 1:
            raise RuntimeError(
                f"Expected exactly one active comparison treatment, got {len(active)}"
            )
        if len(active) == 1 and not incomplete_manifest:
            break
        write(
            output / "boundary-wait.json",
            dict(
                status="WAITING_FOR_RUNNING_TREATMENT",
                checked_at=now(),
                incomplete_manifest=incomplete_manifest,
            ),
        )
        if time.monotonic() >= startup_deadline:
            raise RuntimeError(
                "Comparison did not start a treatment; pilot not launched"
            )
        # Active systemd service does not imply its first treatment is ready.
        time.sleep(3)
    path, run_id = active[0]
    write(
        output / "boundary-wait.json",
        dict(started=now(), run_id=run_id, manifest=str(path)),
    )
    print(json.dumps(dict(waiting_for_comparison_checkpoint=run_id)), flush=True)
    deadline = time.monotonic() + 45 * 60
    while time.monotonic() < deadline:
        try:
            document = yaml.safe_load(path.read_text())
            record = next(r for r in document["runs"] if r["id"] == run_id)
        except (OSError, yaml.YAMLError, TypeError, KeyError, StopIteration):
            time.sleep(3)
            continue
        receipt = path.parent / "evidence" / run_id / "receipt.yaml"
        if record["status"] == "COMPLETED" and receipt.exists():
            completed = {
                r["id"]: r.get("artifact_sha256")
                for r in document["runs"]
                if r["status"] == "COMPLETED"
            }
            write(
                output / "comparison-pause.json",
                dict(
                    requested_at=now(),
                    manifest=str(path),
                    completed=completed,
                    checkpoint=run_id,
                    resume_required=True,
                ),
            )
            command("systemctl", "stop", COMPARISON, timeout=180)
            after = yaml.safe_load(path.read_text())
            preserved = {
                r["id"]: r.get("artifact_sha256")
                for r in after["runs"]
                if r["status"] == "COMPLETED"
            }
            if any(preserved.get(k) != v for k, v in completed.items()):
                raise RuntimeError(
                    "Completed comparison artifact bindings changed during pause"
                )
            print(
                json.dumps(dict(comparison_paused=True, checkpoint=run_id)), flush=True
            )
            return
        if record["status"] == "FAILED":
            raise RuntimeError(
                "Comparison run failed before requested checkpoint; pilot not launched"
            )
        time.sleep(3)
    raise RuntimeError(
        "Comparison checkpoint wait exceeded 45 minutes; pilot not launched"
    )


def finish(output):
    marker = output / "comparison-pause.json"
    if not marker.exists():
        return
    pause = json.loads(marker.read_text())
    if not pause.get("resume_required"):
        return
    # Scale only this pilot's named resources. Existing STELAR resources and
    # comparison declarations/receipts are never edited.
    items = json.loads(kube("-n", NAMESPACE, "get", "statefulsets", "-o", "json"))[
        "items"
    ]
    for item in items:
        name = item["metadata"]["name"]
        if (
            name not in MEMBERS
            or item["metadata"].get("labels", {}).get("kubesizer.campaign") != "sfp1"
        ):
            raise RuntimeError(
                "Unexpected resource in pilot namespace; refusing broad cleanup"
            )
        kube("-n", NAMESPACE, "scale", "statefulset", name, "--replicas=0")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        pods = json.loads(kube("-n", NAMESPACE, "get", "pods", "-o", "json"))["items"]
        if not pods:
            command("systemctl", "start", COMPARISON)
            pause.update(resumed_at=now(), resume_required=False)
            write(marker, pause)
            print(json.dumps(dict(comparison_resumed=True)), flush=True)
            return
        time.sleep(3)
    raise RuntimeError(
        "Pilot pods remain: comparison must not resume into overlapping load"
    )


def run(bundle, output):
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"Use a fresh output directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    if json.loads((bundle / "protocol.json").read_text()) != PLAN:
        raise ValueError("Frozen pilot protocol differs from runner settings")
    for path, expected in json.loads(
        (bundle / "bundle-receipt.json").read_text()
    ).items():
        if digest(bundle / path) != expected:
            raise ValueError("Pilot deployment bundle changed after rendering")
    write(output / "protocol.json", PLAN)
    write(
        output / "source-receipt.json",
        {
            str(p.relative_to(ROOT)): digest(p)
            for p in [
                ROOT / "scripts/run_stateful_pilot.py",
                ROOT / "scripts/stateful_pilot_server.py",
                ROOT / "native_backend/cpp/src/main.cpp",
                ROOT / "native_backend/backend.py",
                ROOT / "native_backend/cpp/build-release/kubesizer-simulator",
            ]
        },
    )
    status = dict(status="WAITING_FOR_COMPARISON_BOUNDARY", started_at=now())
    write(output / "status.json", status)
    try:
        wait_and_pause(output)
        deployments = json.loads(kube("get", "deployments", "-A", "-o", "json"))[
            "items"
        ]
        active = [
            p["metadata"]["namespace"] + "/" + p["metadata"]["name"]
            for p in deployments
            if p["metadata"]["namespace"].endswith("-scv1")
            and p["spec"].get("replicas", 1)
        ]
        if active:
            raise RuntimeError(f"Comparison deployments are not idle: {active}")
        status.update(status="PROVISIONING", comparison_paused_at=now())
        write(output / "status.json", status)
        kube("apply", "-f", str(bundle / "workload.yaml"))
        for name in MEMBERS:
            kube(
                "-n",
                NAMESPACE,
                "rollout",
                "status",
                f"statefulset/{name}",
                "--timeout=600s",
                timeout=620,
            )
        write(
            output / "deployment-context.json",
            json.loads(
                kube(
                    "-n",
                    NAMESPACE,
                    "get",
                    "statefulsets,pods,persistentvolumeclaims",
                    "-o",
                    "json",
                )
            ),
        )
        with Evidence(output) as evidence:
            status.update(status="CHARACTERIZATION", stage_started_at=now())
            write(output / "status.json", status)
            characterization = [
                collect(
                    evidence,
                    output,
                    run_id=f"characterization-{i}",
                    mode="all",
                    rate=PLAN["characterization_rate_rps"],
                    seed=PLAN["seed"] + i,
                    warmup=30,
                    measurement=180,
                )
                for i in range(PLAN["characterization_repetitions"])
            ]
            profile = characterize(output, characterization)
            status.update(status="FREEZING_PREDICTIONS", stage_started_at=now())
            write(output / "status.json", status)
            predictions = predict(output, profile)
            schedule = [
                (m, u, r)
                for m in PLAN["modes"]
                for u in PLAN["load_fractions"]
                for r in range(PLAN["repetitions"])
            ]
            random.Random(PLAN["seed"]).shuffle(schedule)
            write(output / "target-order-frozen.json", schedule)
            runs = []
            for index, (mode, fraction, repetition) in enumerate(schedule):
                cell = f"{mode}-u{round(fraction*100):02d}"
                status.update(
                    status="TARGETS",
                    completed_targets=len(runs),
                    total_targets=len(schedule),
                    current_cell=cell,
                    current_repetition=repetition,
                    stage_started_at=now(),
                )
                write(output / "status.json", status)
                for path, expected in json.loads(
                    (output / "prediction-receipt.json").read_text()
                ).items():
                    if digest(output / path) != expected:
                        raise RuntimeError("Frozen prediction evidence changed")
                result = collect(
                    evidence,
                    output,
                    run_id=f"{cell}-rep{repetition}",
                    mode=mode,
                    rate=predictions["worker_capacity_reference_rps"] * fraction,
                    seed=PLAN["seed"] + 100 + index,
                )
                result.update(cell=cell, load_fraction=fraction)
                runs.append(result)
                write(output / "target-results.json", runs)
                report(output, predictions, runs)
                time.sleep(PLAN["cooldown_seconds"])
            status.update(
                status="COMPLETED", completed_targets=len(runs), completed_at=now()
            )
    except BaseException as error:
        status.update(
            status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=now()
        )
        raise
    finally:
        write(output / "status.json", status)
        finish(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", type=Path)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--finish", action="store_true")
    arguments = parser.parse_args()
    if arguments.render:
        render(arguments.render.resolve())
    elif arguments.finish:
        finish(arguments.output.resolve())
    else:
        if arguments.bundle is None or arguments.output is None:
            parser.error("--bundle and --output are required for collection")
        signal.signal(
            signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt())
        )
        run(arguments.bundle.resolve(), arguments.output.resolve())


if __name__ == "__main__":
    main()

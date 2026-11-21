#!/usr/bin/env python3
"""Frozen cmp1 characterization/holdout mechanism pilot, queued after MinIO.

This is a fixed-configuration prediction/ablation study, not a recommendation
certificate or validation of RabbitMQ/Redis/Knative/TCP implementations.
"""

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
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

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_stateful_pilot as common
from scripts.component_pilot_server import FAMILIES, KEY_PROFILE

NAMESPACE = "kubesizer-components-cmp1"
HOST = "ks-components-cmp1.83.212.80.252.nip.io"
EVIDENCE = "http://127.0.0.1:29591"
PREDECESSOR = "kubesizer-minio-pilot-mio1-r5.service"
PREDECESSOR_ROOT = Path("/home/experiment/experiments/mio1-r5/data")
HOLD = Path("/home/experiment/experiments/comparison-manual-hold.json")
PLAN = dict(
    campaign="cmp1",
    families=list(FAMILIES),
    capacities=[1, 2],
    reference_fractions=[0.25, 0.65, 0.95],
    arrivals=["poisson", "bursty"],
    repetitions=2,
    characterization_repetitions=2,
    characterization_warmup_seconds=15,
    characterization_measurement_seconds=90,
    target_warmup_seconds=15,
    target_measurement_seconds=45,
    maximum_drain_seconds=30,
    cooldown_seconds=5,
    simulation_replications=20,
    seed=26111751,
    mean_slo_seconds=0.25,
    maximum_terminal_failure_rate=0.02,
    minimum_characterization_phase_samples=50,
    burst_interarrival_scv=4,
    burst_distribution="gamma_shape_0.25",
    key_profile=KEY_PROFILE,
    cache_item_bytes=1024,
    cache_ttl_seconds=1,
    link_payload_sizes_bytes=[65536, 262144],
    link_chunk_bytes=4096,
    serverless_idle_seconds=0.2,
    retry_max_attempts=3,
    attempt_timeout_seconds=0.06,
    retry_backoff_seconds=0.005,
    target_outcomes_used_for_inputs=False,
    evidence_scope="explicit occupied-worker/delay spans, declared capacities and key generator; not CPU-only inference",
    study_scope="controlled real-thread HTTP mechanisms; fixed-configuration point comparisons, not sizing certification",
    reference_is_observed_utilization=False,
)
write, digest, command, kube, now, http = (
    common.write,
    common.digest,
    common.command,
    common.kube,
    common.now,
    common.http,
)


def render(destination):
    destination.mkdir(parents=True, exist_ok=True)
    labels = {"kubesizer.campaign": "cmp1"}
    ns = dict(
        apiVersion="v1",
        kind="Namespace",
        metadata=dict(name=NAMESPACE, labels={**labels, "istio-injection": "enabled"}),
    )
    role = dict(
        apiVersion="rbac.authorization.k8s.io/v1",
        kind="Role",
        metadata=dict(name="pilot-runner", namespace=NAMESPACE),
        rules=[
            dict(
                apiGroups=[""],
                resources=["pods", "pods/log", "services", "configmaps"],
                verbs=["get", "list", "watch"],
            ),
            dict(
                apiGroups=[""],
                resources=["services", "configmaps"],
                verbs=["create", "patch", "update"],
            ),
            dict(apiGroups=[""], resources=["pods/portforward"], verbs=["create"]),
            dict(
                apiGroups=["apps"],
                resources=["statefulsets", "statefulsets/scale"],
                verbs=["get", "list", "watch", "create", "patch", "update"],
            ),
            dict(
                apiGroups=["networking.k8s.io"],
                resources=["ingresses"],
                verbs=["get", "list", "create", "patch", "update"],
            ),
        ],
    )
    binding = dict(
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
    )
    workload = [
        dict(
            apiVersion="v1",
            kind="ConfigMap",
            metadata=dict(name="component-source", namespace=NAMESPACE, labels=labels),
            data={
                "server.py": (ROOT / "scripts/component_pilot_server.py").read_text()
            },
        ),
        dict(
            apiVersion="v1",
            kind="Service",
            metadata=dict(name="component", namespace=NAMESPACE, labels=labels),
            spec=dict(
                clusterIP="None",
                selector=labels,
                ports=[dict(name="http", port=8080), dict(name="evidence", port=9091)],
            ),
        ),
        dict(
            apiVersion="apps/v1",
            kind="StatefulSet",
            metadata=dict(name="component", namespace=NAMESPACE, labels=labels),
            spec=dict(
                serviceName="component",
                replicas=1,
                selector=dict(matchLabels=labels),
                template=dict(
                    metadata=dict(
                        labels=labels,
                        annotations={
                            "sidecar.istio.io/proxyCPU": "50m",
                            "sidecar.istio.io/proxyCPULimit": "500m",
                            "sidecar.istio.io/proxyMemory": "64Mi",
                            "sidecar.istio.io/proxyMemoryLimit": "128Mi",
                        },
                    ),
                    spec=dict(
                        nodeSelector={"kubernetes.io/hostname": "skube07"},
                        terminationGracePeriodSeconds=45,
                        containers=[
                            dict(
                                name="component",
                                image=common.IMAGE,
                                command=["python", "-u", "/experiment/server.py"],
                                ports=[
                                    dict(name="http", containerPort=8080),
                                    dict(name="evidence", containerPort=9091),
                                ],
                                resources=dict(
                                    requests={"cpu": "1", "memory": "128Mi"},
                                    limits={"cpu": "2", "memory": "512Mi"},
                                ),
                                volumeMounts=[
                                    dict(
                                        name="source",
                                        mountPath="/experiment",
                                        readOnly=True,
                                    )
                                ],
                                readinessProbe=dict(
                                    httpGet=dict(path="/health", port=8080),
                                    initialDelaySeconds=3,
                                    periodSeconds=3,
                                ),
                            )
                        ],
                        volumes=[
                            dict(name="source", configMap=dict(name="component-source"))
                        ],
                    ),
                ),
            ),
        ),
        dict(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            metadata=dict(name="component", namespace=NAMESPACE, labels=labels),
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
                                            name="component", port=dict(name="http")
                                        )
                                    ),
                                )
                            ]
                        ),
                    )
                ],
            ),
        ),
    ]
    for name, docs in (
        ("bootstrap.yaml", [ns, role, binding]),
        ("workload.yaml", workload),
    ):
        (destination / name).write_text(yaml.safe_dump_all(docs, sort_keys=False))
    write(destination / "protocol.json", PLAN)
    write(
        destination / "bundle-receipt.json",
        {
            name: digest(destination / name)
            for name in ("bootstrap.yaml", "workload.yaml", "protocol.json")
        },
    )


def arrivals(rate, process, rng):
    if process == "poisson":
        return rng.expovariate(rate)
    return rng.gammavariate(0.25, 4 / rate)


class Evidence:
    def __init__(self, output):
        self.output, self.process, self.log = output, None, None

    def __enter__(self):
        self.log = (self.output / "port-forward.log").open("w")
        self.process = subprocess.Popen(
            [
                "kubectl",
                "-n",
                NAMESPACE,
                "port-forward",
                "pod/component-0",
                "29591:9091",
                "--address=127.0.0.1",
            ],
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                self.status()
                return self
            except OSError:
                time.sleep(1)
        self.__exit__(None, None, None)
        raise RuntimeError("Component observer did not become reachable")

    def __exit__(self, *args):
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()

    def status(self):
        return http(EVIDENCE + "/status")

    def idle(self, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.status()
            if value["in_flight"] == 0 and value["background"] == 0:
                return
            time.sleep(0.25)
        raise RuntimeError("Old component requests or attempts did not drain")


def collect(
    output,
    observer,
    *,
    run_id,
    family,
    capacity,
    rate,
    process,
    seed,
    warmup,
    measurement,
):
    directory = output / "runs" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    observer.idle()
    http(EVIDENCE + "/reset", dict(family=family, capacity=capacity, seed=seed))
    rows, snapshots, lock, slots = (
        [],
        [],
        threading.Lock(),
        threading.BoundedSemaphore(256),
    )
    pending, halt = [0], threading.Event()
    start, epoch = time.monotonic(), time.time()
    measurement_end, deadline = (
        start + warmup + measurement,
        start + warmup + measurement + PLAN["maximum_drain_seconds"],
    )

    def monitor():
        while not halt.is_set():
            try:
                snapshots.append(
                    dict(
                        elapsed=time.monotonic() - start,
                        epoch=time.time(),
                        state=observer.status(),
                    )
                )
            except Exception as error:
                snapshots.append(
                    dict(
                        elapsed=time.monotonic() - start,
                        error_type=type(error).__name__,
                    )
                )
            halt.wait(1)

    def send(index, when, key, size, tagged):
        beginning = time.monotonic()
        row = dict(
            id=f"{run_id}:{index}",
            tagged=tagged,
            scheduled_seconds=when - start,
            started_seconds=beginning - start,
            scheduling_delay_seconds=beginning - when,
            key=key,
            size_bytes=size,
        )
        try:
            connection = HTTPConnection(common.INGRESS_IP, timeout=20)
            try:
                connection.request(
                    "POST",
                    "/work",
                    body=json.dumps(
                        dict(id=row["id"], key=key, size_bytes=size)
                    ).encode(),
                    headers={
                        "Host": HOST,
                        "Content-Type": "application/json",
                        "Connection": "close",
                    },
                )
                response = connection.getresponse()
                value = json.loads(response.read())
                row.update(
                    success=response.status == 200 and value.get("success") is True,
                    status=response.status,
                    server_response_seconds=value.get("response_seconds"),
                    error=value.get("error"),
                )
            finally:
                connection.close()
        except Exception as error:
            row.update(success=False, error_type=type(error).__name__)
        finally:
            row["latency_seconds"] = time.monotonic() - beginning
            with lock:
                rows.append(row)
                pending[0] -= int(tagged)
            slots.release()

    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
    rng, payload_rng = random.Random(seed), random.Random(seed + 1_000_000)
    when, index, timed_out = start, 0, False
    try:
        with ThreadPoolExecutor(max_workers=256) as executor:
            while True:
                current = time.monotonic()
                with lock:
                    remaining = pending[0]
                if current >= measurement_end and not remaining:
                    break
                if current > deadline:
                    timed_out = True
                    break
                candidate = when + arrivals(rate, process, rng)
                # Do not sleep past a completed cohort or beyond the drain budget.
                if (
                    candidate >= measurement_end
                    and not remaining
                    and time.monotonic() >= measurement_end
                ):
                    break
                when = candidate
                while time.monotonic() < min(when, deadline):
                    time.sleep(min(0.1, min(when, deadline) - time.monotonic()))
                    with lock:
                        done = time.monotonic() >= measurement_end and pending[0] == 0
                    if done:
                        break
                if time.monotonic() >= deadline:
                    timed_out = True
                    break
                with lock:
                    if time.monotonic() >= measurement_end and not pending[0]:
                        break
                index += 1
                tagged = start + warmup <= when < measurement_end
                key, size = payload_rng.choice(KEY_PROFILE), payload_rng.choice(
                    PLAN["link_payload_sizes_bytes"]
                )
                with lock:
                    pending[0] += int(tagged)
                if slots.acquire(blocking=False):
                    executor.submit(send, index, when, key, size, tagged)
                else:
                    with lock:
                        rows.append(
                            dict(
                                id=f"{run_id}:{index}",
                                tagged=tagged,
                                scheduled_seconds=when - start,
                                scheduling_delay_seconds=0,
                                latency_seconds=None,
                                success=False,
                                error="client_capacity_rejected",
                                key=key,
                                size_bytes=size,
                            )
                        )
                        pending[0] -= int(tagged)
        observer.idle()
    finally:
        halt.set()
        monitor_thread.join(timeout=5)
        with gzip.open(directory / "requests.jsonl.gz", "wt") as stream:
            for row in sorted(rows, key=lambda r: r["scheduled_seconds"]):
                stream.write(json.dumps(row) + "\n")
        events = http(EVIDENCE + "/events")
        with gzip.open(directory / "events.json.gz", "wt") as stream:
            json.dump(events, stream)
        write(directory / "occupancy.json", snapshots)
    cohort = [r for r in rows if r["tagged"]]
    complete = [r for r in cohort if r.get("latency_seconds") is not None]
    successful = [r for r in cohort if r["success"]]
    delays = [r["scheduling_delay_seconds"] for r in cohort]
    summary = dict(
        run_id=run_id,
        family=family,
        capacity=capacity,
        arrival_process=process,
        seed=seed,
        rate_rps=rate,
        started_epoch=epoch,
        measurement_start_epoch=epoch + warmup,
        measurement_end_epoch=epoch + warmup + measurement,
        count=len(cohort),
        failures=len(cohort) - len(successful),
        missing_service_boundaries=sum(
            r.get("server_response_seconds") is None for r in cohort
        ),
        mean_all_terminal_seconds=(
            statistics.fmean(r["latency_seconds"] for r in complete)
            if complete
            else None
        ),
        mean_success_seconds=(
            statistics.fmean(r["latency_seconds"] for r in successful)
            if successful
            else None
        ),
        maximum_scheduling_delay_seconds=max(delays, default=0),
        materially_late_fraction=(
            sum(d > 0.01 for d in delays) / len(delays) if delays else 1
        ),
        timed_out=timed_out,
        evidence_lost=events["lost"],
        final_state=observer.status(),
    )
    write(directory / "summary.json", summary)
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
            dict(
                completed=run_id,
                count=summary["count"],
                failures=summary["failures"],
                rate=rate,
            )
        ),
        flush=True,
    )
    return summary


def profile(output, family, capacity, runs):
    phases, residuals, observations = {}, [], []
    hits, lookups = 0, 0
    for run in runs:
        window_start = datetime.fromtimestamp(
            run["measurement_start_epoch"], timezone.utc
        ).isoformat()
        window_end = datetime.fromtimestamp(
            run["measurement_end_epoch"], timezone.utc
        ).isoformat()

        def observation(phase, seconds):
            observations.append(
                dict(
                    metric=phase,
                    component=family,
                    request_class="r",
                    window_start=window_start,
                    window_end=window_end,
                    value=max(seconds, 1e-9),
                    unit="seconds",
                    boundary="explicit_cohort_phase",
                    kind="sample",
                )
            )

        if (
            run["timed_out"]
            or run["evidence_lost"]
            or run["materially_late_fraction"] > 0.01
            or run.get("missing_service_boundaries", 0)
        ):
            raise ValueError("Characterization failed cohort/client/evidence gate")
        directory = output / "runs" / run["run_id"]
        with gzip.open(directory / "requests.jsonl.gz", "rt") as stream:
            rows = [json.loads(line) for line in stream]
        tagged = {r["id"] for r in rows if r["tagged"]}
        for r in rows:
            if r["id"] in tagged and r.get("server_response_seconds") is not None:
                difference = r["latency_seconds"] - r["server_response_seconds"]
                if difference < -1e-6:
                    raise ValueError(
                        "Client/service measurement boundaries are inconsistent"
                    )
                residuals.append(max(difference, 1e-9))
                observation("client_to_entry", max(difference, 1e-9))
        with gzip.open(directory / "events.json.gz", "rt") as stream:
            events = json.load(stream)["events"]
        for event in events:
            if event["request_id"] not in tagged:
                continue
            phase = event["phase"]
            if phase not in {"response", "worker_admission"}:
                phases.setdefault(phase, []).append(max(event["seconds"], 1e-9))
                observation(phase, event["seconds"])
            if phase == "cache_lookup":
                hits += int(event["hit"])
                lookups += 1
    required = {
        "broker": ["publish", "worker_work", "worker_block"],
        "pool": ["pool_hold"],
        "cache": ["cache_lookup", "cache_backend"],
        "link": ["link_work"],
        "external": ["api_delay"],
        "serverless": ["function_delay"],
    }[family]
    if len(residuals) < 50 or any(len(phases.get(p, [])) < 50 for p in required):
        raise ValueError(
            "Fewer than 50 measured phase/residual samples; no reliable profile"
        )
    result = dict(
        family=family,
        capacity=capacity,
        phases=phases,
        residuals=residuals,
        cache_hit_ratio=hits / lookups if lookups else None,
        source_runs=[r["run_id"] for r in runs],
        characterization_only=True,
        measurement_source=f"../profiles/{family}-c{capacity}-phase-observations.csv",
    )
    measurement_path = (
        output / "profiles" / f"{family}-c{capacity}-phase-observations.csv"
    )
    measurement_path.parent.mkdir(parents=True, exist_ok=True)
    with measurement_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=tuple(observations[0]))
        writer.writeheader()
        writer.writerows(observations)
    result["measurement_sha256"] = digest(measurement_path)
    write(output / "profiles" / f"{family}-c{capacity}.json", result)
    return result


def empirical(values):
    return dict(
        distribution="empirical", samples=list(values), sampling="iid_with_replacement"
    )


def reference(family, capacity, fitted=None):
    phases = fitted["phases"] if fitted else {}
    mean = lambda name, default: statistics.fmean(phases.get(name, [default]))
    return {
        "broker": capacity / mean("worker_work", 0.02),
        "pool": 2 * capacity / mean("pool_hold", 0.04),
        "cache": 4 / mean("cache_backend", 0.03),
        "link": 1 / mean("link_work", 163840 / (1024 * 1024 * capacity)),
        "external": 20 * capacity,
        "serverless": 2 * capacity / mean("function_delay", 0.02),
    }[family]


def model(fitted, rate, process, *, enriched):
    family, capacity, phases = fitted["family"], fitted["capacity"], fitted["phases"]

    def measured(phase):
        if fitted.get("measurement_source"):
            return dict(measurement=phase, unit="seconds")
        return empirical(
            fitted["residuals"] if phase == "client_to_entry" else phases[phase]
        )

    root = {
        "concurrency_weight": 1,
        "latency_boundary": "external_client",
        "latency_components": {"client_to_entry": measured("client_to_entry")},
    }
    nodes, services = {}, {}

    def stage(name, resource, phase, **extra):
        nodes[name] = dict(
            resource=resource,
            service_time=measured(phase),
            unit="occupied_resource_seconds",
            **extra,
        )

    def resource(name, **values):
        services[name] = dict(replica_queue_model="independent_round_robin", **values)

    if family == "broker":
        stage(
            "publish",
            "publisher",
            "publish",
            calls={"job": 1},
            call_policy={"completion": "background"},
        )
        stage(
            "job",
            "worker",
            "worker_work",
            non_cpu_blocking_time=measured("worker_block"),
        )
        resource("publisher", execution_model="delay")
        resource(
            "worker",
            concurrency_limit_per_replica=4,
            **(
                {"admission_queue": {"capacity_per_replica": 8, "timeout_seconds": 0.2}}
                if enriched
                else {}
            ),
        )
        root["completion_boundary"] = "all_work"
        entry, start = "publisher", "publish"
    elif family == "pool":
        stage("hold", "pool", "pool_hold")
        resource(
            "pool",
            execution_model="delay",
            **(
                {
                    "concurrency_limit_per_replica": 2 * capacity,
                    "admission_queue": {
                        "capacity_per_replica": 8,
                        "timeout_seconds": 0.2,
                    },
                }
                if enriched
                else {}
            ),
        )
        entry, start = "pool", "hold"
    elif family == "cache":
        stage("lookup", "cache", "cache_lookup", cache={"miss": "load"})
        stage("load", "backend", "cache_backend")
        cache = dict(
            mode="lru",
            capacity_bytes_per_replica=4096 * capacity,
            ttl_seconds=1,
            key_sizes_bytes={key: 1024 for key in set(KEY_PROFILE)},
            accesses_by_class={"r": dict(sampling="iid", keys=KEY_PROFILE)},
        )
        if not enriched:
            cache = dict(
                mode="fixed_hit_ratio",
                capacity_bytes_per_replica=4096 * capacity,
                observed_capacity_bytes_per_replica=4096 * capacity,
                observed_replicas=1,
                hit_ratio_by_class={"r": fitted["cache_hit_ratio"]},
            )
        resource("cache", execution_model="delay", cache=cache)
        services["cache"]["replica_queue_model"] = (
            "independent_key_hash" if enriched else "independent_round_robin"
        )
        resource("backend", execution_model="delay", concurrency_limit_per_replica=4)
        entry, start = "cache", "lookup"
    elif family == "link":
        stage("transfer", "link", "link_work")
        resource("link", execution_model="processor_sharing" if enriched else "delay")
        entry, start = "link", "transfer"
    elif family == "external":
        nodes["invoke"] = dict(
            calls={"quota": 1},
            retry=dict(
                max_attempts=3,
                attempt_timeout_seconds=0.06,
                backoff=dict(distribution="deterministic", mean=0.005),
            ),
        )
        nodes["quota"] = dict(
            resource="quota",
            service_time=dict(distribution="deterministic", mean=0.000001),
            next="attempt",
        )
        stage("attempt", "external_api", "api_delay")
        resource(
            "quota",
            execution_model="delay",
            **(
                {
                    "rate_limit": dict(
                        rate_per_second=20 * capacity, burst=2, action="reject"
                    )
                }
                if enriched
                else {}
            ),
        )
        resource(
            "external_api",
            execution_model="delay",
            concurrency_limit_per_replica=4,
            admission_queue=dict(capacity_per_replica=8, timeout_seconds=0.2),
            failure_probability=0.1,
        )
        entry, start = "quota", "invoke"
    else:
        stage("invoke", "function", "function_delay")
        # Startup is a declared fixture policy; measured startup spans are retained
        # as a cross-check, never fitted from target outcomes.
        resource(
            "function",
            execution_model="delay",
            concurrency_limit_per_replica=2,
            admission_queue=dict(capacity_per_replica=8, timeout_seconds=0.2),
            **(
                {
                    "lifecycle": dict(
                        startup_delay=dict(distribution="deterministic", mean=0.15),
                        idle_timeout_seconds=0.2,
                        initially_warm=False,
                    )
                }
                if enriched
                else {}
            ),
        )
        entry, start = "function", "invoke"
    root["stage_workflow"] = dict(entries={entry: start}, stages=nodes)
    distribution = (
        dict(distribution="exponential", rate=rate)
        if process == "poisson"
        else dict(distribution="gamma", rate=rate, shape=0.25)
    )
    replicas = {
        name: dict(fixed=capacity if name in {"worker", "function"} else 1)
        for name in services
    }
    result = dict(
        analytical_call_model="response_bounds",
        request_classes={"r": root},
        replica_constraints=replicas,
        traffic_model=dict(
            entry_services={"r": {entry: distribution}}, service_times=services
        ),
    )
    if fitted.get("measurement_source"):
        result["component_measurements"] = dict(
            version=1,
            sources={"phases": dict(format="csv", path=fitted["measurement_source"])},
            estimates={
                phase: dict(
                    operation="samples",
                    unit="seconds",
                    select=dict(
                        source="phases",
                        metric=phase,
                        component=family,
                        request_class="r",
                        unit="seconds",
                        boundary="explicit_cohort_phase",
                    ),
                )
                for phase in [*phases, "client_to_entry"]
            },
        )
    return result


def freeze_predictions(output, fitted):
    from performance_models import System, QueueingNetworkType
    from native_backend.backend import PROTOCOL_VERSION, SIMULATOR_SEMANTICS_VERSION

    family, capacity = fitted["family"], fitted["capacity"]
    anchor, predictions = reference(family, capacity, fitted), []
    for fraction in PLAN["reference_fractions"]:
        for process in PLAN["arrivals"]:
            cell = f"{family}-c{capacity}-{process}-u{round(fraction*100):02d}"
            for enriched in (False, True):
                variant = "enriched" if enriched else "mechanism-omitted"
                raw = model(fitted, anchor * fraction, process, enriched=enriched)
                path = output / "models" / f"{cell}-{variant}.json"
                write(path, raw)
                network = System(
                    cell, raw, QueueingNetworkType.OPEN, workload_base_path=path.parent
                )
                configuration = tuple(
                    network.get_replica_constraints()[name][0]
                    for name in network.get_services()
                )
                network.configure_system_replicas(configuration)
                row = dict(
                    cell=cell,
                    family=family,
                    capacity=capacity,
                    fraction=fraction,
                    process=process,
                    rate_rps=anchor * fraction,
                    variant=variant,
                    model=str(path.relative_to(output)),
                    model_sha256=digest(path),
                    native_protocol=PROTOCOL_VERSION,
                    native_semantics=SIMULATOR_SEMANTICS_VERSION,
                )
                reasons = network.get_strict_analytical_incompatibilities()
                if reasons:
                    row["analytical"] = dict(status="ABSTAINED", reasons=list(reasons))
                else:
                    try:
                        row["analytical"] = dict(
                            status="CONDITIONAL_BOUNDS",
                            bounds=network.get_request_class_latency_bounds(),
                        )
                    except ValueError as error:
                        row["analytical"] = dict(
                            status="ABSTAINED", reasons=[str(error)]
                        )
                try:
                    reps = network.simulate_measurement_replications(
                        warmup_duration=PLAN["target_warmup_seconds"],
                        measurement_duration=PLAN["target_measurement_seconds"],
                        maximum_drain_time=PLAN["maximum_drain_seconds"],
                        seeds=tuple(
                            PLAN["seed"] + 500_000 + i
                            for i in range(PLAN["simulation_replications"])
                        ),
                        workers=2,
                    )
                    values = [
                        statistics.fmean(r.request_class_latencies["r"])
                        for r in reps
                        if r.request_class_latencies["r"]
                    ]
                    rows = [x for r in reps for x in r.request_class_latencies["r"]]
                    failures = sum(r.failed_measured_request_counts["r"] for r in reps)
                    row["simulation"] = dict(
                        status="PREDICTED",
                        mean_seconds=statistics.fmean(rows),
                        mean_of_replications=statistics.fmean(values),
                        replication_means=values,
                        failure_rate=failures / len(rows),
                        count=len(rows),
                        seeds=[r.seed for r in reps],
                        certified_feasible=False,
                        finite_cohort_is_not_stability_proof=True,
                    )
                except Exception as error:
                    row["simulation"] = dict(
                        status="ABSTAINED", reason=f"{type(error).__name__}: {error}"
                    )
                predictions.append(row)
    return predictions


def score(output, predictions, targets):
    rows = []
    for target in targets:
        for prediction in predictions:
            if prediction["cell"] != target["cell"]:
                continue
            sim = prediction["simulation"]
            estimate, observed = (
                sim.get("mean_seconds"),
                target["mean_all_terminal_seconds"],
            )
            eligible = bool(
                target["count"] >= 50
                and not target["timed_out"]
                and not target["evidence_lost"]
                and target["materially_late_fraction"] <= 0.01
                and target["maximum_scheduling_delay_seconds"] <= 0.1
                and not target.get("missing_service_boundaries", 0)
            )
            actual_failure = (
                target["failures"] / target["count"] if target["count"] else None
            )
            pred_pass = (
                estimate is not None
                and estimate <= PLAN["mean_slo_seconds"]
                and sim.get("failure_rate", 1) <= PLAN["maximum_terminal_failure_rate"]
            )
            actual_pass = (
                observed is not None
                and observed <= PLAN["mean_slo_seconds"]
                and actual_failure is not None
                and actual_failure <= PLAN["maximum_terminal_failure_rate"]
            )
            rows.append(
                dict(
                    cell=prediction["cell"],
                    run_id=target["run_id"],
                    variant=prediction["variant"],
                    eligible=eligible,
                    predicted_mean_seconds=estimate,
                    observed_mean_seconds=observed,
                    ape_percent=(
                        100 * abs(estimate - observed) / observed
                        if eligible
                        and estimate is not None
                        and observed
                        and target["failures"] == 0
                        else None
                    ),
                    signed_error_percent=(
                        100 * (estimate - observed) / observed
                        if eligible
                        and estimate is not None
                        and observed
                        and target["failures"] == 0
                        else None
                    ),
                    observed_failure_rate=actual_failure,
                    predicted_failure_rate=sim.get("failure_rate"),
                    point_predicted_slo_pass=pred_pass,
                    point_observed_slo_pass=actual_pass,
                    point_unsafe=bool(eligible and pred_pass and not actual_pass),
                    safety_certificate=False,
                    analytical=prediction["analytical"],
                )
            )
    write(output / "scores.json", rows)
    lines = [
        "# cmp1 controlled component mechanism pilot",
        "",
        "Exploratory fixed-configuration results, not production recommendations or product validation.",
        "",
        "| Family/cell | Variant | Eligible failure-free latency scores | Mean APE (%) | Point-unsafe cases |",
        "|---|---|---:|---:|---:|",
    ]
    for family in FAMILIES:
        for variant in ("mechanism-omitted", "enriched"):
            selected = [
                r
                for r in rows
                if r["cell"].startswith(family + "-") and r["variant"] == variant
            ]
            apes = [r["ape_percent"] for r in selected if r["ape_percent"] is not None]
            lines.append(
                f"| {family} | {variant} | {len(apes)} | {statistics.fmean(apes):.2f} | {sum(r['point_unsafe'] for r in selected)} |"
                if apes
                else f"| {family} | {variant} | 0 | abstain | {sum(r['point_unsafe'] for r in selected)} |"
            )
    lines += [
        "",
        "Failures, abstentions and client-limited runs remain in scores/raw traces. Near-capacity fractions are characterization-based design labels, not measured CPU utilization. Two physical repetitions do not establish low unsafe-decision probability. Finite cohorts do not prove steady-state stability. Monte Carlo variability does not cover measurement/model uncertainty.",
        "",
    ]
    (output / "REPORT.md").write_text("\n".join(lines))


def wait_for_predecessor(output):
    deadline = time.monotonic() + 5 * 3600
    while time.monotonic() < deadline:
        state = command(
            "systemctl", "show", PREDECESSOR, "--property=ActiveState", "--value"
        ).strip()
        status = (
            json.loads((PREDECESSOR_ROOT / "status.json").read_text())
            if (PREDECESSOR_ROOT / "status.json").exists()
            else {}
        )
        pause = (
            json.loads((PREDECESSOR_ROOT / "comparison-pause.json").read_text())
            if (PREDECESSOR_ROOT / "comparison-pause.json").exists()
            else {}
        )
        if (
            state in ("inactive", "failed")
            and status.get("status") in ("COMPLETED", "FAILED")
            and not pause.get("cleanup_required", True)
        ):
            pods = json.loads(
                kube("-n", "kubesizer-minio-pilot-mio1", "get", "pods", "-o", "json")
            )["items"]
            if not pods:
                write(
                    output / "minio-predecessor.json",
                    dict(service_state=state, outcome=status, checked_at=now()),
                )
                return
        time.sleep(10)
    raise RuntimeError(
        "MinIO has not stopped and cleaned its pods; component loads not started"
    )


def finish(output):
    marker = output / "cleanup.json"
    if not marker.exists() or not json.loads(marker.read_text()).get("required"):
        return
    raw = kube(
        "-n",
        NAMESPACE,
        "get",
        "statefulset",
        "component",
        "--ignore-not-found",
        "-o",
        "json",
    )
    if raw.strip():
        if (
            json.loads(raw)["metadata"].get("labels", {}).get("kubesizer.campaign")
            != "cmp1"
        ):
            raise RuntimeError("Unexpected component owner; refusing cleanup")
        kube("-n", NAMESPACE, "scale", "statefulset", "component", "--replicas=0")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if not json.loads(kube("-n", NAMESPACE, "get", "pods", "-o", "json"))["items"]:
            write(
                marker,
                dict(required=False, cleaned_at=now(), comparison_left_stopped=True),
            )
            return
        time.sleep(3)
    raise RuntimeError("Component pilot pods remain after cleanup deadline")


def run(bundle, output):
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Use a fresh component output directory")
    output.mkdir(parents=True, exist_ok=True)
    if json.loads((bundle / "protocol.json").read_text()) != PLAN:
        raise ValueError("Component protocol differs from frozen bundle")
    for name, expected in json.loads(
        (bundle / "bundle-receipt.json").read_text()
    ).items():
        if digest(bundle / name) != expected:
            raise ValueError("Component deployment bundle changed")
    status = dict(
        status="QUEUED_AFTER_MINIO",
        started_at=now(),
        completed_targets=0,
        total_targets=144,
    )
    write(output / "status.json", status)
    write(output / "protocol.json", PLAN)
    source_paths = [
        ROOT / "scripts/run_component_pilot.py",
        ROOT / "scripts/component_pilot_server.py",
        ROOT / "scripts/run_stateful_pilot.py",
        ROOT / "native_backend/cpp/src/main.cpp",
        ROOT / "native_backend/cpp/build-release/kubesizer-simulator",
    ]
    for package in (
        "performance_models",
        "native_backend",
        "metrics",
        "calibration",
        "optimization",
    ):
        source_paths.extend((ROOT / package).glob("*.py"))
    source_receipt = {
        str(p.relative_to(ROOT)): digest(p) for p in sorted(set(source_paths))
    }
    write(output / "source-receipt.json", source_receipt)

    def verify_source():
        if any(
            digest(ROOT / name) != expected for name, expected in source_receipt.items()
        ):
            raise RuntimeError("Frozen component implementation changed")

    try:
        wait_for_predecessor(output)
        verify_source()
        if not HOLD.exists() or not json.loads(HOLD.read_text()).get("active"):
            raise RuntimeError("The explicit comparison hold is not active")
        if command(
            "systemctl", "show", common.COMPARISON, "--property=ActiveState", "--value"
        ).strip() not in ("inactive", "failed"):
            raise RuntimeError("Comparison campaign is not stopped")
        deployments = json.loads(kube("get", "deployments", "-A", "-o", "json"))[
            "items"
        ]
        if any(
            d["metadata"]["namespace"].endswith("-scv1")
            and d["spec"].get("replicas", 1)
            for d in deployments
        ):
            raise RuntimeError("Comparison workloads still active")
        write(output / "cleanup.json", dict(required=True))
        status.update(status="PROVISIONING", stage_started_at=now())
        write(output / "status.json", status)
        kube("apply", "-f", str(bundle / "workload.yaml"))
        kube(
            "-n",
            NAMESPACE,
            "rollout",
            "status",
            "statefulset/component",
            "--timeout=600s",
            timeout=620,
        )
        write(
            output / "deployment-context.json",
            json.loads(kube("-n", NAMESPACE, "get", "pods,statefulsets", "-o", "json")),
        )
        with Evidence(output) as observer:
            predictions = []
            for family in FAMILIES:
                for capacity in PLAN["capacities"]:
                    verify_source()
                    status.update(
                        status="CHARACTERIZATION",
                        family=family,
                        capacity=capacity,
                        stage_started_at=now(),
                    )
                    write(output / "status.json", status)
                    rate = min(4, 0.1 * reference(family, capacity))
                    runs = [
                        collect(
                            output,
                            observer,
                            run_id=f"characterization-{family}-c{capacity}-rep{rep}",
                            family=family,
                            capacity=capacity,
                            rate=rate,
                            process="poisson",
                            seed=PLAN["seed"]
                            + 100 * FAMILIES.index(family)
                            + 10 * capacity
                            + rep,
                            warmup=PLAN["characterization_warmup_seconds"],
                            measurement=PLAN["characterization_measurement_seconds"],
                        )
                        for rep in range(PLAN["characterization_repetitions"])
                    ]
                    fitted = profile(output, family, capacity, runs)
                    status.update(
                        status="FREEZING_PREDICTIONS",
                        family=family,
                        capacity=capacity,
                        stage_started_at=now(),
                    )
                    write(output / "status.json", status)
                    predictions.extend(freeze_predictions(output, fitted))
                    write(output / "predictions-progress.json", predictions)
            write(
                output / "predictions-frozen.json",
                dict(
                    frozen_at=now(), target_outcomes_used=False, predictions=predictions
                ),
            )
            schedule = [
                (f, c, p, u, r)
                for f in FAMILIES
                for c in PLAN["capacities"]
                for p in PLAN["arrivals"]
                for u in PLAN["reference_fractions"]
                for r in range(PLAN["repetitions"])
            ]
            random.Random(PLAN["seed"]).shuffle(schedule)
            write(output / "target-order-frozen.json", schedule)
            frozen = [
                output / "protocol.json",
                output / "source-receipt.json",
                output / "predictions-frozen.json",
                output / "target-order-frozen.json",
                *sorted((output / "profiles").glob("*")),
                *sorted((output / "models").glob("*.json")),
            ]
            receipt = {str(p.relative_to(output)): digest(p) for p in frozen}
            write(output / "prediction-receipt.json", receipt)
            targets = []
            for i, (family, capacity, process, fraction, rep) in enumerate(schedule):
                verify_source()
                if any(
                    digest(output / p) != expected for p, expected in receipt.items()
                ):
                    raise RuntimeError("Frozen component inputs/predictions changed")
                cell = f"{family}-c{capacity}-{process}-u{round(100*fraction):02d}"
                status.update(
                    status="TARGETS",
                    current_cell=cell,
                    current_repetition=rep,
                    completed_targets=len(targets),
                    stage_started_at=now(),
                )
                write(output / "status.json", status)
                rate = next(p["rate_rps"] for p in predictions if p["cell"] == cell)
                result = collect(
                    output,
                    observer,
                    run_id=f"{cell}-rep{rep}",
                    family=family,
                    capacity=capacity,
                    rate=rate,
                    process=process,
                    seed=PLAN["seed"] + 10_000 + i,
                    warmup=PLAN["target_warmup_seconds"],
                    measurement=PLAN["target_measurement_seconds"],
                )
                result["cell"] = cell
                targets.append(result)
                write(output / "target-results.json", targets)
                score(output, predictions, targets)
                time.sleep(PLAN["cooldown_seconds"])
            status.update(
                status="COMPLETED", completed_targets=len(targets), completed_at=now()
            )
    except BaseException as error:
        status.update(
            status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=now()
        )
        try:
            (output / "failure-component.log").write_text(
                kube(
                    "-n",
                    NAMESPACE,
                    "logs",
                    "component-0",
                    "-c",
                    "component",
                    "--tail=200",
                )
            )
        except Exception:
            pass
        raise
    finally:
        write(output / "status.json", status)
        finish(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", type=Path)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--finish", action="store_true")
    args = parser.parse_args()
    if args.render:
        render(args.render.resolve())
    elif args.finish:
        finish(args.output.resolve())
    else:
        signal.signal(
            signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt())
        )
        run(args.bundle.resolve(), args.output.resolve())

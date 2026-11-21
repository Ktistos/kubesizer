#!/usr/bin/env python3
"""Provision and run pgp1 without modifying the frozen comparison campaign."""

import argparse
from concurrent.futures import ThreadPoolExecutor
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
import tempfile
from urllib.request import Request, urlopen

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_stateful_pilot as common

NAMESPACE = "kubesizer-postgres-pilot-pgp1"
MEMBERS = ("primary", "standby1", "standby2")
HOST = "ks-postgres-pgp1.83.212.80.252.nip.io"
PG_IMAGE = (
    "postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24"
)
PLAN = dict(
    campaign="pgp1",
    database_image=PG_IMAGE,
    modes=["async", "quorum1", "all"],
    load_fractions=[0.25, 0.60, 0.90],
    repetitions=2,
    characterization_repetitions=2,
    characterization_rate_rps=12,
    warmup_seconds=60,
    measurement_seconds=240,
    cooldown_seconds=15,
    drain_seconds=90,
    maximum_rate_rps=200,
    read_share=0.75,
    first_standby_read_probability=0.75,
    witnesses=150,
    simulation_replications=20,
    seed=26111621,
    mean_slos_seconds={"read": 0.25, "write": 0.25},
    characterization_scope="CPU by SQL backend/startup process; low-load local SQL latency; replay witness upper proxy",
    within_class_cpu_variance_identified=False,
    wal_batching_modeled=False,
    replication_overlaps_local_commit_in_real_system=True,
    model_overlap_supported=False,
    coverage="one topology and query mix; exploratory",
)
write, digest, now, kube, command = (
    common.write,
    common.digest,
    common.now,
    common.kube,
    common.command,
)


def render(destination):
    destination.mkdir(parents=True, exist_ok=True)
    labels = {"kubesizer.campaign": "pgp1"}
    with tempfile.TemporaryDirectory() as temporary:
        common.render(Path(temporary))
        bootstrap = list(
            yaml.safe_load_all((Path(temporary) / "bootstrap.yaml").read_text())
        )
    for item in bootstrap:
        metadata = item["metadata"]
        if item["kind"] == "Namespace":
            metadata["name"] = NAMESPACE
            metadata["labels"] = {**labels, "istio-injection": "disabled"}
        else:
            metadata["namespace"] = NAMESPACE
    bootstrap[1]["rules"].extend(
        [
            dict(
                apiGroups=["apps"],
                resources=["deployments", "deployments/scale"],
                verbs=["get", "list", "watch", "create", "patch", "update"],
            ),
            dict(apiGroups=[""], resources=["secrets"], verbs=["get"]),
            dict(
                apiGroups=["networking.k8s.io"],
                resources=["networkpolicies"],
                verbs=["get", "create", "patch", "update"],
            ),
        ]
    )
    init = """#!/bin/sh
set -eu
printf '\\nhost replication replicator 0.0.0.0/0 scram-sha-256\\n' >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE replicator WITH REPLICATION LOGIN;
CREATE EXTENSION pg_stat_statements;
CREATE TABLE bench (id integer PRIMARY KEY, version bigint NOT NULL DEFAULT 0, payload text NOT NULL);
INSERT INTO bench (id,payload) SELECT i,repeat(md5(i::text),32) FROM generate_series(1,100000) AS i;
ANALYZE bench;
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v replpass="$POSTGRES_PASSWORD" <<'SQL'
ALTER ROLE replicator PASSWORD :'replpass';
SQL
"""
    workloads = [
        dict(
            apiVersion="v1",
            kind="ConfigMap",
            metadata=dict(name="pilot-source", namespace=NAMESPACE, labels=labels),
            data={
                "postgres_pilot_agent.py": (
                    ROOT / "scripts/postgres_pilot_agent.py"
                ).read_text(),
                "stateful_pilot_server.py": (
                    ROOT / "scripts/stateful_pilot_server.py"
                ).read_text(),
                "init.sh": init,
                "requirements.txt": "psycopg2-binary==2.9.11 --hash=sha256:fa0f693d3c68ae925966f0b14b8edda71696608039f4ed61b1fe9ffa468d16db\n",
            },
        )
    ]
    workloads.append(
        dict(
            apiVersion="networking.k8s.io/v1",
            kind="NetworkPolicy",
            metadata=dict(
                name="database-isolation", namespace=NAMESPACE, labels=labels
            ),
            spec=dict(
                podSelector=dict(
                    matchExpressions=[
                        dict(key="app", operator="In", values=list(MEMBERS))
                    ]
                ),
                policyTypes=["Ingress"],
                ingress=[{"from": [{"podSelector": {}}]}],
            ),
        )
    )
    secret_env = dict(
        name="PGPASSWORD",
        valueFrom=dict(secretKeyRef=dict(name="database-password", key="password")),
    )
    for member, node in zip(MEMBERS, ("skube07", "skube08", "skube09")):
        selector = {"app": member, **labels}
        workloads.append(
            dict(
                apiVersion="v1",
                kind="Service",
                metadata=dict(name=member, namespace=NAMESPACE, labels=labels),
                spec=dict(
                    clusterIP="None",
                    selector=selector,
                    ports=[
                        dict(name="postgres", port=5432),
                        dict(name="evidence", port=9092),
                    ],
                ),
            )
        )
        env = [
            dict(name="PGDATA", value="/var/lib/postgresql/data/pgdata"),
            dict(name="POSTGRES_DB", value="pilot"),
            dict(name="POSTGRES_USER", value="postgres"),
            dict(name="POSTGRES_PASSWORD", valueFrom=secret_env["valueFrom"]),
            secret_env,
        ]
        mounts = [
            dict(name="data", mountPath="/var/lib/postgresql/data"),
            dict(name="source", mountPath="/experiment", readOnly=True),
        ]
        args = [
            "postgres",
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=8",
            "-c",
            "max_connections=100",
            "-c",
            "shared_buffers=128MB",
            "-c",
            "shared_preload_libraries=pg_stat_statements",
            "-c",
            "track_io_timing=on",
            "-c",
            "track_wal_io_timing=on",
            "-c",
            "checkpoint_timeout=30min",
            "-c",
            "max_wal_size=512MB",
            "-c",
            "wal_keep_size=256MB",
            "-c",
            "synchronous_commit=local",
        ]
        if member == "primary":
            mounts.append(
                dict(
                    name="source",
                    mountPath="/docker-entrypoint-initdb.d",
                    readOnly=True,
                )
            )
        database = dict(
            name="database",
            image=PG_IMAGE,
            args=args,
            env=env,
            volumeMounts=mounts,
            ports=[dict(containerPort=5432, name="postgres")],
            resources=dict(
                requests={"cpu": "1", "memory": "384Mi"},
                limits={"cpu": "1", "memory": "768Mi"},
            ),
            readinessProbe=dict(
                exec=dict(command=["sh", "-c", "pg_isready -U postgres -d pilot"]),
                initialDelaySeconds=5,
                periodSeconds=5,
            ),
        )
        observer = dict(
            name="observer",
            image=common.IMAGE,
            command=["python", "/experiment/postgres_pilot_agent.py", "--observer"],
            volumeMounts=[dict(name="source", mountPath="/experiment", readOnly=True)],
            resources=dict(
                requests={"cpu": "25m", "memory": "32Mi"},
                limits={"cpu": "100m", "memory": "96Mi"},
            ),
        )
        pod = dict(
            shareProcessNamespace=True,
            nodeSelector={"kubernetes.io/hostname": node},
            securityContext=dict(fsGroup=70),
            containers=[database, observer],
            volumes=[
                dict(
                    name="source",
                    configMap=dict(name="pilot-source", defaultMode=0o555),
                )
            ],
        )
        if member != "primary":
            script = """set -eu
mkdir -p "$PGDATA"
chown 70:70 "$PGDATA"
if ! test -s "$PGDATA/PG_VERSION"; then
  until pg_isready -h primary -U postgres -d pilot; do sleep 2; done
  gosu postgres pg_basebackup -h primary -U replicator -D "$PGDATA" -R -X stream --checkpoint=fast
  printf "\\nprimary_conninfo = 'host=primary port=5432 user=replicator password=%s application_name=%s'\\n" "$PGPASSWORD" "$MEMBER" >> "$PGDATA/postgresql.auto.conf"
fi
"""
            pod["initContainers"] = [
                dict(
                    name="basebackup",
                    image=PG_IMAGE,
                    command=["sh", "-c", script],
                    env=env + [dict(name="MEMBER", value=member)],
                    volumeMounts=mounts,
                )
            ]
        workloads.append(
            dict(
                apiVersion="apps/v1",
                kind="StatefulSet",
                metadata=dict(name=member, namespace=NAMESPACE, labels=labels),
                spec=dict(
                    serviceName=member,
                    replicas=1,
                    selector=dict(matchLabels=selector),
                    template=dict(metadata=dict(labels=selector), spec=pod),
                    volumeClaimTemplates=[
                        dict(
                            metadata=dict(name="data"),
                            spec=dict(
                                accessModes=["ReadWriteOnce"],
                                storageClassName="longhorn",
                                resources=dict(requests={"storage": "2Gi"}),
                            ),
                        )
                    ],
                ),
            )
        )
    selector = {"app": "gateway", **labels}
    workloads.append(
        dict(
            apiVersion="v1",
            kind="Service",
            metadata=dict(name="gateway", namespace=NAMESPACE, labels=labels),
            spec=dict(
                selector=selector,
                ports=[dict(name="http", port=8080), dict(name="evidence", port=9091)],
            ),
        )
    )
    gateway = dict(
        name="application",
        image=common.IMAGE,
        command=["python", "/experiment/postgres_pilot_agent.py"],
        env=[secret_env, dict(name="PYTHONPATH", value="/deps:/experiment")],
        volumeMounts=[
            dict(name="source", mountPath="/experiment", readOnly=True),
            dict(name="deps", mountPath="/deps"),
        ],
        resources=dict(
            requests={"cpu": "500m", "memory": "256Mi"},
            limits={"cpu": "2", "memory": "768Mi"},
        ),
        readinessProbe=dict(
            httpGet=dict(path="/health", port=8080),
            initialDelaySeconds=5,
            periodSeconds=5,
        ),
    )
    workloads.append(
        dict(
            apiVersion="apps/v1",
            kind="Deployment",
            metadata=dict(name="gateway", namespace=NAMESPACE, labels=labels),
            spec=dict(
                replicas=1,
                selector=dict(matchLabels=selector),
                template=dict(
                    metadata=dict(labels=selector),
                    spec=dict(
                        nodeSelector={"kubernetes.io/hostname": "skube07"},
                        initContainers=[
                            dict(
                                name="driver",
                                image=common.IMAGE,
                                command=[
                                    "python",
                                    "-m",
                                    "pip",
                                    "install",
                                    "--disable-pip-version-check",
                                    "--no-cache-dir",
                                    "--only-binary=:all:",
                                    "--require-hashes",
                                    "--target=/deps",
                                    "-r",
                                    "/experiment/requirements.txt",
                                ],
                                volumeMounts=gateway["volumeMounts"],
                            )
                        ],
                        containers=[gateway],
                        volumes=[
                            dict(name="source", configMap=dict(name="pilot-source")),
                            dict(name="deps", emptyDir={}),
                        ],
                    ),
                ),
            ),
        )
    )
    workloads.append(
        dict(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            metadata=dict(name="gateway", namespace=NAMESPACE, labels=labels),
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
                                            name="gateway", port=dict(name="http")
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
    for name, documents in (
        ("bootstrap.yaml", bootstrap),
        ("workload.yaml", workloads),
    ):
        (destination / name).write_text(yaml.safe_dump_all(documents, sort_keys=False))
    write(destination / "protocol.json", PLAN)
    write(
        destination / "bundle-receipt.json",
        {
            p.name: digest(p)
            for p in destination.iterdir()
            if p.name in ("bootstrap.yaml", "workload.yaml", "protocol.json")
        },
    )


def api(path, data=None, *, timeout=30):
    request = Request(
        "http://127.0.0.1:29291" + path,
        data=None if data is None else json.dumps(data).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def collect(
    output, run_id, mode, rate, seed, *, warmup=60, measurement=240,
    client_address=None, expected_standbys=2, read_share=None,
):
    if read_share is None:
        read_share = PLAN["read_share"]
    directory = output / "runs" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    api("/configure", {"mode": mode}, timeout=60)
    time.sleep(2)
    snapshots = [dict(elapsed=0, **api("/snapshot"))]
    traces, lock, pending = [], threading.Lock(), [0]
    permits = threading.BoundedSemaphore(512)
    monitor_stop = threading.Event()
    start, epoch = time.monotonic(), time.time()
    end = start + warmup + measurement
    timed_out = False

    def monitor():
        while not monitor_stop.wait(3):
            try:
                snapshots.append(
                    dict(elapsed=time.monotonic() - start, **api("/snapshot"))
                )
            except Exception as error:
                snapshots.append(
                    dict(elapsed=time.monotonic() - start, error=str(error))
                )

    def send(index, scheduled, c, tagged):
        begin = time.monotonic()
        row = dict(
            id=f"{run_id}:{index}",
            request_class=c,
            tagged=tagged,
            scheduling_delay_seconds=begin - scheduled,
            scheduled_seconds=scheduled - start,
        )
        try:
            address = client_address or (common.INGRESS_IP,)
            connection = HTTPConnection(*address, timeout=30)
            try:
                # Independent deterministic key stream, unrelated to class/arrival RNG.
                key = random.Random(seed * 1_000_003 + index).randrange(98_000)
                connection.request(
                    "POST",
                    "/work",
                    body=json.dumps(
                        dict(id=row["id"], key=key, mode=mode, request_class=c)
                    ),
                    headers={"Host": HOST, "Content-Type": "application/json"},
                )
                response = connection.getresponse()
                body = json.loads(response.read())
                row.update(
                    success=response.status == 200 and body.get("success") is True,
                    status=response.status,
                    server_response_seconds=body.get("response_seconds"),
                    member=body.get("member"),
                )
                if not row["success"]:
                    row["error"] = body.get("error")
            finally:
                connection.close()
        except Exception as error:
            row.update(success=False, error=f"{type(error).__name__}: {error}")
        finally:
            row["latency_seconds"] = time.monotonic() - begin
            with lock:
                traces.append(row)
                pending[0] -= int(tagged)
            permits.release()

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    arrivals, classes = random.Random(seed), random.Random(seed + 1_000_000)
    next_time, index = start, 0
    try:
        with ThreadPoolExecutor(max_workers=128) as pool:
            while True:
                current = time.monotonic()
                with lock:
                    left = pending[0]
                if current >= end and left == 0:
                    break
                if current > end + PLAN["drain_seconds"]:
                    timed_out = True
                    break
                next_time += arrivals.expovariate(rate)
                if client_address is not None and next_time >= end:
                    break
                time.sleep(max(0, next_time - time.monotonic()))
                if next_time >= end and left == 0:
                    break
                index += 1
                tagged = start + warmup <= next_time < end
                c = "read" if classes.random() < read_share else "write"
                with lock:
                    pending[0] += int(tagged)
                if permits.acquire(blocking=False):
                    pool.submit(send, index, next_time, c, tagged)
                else:
                    with lock:
                        traces.append(
                            dict(
                                id=f"{run_id}:{index}",
                                request_class=c,
                                tagged=tagged,
                                success=False,
                                error="client_capacity_rejected",
                                latency_seconds=0,
                                scheduling_delay_seconds=0,
                            )
                        )
                        pending[0] -= int(tagged)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            snapshot = api("/snapshot")
            if (
                not snapshot["counters"].get("in_flight", 0)
                and len(snapshot["replication"]) == expected_standbys
                and all(
                    r[1] == "streaming" and r[3] is not None and r[3] <= 0
                    for r in snapshot["replication"]
                )
            ):
                break
            time.sleep(1)
        else:
            timed_out = True
        snapshots.append(dict(elapsed=time.monotonic() - start, **api("/snapshot")))
    finally:
        monitor_stop.set()
        thread.join(timeout=35)
        with gzip.open(directory / "requests.jsonl.gz", "wt") as stream:
            for row in traces:
                stream.write(json.dumps(row) + "\n")
        write(directory / "snapshots.json", snapshots)
        with gzip.open(directory / "events.json.gz", "wt") as stream:
            json.dump(api("/events"), stream)
    result = dict(
        run_id=run_id,
        mode=mode,
        rate_rps=rate,
        seed=seed,
        started_epoch=epoch,
        warmup_seconds=warmup,
        measurement_seconds=measurement,
        timed_out=timed_out,
        classes={},
        snapshot_errors=sum("error" in s for s in snapshots),
    )
    for c in ("read", "write"):
        rows = [r for r in traces if r["tagged"] and r["request_class"] == c]
        success = [r for r in rows if r["success"]]
        result["classes"][c] = dict(
            count=len(rows),
            completed=len(success),
            failures=len(rows) - len(success),
            mean_seconds=(
                statistics.fmean(r["latency_seconds"] for r in success)
                if success
                else None
            ),
            maximum_scheduling_delay_seconds=max(
                (r["scheduling_delay_seconds"] for r in rows), default=0
            ),
        )
    write(directory / "summary.json", result)
    write(
        directory / "receipt.json",
        {
            p.name: digest(p)
            for p in directory.iterdir()
            if p.is_file() and p.name != "receipt.json"
        },
    )
    print(json.dumps(dict(completed=run_id, classes=result["classes"])), flush=True)
    return result


def cpu_deltas(first, last):
    old = {
        (n, p["pid"], p["start_ticks"]): p["cpu_seconds"]
        for n, m in first["members"].items()
        for p in m["processes"]
    }
    result = {n: dict(query=0.0, replay=0.0, other=0.0) for n in MEMBERS}
    for n, m in last["members"].items():
        for p in m["processes"]:
            key = n, p["pid"], p["start_ticks"]
            if key not in old:
                continue
            category = (
                "query"
                if p["pid"] in last["pids"][n]
                else "replay" if "startup" in p["command"] else "other"
            )
            result[n][category] += max(0, p["cpu_seconds"] - old[key])
    return result


def characterize(output, runs, witness, *, members=MEMBERS):
    local = {n: [] for n in members}
    residual = {c: [] for c in ("read", "write")}
    cpu = {n: [] for n in members}
    replay = {n: [] for n in members[1:]}
    for run in runs:
        if (
            run["timed_out"]
            or run["snapshot_errors"]
            or any(c["failures"] for c in run["classes"].values())
        ):
            raise ValueError("Incomplete PostgreSQL characterization")
        directory = output / "runs" / run["run_id"]
        snapshots = json.loads((directory / "snapshots.json").read_text())
        window = [
            s
            for s in snapshots
            if run["warmup_seconds"]
            <= s["elapsed"]
            < run["warmup_seconds"] + run["measurement_seconds"]
            and "members" in s
        ]
        if len(window) < 2:
            raise ValueError("Insufficient CPU snapshots")
        deltas = cpu_deltas(window[0], window[-1])
        with gzip.open(directory / "requests.jsonl.gz", "rt") as stream:
            rows = [json.loads(line) for line in stream]
        counts = {n: 0 for n in members}
        for row in rows:
            if (
                row.get("member")
                and window[0]["elapsed"]
                <= row.get("scheduled_seconds", -1)
                < window[-1]["elapsed"]
            ):
                counts[row["member"]] += 1
            if row["tagged"] and row["success"]:
                local[row["member"]].append(row["server_response_seconds"])
                residual[row["request_class"]].append(
                    max(0, row["latency_seconds"] - row["server_response_seconds"])
                )
        for n in members:
            if not counts[n] or deltas[n]["query"] <= 0:
                raise ValueError(f"CPU demand not identifiable for {n}")
            cpu[n].append(deltas[n]["query"] / counts[n])
            if n != "primary":
                if deltas[n]["replay"] <= 0:
                    raise ValueError(
                        f"Replay CPU below process-counter resolution for {n}"
                    )
                replay[n].append(deltas[n]["replay"] / counts["primary"])
    profile = dict(
        local_samples_seconds=local,
        cpu_demands_seconds={n: statistics.fmean(v) for n, v in cpu.items()},
        replay_cpu_demands_seconds={n: statistics.fmean(v) for n, v in replay.items()},
        residual_seconds=residual,
        replay_witness=witness,
        sources=[r["run_id"] for r in runs],
        cpu_variance_identified=False,
        witness_is_upper_proxy=True,
    )
    write(output / "characterization.json", profile)
    return profile


def model(
    profile, mode, rate, enriched, *, cpu_bandwidths=None, read_weights=None,
    include_replay_proxy=True,
):
    empirical = common.empirical
    cpu = profile["cpu_demands_seconds"]
    replay = profile["replay_cpu_demands_seconds"]
    services = {
        "router": dict(
            distribution="deterministic",
            mean=1e-9,
            execution_model="processor_sharing",
            replica_queue_model="pooled",
        )
    }
    if read_weights is None:
        read_weights = {"standby1": 0.75, "standby2": 0.25}
    for n in ("primary", *read_weights):
        bandwidth = (cpu_bandwidths or {}).get(n)
        cores = (
            bandwidth["quota_seconds"] / bandwidth["period_seconds"]
            if bandwidth
            else 1.0
        )
        classes = {
            "write": dict(
                distribution="deterministic",
                mean=(cpu[n] if n == "primary" else replay[n]) / cores,
            )
        }
        if n != "primary":
            classes["read"] = dict(distribution="deterministic", mean=cpu[n] / cores)
        # Residual SQL latency is identified at low load, not a target adjustment.
        local = profile["local_samples_seconds"][n]
        demand = cpu[n]
        residual_mean = max(0, statistics.fmean(local) - demand)
        blocking = {}
        if residual_mean > 1e-8:
            positive = [max(0, x - demand) for x in local]
            scale = residual_mean / statistics.fmean(positive)
            blocking["write" if n == "primary" else "read"] = empirical(
                [max(1e-9, x * scale) for x in positive]
            )
        services[n] = dict(
            execution_model="processor_sharing",
            replica_queue_model="pooled",
            concurrency_limit_per_replica=16 if n == "primary" else 17,
            **classes,
        )
        if bandwidth:
            services[n].update(
                replica_queue_model="independent_random", cpu_bandwidth=dict(bandwidth)
            )
        if blocking:
            services[n]["non_cpu_blocking_time"] = blocking
        if n != "primary" and include_replay_proxy:
            samples = [
                r["standbys"][n]["seconds_since_local_commit"]
                for r in profile["replay_witness"]["samples"]
            ]
            mean = statistics.fmean(samples) - replay[n]
            if mean <= 0:
                raise ValueError("Replay witness cannot be separated from CPU work")
            services[n + "_ack"] = dict(
                processing_slots_per_replica=128,
                replica_queue_model="pooled",
                **empirical([x * mean / statistics.fmean(samples) for x in samples]),
            )
    classes = {
        "read": dict(
            concurrency_weight=0.75,
            calls={"router": read_weights},
            call_execution="parallel",
        ),
        "write": dict(
            concurrency_weight=0.25,
            calls={
                "primary": {n: 1 for n in read_weights},
                **({n: {n + "_ack": 1} for n in read_weights} if include_replay_proxy else {}),
            },
            call_execution="parallel",
        ),
    }
    if enriched:
        classes["read"]["call_policies"] = {"router": {"execution": "choice"}}
        classes["write"]["call_policies"] = {
            "primary": (
                {"completion": "background"}
                if mode == "async"
                else (
                    {"completion": "quorum", "required": 1}
                    if mode == "quorum1"
                    else {"completion": "all"}
                )
            )
        }
    for c in classes:
        classes[c].update(
            latency_boundary="external_client",
            latency_components={
                "client_to_entry": empirical(profile["residual_seconds"][c])
            },
            end_to_end_latency_slos={"mean_seconds": PLAN["mean_slos_seconds"][c]},
        )
    return dict(
        analytical_call_model="response_bounds",
        request_classes=classes,
        replica_constraints={n: {"fixed": 1} for n in services},
        traffic_model=dict(
            entry_services={
                c: {
                    "router" if c == "read" else "primary": dict(
                        distribution="exponential", rate=rate * share
                    )
                }
                for c, share in (("read", 0.75), ("write", 0.25))
            },
            service_times=services,
        ),
    )


def freeze(output, profile):
    from performance_models import System, QueueingNetworkType

    coefficients = {"primary": 0.25 * profile["cpu_demands_seconds"]["primary"]}
    for n, p in (("standby1", 0.75), ("standby2", 0.25)):
        coefficients[n] = (
            0.75 * p * profile["cpu_demands_seconds"][n]
            + 0.25 * profile["replay_cpu_demands_seconds"][n]
        )
    reference = 1 / max(coefficients.values())
    anchor = min(reference, PLAN["maximum_rate_rps"])
    predictions = []
    for mode in PLAN["modes"]:
        for fraction in PLAN["load_fractions"]:
            for enriched in (False, True):
                variant = "enriched" if enriched else "legacy-call-rules"
                cell = f"{mode}-u{round(100*fraction):02d}"
                row = dict(
                    cell=cell,
                    mode=mode,
                    load_fraction=fraction,
                    rate_rps=anchor * fraction,
                    variant=variant,
                )
                try:
                    frozen = model(profile, mode, anchor * fraction, enriched)
                    path = output / "models" / f"{cell}-{variant}.json"
                    write(path, frozen)
                    network = System(cell, frozen, QueueingNetworkType.OPEN)
                    reasons = network.get_strict_analytical_incompatibilities()
                    row["analytical"] = (
                        dict(status="ABSTAINED", reasons=list(reasons))
                        if reasons
                        else dict(
                            status="CONDITIONAL_BOUNDS",
                            bounds=network.get_request_class_latency_bounds(),
                        )
                    )
                    replications = network.simulate_measurement_replications(
                        warmup_duration=60,
                        measurement_duration=240,
                        seeds=tuple(
                            8_000_000 + 1000 * len(predictions) + i for i in range(20)
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
                predictions.append(row)
                print(
                    json.dumps(
                        dict(
                            frozen_prediction=cell,
                            variant=variant,
                            status=row["simulation"]["status"],
                        )
                    ),
                    flush=True,
                )
    result = dict(
        target_outcomes_used=False,
        frozen_at=now(),
        cpu_reference_rps=reference,
        anchor_rps=anchor,
        anchor_capped=anchor < reference,
        work_coefficients=coefficients,
        predictions=predictions,
    )
    write(output / "predictions-frozen.json", result)
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
    return result


def report(output, frozen, runs):
    lines = [
        "# PostgreSQL pilot pgp1",
        "",
        "Exploratory fixed-configuration prediction test on real PostgreSQL, "
        "one primary and two physical streaming standbys. Inputs and predictions were frozen before targets.",
        "",
        "CPU demands are process-counter means; within-class CPU variance is not identified. "
        "Replay acknowledgement samples are upper proxies including polling and marker-query overhead. "
        "The model serializes local completion before child replication, whereas PostgreSQL can overlap "
        "these phases. WAL batching, storage contention and serial replay ordering are not represented "
        "fully. These limitations must accompany any error or decision count.",
        "",
        f"CPU reference {frozen['cpu_reference_rps']:.2f} requests/s; effective design anchor "
        f"{frozen['anchor_rps']:.2f} requests/s; safety rate cap applied: {frozen['anchor_capped']}. "
        "Load labels are design fractions, not measured utilization.",
        "",
        "| Mode | Fraction | Class | Measured ms | Legacy ms | New ms | Eligible |",
        "|---|---:|---|---:|---:|---:|---|",
    ]
    scores = []
    for mode in PLAN["modes"]:
        for fraction in PLAN["load_fractions"]:
            cell = f"{mode}-u{round(100*fraction):02d}"
            observations = [r for r in runs if r["cell"] == cell]
            for c in ("read", "write"):
                count = sum(r["classes"][c]["completed"] for r in observations)
                mean = (
                    sum(
                        r["classes"][c]["mean_seconds"] * r["classes"][c]["completed"]
                        for r in observations
                        if r["classes"][c]["mean_seconds"] is not None
                    )
                    / count
                    if count
                    else None
                )
                eligible = len(observations) == 2 and all(
                    not r["timed_out"]
                    and not r["snapshot_errors"]
                    and not r["classes"][c]["failures"]
                    and r["classes"][c]["maximum_scheduling_delay_seconds"] < 0.05
                    for r in observations
                )
                row = dict(
                    cell=cell,
                    request_class=c,
                    observed_mean_seconds=mean,
                    eligible=eligible,
                )
                for variant in ("legacy-call-rules", "enriched"):
                    item = next(
                        p
                        for p in frozen["predictions"]
                        if p["cell"] == cell and p["variant"] == variant
                    )
                    estimate = (
                        item["simulation"]
                        .get("classes", {})
                        .get(c, {})
                        .get("mean_seconds")
                    )
                    row[variant] = dict(
                        mean_seconds=estimate,
                        ape_percent=(
                            100 * abs(estimate - mean) / mean
                            if estimate is not None and mean
                            else None
                        ),
                        analytical=item.get("analytical"),
                        diagnostic_mean_slo_pass=(
                            estimate <= PLAN["mean_slos_seconds"][c]
                            if estimate is not None
                            else None
                        ),
                        diagnostic_mean_unsafe=(
                            eligible
                            and estimate is not None
                            and mean is not None
                            and estimate <= PLAN["mean_slos_seconds"][c] < mean
                        ),
                    )
                scores.append(row)

                def fmt(value):
                    return f"{1000*value:.2f}" if value is not None else "abstain"

                lines.append(
                    f"| {mode} | {fraction} | {c} | {fmt(mean)} | "
                    f"{fmt(row['legacy-call-rules']['mean_seconds'])} | {fmt(row['enriched']['mean_seconds'])} | {eligible} |"
                )
    lines.extend(
        [
            "",
            "Failed and incomplete runs remain in raw data and outside eligible latency error. "
            "Replication LSN backlog and wait-event trajectories are retained in snapshots. "
            "Cohort completion is not a steady-state stability certificate. Point-mean SLO "
            "comparisons are diagnostic, not validated recommendations. Analytical abstentions "
            "are not replaced by relaxed point predictions. Two physical repeats cannot establish "
            "a low unsafe-decision probability or generalize to other database layouts.",
            "",
        ]
    )
    (output / "REPORT.md").write_text("\n".join(lines))
    write(output / "scores.json", scores)


def finish(output):
    marker = output / "comparison-pause.json"
    if not marker.exists():
        return
    pause = json.loads(marker.read_text())
    if not pause.get("resume_required"):
        return
    for kind, names in (("statefulset", MEMBERS), ("deployment", ("gateway",))):
        for name in names:
            value = json.loads(
                kube(
                    "-n",
                    NAMESPACE,
                    "get",
                    kind,
                    name,
                    "--ignore-not-found",
                    "-o",
                    "json",
                )
                or "null"
            )
            if value is None:
                continue
            if value["metadata"].get("labels", {}).get("kubesizer.campaign") != "pgp1":
                raise RuntimeError("Unexpected pilot resource ownership")
            kube("-n", NAMESPACE, "scale", kind, name, "--replicas=0")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        pods = json.loads(kube("-n", NAMESPACE, "get", "pods", "-o", "json"))["items"]
        if not pods:
            command("systemctl", "start", common.COMPARISON)
            pause.update(resume_required=False, resumed_at=now())
            write(marker, pause)
            return
        time.sleep(3)
    raise RuntimeError("PostgreSQL pilot pods remain; comparison restart refused")


def run(bundle, output):
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Use a new output directory")
    output.mkdir(parents=True, exist_ok=True)
    if json.loads((bundle / "protocol.json").read_text()) != PLAN:
        raise ValueError("Pilot protocol changed")
    for name, sha in json.loads((bundle / "bundle-receipt.json").read_text()).items():
        if digest(bundle / name) != sha:
            raise ValueError("Pilot bundle changed")
    status = dict(status="WAITING_FOR_COMPARISON_BOUNDARY", started_at=now())
    write(output / "status.json", status)
    write(output / "protocol.json", PLAN)
    write(
        output / "source-receipt.json",
        {
            str(p.relative_to(ROOT)): digest(p)
            for p in [
                ROOT / "scripts/run_postgres_pilot.py",
                ROOT / "scripts/postgres_pilot_agent.py",
                ROOT / "scripts/run_stateful_pilot.py",
                ROOT / "native_backend/backend.py",
                ROOT / "native_backend/cpp/src/main.cpp",
                ROOT / "native_backend/cpp/build-release/kubesizer-simulator",
            ]
        },
    )
    forward = None
    log = None
    try:
        common.wait_and_pause(output)
        active = json.loads(kube("get", "deployments", "-A", "-o", "json"))["items"]
        if any(
            d["metadata"]["namespace"].endswith("-scv1")
            and d["spec"].get("replicas", 1)
            for d in active
        ):
            raise RuntimeError("Comparison replicas not idle")
        status.update(status="PROVISIONING", stage_started_at=now())
        write(output / "status.json", status)
        kube("apply", "-f", str(bundle / "workload.yaml"))
        for n in MEMBERS:
            kube(
                "-n",
                NAMESPACE,
                "rollout",
                "status",
                f"statefulset/{n}",
                "--timeout=900s",
                timeout=920,
            )
        kube(
            "-n",
            NAMESPACE,
            "rollout",
            "status",
            "deployment/gateway",
            "--timeout=900s",
            timeout=920,
        )
        write(
            output / "deployment-context.json",
            json.loads(
                kube(
                    "-n",
                    NAMESPACE,
                    "get",
                    "statefulsets,deployments,pods,persistentvolumeclaims",
                    "-o",
                    "json",
                )
            ),
        )
        log = (output / "port-forward.log").open("w")
        forward = subprocess.Popen(
            [
                "kubectl",
                "-n",
                NAMESPACE,
                "port-forward",
                "service/gateway",
                "29291:9091",
                "--address=127.0.0.1",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 60
        while True:
            try:
                snapshot = api("/snapshot")
                if len(snapshot["replication"]) == 2 and all(
                    r[1] == "streaming" for r in snapshot["replication"]
                ):
                    break
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise RuntimeError("Both streaming standbys did not become observable")
            time.sleep(1)
        status.update(status="CHARACTERIZATION", stage_started_at=now())
        write(output / "status.json", status)
        runs = [
            collect(
                output,
                f"characterization-{i}",
                "async",
                PLAN["characterization_rate_rps"],
                PLAN["seed"] + i,
                warmup=90,
                measurement=240,
            )
            for i in range(2)
        ]
        witness = api("/witness", {"count": PLAN["witnesses"]}, timeout=600)
        write(output / "replay-witness.json", witness)
        try:
            profile = characterize(output, runs, witness)
            status.update(status="FREEZING_PREDICTIONS", stage_started_at=now())
            write(output / "status.json", status)
            predictions = freeze(output, profile)
        except ValueError as error:
            # Retain measurements without fabricating unobservable phase inputs.
            write(
                output / "characterization-abstention.json",
                dict(reason=str(error), target_outcomes_used=False),
            )
            raise
        schedule = [
            (m, u, r)
            for m in PLAN["modes"]
            for u in PLAN["load_fractions"]
            for r in range(2)
        ]
        random.Random(PLAN["seed"]).shuffle(schedule)
        write(output / "target-order-frozen.json", schedule)
        targets = []
        for i, (mode, fraction, rep) in enumerate(schedule):
            cell = f"{mode}-u{round(100*fraction):02d}"
            status.update(
                status="TARGETS",
                completed_targets=len(targets),
                total_targets=18,
                current_cell=cell,
                current_repetition=rep,
                stage_started_at=now(),
            )
            write(output / "status.json", status)
            for name, sha in json.loads(
                (output / "prediction-receipt.json").read_text()
            ).items():
                if digest(output / name) != sha:
                    raise RuntimeError("Frozen PostgreSQL input changed")
            result = collect(
                output,
                f"{cell}-rep{rep}",
                mode,
                predictions["anchor_rps"] * fraction,
                PLAN["seed"] + 100 + i,
            )
            result.update(cell=cell, load_fraction=fraction)
            targets.append(result)
            write(output / "target-results.json", targets)
            report(output, predictions, targets)
            time.sleep(PLAN["cooldown_seconds"])
        status.update(status="COMPLETED", completed_targets=18, completed_at=now())
    except BaseException as error:
        status.update(
            status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=now()
        )
        raise
    finally:
        write(output / "status.json", status)
        if forward is not None:
            forward.terminate()
            try:
                forward.wait(timeout=15)
            except subprocess.TimeoutExpired:
                forward.kill()
                forward.wait()
        if log is not None:
            log.close()
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

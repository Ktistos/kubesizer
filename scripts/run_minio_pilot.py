#!/usr/bin/env python3
"""Queue a real distributed-MinIO measurement/semantics pilot after pgp1.

MinIO's multi-round drive protocol is not replaced with the repository's toy
one-round quorum. Data are retained for a phase-coverage audit; unsupported
accuracy/sizing claims explicitly abstain.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import random
import re
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from urllib.request import urlopen

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_stateful_pilot as common

NAMESPACE = "kubesizer-minio-pilot-mio1"
MEMBERS = ("minio1", "minio2", "minio3", "minio4")
IMAGE = "ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9"
PREDECESSOR = "kubesizer-postgres-pilot-pgp1.service"
PG_ROOT = Path("/home/experiment/experiments/pgp1/data")
S3_HOST = "ks-minio-mio1.83.212.80.252.nip.io"
# Observed Okeanos node and Calico VXLAN source addresses. Remote hostNetwork
# ingress uses its VXLAN address; the local node uses its 10.0.0.x address.
INGRESS_NODE_CIDRS = tuple(f"10.0.0.{n}/32" for n in range(1, 10)) + tuple(
    ip + "/32"
    for ip in (
        "10.233.116.192",
        "10.233.122.0",
        "10.233.85.64",
        "10.233.74.192",
        "10.233.72.64",
        "10.233.119.64",
        "10.233.94.0",
        "10.233.94.64",
        "10.233.107.64",
    )
)
STORAGE_TRACE_SCRIPT = (
    'mc alias set pilot http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null '
    "&& exec mc --json admin trace --call storage pilot"
)
CLASSES = {
    "get_64k": ("GET", 64 * 1024),
    "put_64k": ("PUT", 64 * 1024),
    "get_1m": ("GET", 1024 * 1024),
    "put_1m": ("PUT", 1024 * 1024),
}
PLAN = dict(
    campaign="mio1",
    image=IMAGE,
    predecessor=PREDECESSOR,
    physical_members=4,
    drives_per_member=1,
    parity="EC:2",
    volume_gib_per_member=2,
    classes={n: dict(method=m, size_bytes=s) for n, (m, s) in CLASSES.items()},
    class_shares={n: 0.25 for n in CLASSES},
    object_keys_per_size=64,
    characterization_repetitions=2,
    characterization_rate_rps=2,
    characterization_warmup_seconds=15,
    characterization_measurement_seconds=90,
    pilot_rates_rps=[4, 12, 24, 48],
    pilot_warmup_seconds=15,
    pilot_measurement_seconds=45,
    target_load_fractions=[0.25, 0.6, 0.9],
    target_repetitions=2,
    target_warmup_seconds=60,
    target_measurement_seconds=240,
    drain_seconds=90,
    seed=26111641,
    mean_slo_seconds=0.25,
    read_write_quorum_is_not_whole_operation_completion=True,
    current_model_coverage="ABSTAIN_MULTIPHASE_STORAGE_PROTOCOL",
    purpose="Real S3 measurements and phase-coverage audit before a faithful accuracy comparison",
    storage_readiness="all four drives healthy plus three verified PUT/GET cycles before population",
    health_policy="complete error-free client/evidence windows; latency SLO crossings remain outcomes, not invalid measurements",
    client_transport="HTTP ingress to minio1; kubectl forwards carry telemetry and low-rate preflight only",
    latency_boundary="VM S3 client through ingress, including content verification",
)
write, digest, kube, command, now = (
    common.write,
    common.digest,
    common.kube,
    common.command,
    common.now,
)


def render(destination):
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        common.render(Path(temporary))
        bootstrap = list(
            yaml.safe_load_all((Path(temporary) / "bootstrap.yaml").read_text())
        )
    for document in bootstrap:
        if document["kind"] == "Namespace":
            document["metadata"].update(
                name=NAMESPACE,
                labels={"kubesizer.campaign": "mio1", "istio-injection": "disabled"},
            )
        else:
            document["metadata"]["namespace"] = NAMESPACE
    bootstrap[1]["rules"].extend(
        [
            dict(apiGroups=[""], resources=["secrets"], verbs=["get"]),
            dict(apiGroups=[""], resources=["pods/exec"], verbs=["create"]),
            dict(
                apiGroups=["networking.k8s.io"],
                resources=["networkpolicies"],
                verbs=["get", "create", "patch", "update"],
            ),
        ]
    )
    labels = {"kubesizer.campaign": "mio1"}
    workloads = [
        dict(
            apiVersion="v1",
            kind="ConfigMap",
            metadata=dict(name="observer-source", namespace=NAMESPACE, labels=labels),
            data={
                "observer.py": (ROOT / "scripts/minio_pilot_observer.py").read_text()
            },
        ),
        dict(
            apiVersion="networking.k8s.io/v1",
            kind="NetworkPolicy",
            metadata=dict(name="storage-isolation", namespace=NAMESPACE, labels=labels),
            spec=dict(
                podSelector={},
                policyTypes=["Ingress"],
                ingress=[
                    {"from": [{"podSelector": {}}]},
                    {
                        "from": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": "ingress-nginx"
                                    }
                                }
                            }
                        ]
                        + [{"ipBlock": {"cidr": cidr}} for cidr in INGRESS_NODE_CIDRS],
                        "ports": [{"protocol": "TCP", "port": 9000}],
                    },
                ],
            ),
        ),
    ]
    for name, node in zip(MEMBERS, ("skube06", "skube07", "skube08", "skube09")):
        selector = {**labels, "app": name}
        workloads.append(
            dict(
                apiVersion="v1",
                kind="Service",
                metadata=dict(name=name, namespace=NAMESPACE, labels=labels),
                spec=dict(
                    clusterIP="None",
                    publishNotReadyAddresses=True,
                    selector=selector,
                    ports=[
                        dict(name="s3", port=9000),
                        dict(name="evidence", port=9092),
                    ],
                ),
            )
        )
        env = [
            dict(name=k, value=v)
            for k, v in {
                "MINIO_STORAGE_CLASS_STANDARD": "EC:2",
                "MINIO_ERASURE_SET_DRIVE_COUNT": "4",
                "MINIO_PROMETHEUS_AUTH_TYPE": "public",
                "MINIO_BROWSER": "off",
                "GOMEMLIMIT": "640MiB",
            }.items()
        ]
        env += [
            dict(
                name=key,
                valueFrom=dict(
                    secretKeyRef=dict(name="object-store-credentials", key=field)
                ),
            )
            for key, field in (
                ("MINIO_ROOT_USER", "access_key"),
                ("MINIO_ROOT_PASSWORD", "secret_key"),
            )
        ]
        database = dict(
            name="minio",
            image=IMAGE,
            args=["server", "--address", ":9000", "http://minio{1...4}:9000/data"],
            env=env,
            ports=[dict(name="s3", containerPort=9000)],
            volumeMounts=[dict(name="data", mountPath="/data")],
            resources=dict(
                requests={"cpu": "1", "memory": "512Mi"},
                limits={"cpu": "1", "memory": "1Gi"},
            ),
            readinessProbe=dict(
                httpGet=dict(path="/minio/health/ready", port=9000),
                initialDelaySeconds=10,
                periodSeconds=5,
            ),
        )
        observer = dict(
            name="observer",
            image=common.IMAGE,
            command=["python", "/experiment/observer.py"],
            volumeMounts=[dict(name="source", mountPath="/experiment", readOnly=True)],
            resources=dict(
                requests={"cpu": "25m", "memory": "32Mi"},
                limits={"cpu": "100m", "memory": "96Mi"},
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
                        metadata=dict(labels=selector),
                        spec=dict(
                            shareProcessNamespace=True,
                            nodeSelector={"kubernetes.io/hostname": node},
                            containers=[database, observer],
                            volumes=[
                                dict(
                                    name="source",
                                    configMap=dict(name="observer-source"),
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
                                resources=dict(requests={"storage": "2Gi"}),
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
                name="s3-pilot",
                namespace=NAMESPACE,
                labels=labels,
                annotations={
                    "nginx.ingress.kubernetes.io/proxy-body-size": "2m",
                    "nginx.ingress.kubernetes.io/proxy-request-buffering": "off",
                    "nginx.ingress.kubernetes.io/proxy-buffering": "off",
                    "nginx.ingress.kubernetes.io/proxy-read-timeout": "35",
                },
            ),
            spec=dict(
                ingressClassName="nginx",
                rules=[
                    dict(
                        host=S3_HOST,
                        http=dict(
                            paths=[
                                dict(
                                    path="/",
                                    pathType="Prefix",
                                    backend=dict(
                                        service=dict(
                                            name="minio1", port=dict(number=9000)
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
            n: digest(destination / n)
            for n in ("bootstrap.yaml", "workload.yaml", "protocol.json")
        },
    )


def wait_for_postgres(output, *, timeout=7 * 3600):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = command(
            "systemctl", "show", PREDECESSOR, "--property=ActiveState", "--value"
        ).strip()
        status = (
            json.loads((PG_ROOT / "status.json").read_text())
            if (PG_ROOT / "status.json").exists()
            else {}
        )
        pause = (
            json.loads((PG_ROOT / "comparison-pause.json").read_text())
            if (PG_ROOT / "comparison-pause.json").exists()
            else {}
        )
        if (
            state in ("inactive", "failed")
            and status.get("status") in ("COMPLETED", "FAILED")
            and not pause.get("resume_required", False)
        ):
            pods = json.loads(
                kube("-n", "kubesizer-postgres-pilot-pgp1", "get", "pods", "-o", "json")
            )["items"]
            if not pods:
                write(
                    output / "postgres-predecessor.json",
                    dict(checked_at=now(), service_state=state, outcome=status),
                )
                return
        time.sleep(10)
    raise RuntimeError(
        "PostgreSQL did not reach a stopped, cleaned-up state; MinIO not launched"
    )


class Evidence:
    def __init__(self, output):
        self.output, self.processes, self.threads = output, [], []
        self.trace_stop = threading.Event()
        self.trace_error = None
        self.trace_count = 0
        self.snapshot_lock = threading.Lock()

    def __enter__(self):
        for i, name in enumerate(MEMBERS):
            log = (self.output / f"forward-{name}.log").open("w")
            process = subprocess.Popen(
                [
                    "kubectl",
                    "-n",
                    NAMESPACE,
                    "port-forward",
                    f"pod/{name}-0",
                    f"{29400+i}:9000",
                    f"{29410+i}:9092",
                    "--address=127.0.0.1",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            self.processes.append((process, log))
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                if not self.snapshot()["errors"]:
                    return self
            except OSError:
                time.sleep(1)
        self.__exit__(None, None, None)
        raise RuntimeError("MinIO evidence forwards unavailable")

    def __exit__(self, *args):
        self.trace_stop.set()
        for process, log in reversed(self.processes):
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            if log is not None:
                log.close()
        for thread in self.threads:
            thread.join(timeout=10)

    def snapshot(self):
        # Partial telemetry is retained as missing evidence. It must not erase
        # request outcomes or make an incomplete window appear healthy.
        with self.snapshot_lock:
            result = dict(epoch=time.time(), members={}, errors=[])
            for i, name in enumerate(MEMBERS):
                member = {}
                for source, url in (
                    ("process", f"http://127.0.0.1:{29410+i}/status"),
                    ("prometheus", f"http://127.0.0.1:{29400+i}/minio/v2/metrics/node"),
                ):
                    try:
                        with urlopen(url, timeout=3) as response:
                            if source == "process":
                                member.update(json.load(response))
                            else:
                                member["prometheus_node_metrics"] = (
                                    response.read().decode()
                                )
                    except (OSError, ValueError) as error:
                        result["errors"].append(
                            dict(
                                member=name,
                                source=source,
                                error_type=type(error).__name__,
                            )
                        )
                result["members"][name] = member
            return result

    def trace(self):
        # Credentials stay in the pod's environment; no command includes values.
        script = STORAGE_TRACE_SCRIPT
        errors = (self.output / "storage-trace.stderr.log").open("w")
        process = subprocess.Popen(
            [
                "kubectl",
                "-n",
                NAMESPACE,
                "exec",
                "minio1-0",
                "-c",
                "minio",
                "--",
                "sh",
                "-c",
                script,
            ],
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
        )
        self.processes.append((process, errors))

        def drain():
            try:
                with gzip.open(self.output / "storage-trace.jsonl.gz", "wt") as stream:
                    for value in storage_trace_records(process.stdout):
                        # Normalize multiline mc JSON into redacted JSON lines.
                        value = redact(value)
                        stream.write(json.dumps(value) + "\n")
                        if value.get("status") == "error":
                            raise RuntimeError("MinIO storage trace reported an error")
                        self.trace_count += 1
                    if not self.trace_stop.is_set():
                        raise RuntimeError(
                            "MinIO storage trace ended during collection"
                        )
            except Exception as error:
                if not self.trace_stop.is_set():
                    self.trace_error = f"{type(error).__name__}: {error}"

        thread = threading.Thread(target=drain, daemon=True)
        thread.start()
        self.threads.append(thread)


def storage_trace_records(stream):
    """Decode successive mc objects, including its pretty-printed JSON output."""
    decoder, pending = json.JSONDecoder(), ""
    for line in stream:
        pending += line
        if len(pending) > 4 * 1024 * 1024:
            raise RuntimeError("Invalid or oversized MinIO trace JSON")
        pending = pending.lstrip()
        while pending:
            try:
                value, end = decoder.raw_decode(pending)
            except json.JSONDecodeError:
                break
            if not isinstance(value, dict):
                raise RuntimeError("MinIO trace record must be a JSON object")
            yield value
            pending = pending[end:].lstrip()
    if pending.strip():
        raise RuntimeError("Incomplete MinIO trace JSON")


def redact(value):
    if isinstance(value, list):
        return [redact(v) for v in value]
    if not isinstance(value, dict):
        return value
    return {
        k: redact(v)
        for k, v in value.items()
        if not re.search(
            r"header|credential|authorization|password|secret|access.?key|signature|token",
            k,
            re.I,
        )
    }


def client(port=None, *, http_client=None):
    from minio import Minio
    import urllib3
    import base64

    credentials = json.loads(
        kube("-n", NAMESPACE, "get", "secret", "object-store-credentials", "-o", "json")
    )["data"]
    return Minio(
        f"127.0.0.1:{port}" if port is not None else S3_HOST,
        access_key=base64.b64decode(credentials["access_key"]).decode(),
        secret_key=base64.b64decode(credentials["secret_key"]).decode(),
        secure=False,
        http_client=(
            http_client
            if http_client is not None
            else urllib3.PoolManager(
                maxsize=64, retries=False, timeout=urllib3.Timeout(connect=5, read=30)
            )
        ),
    )


def payload(size):
    # Deterministic, incompressible synthetic bytes, prepared once per process.
    return random.Random(26111641 + size).randbytes(size)


def healthy_storage_layout(info):
    details = info.get("info", {})
    backend, servers = details.get("backend", {}), details.get("servers", [])
    return (
        backend.get("backendType") == "Erasure"
        and backend.get("onlineDisks") == 4
        and backend.get("offlineDisks") == 0
        and backend.get("standardSCParity") == 2
        and backend.get("totalSets") == [1]
        and backend.get("totalDrivesPerSet") == [4]
        and len(servers) == 4
        and all(
            s.get("state") == "online"
            and len(s.get("drives", [])) == 1
            and s["drives"][0].get("state") == "ok"
            for s in servers
        )
    )


def admin_info():
    script = 'mc alias set pilot http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc --json admin info pilot'
    return json.loads(
        kube(
            "-n", NAMESPACE, "exec", "minio1-0", "-c", "minio", "--", "sh", "-c", script
        )
    )


def wait_for_storage(output, clients, *, timeout=300):
    """Readiness is a read/write health gate, never a retry policy for targets."""
    deadline, successes, checks = time.monotonic() + timeout, 0, []
    while time.monotonic() < deadline:
        check = dict(checked_at=now(), consecutive_successes=successes)
        try:
            info = admin_info()
            check["healthy_layout"] = healthy_storage_layout(info)
            if not check["healthy_layout"]:
                raise RuntimeError("Four-drive storage layout is not fully healthy")
            for s3 in clients:
                if not s3.bucket_exists("mio1-data"):
                    s3.make_bucket("mio1-data")
                for size in sorted({s for _, s in CLASSES.values()}):
                    content = payload(size)
                    key = f"size-{size}/key-00"
                    s3.put_object("mio1-data", key, io.BytesIO(content), size)
                    response = s3.get_object("mio1-data", key)
                    try:
                        if response.read() != content:
                            raise ValueError("Storage preflight payload mismatch")
                    finally:
                        response.close()
                        response.release_conn()
            successes += 1
            check.update(success=True, consecutive_successes=successes)
            write(output / "minio-admin-info.json", redact(info))
        except Exception as error:
            successes = 0
            check.update(
                success=False,
                error_type=type(error).__name__,
                error_code=getattr(error, "code", None),
            )
            if isinstance(error, ValueError) or getattr(error, "code", "") in {
                "AccessDenied",
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
            }:
                checks.append(check)
                write(
                    output / "storage-readiness.json",
                    dict(checks=checks, ready=False, completed_at=now()),
                )
                raise
        checks.append(check)
        write(
            output / "storage-readiness.json",
            dict(checks=checks, ready=successes >= 3, completed_at=now()),
        )
        if successes >= 3:
            return
        time.sleep(3)
    raise RuntimeError(
        "MinIO read/write storage readiness did not stabilize before timeout"
    )


def collect(output, s3, run_id, rate, seed, classes, *, warmup, measurement, evidence):
    directory = output / "runs" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    before = safe_snapshot(evidence)
    rows, snapshots, lock, pending = [], [], threading.Lock(), [0]
    permits = threading.BoundedSemaphore(256)
    stop = threading.Event()
    contents = {size: payload(size) for _, size in CLASSES.values()}

    def monitor():
        while not stop.wait(5):
            try:
                snapshots.append(
                    dict(elapsed=time.monotonic() - start, **safe_snapshot(evidence))
                )
            except Exception as error:
                snapshots.append(
                    dict(elapsed=time.monotonic() - start, error=str(error))
                )

    def send(index, scheduled, c, tagged):
        begin = time.monotonic()
        method, size = CLASSES[c]
        key = f"size-{size}/key-{random.Random(seed*1_000_003+index).randrange(64):02d}"
        row = dict(
            request_id=f"{run_id}:{index}",
            request_class=c,
            key=key,
            tagged=tagged,
            scheduled_seconds=scheduled - start,
            scheduling_delay_seconds=begin - scheduled,
        )
        try:
            if method == "PUT":
                s3.put_object(
                    "mio1-data",
                    key,
                    io.BytesIO(contents[size]),
                    size,
                    metadata={"experiment": run_id},
                    num_parallel_uploads=1,
                )
            else:
                response = s3.get_object("mio1-data", key)
                try:
                    value = response.read()
                    if (
                        len(value) != size
                        or hashlib.sha256(value).digest()
                        != hashlib.sha256(contents[size]).digest()
                    ):
                        raise ValueError("Object content verification failed")
                finally:
                    response.close()
                    response.release_conn()
            row["success"] = True
        except Exception as error:
            row.update(success=False, error=f"{type(error).__name__}: {error}")
        finally:
            row["latency_seconds"] = time.monotonic() - begin
            with lock:
                rows.append(row)
                pending[0] -= int(tagged)
            permits.release()

    start = time.monotonic()
    epoch = time.time()
    end = start + warmup + measurement
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
    arrivals, choices = random.Random(seed), random.Random(seed + 1_000_000)
    next_time, index, timed_out = start, 0, False
    try:
        with ThreadPoolExecutor(max_workers=64) as pool:
            while True:
                with lock:
                    left = pending[0]
                current = time.monotonic()
                if current >= end and left == 0:
                    break
                if current >= end + 90:
                    timed_out = True
                    break
                next_time += arrivals.expovariate(rate)
                time.sleep(max(0, next_time - time.monotonic()))
                if next_time >= end and left == 0:
                    break
                index += 1
                tagged = start + warmup <= next_time < end
                c = choices.choice(classes)
                with lock:
                    pending[0] += int(tagged)
                if permits.acquire(blocking=False):
                    pool.submit(send, index, next_time, c, tagged)
                else:
                    with lock:
                        rows.append(
                            dict(
                                request_id=f"{run_id}:{index}",
                                request_class=c,
                                tagged=tagged,
                                success=False,
                                error="client_capacity_rejected",
                                client_capacity_rejected=True,
                                latency_seconds=0,
                                scheduling_delay_seconds=0,
                            )
                        )
                        pending[0] -= int(tagged)
    finally:
        stop.set()
        monitor_thread.join(timeout=60)
        with gzip.open(directory / "requests.jsonl.gz", "wt") as stream:
            for row in rows:
                stream.write(json.dumps(row) + "\n")
        write(
            directory / "snapshots.json",
            dict(before=before, samples=snapshots, after=safe_snapshot(evidence)),
        )
    retained = json.loads((directory / "snapshots.json").read_text())
    snapshot_errors = sum(
        bool(s.get("errors") or s.get("error"))
        for s in [retained["before"], *retained["samples"], retained["after"]]
    )
    summary = dict(
        run_id=run_id,
        started_epoch=epoch,
        rate_rps=rate,
        seed=seed,
        warmup_seconds=warmup,
        measurement_seconds=measurement,
        timed_out=timed_out,
        snapshot_errors=snapshot_errors,
        evidence_complete=snapshot_errors == 0,
        client_transport=PLAN["client_transport"],
        latency_boundary=PLAN["latency_boundary"],
        classes={},
    )
    for c in classes:
        samples = [r for r in rows if r["tagged"] and r["request_class"] == c]
        complete = [r for r in samples if r["success"]]
        summary["classes"][c] = dict(
            count=len(samples),
            completed=len(complete),
            failures=len(samples) - len(complete),
            mean_seconds=(
                sum(r["latency_seconds"] for r in complete) / len(complete)
                if complete
                else None
            ),
            maximum_scheduling_delay_seconds=max(
                (r["scheduling_delay_seconds"] for r in samples), default=0
            ),
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
    print(json.dumps(dict(completed=run_id, classes=summary["classes"])), flush=True)
    return summary


def safe_snapshot(evidence):
    try:
        return evidence.snapshot()
    except Exception as error:
        return dict(
            epoch=time.time(),
            members={},
            errors=[dict(source="snapshot", error_type=type(error).__name__)],
        )


def healthy(run):
    return (
        not run["timed_out"]
        and not run["snapshot_errors"]
        and all(
            c["completed"] > 0
            and c["failures"] == 0
            and c["maximum_scheduling_delay_seconds"] < 0.05
            and c["mean_seconds"] is not None
            and math.isfinite(c["mean_seconds"])
            and c["mean_seconds"] >= 0
            for c in run["classes"].values()
        )
    )


def report(output, runs, anchor):
    lines = [
        "# Distributed MinIO measurement and semantics pilot",
        "",
        "Four fixed members, one persistent drive each, configured EC:2. Real GET/PUT "
        "operations and content verification; no synthetic object-store acknowledgements.",
        "",
        f"Client boundary: {PLAN['latency_boundary']}. Earlier port-forward target runs are retained separately and are not pooled into this report.",
        "",
        "This first stage retains S3 timings, process CPU/I/O, exporter metrics and "
        "storage-call traces. It does not identify a complete multi-round execution "
        "model. Both analytical and simulation accuracy evaluation ABSTAIN until "
        "metadata/data phases and shared resource demands can be mapped faithfully. "
        "No toy one-round 3-of-4 result is presented as a MinIO prediction.",
        "",
        f"The target anchor is {anchor:.2f} requests/s, selected from characterization-stage "
        "client/health pilots only. Fractions are relative to the highest healthy "
        "tested rate, not proof of server capacity. A healthy final pilot only gives "
        "a lower bound on achievable throughput.",
        "",
        "| Run | Class | Rate rps | Completed | Failures | Mean ms | Telemetry complete |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for r in runs:
        for c, v in r["classes"].items():
            mean = (
                f"{1000*v['mean_seconds']:.2f}"
                if v["mean_seconds"] is not None
                else "undefined"
            )
            lines.append(
                f"| {r['run_id']} | {c} | {r['rate_rps']:.2f} | {v['completed']} | {v['failures']} | {mean} | {r.get('snapshot_errors',0)==0} |"
            )
    lines += [
        "",
        "Latency means above are observed performance, not prediction accuracy. "
        "Failures and incomplete outcomes remain in raw data. Two target repeats "
        "cannot establish stable tails, a low unsafe-decision rate or general "
        "stateful sizing reliability. Longhorn introduces an additional shared, "
        "replicated storage layer. Existing STELAR buckets and data were not used.",
        "",
    ]
    (output / "REPORT.md").write_text("\n".join(lines))
    write(
        output / "model-coverage.json",
        {
            mode: dict(
                status="ABSTAINED",
                reason="Unresolved multi-round metadata/data phases and per-resource service demand",
            )
            for mode in ("analytical", "simulation")
        },
    )


def capture_storage_failure(output):
    """Retain diagnosis before cleanup removes the failed attempt's pods."""
    failures = []
    for name in MEMBERS:
        try:
            text = kube(
                "-n", NAMESPACE, "logs", f"{name}-0", "-c", "minio", "--tail=300"
            )
            (output / f"failure-{name}.log").write_text(text)
        except Exception as error:
            failures.append(dict(member=name, error_type=type(error).__name__))
    try:
        write(output / "failure-storage-info.json", redact(admin_info()))
    except Exception as error:
        failures.append(dict(source="admin_info", error_type=type(error).__name__))
    write(
        output / "failure-diagnostics-status.json",
        dict(checked_at=now(), unavailable=failures),
    )


def finish(output):
    marker = output / "comparison-pause.json"
    if not marker.exists():
        return
    pause = json.loads(marker.read_text())
    if not pause.get("cleanup_required", pause.get("resume_required", False)):
        return
    for name in MEMBERS:
        raw = kube(
            "-n",
            NAMESPACE,
            "get",
            "statefulset",
            name,
            "--ignore-not-found",
            "-o",
            "json",
        )
        if not raw.strip():
            continue
        value = json.loads(raw)
        if value["metadata"].get("labels", {}).get("kubesizer.campaign") != "mio1":
            raise RuntimeError("Unexpected MinIO resource owner")
        kube("-n", NAMESPACE, "scale", "statefulset", name, "--replicas=0")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if not json.loads(kube("-n", NAMESPACE, "get", "pods", "-o", "json"))["items"]:
            if pause.get("resume_required"):
                command("systemctl", "start", common.COMPARISON)
                pause["resumed_at"] = now()
            pause.update(
                resume_required=False, cleanup_required=False, cleaned_at=now()
            )
            write(marker, pause)
            return
        time.sleep(3)
    raise RuntimeError("MinIO pods remain; overlapping comparison restart refused")


def import_characterizations(source, output):
    """Reuse only receipt-verified matching characterization, never targets."""
    origin = json.loads((source / "protocol.json").read_text())
    for key in (
        "image",
        "physical_members",
        "drives_per_member",
        "parity",
        "classes",
        "class_shares",
        "characterization_rate_rps",
        "characterization_warmup_seconds",
        "characterization_measurement_seconds",
        "seed",
    ):
        if origin.get(key) != PLAN[key]:
            raise ValueError(f"Imported characterization changes {key}")
    imported = {}
    for c in CLASSES:
        for rep in range(2):
            run_id = f"characterization-{c}-rep{rep}"
            path = source / "runs" / run_id
            if not (path / "receipt.json").exists():
                continue
            receipt = json.loads((path / "receipt.json").read_text())
            if not receipt or any(
                Path(name).name != name or digest(path / name) != sha
                for name, sha in receipt.items()
            ):
                raise ValueError("Imported characterization receipt mismatch")
            summary = json.loads((path / "summary.json").read_text())
            if (
                summary.get("run_id") != run_id
                or summary.get("rate_rps") != 2
                or summary.get("seed")
                != PLAN["seed"] + 2 * list(CLASSES).index(c) + rep
                or summary.get("warmup_seconds")
                != PLAN["characterization_warmup_seconds"]
                or summary.get("measurement_seconds")
                != PLAN["characterization_measurement_seconds"]
                or set(summary.get("classes", {})) != {c}
                or not healthy(summary)
            ):
                raise ValueError("Imported characterization is incomplete or unhealthy")
            destination = output / "runs" / run_id
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(path, destination)
            imported[run_id] = summary
    write(
        output / "characterization-import.json",
        dict(
            source=str(source),
            receipt_verified=True,
            imported_run_ids=list(imported),
            target_runs_imported=False,
            origin_protocol_sha256=digest(source / "protocol.json"),
        ),
    )
    return imported


def import_frozen_inputs(source, output):
    """Keep rates/order fixed across a declared collector/transport amendment."""
    required = {
        "anchor-frozen.json",
        "target-order-frozen.json",
        "characterization-results.json",
        "pilot-results.json",
    }
    receipt_path = source / "input-freeze-receipt.json"
    receipt = json.loads(receipt_path.read_text())
    if set(receipt) != required or any(
        digest(source / name) != expected for name, expected in receipt.items()
    ):
        raise ValueError("Frozen MinIO input receipt mismatch")
    origin = json.loads((source / "protocol.json").read_text())
    transport_fields = {"client_transport", "latency_boundary"}
    if any(origin.get(k) != v for k, v in PLAN.items() if k not in transport_fields):
        raise ValueError(
            "Frozen MinIO protocol differs beyond declared transport amendment"
        )
    anchor = json.loads((source / "anchor-frozen.json").read_text())
    rate = anchor.get("anchor_rps")
    if (
        isinstance(rate, bool)
        or not isinstance(rate, (int, float))
        or not math.isfinite(rate)
        or rate <= 0
        or anchor.get("target_outcomes_used") is not False
    ):
        raise ValueError("Invalid or target-derived frozen MinIO rate")
    schedule = [
        (u, r)
        for u in PLAN["target_load_fractions"]
        for r in range(PLAN["target_repetitions"])
    ]
    random.Random(PLAN["seed"]).shuffle(schedule)
    if json.loads((source / "target-order-frozen.json").read_text()) != [
        list(x) for x in schedule
    ]:
        raise ValueError("Frozen MinIO target schedule differs")
    for name in required:
        shutil.copyfile(source / name, output / name)
    write(output / "original-input-freeze-receipt.json", receipt)
    write(
        output / "input-amendment.json",
        dict(
            source=str(source),
            source_protocol_sha256=digest(source / "protocol.json"),
            old_transport=origin.get(
                "client_transport", "kubectl port-forward to minio1"
            ),
            new_transport=PLAN["client_transport"],
            rates_and_order_unchanged=True,
            target_outcomes_imported=False,
            all_targets_fresh_for_new_boundary=True,
            source_receipt_sha256=digest(receipt_path),
        ),
    )
    return dict(
        anchor=anchor, pilots=json.loads((source / "pilot-results.json").read_text())
    )


def run(
    bundle,
    output,
    *,
    comparison_held=False,
    characterization_from=None,
    frozen_inputs_from=None,
):
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Use a fresh output")
    output.mkdir(parents=True, exist_ok=True)
    for name, sha in json.loads((bundle / "bundle-receipt.json").read_text()).items():
        if digest(bundle / name) != sha:
            raise ValueError("Frozen MinIO bundle changed")
    if json.loads((bundle / "protocol.json").read_text()) != PLAN:
        raise ValueError("MinIO protocol differs")
    status = dict(status="QUEUED_AFTER_POSTGRES", started_at=now())
    write(output / "status.json", status)
    write(output / "protocol.json", PLAN)
    for name in ("dependency-versions.json",):
        source = ROOT.parent / name
        if source.exists():
            write(output / name, json.loads(source.read_text()))
    wheels = ROOT.parent / "dependency-wheels.sha256"
    if wheels.exists():
        (output / "dependency-wheels.sha256").write_text(wheels.read_text())
    write(
        output / "source-receipt.json",
        {
            str(p.relative_to(ROOT)): digest(p)
            for p in (
                ROOT / "scripts/run_minio_pilot.py",
                ROOT / "scripts/minio_pilot_observer.py",
                ROOT / "scripts/run_stateful_pilot.py",
            )
        },
    )
    try:
        if frozen_inputs_from:
            if characterization_from:
                raise ValueError(
                    "Use either frozen-input continuation or characterization import, not both"
                )
            frozen_inputs = import_frozen_inputs(frozen_inputs_from, output)
            characterization_from = frozen_inputs_from
        else:
            frozen_inputs = None
        wait_for_postgres(output)
        status.update(status="WAITING_FOR_COMPARISON_BOUNDARY", stage_started_at=now())
        write(output / "status.json", status)
        if comparison_held:
            state = command(
                "systemctl",
                "show",
                common.COMPARISON,
                "--property=ActiveState",
                "--value",
            ).strip()
            if state not in {"inactive", "failed"}:
                raise RuntimeError(
                    "Comparison must be stopped before using --comparison-held"
                )
            write(
                output / "comparison-pause.json",
                dict(
                    resume_required=False,
                    cleanup_required=True,
                    comparison_held_by_user=True,
                    recorded_at=now(),
                ),
            )
        else:
            common.wait_and_pause(output)
        deployments = json.loads(kube("get", "deployments", "-A", "-o", "json"))[
            "items"
        ]
        if any(
            d["metadata"]["namespace"].endswith("-scv1")
            and d["spec"].get("replicas", 1)
            for d in deployments
        ):
            raise RuntimeError("Comparison replicas still active")
        component_pods = json.loads(
            kube("-n", "kubesizer-components-cmp1", "get", "pods", "-o", "json")
        )["items"]
        if component_pods:
            raise RuntimeError(
                "Component pilot pods remain; refusing overlapping MinIO load"
            )
        status.update(status="PROVISIONING", stage_started_at=now())
        write(output / "status.json", status)
        kube("apply", "-f", str(bundle / "workload.yaml"))
        for name in MEMBERS:
            kube(
                "-n",
                NAMESPACE,
                "rollout",
                "status",
                f"statefulset/{name}",
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
                    "statefulsets,pods,persistentvolumeclaims",
                    "-o",
                    "json",
                )
            ),
        )
        with Evidence(output) as evidence:
            s3 = client()
            status.update(status="VERIFYING_STORAGE_WRITES", stage_started_at=now())
            write(output / "status.json", status)
            wait_for_storage(output, [client(29400 + i) for i in range(4)])
            s3.make_bucket("mio1-data") if not s3.bucket_exists("mio1-data") else None
            status.update(status="PREPARING_OBJECTS", stage_started_at=now())
            write(output / "status.json", status)
            for size in sorted({s for _, s in CLASSES.values()}):
                content = payload(size)
                for key in range(64):
                    s3.put_object(
                        "mio1-data",
                        f"size-{size}/key-{key:02d}",
                        io.BytesIO(content),
                        size,
                    )
            admin_script = 'mc alias set pilot http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc --json admin info pilot'
            info = json.loads(
                kube(
                    "-n",
                    NAMESPACE,
                    "exec",
                    "minio1-0",
                    "-c",
                    "minio",
                    "--",
                    "sh",
                    "-c",
                    admin_script,
                )
            )
            write(output / "minio-admin-info.json", redact(info))
            backend = info.get("info", {}).get("backend", {})
            if not healthy_storage_layout(info):
                raise RuntimeError(
                    "Observed MinIO layout differs from the declared healthy four-drive EC:2 set"
                )
            write(
                output / "minio-version.json",
                dict(
                    version=kube(
                        "-n",
                        NAMESPACE,
                        "exec",
                        "minio1-0",
                        "-c",
                        "minio",
                        "--",
                        "minio",
                        "--version",
                    )
                ),
            )
            evidence.trace()
            trace_deadline = time.monotonic() + 30
            while not evidence.trace_count:
                if evidence.trace_error:
                    raise RuntimeError(evidence.trace_error)
                if time.monotonic() >= trace_deadline:
                    raise RuntimeError(
                        "No storage trace captured during the live preflight"
                    )
                s3.put_object(
                    "mio1-data", "size-65536/key-00", io.BytesIO(payload(65536)), 65536
                )
                time.sleep(1)
            if evidence.trace_error or evidence.processes[-1][0].poll() is not None:
                raise RuntimeError(
                    evidence.trace_error or "Storage trace exited during preflight"
                )
            write(
                output / "storage-trace-preflight.json",
                dict(
                    checked_at=now(),
                    records=evidence.trace_count,
                    probe="PUT identical 64 KiB payload to prefilled key before characterization",
                ),
            )
            status.update(status="CHARACTERIZATION", stage_started_at=now())
            write(output / "status.json", status)
            characterizations = []
            imported = (
                import_characterizations(characterization_from, output)
                if characterization_from
                else {}
            )
            for i, c in enumerate(CLASSES):
                for rep in range(2):
                    run_id = f"characterization-{c}-rep{rep}"
                    characterizations.append(
                        imported[run_id]
                        if run_id in imported
                        else collect(
                            output,
                            s3,
                            f"characterization-{c}-rep{rep}",
                            2,
                            PLAN["seed"] + 2 * i + rep,
                            [c],
                            warmup=15,
                            measurement=90,
                            evidence=evidence,
                        )
                    )
                    if evidence.trace_error:
                        raise RuntimeError(evidence.trace_error)
            write(output / "characterization-results.json", characterizations)
            if any(not healthy(r) for r in characterizations):
                raise RuntimeError(
                    "MinIO low-load characterization failed health/client gate"
                )
            pilots = frozen_inputs["pilots"] if frozen_inputs else []
            for i, rate in enumerate([] if frozen_inputs else PLAN["pilot_rates_rps"]):
                status.update(
                    status="CAPACITY_PILOTS",
                    pilot_rate_rps=rate,
                    stage_started_at=now(),
                )
                write(output / "status.json", status)
                result = collect(
                    output,
                    s3,
                    f"pilot-rate-{rate}",
                    rate,
                    PLAN["seed"] + 30 + i,
                    list(CLASSES),
                    warmup=15,
                    measurement=45,
                    evidence=evidence,
                )
                pilots.append(result)
                write(output / "pilot-results.json", pilots)
                if not healthy(result):
                    break
            valid = [r["rate_rps"] for r in pilots if healthy(r)]
            if not valid:
                raise RuntimeError("No healthy mixed S3 pilot rate")
            anchor = max(valid)
            if frozen_inputs and anchor != frozen_inputs["anchor"]["anchor_rps"]:
                raise ValueError(
                    "Frozen anchor disagrees with retained pilot selection"
                )
            if not frozen_inputs:
                write(
                    output / "anchor-frozen.json",
                    dict(
                        frozen_at=now(),
                        anchor_rps=anchor,
                        target_outcomes_used=False,
                        basis="Highest healthy tested mixed pilot; not a saturation proof",
                        model_accuracy_status="ABSTAINED_MULTIPHASE_PROTOCOL",
                        sources=[r["run_id"] for r in characterizations + pilots],
                    ),
                )
            schedule = [(u, r) for u in PLAN["target_load_fractions"] for r in range(2)]
            random.Random(PLAN["seed"]).shuffle(schedule)
            if not frozen_inputs:
                write(output / "target-order-frozen.json", schedule)
            frozen_paths = [
                output / "anchor-frozen.json",
                output / "target-order-frozen.json",
                output / "characterization-results.json",
                output / "pilot-results.json",
            ]
            receipt = {str(p.relative_to(output)): digest(p) for p in frozen_paths}
            write(output / "input-freeze-receipt.json", receipt)
            if frozen_inputs:
                original = json.loads(
                    (output / "original-input-freeze-receipt.json").read_text()
                )
                if receipt != original:
                    raise ValueError(
                        "Retained frozen MinIO inputs changed during continuation"
                    )
            targets = []
            for i, (fraction, rep) in enumerate(schedule):
                status.update(
                    status="TARGETS",
                    completed_targets=len(targets),
                    total_targets=6,
                    current_fraction=fraction,
                    current_repetition=rep,
                    stage_started_at=now(),
                )
                write(output / "status.json", status)
                if any(digest(output / n) != sha for n, sha in receipt.items()):
                    raise RuntimeError("Frozen MinIO inputs changed")
                targets.append(
                    collect(
                        output,
                        s3,
                        f"target-u{round(100*fraction):02d}-rep{rep}",
                        anchor * fraction,
                        PLAN["seed"] + 100 + i,
                        list(CLASSES),
                        warmup=60,
                        measurement=240,
                        evidence=evidence,
                    )
                )
                write(output / "target-results.json", targets)
                report(output, targets, anchor)
                time.sleep(15)
            if evidence.trace_error:
                raise RuntimeError(evidence.trace_error)
            write(
                output / "storage-trace-status.json",
                dict(
                    records=evidence.trace_count,
                    process_running=evidence.processes[-1][0].poll() is None,
                    collection_error=evidence.trace_error,
                    phase_accuracy_comparison="ABSTAINED_PENDING_TRACE_MAPPING",
                ),
            )
            if (
                evidence.trace_count == 0
                or evidence.processes[-1][0].poll() is not None
            ):
                raise RuntimeError("No live drive/storage trace was captured")
            status.update(status="COMPLETED", completed_targets=6, completed_at=now())
            if any(r.get("snapshot_errors", 0) for r in targets):
                status["status"] = "COMPLETED_WITH_EVIDENCE_GAPS"
    except BaseException as error:
        status.update(
            status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=now()
        )
        capture_storage_failure(output)
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
    parser.add_argument("--comparison-held", action="store_true")
    parser.add_argument("--characterization-from", type=Path)
    parser.add_argument("--frozen-inputs-from", type=Path)
    args = parser.parse_args()
    if args.render:
        render(args.render.resolve())
    elif args.finish:
        finish(args.output.resolve())
    else:
        signal.signal(
            signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt())
        )
        run(
            args.bundle.resolve(),
            args.output.resolve(),
            comparison_held=args.comparison_held,
            characterization_from=args.characterization_from,
            frozen_inputs_from=args.frozen_inputs_from,
        )

#!/usr/bin/env python3
"""Compare VM ingress, private ingress, and direct pod paths to the same MinIO set."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import io
import json
from pathlib import Path
import random
import selectors
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import characterize_minio_mixed as mixed
from scripts import characterize_minio_put as serial
from scripts import run_minio_pilot as pilot


SIZE = 1024 * 1024
ROUNDS = 8
PATHS = ("vm_ingress", "pod_ingress", "pod_direct")
OBSERVER = "minio4-0"
POD_DIR = "/tmp/kubesizer-minio-path-v1"
SCRIPTS = (
    "characterize_minio_paths.py",
    "characterize_minio_mixed.py",
    "characterize_minio_put.py",
    "run_minio_pilot.py",
    "run_stateful_pilot.py",
)
EXEC = (
    "kubectl",
    "-n",
    pilot.NAMESPACE,
    "exec",
    "-i",
    OBSERVER,
    "-c",
    "observer",
    "--",
)


def measure_block(client, mode, keys, content, bucket):
    s3, timings = client
    rows = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        for index, key_map in enumerate(keys):
            barrier = threading.Barrier(2) if mode == "mixed" else None
            jobs = [
                pool.submit(
                    mixed.operate,
                    s3,
                    timings,
                    method,
                    key,
                    content,
                    barrier,
                    bucket=bucket,
                )
                for method, key in key_map.items()
            ]
            rows.extend(dict(round=index, **job.result()) for job in jobs)
            time.sleep(0.35)
    return rows


def worker():
    from minio import Minio

    params = json.loads(sys.stdin.readline())
    resolve = socket.getaddrinfo

    def resolve_ingress(host, port, *args, **kwargs):
        return resolve(
            params["private_ip"] if host == pilot.S3_HOST else host,
            port,
            *args,
            **kwargs,
        )

    socket.getaddrinfo = resolve_ingress
    clients = {}
    for path, endpoint in (
        ("pod_ingress", pilot.S3_HOST),
        ("pod_direct", "minio1:9000"),
    ):

        def build_client(*, http_client):
            return Minio(
                endpoint,
                access_key=params["access_key"],
                secret_key=params["secret_key"],
                secure=False,
                http_client=http_client,
            )

        clients[path] = mixed.measured_client(build_client)
    content = pilot.payload(SIZE)
    print(json.dumps(dict(ready=True)), flush=True)
    for line in sys.stdin:
        block = json.loads(line)
        try:
            rows = measure_block(
                clients[block["path"]],
                block["mode"],
                block["keys"],
                content,
                block.get("bucket") or params["bucket"],
            )
            print(json.dumps(dict(rows=rows)), flush=True)
        except Exception as error:
            # Error type/code is sufficient here and never writes credential values.
            print(
                json.dumps(
                    dict(error=type(error).__name__, code=getattr(error, "code", None))
                ),
                flush=True,
            )


def stage_worker(deps, *, additional_scripts=(), exec_command=EXEC):
    import yaml

    package = io.BytesIO()
    with tarfile.open(fileobj=package, mode="w:gz") as tar:
        tar.add(deps.resolve(), arcname="deps")
        tar.add(Path(yaml.__file__).parent, arcname="deps/yaml")
        for name in (*SCRIPTS, *additional_scripts):
            tar.add(ROOT / "scripts" / name, arcname=f"scripts/{name}")
    subprocess.run(
        [
            *exec_command,
            "python",
            "-c",
            "import pathlib,sys,tarfile; p=pathlib.Path(sys.argv[1]); p.mkdir(exist_ok=True); "
            "tarfile.open(fileobj=sys.stdin.buffer,mode='r|gz').extractall(p,filter='data')",
            POD_DIR,
        ],
        input=package.getvalue(),
        check=True,
        timeout=90,
    )


@contextmanager
def pod_session(output, private_ip, bucket, *, script, timeout=85,
                exec_command=EXEC, extra_params=None):
    secret = json.loads(
        pilot.kube(
            "-n",
            pilot.NAMESPACE,
            "get",
            "secret",
            "object-store-credentials",
            "-o",
            "json",
        )
    )["data"]
    params = dict(
        private_ip=private_ip,
        bucket=bucket,
        access_key=base64.b64decode(secret["access_key"]).decode(),
        secret_key=base64.b64decode(secret["secret_key"]).decode(),
    )
    if extra_params:
        params.update(extra_params)
    with (output / "pod-client.log").open(
        "w"
    ) as log, selectors.DefaultSelector() as selector:
        process = subprocess.Popen(
            [
                *exec_command,
                "env",
                f"PYTHONPATH={POD_DIR}/deps",
                "python",
                "-u",
                f"{POD_DIR}/scripts/{script}",
                "--worker",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            bufsize=1,
        )
        selector.register(process.stdout, selectors.EVENT_READ)

        def exchange(request):
            process.stdin.write(json.dumps(request) + "\n")
            process.stdin.flush()
            if not selector.select(timeout=timeout):
                raise RuntimeError("Pod client did not reply before timeout")
            line = process.stdout.readline()
            if not line:
                raise RuntimeError("Pod client stopped. See pod-client.log")
            result = json.loads(line)
            if "error" in result:
                raise RuntimeError(
                    f"Pod client error: {result['error']} {result['code']}"
                )
            return result

        try:
            if exchange(params) != {"ready": True}:
                raise RuntimeError("Pod client failed startup")
            yield exchange
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=10)
            process.stdout.close()


@contextmanager
def pod_client(output, private_ip, bucket):
    with pod_session(
        output, private_ip, bucket, script="characterize_minio_paths.py"
    ) as exchange:
        yield lambda path, mode, keys, selected_bucket=None: exchange(
            dict(path=path, mode=mode, keys=keys, bucket=selected_bucket)
        )["rows"]


def private_ingress():
    pods = json.loads(pilot.kube("-n", "ingress-nginx", "get", "pods", "-o", "json"))[
        "items"
    ]
    controllers = [
        pod
        for pod in pods
        if pod["metadata"]["name"].startswith("ingress-nginx-controller-")
        and pod["spec"]["nodeName"] == "skube05"
        and pod["status"]["phase"] == "Running"
        and any(
            c["type"] == "Ready" and c["status"] == "True"
            for c in pod["status"].get("conditions", [])
        )
    ]
    if len(controllers) != 1:
        raise RuntimeError("Expected one Ready ingress controller on skube05")
    controller = controllers[0]
    return dict(
        pod=controller["metadata"]["name"],
        ip=controller["status"]["podIP"],
        node=controller["spec"]["nodeName"],
    )


def collect(output, status, evidence, deps, bucket):
    pilot.wait_for_storage(output, [pilot.client(29400 + i) for i in range(4)])
    vm = mixed.measured_client()
    s3, _ = vm
    if s3.bucket_exists(bucket):
        raise RuntimeError("Use a fresh client-path bucket")
    s3.make_bucket(bucket)
    content = pilot.payload(SIZE)
    old = bytes([content[0] ^ 0xFF]) + content[1:]
    for path in PATHS:
        serial.put(s3, bucket, f"size-{SIZE}/warm-{path}-get", content)
        serial.put(s3, bucket, f"size-{SIZE}/warm-{path}-put", old)
    warm = {
        path: [
            {
                "GET": f"size-{SIZE}/warm-{path}-get",
                "PUT": f"size-{SIZE}/warm-{path}-put",
            }
        ]
        for path in PATHS
    }
    controller = private_ingress()
    private_ip = controller["ip"]
    stage_worker(deps)
    order = [
        (rep, SIZE, mode, path)
        for rep in range(2)
        for path in PATHS
        for mode in mixed.MODES
    ]
    random.Random(26111867).shuffle(order)
    keys = [
        [
            {
                method: f"size-{SIZE}/b{block:02}-r{round_:02}-{method.lower()}"
                for method in mixed.MODES[mode]
            }
            for round_ in range(ROUNDS)
        ]
        for block, (_, _, mode, _) in enumerate(order)
    ]
    pilot.write(
        output / "protocol.json",
        dict(
            purpose="client path characterization",
            bucket=bucket,
            blocks=[part[:3] for part in order],
            block_paths=[part[3] for part in order],
            rounds_per_block=ROUNDS,
            counter_fields=mixed.COUNTERS,
            vm_endpoint=pilot.S3_HOST,
            pod_ingress_private_ip=private_ip,
            pod_direct_endpoint="minio1:9000",
            pod_client=OBSERVER,
            nominal_volume_sample_interval_seconds=mixed.SAMPLE_PERIOD,
        ),
    )
    status.update(status="PRELOADING")
    pilot.write(output / "status.json", status)
    for block_keys in keys:
        for round_keys in block_keys:
            for method, key in round_keys.items():
                serial.put(s3, bucket, key, content if method == "GET" else old)
    with pod_client(output, private_ip, bucket) as in_pod:
        warmups = {
            path: in_pod(path, "mixed", warm[path])
            for path in ("pod_ingress", "pod_direct")
        }
        warmups["vm_ingress"] = measure_block(
            vm, "mixed", warm["vm_ingress"], content, bucket
        )
        pilot.write(
            output / "route-preflight.json",
            dict(
                private_controller=controller,
                ingress_controller_identity_for_vm="unknown",
                warmups=warmups,
            ),
        )
        evidence.trace()
        deadline = time.monotonic() + 20
        while (
            not evidence.trace_count
            and not evidence.trace_error
            and time.monotonic() < deadline
        ):
            serial.put(s3, bucket, "trace-preflight", content)
            time.sleep(0.5)
        if not evidence.trace_count or evidence.trace_error:
            raise RuntimeError(f"Trace startup failed: {evidence.trace_error}")
        serial.snapshot(output, evidence, "before")
        snapshot = json.loads((output / "snapshots/before.json").read_text())
        if snapshot["errors"]:
            raise RuntimeError("Initial telemetry missing")
        devices = {
            name: value["volume_stat"]["device"]
            for name, value in snapshot["members"].items()
        }
        pilot.write(output / "volume-devices.json", devices)
        rows = []
        with mixed.volume_samples(output, devices), (output / "requests.jsonl").open(
            "w"
        ) as stream:
            time.sleep(2)
            for block, ((rep, _, mode, path), block_keys) in enumerate(
                zip(order, keys)
            ):
                status.update(
                    status="CHARACTERIZING",
                    current_block=block,
                    blocks=len(order),
                    path=path,
                    completed=len(rows),
                )
                pilot.write(output / "status.json", status)
                measured = (
                    measure_block(vm, mode, block_keys, content, bucket)
                    if path == "vm_ingress"
                    else in_pod(path, mode, block_keys)
                )
                for row in measured:
                    row.update(block=block, repetition=rep, mode=mode, path=path)
                    stream.write(json.dumps(row) + "\n")
                    rows.append(row)
                stream.flush()
                if evidence.trace_error:
                    raise RuntimeError(evidence.trace_error)
                time.sleep(1)
            time.sleep(3)
        serial.snapshot(output, evidence, "after")
    return s3, rows, content


def run(output, deps, bucket):
    output.mkdir(parents=True, exist_ok=False)
    status = dict(status="PREFLIGHT", started_at=pilot.now())
    pilot.write(output / "status.json", status)
    try:
        pilot.write(output / "original-statefulsets.json", serial.preflight())
        pilot.write(
            output / "comparison-pause.json",
            dict(cleanup_required=True, resume_required=False),
        )
        status.update(status="PROVISIONING")
        pilot.write(output / "status.json", status)
        for member in pilot.MEMBERS:
            pilot.kube(
                "-n", pilot.NAMESPACE, "scale", "statefulset", member, "--replicas=1"
            )
        for member in pilot.MEMBERS:
            pilot.kube(
                "-n",
                pilot.NAMESPACE,
                "rollout",
                "status",
                f"statefulset/{member}",
                "--timeout=600s",
                timeout=620,
            )
        with pilot.Evidence(output) as evidence:
            s3, rows, content = collect(output, status, evidence, deps, bucket)
            if evidence.trace_error or evidence.processes[-1][0].poll() is not None:
                raise RuntimeError(
                    f"Trace closed during measurement: {evidence.trace_error}"
                )
        status.update(status="VERIFYING", completed=len(rows))
        pilot.write(output / "status.json", status)
        for row in rows:
            if row["method"] == "PUT":
                serial.verify(s3, bucket, row["key"], content)
        mixed.describe_phases(output, rows, bucket)
        pilot.write(
            output / "evidence-receipt.json",
            {
                str(path.relative_to(output)): pilot.digest(path)
                for path in sorted(output.rglob("*"))
                if path.is_file()
                and path.suffix in {".json", ".gz", ".jsonl"}
                and path.name not in {"status.json", "comparison-pause.json"}
            },
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
        pilot.write(output / "status.json", status)
        pilot.finish(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--deps", type=Path)
    parser.add_argument("--bucket", default="mio1-client-path-v1")
    args = parser.parse_args()
    if args.worker:
        worker()
    else:
        if args.output is None or args.deps is None:
            parser.error("--output and --deps are required on the VM")

        def interrupt(*_):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupt)
        run(args.output.resolve(), args.deps.resolve(), args.bucket)

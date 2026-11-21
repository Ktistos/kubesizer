#!/usr/bin/env python3
"""Run MinIO fragment timing on the isolated four-member mio1 deployment."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import os
from pathlib import Path
import random
import shlex
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import characterize_minio_mixed as mixed
from scripts import characterize_minio_paths as paths
from scripts import characterize_minio_put as serial
from scripts import run_minio_pilot as pilot


PROFILED = os.getenv("KUBESIZER_PROFILED_BUCKET", "mio1-fragment-io-v1")
CONTROL = os.getenv("KUBESIZER_CONTROL_BUCKET", "mio1-fragment-control-v1")
BALANCED = os.getenv("KUBESIZER_BALANCED_FRAGMENT_BLOCKS") == "1"
PROBE_VOLUME = "kubesizer-fragment-probe"
MOUNT = "/var/run/kubesizer-fragment"
COMMAND = [
    "sh",
    "-c",
    f"while [ ! -x {MOUNT}/minio ]; do sleep 1; done; "
    f'export KUBESIZER_PHASE_BUCKET={shlex.quote(PROFILED)}; exec {MOUNT}/minio "$@"',
    "minio",
]
SHIP = """
import gzip, hashlib, os, pathlib, shutil, sys
dest = pathlib.Path(sys.argv[1]); expected = sys.argv[2]
temp = dest.with_suffix('.tmp')
with gzip.GzipFile(fileobj=sys.stdin.buffer) as code, temp.open('wb') as file:
    shutil.copyfileobj(code, file)
with temp.open('rb') as file:
    actual = hashlib.file_digest(file, 'sha256').hexdigest()
if actual != expected:
    raise RuntimeError('Instrumented binary checksum mismatch')
os.chmod(temp, 0o755)
temp.replace(dest)
print(actual)
"""


def patched_parts(original):
    spec = original["spec"]["template"]["spec"]
    containers = json.loads(json.dumps(spec["containers"]))
    volumes = spec.get("volumes", []) + [dict(name=PROBE_VOLUME, emptyDir={})]
    for container in containers:
        if container["name"] not in {"minio", "observer"}:
            raise RuntimeError("Unexpected container in MinIO test layout")
        container.setdefault("volumeMounts", []).append(
            dict(name=PROBE_VOLUME, mountPath=MOUNT)
        )
        if container["name"] == "minio":
            container["command"] = COMMAND
            entry_limit = os.getenv("KUBESIZER_ENTRY_CPU_LIMIT")
            if original["metadata"]["name"] == "minio1" and entry_limit:
                container["resources"]["limits"]["cpu"] = entry_limit
        elif (
            original["metadata"]["name"] == "minio4"
            and os.getenv("KUBESIZER_SWEEP_CLIENT") == "1"
        ):
            container["resources"]["limits"].update(cpu="500m", memory="160Mi")
    return dict(containers=containers, volumes=volumes)


def patch(member, replicas, parts):
    body = dict(spec=dict(replicas=replicas, template=dict(spec=parts)))
    pilot.kube(
        "-n",
        pilot.NAMESPACE,
        "patch",
        "statefulset",
        member,
        "--type=merge",
        "-p",
        json.dumps(body),
    )


def restore(output):
    marker = output / "restore-template.json"
    if not marker.exists() or json.loads(marker.read_text())["restored"]:
        return
    originals = json.loads((output / "original-statefulsets.json").read_text())["items"]
    pilot.finish(output)
    for original in originals:
        member = original["metadata"]["name"]
        current = json.loads(
            pilot.kube(
                "-n", pilot.NAMESPACE, "get", "statefulset", member, "-o", "json"
            )
        )
        actual = current["spec"]["template"]["spec"]
        old = original["spec"]["template"]["spec"]
        fields = dict(containers=old["containers"], volumes=old.get("volumes"))
        if all(actual.get(key) == value for key, value in fields.items()):
            continue
        expected = patched_parts(original)
        if any(actual.get(key) != value for key, value in expected.items()):
            raise RuntimeError(f"Template drift during experiment on {member}")
        patch(member, 0, fields)
        updated = json.loads(
            pilot.kube(
                "-n", pilot.NAMESPACE, "get", "statefulset", member, "-o", "json"
            )
        )
        if updated["spec"]["replicas"] != 0 or any(
            updated["spec"]["template"]["spec"].get(key) != value
            for key, value in fields.items()
        ):
            raise RuntimeError(
                f"Template restoration could not be verified on {member}"
            )
    pilot.write(marker, dict(restored=True, completed_at=pilot.now()))


def upload(member, binary, digest):
    with binary.open("rb") as stream:
        result = subprocess.run(
            [
                "kubectl",
                "-n",
                pilot.NAMESPACE,
                "exec",
                "-i",
                f"{member}-0",
                "-c",
                "observer",
                "--",
                "python",
                "-c",
                SHIP,
                f"{MOUNT}/minio",
                digest,
            ],
            stdin=stream,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=200,
        )
    if result.returncode or result.stdout.decode().strip() != digest:
        raise RuntimeError(
            f"Binary upload failed for {member}: {result.stderr.decode()[-1000:]}"
        )
    return member, digest


def provision(output, originals, binary, digest):
    for original in originals:
        patch(original["metadata"]["name"], 1, patched_parts(original))
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        pods = json.loads(
            pilot.kube("-n", pilot.NAMESPACE, "get", "pods", "-o", "json")
        )["items"]
        ready = {
            pod["metadata"]["name"]
            for pod in pods
            if any(
                item["name"] == "observer" and "running" in item["state"]
                for item in pod["status"].get("containerStatuses", [])
            )
        }
        if ready == {f"{member}-0" for member in pilot.MEMBERS}:
            break
        time.sleep(2)
    else:
        raise RuntimeError("The observer containers did not start")
    with ThreadPoolExecutor(max_workers=4) as pool:
        uploads = dict(pool.map(lambda m: upload(m, binary, digest), pilot.MEMBERS))
    pilot.write(output / "uploaded-binaries.json", uploads)
    for member in pilot.MEMBERS:
        pilot.kube(
            "-n",
            pilot.NAMESPACE,
            "rollout",
            "status",
            f"statefulset/{member}",
            "--timeout=480s",
            timeout=500,
        )


def collect(output, status, evidence, deps):
    pilot.wait_for_storage(output, [pilot.client(29400 + i) for i in range(4)])
    s3 = pilot.client()
    content = pilot.payload(paths.SIZE)
    old = bytes([content[0] ^ 0xFF]) + content[1:]
    rng = random.Random(26111881)
    conditions = [
        (bucket, mode) for bucket in (CONTROL, PROFILED) for mode in ("write", "mixed")
    ]
    rounds = 2 if BALANCED else paths.ROUNDS
    if BALANCED:
        rng.shuffle(conditions)
        order = [
            (rep, *conditions[(rep + position) % len(conditions)])
            for rep in range(8)
            for position in range(len(conditions))
        ]
    else:
        order = [(rep, *condition) for rep in range(2) for condition in conditions]
        rng.shuffle(order)
    keys = [
        [
            {
                method: f"size-{paths.SIZE}/b{block:02}-r{n:02}-{method.lower()}"
                for method in mixed.MODES[mode]
            }
            for n in range(rounds)
        ]
        for block, (_, _, mode) in enumerate(order)
    ]
    pilot.write(
        output / "protocol.json",
        dict(
            purpose="receiver fragment read/write/flush and sender stream observation",
            profiled_bucket=PROFILED,
            control_bucket=CONTROL,
            blocks=order,
            rounds_per_block=rounds,
            client="pod direct to minio1:9000",
            object_size_bytes=paths.SIZE,
            separate_keys=True,
        ),
    )
    status.update(status="PRELOADING")
    pilot.write(output / "status.json", status)
    paths.stage_worker(deps)
    warm = {
        method: f"size-{paths.SIZE}/warm-{method.lower()}" for method in ("GET", "PUT")
    }
    for bucket in (CONTROL, PROFILED):
        if s3.bucket_exists(bucket):
            raise RuntimeError(f"Use a fresh bucket for this run: {bucket}")
        s3.make_bucket(bucket)
        for method, key in warm.items():
            serial.put(s3, bucket, key, content if method == "GET" else old)
    for (_, bucket, _), block_keys in zip(order, keys):
        for key_map in block_keys:
            for method, key in key_map.items():
                serial.put(s3, bucket, key, content if method == "GET" else old)
    with paths.pod_client(output, "", PROFILED) as in_pod:
        for bucket in (CONTROL, PROFILED):
            in_pod("pod_direct", "mixed", [warm], bucket)
        evidence.trace()
        deadline = time.monotonic() + 20
        while (
            not evidence.trace_count
            and not evidence.trace_error
            and time.monotonic() < deadline
        ):
            in_pod("pod_direct", "mixed", [warm], PROFILED)
        if not evidence.trace_count or evidence.trace_error:
            raise RuntimeError(f"Storage trace startup failed: {evidence.trace_error}")
        serial.snapshot(output, evidence, "before")
        rows = []
        with (output / "requests.jsonl").open("w") as stream:
            for block, ((rep, bucket, mode), block_keys) in enumerate(zip(order, keys)):
                status.update(
                    status="MEASURING",
                    block=block,
                    blocks=len(order),
                    completed=len(rows),
                )
                pilot.write(output / "status.json", status)
                for row in in_pod("pod_direct", mode, block_keys, bucket):
                    row.update(block=block, repetition=rep, bucket=bucket, mode=mode)
                    rows.append(row)
                    stream.write(json.dumps(row) + "\n")
                stream.flush()
                if evidence.trace_error:
                    raise RuntimeError(evidence.trace_error)
                time.sleep(1)
        time.sleep(2)
        serial.snapshot(output, evidence, "after")
    if evidence.trace_error or evidence.processes[-1][0].poll() is not None:
        raise RuntimeError(f"Storage trace stopped early: {evidence.trace_error}")
    return s3, rows, content


def save_probe_records(output):
    records = []
    for member in pilot.MEMBERS:
        log = pilot.kube(
            "-n", pilot.NAMESPACE, "logs", f"{member}-0", "-c", "minio", timeout=18
        )
        records.extend(
            dict(member=member, **json.loads(line.split("KUBESIZER_FRAGMENT ", 1)[1]))
            for line in log.splitlines()
            if line.startswith("KUBESIZER_FRAGMENT ")
        )
    pilot.write(output / "probe-records.json", records)
    if not records:
        raise RuntimeError("No instrumented timing records were collected")


def run(output, deps, binary, digest):
    output.mkdir(parents=True, exist_ok=False)
    status = dict(status="PREFLIGHT", started_at=pilot.now())
    pilot.write(output / "status.json", status)
    try:
        original = serial.preflight()
        pilot.write(output / "original-statefulsets.json", original)
        pilot.write(
            output / "comparison-pause.json",
            dict(cleanup_required=True, resume_required=False),
        )
        pilot.write(output / "restore-template.json", dict(restored=False))
        status.update(status="UPLOADING_INSTRUMENTED_BINARY")
        pilot.write(output / "status.json", status)
        provision(output, original["items"], binary, digest)
        with pilot.Evidence(output) as evidence:
            s3, rows, content = collect(output, status, evidence, deps)
        status.update(status="VERIFYING", completed=len(rows))
        pilot.write(output / "status.json", status)
        save_probe_records(output)
        for row in rows:
            if row["method"] == "PUT":
                serial.verify(s3, row["bucket"], row["key"], content)
        with gzip.open(output / "storage-trace.jsonl.gz", "rt") as stream:
            raw = [json.loads(line) for line in stream]
        joined = {}
        for bucket in (CONTROL, PROFILED):
            selected = [row for row in rows if row["bucket"] == bucket]
            joined.update(serial.correlate(selected, raw, bucket))
        pilot.write(output / "request-storage-calls.json", joined)
        ids = {row["request_id"] for row in rows}
        if len(ids) != len(rows) or None in ids or "" in ids:
            raise RuntimeError("Missing or duplicate S3 request IDs")
        pilot.write(
            output / "evidence-receipt.json",
            {
                str(path.relative_to(output)): pilot.digest(path)
                for path in sorted(output.rglob("*"))
                if path.is_file()
                and path.suffix in {".json", ".gz", ".jsonl"}
                and path.name
                not in {"status.json", "comparison-pause.json", "restore-template.json"}
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
        restore(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deps", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--digest")
    parser.add_argument("--finish", action="store_true")
    args = parser.parse_args()
    if args.finish:
        restore(args.output.resolve())
    else:
        if args.deps is None or args.binary is None or args.digest is None:
            parser.error("--deps, --binary and --digest are required for collection")

        def interrupt(*_):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupt)
        run(
            args.output.resolve(),
            args.deps.resolve(),
            args.binary.resolve(),
            args.digest,
        )

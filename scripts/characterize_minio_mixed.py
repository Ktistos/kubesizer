#!/usr/bin/env python3
"""Measure isolated and paired S3 operations on the existing mio1 pilot."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import gzip
import io
import json
from pathlib import Path
import random
import signal
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import characterize_minio_put as serial
from scripts import run_minio_pilot as pilot


BUCKET = "mio1-mixed-phase-v1"
ROUNDS = 10
MODES = {"read": ("GET",), "write": ("PUT",), "mixed": ("GET", "PUT")}
COUNTERS = (
    "reads",
    "merged_reads",
    "sectors_read",
    "read_ms",
    "writes",
    "merged_writes",
    "sectors_written",
    "write_ms",
    "in_flight",
    "busy_ms",
    "weighted_ms",
    "discards",
    "merged_discards",
    "sectors_discarded",
    "discard_ms",
    "flushes",
    "flush_ms",
)
SAMPLE_PERIOD = 0.02
SAMPLE_VOLUME = """
import json, sys, time

device, period = sys.argv[1], float(sys.argv[2])
deadline = time.monotonic() + 600
while time.monotonic() < deadline:
    before = time.time_ns()
    with open('/proc/diskstats') as stream:
        row = next(parts for parts in map(str.split, stream) if ':'.join(parts[:2]) == device)
    after = time.time_ns()
    print(json.dumps([(before + after) // 2, *map(int, row[3:])]), flush=True)
    time.sleep(period)
"""


def measured_client(client_factory=pilot.client, *, maxsize=4):
    import urllib3

    context = threading.local()

    class TimedConnection(urllib3.connection.HTTPConnection):
        def endheaders(self, *args, **kwargs):
            result = super().endheaders(*args, **kwargs)
            if getattr(context, "put", False):
                context.headers_sent = time.time_ns()
            return result

        def request(self, method, url, *args, **kwargs):
            result = super().request(method, url, *args, **kwargs)
            if method == "PUT" and getattr(context, "put", False):
                context.body_sent = time.time_ns()
            return result

    class TimedPool(urllib3.HTTPConnectionPool):
        ConnectionCls = TimedConnection

    manager = urllib3.PoolManager(
        maxsize=maxsize, retries=False, timeout=urllib3.Timeout(connect=5, read=30)
    )
    manager.pool_classes_by_scheme = {
        **manager.pool_classes_by_scheme,
        "http": TimedPool,
    }
    return client_factory(http_client=manager), context


def operate(s3, timings, method, key, content, barrier=None, *, bucket=BUCKET):
    if barrier is not None:
        barrier.wait(timeout=10)
    started_epoch_ns, started = time.time_ns(), time.monotonic_ns()
    if method == "GET":
        response = s3.get_object(bucket, key)
        headers_ns = time.time_ns()
        try:
            received = response.read()
            body_end_epoch_ns, body_end = time.time_ns(), time.monotonic_ns()
            if received != content:
                raise RuntimeError(f"GET payload differs for {key}")
            request_id = response.headers.get("x-amz-request-id")
        finally:
            response.close()
            response.release_conn()
        phase = dict(
            headers_epoch_ns=headers_ns,
            last_byte_epoch_ns=body_end_epoch_ns,
            last_byte_seconds=(body_end - started) / 1e9,
        )
    else:
        timings.put, timings.headers_sent, timings.body_sent = True, None, None
        try:
            receipt = s3.put_object(
                bucket, key, io.BytesIO(content), len(content), num_parallel_uploads=1
            )
        finally:
            timings.put = False
        request_id = receipt.http_headers.get("x-amz-request-id")
        if timings.headers_sent is None or timings.body_sent is None:
            raise RuntimeError(f"Missing PUT body write boundary for {key}")
        phase = dict(
            headers_sent_epoch_ns=timings.headers_sent,
            body_sent_epoch_ns=timings.body_sent,
        )
    return dict(
        method=method,
        key=key,
        bytes=len(content),
        request_id=request_id,
        start_epoch_ns=started_epoch_ns,
        end_epoch_ns=time.time_ns(),
        seconds=(time.monotonic_ns() - started) / 1e9,
        **phase,
    )


@contextmanager
def volume_samples(output, devices):
    processes = []
    with ExitStack() as stack:
        try:
            for name, device in devices.items():
                out = stack.enter_context((output / f"volume-{name}.jsonl").open("w"))
                err = stack.enter_context((output / f"volume-{name}.log").open("w"))
                processes.append(
                    subprocess.Popen(
                        [
                            "kubectl",
                            "-n",
                            pilot.NAMESPACE,
                            "exec",
                            f"{name}-0",
                            "-c",
                            "observer",
                            "--",
                            "python",
                            "-u",
                            "-c",
                            SAMPLE_VOLUME,
                            device,
                            str(SAMPLE_PERIOD),
                        ],
                        stdout=out,
                        stderr=err,
                    )
                )
            deadline = time.monotonic() + 20
            while not all(
                (output / f"volume-{name}.jsonl").stat().st_size > 0 for name in devices
            ):
                if time.monotonic() >= deadline or any(
                    p.poll() is not None for p in processes
                ):
                    raise RuntimeError("Volume sampler startup failed. See volume logs")
                time.sleep(0.15)
            yield
            if any(p.poll() is not None for p in processes):
                raise RuntimeError("A volume sampler ended during measurement")
        finally:
            for process in processes:
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def describe_phases(output, rows, bucket=BUCKET):
    with gzip.open(output / "storage-trace.jsonl.gz", "rt") as stream:
        calls = serial.correlate(rows, map(json.loads, stream), bucket)
    pilot.write(output / "request-storage-calls.json", calls)
    ids = {row["request_id"] for row in rows}
    if None in ids or "" in ids or len(ids) != len(rows):
        raise RuntimeError("Missing or duplicate S3 request IDs")
    for row in rows:
        if row["method"] == "PUT":
            expected = ["storage.RenameData"]
            if row["bytes"] == 1024 * 1024:
                expected.append("storage.CreateFile")
            for api in expected:
                members = {
                    r["host"].split(":", 1)[0]
                    for r in calls[row["key"]]
                    if r["api"] == api and not r.get("error")
                }
                if members != set(pilot.MEMBERS):
                    raise RuntimeError(f"Incomplete {api} evidence for {row['key']}")
        elif not any(r["api"] in serial.META for r in calls[row["key"]]):
            raise RuntimeError(f"Missing GET metadata read for {row['key']}")


def collect(output, status, evidence):
    pilot.wait_for_storage(output, [pilot.client(29400 + i) for i in range(4)])
    s3, timings = measured_client()
    if s3.bucket_exists(BUCKET):
        raise RuntimeError("Expected a fresh mixed characterization bucket")
    s3.make_bucket(BUCKET)
    content = {size: pilot.payload(size) for size in (64 * 1024, 1024 * 1024)}
    order = [
        (rep, size, mode) for rep in range(2) for size in content for mode in MODES
    ]
    random.Random(26111842).shuffle(order)
    keys = [
        [
            {
                method: f"size-{size}/b{block:02}-r{round_:02}-{method.lower()}"
                for method in MODES[mode]
            }
            for round_ in range(ROUNDS)
        ]
        for block, (_, size, mode) in enumerate(order)
    ]
    pilot.write(
        output / "protocol.json",
        dict(
            purpose="mixed characterization",
            bucket=BUCKET,
            blocks=order,
            rounds_per_block=ROUNDS,
            counter_fields=COUNTERS,
            nominal_volume_sample_interval_seconds=SAMPLE_PERIOD,
            boundary="VM S3 client through r7 ingress, GET through buffered completion and byte verification",
            key_reuse_between_measured_requests=False,
            paired_operations_use_distinct_keys=True,
        ),
    )
    status.update(status="PRELOADING")
    pilot.write(output / "status.json", status)
    for (_, size, _), block_keys in zip(order, keys):
        data = content[size]
        old = bytes([data[0] ^ 0xFF]) + data[1:]
        for round_keys in block_keys:
            for method, key in round_keys.items():
                serial.put(s3, BUCKET, key, data if method == "GET" else old)
    evidence.trace()
    deadline = time.monotonic() + 20
    while (
        not evidence.trace_count
        and not evidence.trace_error
        and time.monotonic() < deadline
    ):
        serial.put(s3, BUCKET, "preflight", content[64 * 1024])
        time.sleep(0.5)
    if evidence.trace_error or not evidence.trace_count:
        raise RuntimeError(f"Storage trace missing: {evidence.trace_error}")
    serial.snapshot(output, evidence, "before")
    before = json.loads((output / "snapshots/before.json").read_text())
    if before["errors"]:
        raise RuntimeError(f"Initial telemetry snapshot incomplete: {before['errors']}")
    devices = {
        name: value["volume_stat"]["device"]
        for name, value in before["members"].items()
    }
    pilot.write(output / "volume-devices.json", devices)
    rows = []
    with volume_samples(output, devices), ThreadPoolExecutor(max_workers=2) as pool:
        time.sleep(3)
        with (output / "requests.jsonl").open("w") as stream:
            for block, ((rep, size, mode), block_keys) in enumerate(zip(order, keys)):
                status.update(
                    status="CHARACTERIZING",
                    current_block=block,
                    blocks=len(order),
                    completed=len(rows),
                )
                pilot.write(output / "status.json", status)
                for round_, round_keys in enumerate(block_keys):
                    barrier = threading.Barrier(2) if mode == "mixed" else None
                    jobs = [
                        pool.submit(
                            operate, s3, timings, method, key, content[size], barrier
                        )
                        for method, key in round_keys.items()
                    ]
                    for job in jobs:
                        row = dict(
                            block=block,
                            repetition=rep,
                            mode=mode,
                            round=round_,
                            **job.result(),
                        )
                        rows.append(row)
                        stream.write(json.dumps(row) + "\n")
                        stream.flush()
                    if evidence.trace_error:
                        raise RuntimeError(evidence.trace_error)
                    time.sleep(0.4)
                time.sleep(1)
        time.sleep(3)
    if evidence.trace_error or evidence.processes[-1][0].poll() is not None:
        raise RuntimeError(f"Storage trace interrupted: {evidence.trace_error}")
    serial.snapshot(output, evidence, "after")
    return s3, rows, content


def run(output):
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
            s3, rows, content = collect(output, status, evidence)
        status.update(status="VERIFYING", completed=len(rows))
        pilot.write(output / "status.json", status)
        for row in rows:
            if row["method"] == "PUT":
                serial.verify(s3, BUCKET, row["key"], content[row["bytes"]])
        describe_phases(output, rows)
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
        status.update(
            status="CAPTURED",
            completed_at=pilot.now(),
            requests_by_method=dict(Counter(row["method"] for row in rows)),
        )
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
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    def interrupt(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    run(args.output.resolve())

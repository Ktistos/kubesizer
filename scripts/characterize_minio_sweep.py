#!/usr/bin/env python3
"""Collect sustained PUT-only and matched PUT-rate mixed windows on the mio1 pilot."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import gzip
import json
from pathlib import Path
import random
import signal
from statistics import fmean, quantiles
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import characterize_minio_fragments as fragments
from scripts import characterize_minio_mixed as mixed
from scripts import characterize_minio_paths as paths
from scripts import characterize_minio_put as serial
from scripts import run_minio_pilot as pilot


RATES = (3.0, 7.2, 10.8)
WARMUP = 15
MEASUREMENT = 55
REPETITIONS = 2
PUT_KEYS = 128
GET_KEYS = 48
MAX_IN_FLIGHT = 48
SEED = 26111892
BUCKET = fragments.PROFILED


def arrivals(rate, method, seed, horizon):
    rng = random.Random(seed)
    elapsed = rng.expovariate(rate)
    i = 0
    size = paths.SIZE
    key_count = PUT_KEYS if method == "PUT" else GET_KEYS
    while elapsed < horizon:
        yield elapsed, method, f"size-{size}/{method.lower()}-{i % key_count:03d}"
        elapsed += rng.expovariate(rate)
        i += 1


def client_cpu():
    for path in (
        "/sys/fs/cgroup/cpu.stat",
        "/sys/fs/cgroup/cpu/cpu.stat",
        "/sys/fs/cgroup/cpu,cpuacct/cpu.stat",
    ):
        try:
            return {
                name: int(value)
                for name, value in (
                    line.split() for line in Path(path).read_text().splitlines()
                )
            }
        except FileNotFoundError:
            continue
    return {}


def measure(client, content, bucket, *, rate, seed, mode, get_rate=None,
            warmup=WARMUP, measurement=MEASUREMENT, max_in_flight=MAX_IN_FLIGHT):
    horizon = warmup + measurement
    schedule = [] if mode == "read" else list(arrivals(rate, "PUT", seed, horizon))
    if mode in {"read", "mixed"}:
        read_rate = rate if get_rate is None else get_rate
        if read_rate <= 0:
            raise ValueError("A mixed window requires a positive GET rate")
        schedule.extend(arrivals(read_rate, "GET", seed + 1_000_000, horizon))
    schedule.sort()
    s3, timings = client
    active = set()
    guard = threading.Lock()
    permits = threading.BoundedSemaphore(max_in_flight)
    rows, jobs = [], []
    cpu_before = client_cpu()
    epoch, start = time.time_ns(), time.monotonic()

    def launch(row):
        method, key = row["method"], row["key"]
        row["scheduling_delay_seconds"] = time.monotonic() - (
            start + row["scheduled_seconds"]
        )
        row["sdk_start_epoch_ns"] = time.time_ns()
        try:
            row.update(
                mixed.operate(s3, timings, method, key, content, bucket=bucket),
                success=True,
            )
        except Exception as error:
            row.update(
                error_type=type(error).__name__,
                error_code=getattr(error, "code", None),
                success=False,
            )
        finally:
            row["sdk_finish_epoch_ns"] = time.time_ns()
            if method == "PUT":
                with guard:
                    active.remove(key)
            permits.release()

    with ThreadPoolExecutor(max_workers=max_in_flight) as pool:
        for offset, method, key in schedule:
            time.sleep(max(0, start + offset - time.monotonic()))
            row = dict(
                scheduled_seconds=offset,
                method=method,
                key=key,
                tagged=offset >= warmup,
            )
            rows.append(row)
            if not permits.acquire(blocking=False):
                row.update(success=False, error_type="client_capacity_rejected")
                continue
            if method == "PUT":
                with guard:
                    if key in active:
                        row.update(success=False, error_type="concurrent_key_rejected")
                        permits.release()
                        continue
                    active.add(key)
            jobs.append(pool.submit(launch, row))
        time.sleep(max(0, start + horizon - time.monotonic()))
        pending_at_stop = sum(not job.done() for job in jobs)
    return dict(
        started_epoch_ns=epoch,
        ended_epoch_ns=time.time_ns(),
        rows=rows,
        pending_at_stop=pending_at_stop,
        client_cpu_before=cpu_before,
        client_cpu_after=client_cpu(),
    )


def worker():
    from minio import Minio

    params = json.loads(sys.stdin.readline())

    def client_factory(*, http_client):
        return Minio(
            "minio1:9000",
            access_key=params["access_key"],
            secret_key=params["secret_key"],
            secure=False,
            http_client=http_client,
        )

    client = mixed.measured_client(client_factory, maxsize=MAX_IN_FLIGHT)
    content = pilot.payload(paths.SIZE)
    print(json.dumps(dict(ready=True)), flush=True)
    for line in sys.stdin:
        request = json.loads(line)
        try:
            if request.get("warmup"):
                result = dict(
                    rows=paths.measure_block(
                        client,
                        "mixed",
                        [{"GET": request["get"], "PUT": request["put"]}],
                        content,
                        params["bucket"],
                    )
                )
            else:
                result = measure(client, content, params["bucket"], **request)
            print(json.dumps(result), flush=True)
        except Exception as error:
            print(
                json.dumps(
                    dict(error=type(error).__name__, code=getattr(error, "code", None))
                ),
                flush=True,
            )


@contextmanager
def probe_streams(output):
    processes, threads, errors = [], [], []
    stopping = threading.Event()

    def drain(member, process):
        try:
            with gzip.open(output / f"probe-{member}.jsonl.gz", "wt") as stream:
                for line in process.stdout:
                    if line.startswith("KUBESIZER_FRAGMENT "):
                        record = json.loads(line.split(" ", 1)[1])
                        stream.write(json.dumps(record) + "\n")
            if not stopping.is_set():
                errors.append(f"{member} probe stream closed")
        except Exception as error:
            errors.append(f"{member} probe stream: {type(error).__name__}")

    try:
        for member in pilot.MEMBERS:
            log = (output / f"probe-{member}.log").open("w")
            process = subprocess.Popen(
                [
                    "kubectl",
                    "-n",
                    pilot.NAMESPACE,
                    "logs",
                    "-f",
                    f"{member}-0",
                    "-c",
                    "minio",
                    "--since=10s",
                ],
                stdout=subprocess.PIPE,
                stderr=log,
                text=True,
            )
            processes.append((process, log))
            thread = threading.Thread(target=drain, args=(member, process), daemon=True)
            thread.start()
            threads.append(thread)
        yield errors
    finally:
        stopping.set()
        for process, log in processes:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            log.close()
        for thread in threads:
            thread.join(timeout=10)
        if any(thread.is_alive() for thread in threads):
            errors.append("Probe stream did not finish flushing")
        pilot.write(
            output / "probe-status.json", dict(errors=errors, finished_at=pilot.now())
        )


def summarize_window(request, measured):
    rows = measured["rows"]
    classes = {}
    for method in ("PUT", "GET"):
        group = [row for row in rows if row["tagged"] and row["method"] == method]
        if not group:
            continue
        successful = [row for row in group if row["success"]]
        classes[method] = dict(
            offered=len(group),
            successes=len(successful),
            errors=dict(
                Counter(row["error_type"] for row in group if not row["success"])
            ),
            mean_ms=(
                1000 * fmean(row["seconds"] for row in successful)
                if successful
                else None
            ),
        )
    starts = [
        row["scheduling_delay_seconds"]
        for row in rows
        if row["tagged"] and row.get("scheduling_delay_seconds") is not None
    ]
    return dict(
        **request,
        classes=classes,
        total_failures=sum(not row["success"] for row in rows),
        scheduling_delay_p95_ms=(
            1000 * quantiles(starts, n=20, method="inclusive")[-1]
            if len(starts) > 1
            else None
        ),
        pending_at_stop=measured["pending_at_stop"],
    )


def wait_for_volume_headroom(output, status, evidence, planned):
    # Do not depend on trash collection during a window. EC:2 emits one
    # half-size shard per drive. Add room for metadata and the free-space floor.
    count = len(
        list(arrivals(planned["rate"], "PUT", planned["seed"], WARMUP + MEASUREMENT))
    )
    required = count * (paths.SIZE // 2 + 8192) + 64 * 2**20
    status.update(
        status="WAITING_FOR_VOLUME_HEADROOM", required_free_bytes_per_member=required
    )
    deadline = time.monotonic() + 12 * 60
    while time.monotonic() < deadline:
        sample = pilot.safe_snapshot(evidence)
        free = {}
        for member, row in sample["members"].items():
            values = [
                float(line.split()[-1])
                for line in row.get("prometheus_node_metrics", "").splitlines()
                if line.startswith("minio_node_drive_free_bytes{")
            ]
            if values:
                free[member] = min(values)
        status["last_drive_free_bytes"] = free
        pilot.write(output / "status.json", status)
        if (
            not sample["errors"]
            and set(free) == set(pilot.MEMBERS)
            and min(free.values()) >= required
        ):
            return
        time.sleep(10)
    raise RuntimeError(
        "MinIO's normal cleanup did not restore enough free space for this window"
    )


def collect(
    output,
    status,
    evidence,
    deps,
    *,
    plan=None,
    use_preloaded_bucket=False,
    require_space=False,
):
    if require_space:
        if not plan or not use_preloaded_bucket:
            raise ValueError(
                "The headroom check requires an explicit plan and a preloaded bucket"
            )
        wait_for_volume_headroom(output, status, evidence, plan[0])
    pilot.wait_for_storage(output, [pilot.client(29400 + i) for i in range(4)])
    if BUCKET == "mio1-fragment-io-v1":
        raise ValueError("Set a new KUBESIZER_PROFILED_BUCKET for the sustained sweep")
    s3 = pilot.client()
    if s3.bucket_exists(BUCKET) != use_preloaded_bucket:
        raise RuntimeError("The sweep bucket does not match the planned preload state")
    status.update(status="PRELOADING")
    pilot.write(output / "status.json", status)
    paths.stage_worker(
        deps,
        additional_scripts=(Path(__file__).name, "characterize_minio_fragments.py"),
    )
    if not use_preloaded_bucket:
        s3.make_bucket(BUCKET)
        content = pilot.payload(paths.SIZE)
        old = bytes([content[0] ^ 0xFF]) + content[1:]
        for count, method in ((PUT_KEYS, "put"), (GET_KEYS, "get")):
            for n in range(count):
                serial.put(
                    s3,
                    BUCKET,
                    f"size-{paths.SIZE}/{method}-{n:03d}",
                    content if method == "get" else old,
                )
    order = plan
    if order is None:
        order = []
        for level, rate in enumerate(RATES):
            for rep in range(REPETITIONS):
                modes = (
                    ("write", "mixed") if (level + rep) % 2 == 0 else ("mixed", "write")
                )
                for mode in modes:
                    order.append(
                        dict(
                            rate=rate, rep=rep, mode=mode, seed=SEED + level * 100 + rep
                        )
                    )
    pilot.write(
        output / "protocol.json",
        dict(
            plan=order,
            measured_seconds=MEASUREMENT,
            warmup_seconds=WARMUP,
            arrival="independent Poisson streams",
            equal_put_schedule_within_pair=True,
            payload_bytes=paths.SIZE,
            separate_read_write_keys=True,
            maximum_client_in_flight=MAX_IN_FLIGHT,
            bucket=BUCKET,
            endpoint="observer pod minio4-0 to minio1:9000",
            client_resource_limits=dict(cpu="500m", memory="160Mi"),
            high_rate_guard="stop after a window with S3 or client failures, probe errors or scheduling p95 above 20 ms",
        ),
    )
    summaries = []
    with paths.pod_session(
        output, "", BUCKET, script=Path(__file__).name, timeout=130
    ) as exchange, probe_streams(output) as errors:
        exchange(
            dict(
                warmup=True,
                get=f"size-{paths.SIZE}/get-000",
                put=f"size-{paths.SIZE}/put-000",
            )
        )
        time.sleep(2)
        for n, planned in enumerate(order):
            if require_space:
                wait_for_volume_headroom(output, status, evidence, planned)
            status.update(
                status="SWEEP",
                block=n,
                blocks=len(order),
                condition=planned,
                completed_windows=len(summaries),
            )
            pilot.write(output / "status.json", status)
            block = f"window-{n:02d}"
            serial.snapshot(output, evidence, f"{block}-before")
            request = {k: planned[k] for k in ("rate", "mode", "seed")}
            if "get_rate" in planned:
                request["get_rate"] = planned["get_rate"]
            measured = exchange(request)
            serial.snapshot(output, evidence, f"{block}-after")
            summary = summarize_window(planned, measured)
            summaries.append(dict(block=block, **summary))
            with gzip.open(output / f"{block}.json.gz", "wt") as stream:
                json.dump(measured, stream)
            pilot.write(output / "windows.json", summaries)
            delay = summary["scheduling_delay_p95_ms"]
            if errors or summary["total_failures"] or delay is None or delay > 20:
                return summaries, dict(probe_errors=list(errors), last_window=summary)
            time.sleep(3)
        time.sleep(2)
    return summaries, None


def run(
    output,
    deps,
    binary,
    digest,
    *,
    plan=None,
    use_preloaded_bucket=False,
    require_space=False,
):
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
        status.update(status="PROVISIONING")
        pilot.write(output / "status.json", status)
        fragments.provision(output, original["items"], binary, digest)
        with pilot.Evidence(output) as evidence:
            summaries, stopped = collect(
                output,
                status,
                evidence,
                deps,
                plan=plan,
                use_preloaded_bucket=use_preloaded_bucket,
                require_space=require_space,
            )
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
        status.update(
            status="STOPPED_AT_GUARD" if stopped else "CAPTURED",
            completed_at=pilot.now(),
            completed_windows=len(summaries),
            guard=stopped,
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
        fragments.restore(output)
    return status


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--deps", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--digest")
    parser.add_argument("--finish", action="store_true")
    args = parser.parse_args()
    if args.worker:
        worker()
    elif args.finish:
        fragments.restore(args.output.resolve())
    else:
        if (
            args.output is None
            or args.deps is None
            or args.binary is None
            or args.digest is None
        ):
            parser.error(
                "--output, --deps, --binary and --digest are required for collection"
            )

        def interrupt(*_):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupt)
        run(
            args.output.resolve(),
            args.deps.resolve(),
            args.binary.resolve(),
            args.digest,
        )

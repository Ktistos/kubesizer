#!/usr/bin/env python3
"""Measure serial MinIO overwrites through the existing mio1-r7 ingress."""

import argparse
import gzip
import io
import json
from collections import Counter, defaultdict
from pathlib import Path
import signal
from statistics import median
from subprocess import TimeoutExpired
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_minio_pilot as pilot


BUCKET = "mio1-put-phase-v1"
SIZES = (64 * 1024, 1024 * 1024, 1024 * 1024, 64 * 1024)
REQUESTS_PER_BLOCK = 12
GAP_SECONDS = 0.5
META = {"storage.ReadXL", "storage.ReadVersion"}
READ_MEMBER_COUNTERS = """
for file in /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/cpu/cpu.stat /sys/fs/cgroup/cpu,cpuacct/cpu.stat; do
    if [ -r "$file" ]; then cat "$file"; break; fi
done
if [ -r /sys/fs/cgroup/cpu.max ]; then
    read quota period < /sys/fs/cgroup/cpu.max
    if [ "$quota" = max ]; then quota=-1; fi
    printf 'quota_us %s\\nperiod_us %s\\n' "$quota" "$period"
else
    for dir in /sys/fs/cgroup/cpu /sys/fs/cgroup/cpu,cpuacct; do
        if [ -r "$dir/cpu.cfs_quota_us" ] && [ -r "$dir/cpu.cfs_period_us" ]; then
            printf 'quota_us %s\\nperiod_us %s\\n' "$(cat "$dir/cpu.cfs_quota_us")" "$(cat "$dir/cpu.cfs_period_us")"
            break
        fi
    done
fi
printf '\\nMOUNTINFO\\n'
cat /proc/self/mountinfo
printf '\\nDISKSTATS\\n'
cat /proc/diskstats
"""


def preflight():
    services = (
        pilot.common.COMPARISON,
        pilot.PREDECESSOR,
        "kubesizer-components-cmp1.service",
        "kubesizer-minio-pilot-mio1-r7.service",
    )
    for unit in services:
        state = pilot.command(
            "systemctl", "show", unit, "-p", "ActiveState", "--value"
        ).strip()
        if state not in {"inactive", "failed"}:
            raise RuntimeError(f"Experiment service still active: {unit} ({state})")
    deployments = json.loads(pilot.kube("get", "deployments", "-A", "-o", "json"))[
        "items"
    ]
    if any(
        d["metadata"]["namespace"].endswith("-scv1") and d["spec"].get("replicas", 1)
        for d in deployments
    ):
        raise RuntimeError("Comparison deployment replicas remain active")
    for ns in (
        pilot.NAMESPACE,
        "kubesizer-components-cmp1",
        "kubesizer-postgres-pilot-pgp1",
    ):
        if json.loads(pilot.kube("-n", ns, "get", "pods", "-o", "json"))["items"]:
            raise RuntimeError(f"Experiment pods already present in {ns}")
    layout = json.loads(
        pilot.kube("-n", pilot.NAMESPACE, "get", "statefulsets", "-o", "json")
    )
    members = layout["items"]
    if {m["metadata"]["name"] for m in members} != set(pilot.MEMBERS):
        raise RuntimeError("Unexpected members in the MinIO pilot namespace")
    for m in members:
        spec = m["spec"]
        containers = spec["template"]["spec"]["containers"]
        if (
            m["metadata"].get("labels", {}).get("kubesizer.campaign") != "mio1"
            or spec["replicas"] != 0
            or not any(
                c["name"] == "minio" and c["image"] == pilot.IMAGE for c in containers
            )
        ):
            raise RuntimeError("MinIO pilot owner, image or stopped state differs")
    return layout


def member_counters(text):
    cpu, text = text.split("\nMOUNTINFO\n", 1)
    mounts, stats = text.split("\nDISKSTATS\n", 1)
    cpu_stat = {
        key: int(value)
        for key, value in (line.split() for line in cpu.splitlines() if line.strip())
    }
    device = next(
        (
            parts[2]
            for parts in map(str.split, mounts.splitlines())
            if len(parts) > 4 and parts[4] == "/data"
        ),
        None,
    )
    row = next(
        (
            parts
            for parts in map(str.split, stats.splitlines())
            if len(parts) >= 14 and ":".join(parts[:2]) == device
        ),
        None,
    )
    volume_stat = None
    if row:
        volume_stat = dict(
            device=device,
            reads=int(row[3]),
            read_ms=int(row[6]),
            writes=int(row[7]),
            write_ms=int(row[10]),
            busy_ms=int(row[12]),
            weighted_ms=int(row[13]),
        )
    return cpu_stat, volume_stat


def snapshot(output, evidence, name):
    result = pilot.safe_snapshot(evidence)
    for member in pilot.MEMBERS:
        try:
            text = pilot.kube(
                "-n",
                pilot.NAMESPACE,
                "exec",
                f"{member}-0",
                "-c",
                "minio",
                "--",
                "sh",
                "-c",
                READ_MEMBER_COUNTERS,
                timeout=15,
            )
            cpu_stat, volume_stat = member_counters(text)
            counters = result["members"][member]
            counters["cgroup_cpu_stat"] = cpu_stat
            if "nr_throttled" not in cpu_stat:
                result["errors"].append(
                    dict(member=member, source="cpu.stat", error_type="MissingCounters")
                )
            if volume_stat is not None:
                counters["volume_stat"] = volume_stat
            else:
                result["errors"].append(
                    dict(
                        member=member, source="diskstats", error_type="MissingCounters"
                    )
                )
        except (RuntimeError, ValueError, KeyError, OSError, TimeoutExpired) as error:
            result["errors"].append(
                dict(member=member, source="cpu.stat", error_type=type(error).__name__)
            )
    pilot.write(output / "snapshots" / f"{name}.json", result)


def put(s3, bucket, key, content):
    start_epoch, start = time.time(), time.monotonic()
    result = s3.put_object(
        bucket, key, io.BytesIO(content), len(content), num_parallel_uploads=1
    )
    end = time.monotonic()
    return dict(
        key=key,
        request_id=result.http_headers.get("x-amz-request-id"),
        bytes=len(content),
        start_epoch=start_epoch,
        end_epoch=time.time(),
        seconds=end - start,
    )


def verify(s3, bucket, key, content):
    response = s3.get_object(bucket, key)
    try:
        if response.read() != content:
            raise RuntimeError(f"Readback differs for {key}")
    finally:
        response.close()
        response.release_conn()


def correlate(rows, trace, bucket=BUCKET):
    """Join object paths, using a rename's temporary ID for fragment creates."""
    keys = {row["key"] for row in rows}
    by_key, temp_keys = defaultdict(list), defaultdict(set)
    creates = []
    for record in trace:
        parts = record["path"].split()
        api = record["api"]
        if (
            api == "storage.CreateFile"
            and len(parts) >= 3
            and parts[1] == ".minio.sys/tmp"
        ):
            creates.append((parts[2].split("/", 1)[0], record))
        elif bucket in parts:
            i = parts.index(bucket)
            if i + 1 == len(parts):
                continue
            key = "/".join(parts[i + 1].split("/")[:2])
            if key not in keys:
                continue
            by_key[key].append(record)
            if api == "storage.RenameData" and i == 3:
                temp_keys[parts[1]].add(key)
    for tmp, record in creates:
        if len(temp_keys[tmp]) == 1:
            by_key[next(iter(temp_keys[tmp]))].append(record)
    return {
        key: sorted(by_key[key], key=lambda r: (r["time"], r["host"], r["api"]))
        for key in sorted(keys)
    }


def phase_report(output, rows, bucket):
    with gzip.open(output / "storage-trace.jsonl.gz", "rt") as stream:
        calls = correlate(rows, map(json.loads, stream), bucket)
    pilot.write(output / "request-storage-calls.json", calls)
    rows_by_size = defaultdict(list)
    for row in rows:
        rows_by_size[row["bytes"]].append(row)
    summary = {}
    for size, group in sorted(rows_by_size.items()):
        all_calls = [record for row in group for record in calls[row["key"]]]
        summary[size] = dict(
            requests=len(group),
            client_mean_seconds=sum(r["seconds"] for r in group) / len(group),
            phase_coverage={
                phase: dict(
                    requests_with_records=sum(
                        any(r["api"] in apis for r in calls[row["key"]])
                        for row in group
                    ),
                    calls_by_member=dict(
                        Counter(r["host"] for r in all_calls if r["api"] in apis)
                    ),
                    median_call_seconds=(
                        median(values) / 1e9
                        if (
                            values := [
                                r["duration"] for r in all_calls if r["api"] in apis
                            ]
                        )
                        else None
                    ),
                )
                for phase, apis in (
                    ("metadata", META),
                    ("fragment_creation", {"storage.CreateFile"}),
                    ("finalization", {"storage.RenameData"}),
                )
            },
        )
    pilot.write(output / "phase-coverage.json", summary)
    ids = {row["request_id"] for row in rows}
    if None in ids or "" in ids or len(ids) != len(rows):
        raise RuntimeError("Missing or duplicate S3 response request IDs")
    for row in rows:
        required = [{"storage.RenameData"}]
        if row["bytes"] >= 1024 * 1024:
            required.append({"storage.CreateFile"})
        for apis in required:
            hosts = {
                r["host"].split(":", 1)[0]
                for r in calls[row["key"]]
                if r["api"] in apis and not r.get("error")
            }
            if hosts != set(pilot.MEMBERS):
                raise RuntimeError(
                    f"Incomplete {apis} coverage for {row['key']}: {sorted(hosts)}"
                )


def run(output, bucket):
    output.mkdir(parents=True, exist_ok=False)
    status = dict(status="PREFLIGHT", started_at=pilot.now())
    pilot.write(output / "status.json", status)
    pilot.write(
        output / "protocol.json",
        dict(
            purpose="characterization",
            bucket=bucket,
            layout="existing four-member EC:2 mio1-r7",
            sizes_bytes=SIZES,
            overwrites_per_block=REQUESTS_PER_BLOCK,
            post_request_gap_seconds=GAP_SECONDS,
            boundary="VM client through the r7 ingress to PUT acknowledgement; separate full GET readback",
            target_data_used=False,
            workload="serial overwrites with unique object keys",
        ),
    )
    try:
        pilot.write(output / "original-statefulsets.json", preflight())
        pilot.write(
            output / "comparison-pause.json",
            dict(cleanup_required=True, resume_required=False),
        )
        for name in pilot.MEMBERS:
            pilot.kube(
                "-n", pilot.NAMESPACE, "scale", "statefulset", name, "--replicas=1"
            )
        status.update(status="PROVISIONING")
        pilot.write(output / "status.json", status)
        for name in pilot.MEMBERS:
            pilot.kube(
                "-n",
                pilot.NAMESPACE,
                "rollout",
                "status",
                f"statefulset/{name}",
                "--timeout=600s",
                timeout=620,
            )
        pilot.write(
            output / "deployment-context.json",
            json.loads(
                pilot.kube(
                    "-n",
                    pilot.NAMESPACE,
                    "get",
                    "statefulsets,pods,persistentvolumeclaims,ingress",
                    "-o",
                    "json",
                )
            ),
        )
        rows = []
        blocks = [
            (
                pilot.payload(size),
                [
                    f"size-{size}/block-{block}-key-{i:02d}"
                    for i in range(REQUESTS_PER_BLOCK)
                ],
            )
            for block, size in enumerate(SIZES)
        ]
        with pilot.Evidence(output) as evidence:
            pilot.wait_for_storage(output, [pilot.client(29400 + i) for i in range(4)])
            s3 = pilot.client()
            if s3.bucket_exists(bucket):
                raise RuntimeError("Use a fresh characterization bucket")
            s3.make_bucket(bucket)
            for content, keys in blocks:
                old_content = bytes([content[0] ^ 0xFF]) + content[1:]
                for key in keys:
                    put(s3, bucket, key, old_content)
            evidence.trace()
            ready = time.monotonic() + 25
            while (
                not evidence.trace_count
                and not evidence.trace_error
                and time.monotonic() < ready
            ):
                put(s3, bucket, "preflight", blocks[0][0])
                time.sleep(1)
            if evidence.trace_error or not evidence.trace_count:
                raise RuntimeError(f"Storage trace unavailable: {evidence.trace_error}")
            snapshot(output, evidence, "idle-before")
            time.sleep(8)
            snapshot(output, evidence, "idle-after")
            with (output / "put-requests.jsonl").open("w") as stream:
                for block, (content, keys) in enumerate(blocks):
                    status.update(
                        status="CHARACTERIZING",
                        current_block=block,
                        completed=len(rows),
                    )
                    pilot.write(output / "status.json", status)
                    snapshot(output, evidence, f"block-{block}-before")
                    for key in keys:
                        row = dict(block=block, **put(s3, bucket, key, content))
                        stream.write(json.dumps(row) + "\n")
                        stream.flush()
                        rows.append(row)
                        if evidence.trace_error:
                            raise RuntimeError(evidence.trace_error)
                        time.sleep(GAP_SECONDS)
                    snapshot(output, evidence, f"block-{block}-after")
            time.sleep(3)
            if evidence.trace_error or evidence.processes[-1][0].poll() is not None:
                raise RuntimeError(f"Storage trace interrupted: {evidence.trace_error}")
        status.update(status="VERIFYING", completed=len(rows))
        pilot.write(output / "status.json", status)
        for content, keys in blocks:
            for key in keys:
                verify(s3, bucket, key, content)
        phase_report(output, rows, bucket)
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
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--bucket", default=BUCKET)
    args = parser.parse_args()

    def interrupt(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    run(args.output.resolve(), args.bucket)

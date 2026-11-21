#!/usr/bin/env python3
"""Analyze matched PUT-rate windows and active receiver streams in the MinIO sweep."""

import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
from statistics import fmean, quantiles
import tarfile


MEMBERS = ("minio1", "minio2", "minio3", "minio4")
NS = 1_000_000_000
EDGE_SECONDS = 2


def fragment_window(records, start, stop):
    completed, events = [], Counter()
    exposure = 0
    for row in records:
        begin = row["start_ns"]
        end = begin + row["elapsed_ns"]
        if start <= end < stop and row["success"]:
            completed.append(row)
        lo, hi = max(start, begin), min(stop, end)
        if lo < hi:
            events[lo] += 1
            events[hi] -= 1
            exposure += hi - lo
    duration = (stop - start) / NS
    active, peak = 0, 0
    for _, delta in sorted(events.items()):
        active += delta
        peak = max(peak, active)
    return completed, dict(
        completed_fragments=len(completed),
        fragments_per_second=len(completed) / duration,
        mib_per_second=sum(r["write_bytes"] for r in completed) / (2**20 * duration),
        mean_active_streams=exposure / (stop - start),
        max_active_streams=peak,
    )


def cpu_delta(before, after):
    fields = ("nr_periods", "nr_throttled")
    if any(field not in before or field not in after for field in fields):
        return None
    periods, throttled = (after[field] - before[field] for field in fields)
    return throttled / periods if periods > 0 and throttled >= 0 else None


def analyze(archive, *, include_phase_spans=False, skip_failed_windows=False):
    with tarfile.open(archive) as tar:

        def raw(name):
            with tar.extractfile(f"./{name}") as stream:
                return stream.read()

        def read(name):
            return json.loads(raw(name))

        receipt = read("evidence-receipt.json")
        for name, expected in receipt.items():
            if hashlib.sha256(raw(name)).hexdigest() != expected:
                raise ValueError(f"Evidence hash differs for {name}")
        status, protocol = read("status.json"), read("protocol.json")
        if (
            status["status"] not in {"CAPTURED", "STOPPED_AT_GUARD"}
            or not read("restore-template.json")["restored"]
        ):
            raise ValueError("Sweep collection or restoration is incomplete")
        if read("probe-status.json")["errors"]:
            raise ValueError("A streamed fragment log failed")
        receiver = defaultdict(list)
        by_fragment = defaultdict(list)
        for member in MEMBERS:
            for line in gzip.decompress(raw(f"probe-{member}.jsonl.gz")).splitlines():
                record = dict(member=member, **json.loads(line))
                by_fragment[record["temporary_path"]].append(record)
                if record["side"] == "receive":
                    receiver[member].append(record)
        windows, put_schedules = [], {}
        warmup = protocol["warmup_seconds"]
        measured = protocol["measured_seconds"]
        duration = measured - 2 * EDGE_SECONDS
        for planned, summary in zip(protocol["plan"], read("windows.json")):
            if any(summary[key] != value for key, value in planned.items()):
                raise ValueError("Retained run differs from the planned sequence")
            if skip_failed_windows and summary["total_failures"]:
                continue
            block = summary["block"]
            payload = json.loads(gzip.decompress(raw(f"{block}.json.gz")))
            snapshots = [
                read(f"snapshots/{block}-{when}.json") for when in ("before", "after")
            ]
            errors = [error for s in snapshots for error in s["errors"]]
            clock_gap = max(
                abs(s["members"][m]["epoch"] - s["epoch"])
                for s in snapshots
                for m in MEMBERS
            )
            other_node_gap = max(
                abs(s["members"][m]["epoch"] - s["members"]["minio4"]["epoch"])
                for s in snapshots
                for m in MEMBERS
            )
            lower, upper = warmup + EDGE_SECONDS, warmup + measured - EDGE_SECONDS
            start = payload["started_epoch_ns"] + lower * NS
            stop = start + duration * NS
            puts = [r for r in payload["rows"] if r["method"] == "PUT"]
            pair = summary["rate"], summary["rep"]
            schedule = [(r["scheduled_seconds"], r["key"]) for r in puts]
            if put_schedules.setdefault(pair, schedule) != schedule:
                raise ValueError("PUT arrival schedules differ within a paired run")
            put_offered = sum(lower <= r["scheduled_seconds"] < upper for r in puts)
            put_completed = sum(
                r["success"] and start <= r["end_epoch_ns"] < stop for r in puts
            )
            phases, per_member = [], {}
            for member in MEMBERS:
                done, metrics = fragment_window(receiver[member], start, stop)
                phases.extend(done)
                before, after = (s["members"][member] for s in snapshots)
                cpu = None
                if (
                    len(before.get("processes", []))
                    == len(after.get("processes", []))
                    == 1
                ):
                    x, y = before["processes"][0], after["processes"][0]
                    elapsed = after["epoch"] - before["epoch"]
                    if (x["pid"], x["start_ticks"]) == (
                        y["pid"],
                        y["start_ticks"],
                    ) and elapsed > 0:
                        cpu = (y["cpu_seconds"] - x["cpu_seconds"]) / elapsed
                per_member[member] = dict(
                    **metrics,
                    process_cpu_cores=cpu,
                    cpu_throttled_period_fraction=cpu_delta(
                        before.get("cgroup_cpu_stat", {}),
                        after.get("cgroup_cpu_stat", {}),
                    ),
                )
            paths = {row["temporary_path"] for row in phases}
            incomplete = 0
            for temporary in paths:
                records = by_fragment[temporary]
                receives = [
                    r for r in records if r["side"] == "receive" and r["success"]
                ]
                sends = [r for r in records if r["side"] == "send" and r["success"]]
                if (
                    Counter(r["member"] for r in receives) != dict.fromkeys(MEMBERS, 1)
                    or Counter(r["peer"].split(":", 1)[0] for r in sends)
                    != dict.fromkeys(MEMBERS[1:], 1)
                    or any(
                        r["read_bytes"] != r["size"] or r["write_bytes"] != r["size"]
                        for r in receives
                    )
                    or any(r["read_bytes"] != r["size"] for r in sends)
                ):
                    incomplete += 1
            phase_mean = (
                {
                    field.replace("_ns", "_ms"): fmean(row[field] for row in phases)
                    / 1e6
                    for field in ("elapsed_ns", "read_ns", "write_ns", "flush_ns")
                }
                if phases
                else {}
            )
            reads = [r["read_ns"] / 1e6 for r in phases]
            windows.append(
                dict(
                    **summary,
                    guarded_seconds=duration,
                    put_offered_per_second=put_offered / duration,
                    put_completed_per_second=put_completed / duration,
                    per_member=per_member,
                    completed_fragments=len(phases),
                    equivalent_put_fragment_rate=len(phases)
                    / (len(MEMBERS) * duration),
                    fragment_paths_in_window=len(paths),
                    incomplete_fragments=incomplete,
                    receiver_phase_mean_ms=phase_mean,
                    receiver_read_p95_ms=(
                        quantiles(reads, n=20, method="inclusive")[-1]
                        if len(reads) > 1
                        else None
                    ),
                    telemetry_errors=errors,
                    snapshot_epoch_gap_seconds=clock_gap,
                    approximate_inter_member_epoch_gap_seconds=other_node_gap,
                    client_cpu_throttled_period_fraction=cpu_delta(
                        payload["client_cpu_before"], payload["client_cpu_after"]
                    ),
                )
            )
            if include_phase_spans:
                spans = []
                for temporary in paths:
                    local = [
                        r for r in by_fragment[temporary] if r["member"] == "minio1"
                    ]
                    if len(local) != 4 or any(not r["success"] for r in local):
                        raise ValueError("Incomplete coordinator fragment span")
                    first = min(r["start_ns"] for r in local)
                    last = max(r["start_ns"] + r["elapsed_ns"] for r in local)
                    if start <= first and last < stop:
                        spans.append((last - first) / 1e6)
                bodies = [
                    (r["last_byte_epoch_ns"] - r["headers_epoch_ns"]) / 1e6
                    for r in payload["rows"]
                    if r["method"] == "GET"
                    and r["success"]
                    and start <= r["headers_epoch_ns"]
                    and r["last_byte_epoch_ns"] < stop
                ]
                windows[-1]["phase_spans"] = dict(
                    put_ms=fmean(spans),
                    put_count=len(spans),
                    get_body_ms=fmean(bodies) if bodies else None,
                    get_body_count=len(bodies),
                )
    with archive.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return dict(
        archive_sha256=digest,
        verified_files=len(receipt),
        status=status,
        windows=windows,
        edge_seconds=EDGE_SECONDS,
        warning="Per-node probe windows use approximate clock alignment. Snapshot epochs include retrieval time.",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.archive), indent=2, allow_nan=False))

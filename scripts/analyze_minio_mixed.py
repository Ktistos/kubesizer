#!/usr/bin/env python3
"""Summarize client, storage-trace, and block-device evidence for the mixed pilot."""

import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
from statistics import fmean, median
import tarfile


def analyze(archive):
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
            status["status"] != "CAPTURED"
            or read("comparison-pause.json")["cleanup_required"]
        ):
            raise ValueError("Expected completed collection and cleanup")
        rows = [json.loads(line) for line in raw("requests.jsonl").splitlines()]
        calls = read("request-storage-calls.json")
        fields = protocol["counter_fields"]
        volumes = {
            member: [
                json.loads(line) for line in raw(f"volume-{member}.jsonl").splitlines()
            ]
            for member in read("volume-devices.json")
        }
    timestamps = {
        member: [sample[0] for sample in data] for member, data in volumes.items()
    }
    counter_indices = {
        name: fields.index(name) + 1
        for name in (
            "reads",
            "sectors_read",
            "read_ms",
            "writes",
            "sectors_written",
            "write_ms",
            "busy_ms",
            "in_flight",
        )
    }
    for member, stamps in timestamps.items():
        if len(stamps) < 2 or any(b <= a for a, b in zip(stamps, stamps[1:])):
            raise ValueError(f"Invalid sampling timeline on {member}")
        for name, idx in counter_indices.items():
            if name != "in_flight" and any(
                b[idx] < a[idx] for a, b in zip(volumes[member], volumes[member][1:])
            ):
                raise ValueError(f"Counter {name} reset on {member}")

    def within(member, begin, end):
        stamps = timestamps[member]
        lo, hi = bisect_left(stamps, begin), bisect_right(stamps, end)
        return volumes[member][lo:hi]

    def deltas(data):
        return {
            name: data[-1][idx] - data[0][idx]
            for name, idx in counter_indices.items()
            if name != "in_flight"
        }

    def start_ns(call):
        return round(
            datetime.fromisoformat(call["time"].replace("Z", "+00:00")).timestamp()
            * 1e9
        )

    by_block, by_pair = defaultdict(list), defaultdict(list)
    for row in rows:
        by_block[row["block"]].append(row)
        if row["mode"] == "mixed":
            by_pair[row["block"], row["round"]].append(row)
    if len(rows) != len(calls) or len({r["key"] for r in rows}) != len(rows):
        raise ValueError("Requests and trace associations differ")
    blocks = []
    phase_stats = defaultdict(list)
    block_paths = protocol.get("block_paths", [""] * len(protocol["blocks"]))
    if len(block_paths) != len(protocol["blocks"]):
        raise ValueError("Client paths do not match the block schedule")
    for block, (repetition, size, mode) in enumerate(protocol["blocks"]):
        group = by_block[block]
        path = block_paths[block]
        if any(row.get("path", "") != path for row in group):
            raise ValueError(f"Unexpected client path in block {block}")
        methods = (
            ("GET", "PUT")
            if mode == "mixed"
            else (("GET",) if mode == "read" else ("PUT",))
        )
        if Counter(row["method"] for row in group) != {
            method: protocol["rounds_per_block"] for method in methods
        }:
            raise ValueError(f"Missing observations in block {block}")
        per_method = {}
        block_calls = [call for row in group for call in calls[row["key"]]]
        for method in methods:
            requests = [r for r in group if r["method"] == method]
            api_counts = Counter(
                call["api"] for row in requests for call in calls[row["key"]]
            )
            timing = {}
            if method == "GET":
                timing["mean_last_byte_ms"] = 1000 * fmean(
                    r["last_byte_seconds"] for r in requests
                )
                timing["mean_after_headers_ms"] = (
                    fmean(
                        r["last_byte_epoch_ns"] - r["headers_epoch_ns"]
                        for r in requests
                    )
                    / 1e6
                )
            else:
                timing["mean_client_socket_body_ms"] = (
                    fmean(
                        r["body_sent_epoch_ns"] - r["headers_sent_epoch_ns"]
                        for r in requests
                    )
                    / 1e6
                )
            per_method[method] = dict(
                n=len(requests),
                mean_ms=1000 * fmean(r["seconds"] for r in requests),
                median_ms=1000 * median(r["seconds"] for r in requests),
                mean_excluding_first_round_ms=1000
                * fmean(r["seconds"] for r in requests if r["round"] != 0),
                storage_calls=dict(api_counts),
                **timing,
            )
        per_member = {}
        for member in volumes:
            member_calls = [
                call for call in block_calls if call["host"].split(":", 1)[0] == member
            ]
            if not member_calls:
                continue
            # All anchors here come from the volume's own node clock.
            begin = min(map(start_ns, member_calls)) - 100_000_000
            end = (
                max(start_ns(call) + call["duration"] for call in member_calls)
                + 100_000_000
            )
            samples = within(member, begin, end)
            if len(samples) < 2:
                raise ValueError(f"No volume samples for {member} in block {block}")
            per_member[member] = dict(
                **deltas(samples),
                max_sampled_in_flight=max(
                    sample[counter_indices["in_flight"]] for sample in samples
                ),
                sample_count=len(samples),
            )
            for call in member_calls:
                api = call["api"]
                if api not in {"storage.CreateFile", "storage.RenameData"}:
                    continue
                start = start_ns(call)
                samples = within(member, start, start + call["duration"])
                activity = deltas(samples) if len(samples) >= 2 else None
                phase_stats[path, size, mode, api].append(
                    (call["duration"] / 1e6, activity)
                )
        blocks.append(
            dict(
                block=block,
                rep=repetition,
                size_bytes=size,
                mode=mode,
                timings=per_method,
                volume_changes=per_member,
                **({"path": path} if path else {}),
            )
        )

    overlaps, transfer_overlaps = [], []
    for pair in by_pair.values():
        if len(pair) != 2:
            raise ValueError("A paired round is incomplete")
        overlaps.append(
            max(
                0,
                min(r["end_epoch_ns"] for r in pair)
                - max(r["start_epoch_ns"] for r in pair),
            )
            / 1e6
        )
        get = next(row for row in pair if row["method"] == "GET")
        put = next(row for row in pair if row["method"] == "PUT")
        transfer_overlaps.append(
            max(
                0,
                min(get["last_byte_epoch_ns"], put["end_epoch_ns"])
                - max(get["headers_epoch_ns"], put["body_sent_epoch_ns"]),
            )
            / 1e6
        )
    phase_activity = []
    for (path, size, mode, api), data in sorted(phase_stats.items()):
        covered = [activity for _, activity in data if activity is not None]
        phase_activity.append(
            dict(
                size_bytes=size,
                mode=mode,
                api=api,
                calls=len(data),
                median_call_ms=median(length for length, _ in data),
                calls_with_internal_samples=len(covered),
                calls_with_internal_write_completions=sum(
                    a["writes"] > 0 for a in covered
                ),
                median_internal_volume_busy_ms=(
                    median(a["busy_ms"] for a in covered) if covered else None
                ),
                **({"path": path} if path else {}),
            )
        )
    return dict(
        archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
        verified_files=len(receipt),
        requests=len(rows),
        blocks=blocks,
        pair_count=len(overlaps),
        positive_overlap_pairs=sum(o > 0 for o in overlaps),
        median_pair_overlap_ms=median(overlaps),
        pairs_with_get_body_and_put_after_send_overlap=sum(
            o > 0 for o in transfer_overlaps
        ),
        median_get_body_and_put_after_send_overlap_ms=median(transfer_overlaps),
        same_node_trace_activity=phase_activity,
        volume_sample_median_gap_ms={
            member: median(b - a for a, b in zip(stamps, stamps[1:])) / 1e6
            for member, stamps in timestamps.items()
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.archive), indent=2, allow_nan=False))

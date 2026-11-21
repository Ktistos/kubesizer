#!/usr/bin/env python3
"""Join instrumented MinIO I/O timings to verified benchmark PUTs."""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
from statistics import fmean, median
import tarfile


MEMBERS = {"minio1", "minio2", "minio3", "minio4"}
RECEIVER_FIELDS = ("elapsed_ns", "read_ns", "write_ns", "flush_ns", "other_ns")


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
        if (
            read("status.json")["status"] != "CAPTURED"
            or not read("restore-template.json")["restored"]
        ):
            raise ValueError(
                "The pilot must finish collection and template restoration"
            )
        protocol = read("protocol.json")
        rows = [json.loads(line) for line in raw("requests.jsonl").splitlines()]
        calls = read("request-storage-calls.json")
        probes = read("probe-records.json")
        snapshot_errors = [
            error
            for when in ("before", "after")
            for error in read(f"snapshots/{when}.json")["errors"]
        ]

    receivers, senders = defaultdict(list), defaultdict(list)
    for record in probes:
        if record["side"] == "receive":
            receivers[record["temporary_path"], record["member"]].append(record)
        elif record["side"] == "send":
            senders[record["temporary_path"], record["peer"].split(":", 1)[0]].append(
                record
            )
        else:
            raise ValueError(f"Unknown probe side {record['side']}")

    def verify_receiver(receive, call):
        if (
            not receive["success"]
            or receive["read_bytes"] != call["size"]
            or receive["write_bytes"] != call["size"]
            or receive["size"] != call["size"]
        ):
            raise ValueError(
                f"Receiver byte count differs for {receive['temporary_path']}"
            )
        receive["other_ns"] = receive["elapsed_ns"] - sum(
            receive[x] for x in ("read_ns", "write_ns", "flush_ns")
        )
        if receive["other_ns"] < 0 or any(receive[x] < 0 for x in RECEIVER_FIELDS):
            raise ValueError("Receiver phase timings exceed the call time")
        return receive

    if len(rows) != len(calls) or len({r["key"] for r in rows}) != len(rows):
        raise ValueError("Request and trace associations differ")
    by_block, received_by_block, sent_by_block = (
        defaultdict(list),
        defaultdict(list),
        defaultdict(list),
    )
    request_probe_groups = {}
    last_branch_receivers = defaultdict(list)
    for row in rows:
        block = row["block"]
        by_block[block].append(row)
        if row["method"] != "PUT":
            continue
        storage = calls[row["key"]]
        fragments = [
            record
            for record in storage
            if record["api"] == "storage.CreateFile" and not record.get("error")
        ]
        hosts = {record["host"].split(":", 1)[0] for record in fragments}
        if hosts != MEMBERS or len(fragments) != len(MEMBERS):
            raise ValueError(f"Unresolved fragment calls for {row['key']}")
        receive_group, send_group = [], []
        for call in fragments:
            member = call["host"].split(":", 1)[0]
            temporary = call["path"].split()[2]
            receives, sends = receivers[temporary, member], senders[temporary, member]
            if row["bucket"] == protocol["control_bucket"]:
                if receives or sends:
                    raise ValueError(
                        f"The control PUT had active timing probes: {row['key']}"
                    )
                continue
            if len(receives) != 1:
                raise ValueError(
                    f"Receiver coverage is incomplete for {row['key']}/{member}"
                )
            receive_group.append(verify_receiver(receives[0], call))
            if member == "minio1":
                if sends:
                    raise ValueError(
                        f"Unexpected peer send to the coordinator for {row['key']}"
                    )
            elif (
                len(sends) != 1
                or not sends[0]["success"]
                or sends[0]["member"] != "minio1"
                or sends[0]["read_bytes"] != call["size"]
            ):
                raise ValueError(
                    f"Sender coverage is incomplete for {row['key']}/{member}"
                )
            else:
                send_group.append(sends[0])
        if receive_group:
            local = next(
                probe for probe in receive_group if probe["member"] == "minio1"
            )
            last = max(
                [local, *send_group],
                key=lambda probe: probe["start_ns"] + probe["elapsed_ns"],
            )
            member = last.get("peer", "minio1").split(":", 1)[0]
            last_branch_receivers[block].append(
                next(probe for probe in receive_group if probe["member"] == member)
            )
            request_probe_groups[row["key"]] = dict(
                receivers=receive_group,
                senders=send_group,
                last_observed_fragment_completion_member=member,
            )
            received_by_block[block].append(receive_group)
            sent_by_block[block].extend(send_group)

    def timing_means(data, fields):
        return {
            field.replace("_ns", "_ms"): fmean(row[field] for row in data) / 1e6
            for field in fields
        }

    block_summaries = []
    for block, (repetition, bucket, mode) in enumerate(protocol["blocks"]):
        group = by_block[block]
        methods = ("GET", "PUT") if mode == "mixed" else ("PUT",)
        if Counter(r["method"] for r in group) != {
            m: protocol["rounds_per_block"] for m in methods
        }:
            raise ValueError(f"Incomplete operation counts in block {block}")
        client_ms = {
            m: 1000 * fmean(r["seconds"] for r in group if r["method"] == m)
            for m in methods
        }
        read_body_ms = [
            r["last_byte_epoch_ns"] - r["headers_epoch_ns"]
            for r in group
            if r["method"] == "GET"
        ]
        receive_groups, sends = received_by_block[block], sent_by_block[block]
        receives = [record for group in receive_groups for record in group]
        slowest_receives = [
            max(group, key=lambda r: r["elapsed_ns"]) for group in receive_groups
        ]
        result = dict(
            block=block,
            repetition=repetition,
            bucket=bucket,
            mode=mode,
            client_mean_ms=client_ms,
            get_body_mean_ms=fmean(read_body_ms) / 1e6 if read_body_ms else None,
            fragment_receiver_records=len(receives),
            fragment_sender_records=len(sends),
        )
        if receives:
            result["receiver_phase_mean_ms"] = timing_means(receives, RECEIVER_FIELDS)
            result["slowest_receiver_phase_mean_ms"] = timing_means(
                slowest_receives, RECEIVER_FIELDS
            )
            result["last_completion_receiver_phase_mean_ms"] = timing_means(
                last_branch_receivers[block], RECEIVER_FIELDS
            )
            result["sender_stream_mean_ms"] = timing_means(
                sends, ("elapsed_ns", "read_ns")
            )
            result["direct_io_receiver_records"] = sum(r["direct_io"] for r in receives)
            result["receiver_means_by_member"] = {
                member: timing_means(
                    [r for r in receives if r["member"] == member], RECEIVER_FIELDS
                )
                for member in sorted(MEMBERS)
            }
        block_summaries.append(result)
    by_condition = {
        (b["repetition"], b["bucket"], b["mode"]): b for b in block_summaries
    }
    paired = {}
    for bucket in (protocol["control_bucket"], protocol["profiled_bucket"]):
        differences = []
        for rep in sorted({b["repetition"] for b in block_summaries}):
            alone = by_condition[rep, bucket, "write"]
            mixed = by_condition[rep, bucket, "mixed"]
            row = dict(
                repetition=rep,
                client_put_delta_ms=mixed["client_mean_ms"]["PUT"]
                - alone["client_mean_ms"]["PUT"],
            )
            for field in (
                "receiver_phase_mean_ms",
                "last_completion_receiver_phase_mean_ms",
                "sender_stream_mean_ms",
            ):
                if field in alone:
                    row[field] = {
                        name: value - alone[field][name]
                        for name, value in mixed[field].items()
                    }
            differences.append(row)
        effects = [row["client_put_delta_ms"] for row in differences]
        paired[bucket] = dict(
            positive_put_delta_groups=sum(value > 0 for value in effects),
            mean_put_delta_ms=fmean(effects),
            median_put_delta_ms=median(effects),
            differences=differences,
        )
    with archive.open("rb") as stream:
        archive_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    return dict(
        archive_sha256=archive_hash,
        verified_files=len(receipt),
        requests=len(rows),
        profiled_puts=len(request_probe_groups),
        snapshot_errors=snapshot_errors,
        blocks=block_summaries,
        paired_group_differences=paired,
        matched_probes_by_key=request_probe_groups,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.archive), indent=2, allow_nan=False))

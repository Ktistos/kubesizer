#!/usr/bin/env python3
"""Verify and summarize matched MinIO CPU quota intervention arms."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import tarfile
import tempfile

from analyze_minio_sweep import MEMBERS, analyze as analyze_sweep


def read(path):
    return json.loads(path.read_text())


def analyze(archive):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        with tarfile.open(archive) as evidence:
            evidence.extractall(root, filter="data")
        plan = read(root / "campaign-plan.json")["plan"]
        status = read(root / "campaign-status.json")
        if status["status"] not in {"CAPTURED", "STOPPED_AT_GUARD"}:
            raise ValueError("Campaign has no terminal collection outcome")
        if status["status"] == "CAPTURED" and len(status["completed"]) != len(plan):
            raise ValueError("One or more CPU intervention arms are missing")
        arms, schedules = [], {}
        for completed, planned in zip(
            status["completed"], plan, strict=status["status"] == "CAPTURED"
        ):
            if any(completed[key] != value for key, value in planned.items()):
                raise ValueError(
                    "Retained campaign order differs from the planned order"
                )
            arm = root / planned["name"]
            arm_archive = root / "arm.tar.gz"
            with tarfile.open(arm_archive, "w:gz") as stream:
                stream.add(arm, arcname=".")
            result = analyze_sweep(arm_archive)
            arm_archive.unlink()
            (window,) = result["windows"]
            if window["telemetry_errors"] or window["incomplete_fragments"]:
                raise ValueError(f"Incomplete hardware or probe evidence in {arm.name}")
            if any(
                window[k] != planned[k]
                for k in ("entry_cores", "rep", "rate", "seed", "mode")
            ):
                raise ValueError("Window and treatment metadata differ")
            requested = planned["entry_cores"]
            quota = {}
            for suffix in ("before", "after"):
                snapshot = read(arm / "snapshots" / f"{window['block']}-{suffix}.json")
                verified = {}
                for member in MEMBERS:
                    stats = snapshot["members"][member]["cgroup_cpu_stat"]
                    actual = stats["quota_us"] / stats["period_us"]
                    expected = requested if member == "minio1" else 1
                    if actual != expected:
                        raise ValueError(
                            f"{arm.name}: unexpected cgroup CPU quota on {member}: {actual}"
                        )
                    verified[member] = {
                        "quota_us": stats["quota_us"],
                        "period_us": stats["period_us"],
                        "cores": actual,
                    }
                quota[suffix] = verified
            with gzip.open(arm / f"{window['block']}.json.gz", "rt") as stream:
                rows = json.load(stream)["rows"]
            key = requested, planned["rep"]
            schedules[key] = [
                (r["method"], r["key"], r["scheduled_seconds"], r["tagged"])
                for r in rows
            ]
            arms.append(
                dict(
                    name=planned["name"],
                    outcome=completed["outcome"],
                    verified_cgroup_quota=quota,
                    archive_files_verified=result["verified_files"],
                    **window,
                )
            )

    by_condition = {(a["entry_cores"], a["rep"]): a for a in arms}
    comparisons = []
    for rep in sorted({a["rep"] for a in arms}):
        low, high = (by_condition.get((cpu, rep)) for cpu in (1, 2))
        if not low or not high:
            continue
        if schedules[1, rep] != schedules[2, rep]:
            raise ValueError(
                "Matched CPU intervention arms have different arrival schedules"
            )
        cpu_low, cpu_high = low["per_member"]["minio1"], high["per_member"]["minio1"]
        upper_latency = {m: high["classes"][m]["mean_ms"] for m in ("GET", "PUT")}
        lower_latency = {m: low["classes"][m]["mean_ms"] for m in ("GET", "PUT")}
        comparisons.append(
            dict(
                repetition=rep,
                effect="two CPU cores minus one CPU core",
                s3_mean_ms_delta={
                    m: upper_latency[m] - lower_latency[m] for m in upper_latency
                },
                fragment_mean_phase_ms_delta={
                    k: high["receiver_phase_mean_ms"][k] - v
                    for k, v in low["receiver_phase_mean_ms"].items()
                },
                fragment_throughput_equivalent_put_per_s_delta=(
                    high["equivalent_put_fragment_rate"]
                    - low["equivalent_put_fragment_rate"]
                ),
                entry_cpu_throttled_period_fraction_delta=(
                    cpu_high["cpu_throttled_period_fraction"]
                    - cpu_low["cpu_throttled_period_fraction"]
                ),
            )
        )
    with archive.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return dict(
        archive_sha256=digest, status=status, arms=arms, comparisons=comparisons
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.archive), indent=2, allow_nan=False))

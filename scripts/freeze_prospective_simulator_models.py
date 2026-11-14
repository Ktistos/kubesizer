#!/usr/bin/env python3
"""Freeze outcome-blind models and engine policy for the SCV1 campaign.

Only preregistered declarations, low-load characterization, and representative
capacity pilots are readable inputs.  The script refuses to run once a target
directory exists and writes a new, content-addressed output directory.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
ENGINES = (
    "kubesizer_analytical", "kubesizer_simulation", "lqsim",
    "muqsim", "perfsim", "misim",
)
REPRESENTATIVE_FAMILIES = (
    "cpu_fcfs", "cpu_sharing", "blocking_pool", "sequential_chain",
    "repeated_calls", "fork_join", "diamond_shared", "nested_fork",
    "multiclass_shared",
)
STRICT_FAMILIES = (
    "ps_single", "ps_chain3", "ps_fork2", "fcfs_single",
    "fcfs_chain3", "fcfs_fork2", "fifo4_single",
)
TRACK_ENGINES = {
    "processor_sharing_cpu": {
        "kubesizer_analytical", "kubesizer_simulation", "lqsim", "perfsim"},
    "single_server_fcfs_cpu": {
        "kubesizer_analytical", "kubesizer_simulation", "lqsim", "muqsim"},
    "four_worker_fifo_delay": {
        "kubesizer_simulation", "lqsim", "misim"},
    "representative_capability": set(ENGINES),
}
EXTERNAL_ENGINES = {"lqsim", "muqsim", "perfsim", "misim"}
MODEL_REPLAY = ROOT / "results/method-independent-v2-preliminary-20261019/replay.py"
ZERO_CPU_EPSILON_SECONDS = 1e-9


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a mapping in {path}")
    return value


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def import_module(name: str, path: Path) -> Any:
    specification = importlib.util.spec_from_file_location(name, path)
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


def require_outcome_blind_inputs(data: Path, campaign: dict[str, Any]) -> None:
    forbidden = []
    for family in campaign["families"]:
        for stage in ("core", "core-overload"):
            path = data / family / stage
            if path.exists():
                forbidden.append(str(path))
    if forbidden:
        raise RuntimeError(f"target outcome directories already exist: {forbidden}")

    expected = {family: 5 for family in campaign["families"]}
    for family in REPRESENTATIVE_FAMILIES:
        expected[family] += 3
    observed = {}
    for family in campaign["families"]:
        count = 0
        for stage in ("characterization", "pilot"):
            manifest_path = data / family / stage / "study_manifest.yaml"
            if not manifest_path.exists():
                continue
            manifest = read_yaml(manifest_path)
            runs = manifest.get("runs", [])
            if manifest.get("status") != "COMPLETED" or any(
                    run.get("status") != "COMPLETED" for run in runs):
                raise ValueError(f"incomplete input stage: {manifest_path}")
            count += len(runs)
        observed[family] = count
    if observed != expected:
        raise ValueError(f"input matrix differs from preregistration: {observed}")


def verify_capacity_receipt(path: Path, data: Path) -> dict[str, Any]:
    receipt = read_yaml(path)
    values = receipt.get("estimated_capacity_rps", {})
    if set(values) != {"c1"} or float(values["c1"]) <= 0:
        raise ValueError(f"invalid representative capacity receipt: {path}")
    remote_root = Path("/home/experiment/experiments/scv1/data")
    for remote, expected in receipt.get("pilot_artifact_sha256", {}).items():
        remote_path = Path(remote)
        try:
            relative = remote_path.relative_to(remote_root)
        except ValueError as error:
            raise ValueError(f"capacity receipt escapes campaign data: {remote}") from error
        local = data / relative
        if not local.is_file() or sha256(local) != expected:
            raise ValueError(f"capacity artifact hash mismatch: {local}")
    return receipt


def freeze_common_models(data: Path, bundle: Path, output: Path) -> tuple[dict[str, Any], dict[str, str]]:
    replay = import_module("scv1_model_replay", MODEL_REPLAY)
    replay.OUTPUT = output
    replay.base.OUTPUT = output
    replay.base.DECLARATIONS = bundle
    replay.base.BINDINGS.clear()

    source = output / "source"
    source.mkdir()
    (source / "campaign").symlink_to(data, target_is_directory=True)
    (source / "bundle").symlink_to(bundle, target_is_directory=True)

    metadata = {}
    for family in REPRESENTATIVE_FAMILIES:
        metadata[family] = replay.freeze_family(family)
    for family in STRICT_FAMILIES:
        source_path = bundle / f"oracle-runner-workload-{family}.yaml"
        source_model = read_yaml(source_path)
        model = {
            "request_classes": source_model["request_classes"],
            "traffic_model": source_model["traffic_model"],
        }
        services = model.get("traffic_model", {}).get("service_times", {})
        if not services or any(
                specification.get("distribution") != "deterministic"
                or float(specification.get("mean", -1))
                + float(specification.get("non_cpu_blocking_time", {}).get("mean", 0))
                != 0.01
                for specification in services.values()):
            raise ValueError(f"strict family has a non-preregistered demand: {family}")
        compatibility = None
        if family == "fifo4_single":
            service = services["api"]
            if float(service["mean"]) != 0.0:
                raise ValueError("four-worker fixture no longer has zero CPU demand")
            blocking = service["non_cpu_blocking_time"]
            service["mean"] = ZERO_CPU_EPSILON_SECONDS
            blocking["mean"] = float(blocking["mean"]) - ZERO_CPU_EPSILON_SECONDS
            compatibility = {
                "reason": "current duration parser requires positive CPU demand",
                "cpu_epsilon_seconds": ZERO_CPU_EPSILON_SECONDS,
                "total_local_duration_seconds": service["mean"] + blocking["mean"],
                "analytical_policy": (
                    "abstain because the analytical solver does not model queueing "
                    "on a non-CPU concurrency pool"
                ),
            }
        destination = output / f"{family}.frozen.json"
        write_json(destination, model)
        metadata[family] = {
            "construction": "preregistered_declared_strict_model",
            "source_sha256": sha256(source_path),
            "workload_sha256": sha256(destination),
            "compatibility": compatibility,
        }
        replay.base.BINDINGS[str(source_path)] = sha256(source_path)
    return metadata, dict(sorted(replay.base.BINDINGS.items()))


def engine_cell_policy(cells_path: Path, output: Path) -> dict[str, int]:
    with cells_path.open(newline="", encoding="utf-8") as stream:
        cells = list(csv.DictReader(stream))
    fields = (
        "cell_id", "engine", "policy", "reason", "information_condition",
    )
    counts = {"predict": 0, "abstain": 0, "ineligible": 0}
    path = output / "engine-cell-policy.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for cell in cells:
            track = cell["track"]
            replicas = int(cell["replicas"])
            for engine in ENGINES:
                if engine not in TRACK_ENGINES[track]:
                    policy, reason = "ineligible", "engine excluded by preregistered semantic track"
                    if (track == "four_worker_fifo_delay"
                            and engine == "kubesizer_analytical"):
                        reason = (
                            "pre-outcome semantic abstention: analytical solver does not "
                            "queue on the non-CPU concurrency pool"
                        )
                elif engine in EXTERNAL_ENGINES and replicas != 1:
                    policy, reason = (
                        "abstain",
                        "frozen adapters preserve only one-replica semantics; no silent pooled-queue translation",
                    )
                else:
                    policy, reason = "predict", "supported by frozen adapter policy"
                counts[policy] += 1
                writer.writerow({
                    "cell_id": cell["cell_id"], "engine": engine,
                    "policy": policy, "reason": reason,
                    "information_condition": "portable_low_load_measurements_only",
                })
    return counts


def validate_current_models(output: Path) -> None:
    paths = sorted(output.glob("*.frozen.json"))
    if len(paths) != len(REPRESENTATIVE_FAMILIES) + len(STRICT_FAMILIES):
        raise ValueError("not every frozen model was materialized")
    program = """
import json
from pathlib import Path
import sys
from performance_models import QueueingNetworkType, System
for value in sys.argv[1:]:
    path = Path(value)
    System(path.stem, json.loads(path.read_text()), QueueingNetworkType.OPEN)
"""
    subprocess.run(
        [sys.executable, "-c", program, *(str(path) for path in paths)],
        cwd=ROOT, check=True, text=True, capture_output=True,
    )


def implementation_bindings() -> dict[str, str]:
    explicit = [
        Path(__file__).resolve(),
        ROOT / "scripts/repair_simulator_comparison_pilot.py",
        ROOT / "scripts/run_simulator_comparison_campaign.py",
        ROOT / "scripts/freeze_simulator_comparison_campaign.py",
        ROOT / "scripts/score_prospective_simulator_comparison.py",
        ROOT / "experiments/simulator-comparison-v1/design.yaml",
        ROOT / "experiments/simulator-comparison-v1/PROTOCOL.md",
        ROOT / "experiments/simulator-comparison-v1/PRETARGET_AMENDMENT.md",
        ROOT / "experiments/simulator-comparison-v1/kubesizer-simulator-comparison-targets.service",
        ROOT / "scripts/lqsim_all_family_adapter.py",
        ROOT / "scripts/muqsim_all_family_adapter.py",
        ROOT / "scripts/perfsim_all_family_adapter.py",
        ROOT / "scripts/misim_all_family_adapter.py",
        ROOT / "scripts/replay_lqsim_all_families.py",
        ROOT / "scripts/replay_muqsim_all_families.py",
        ROOT / "scripts/replay_perfsim_all_families.py",
        ROOT / "scripts/replay_misim_all_families.py",
        ROOT / "scripts/patches/uqsim-observation.patch",
        ROOT / "scripts/patches/uqsim-path-observation.patch",
        ROOT / "scripts/patches/perfsim-event-transition.patch",
        MODEL_REPLAY,
        Path("/home/ktistos/work/Kubesizer-analytical/results/method-independent-preliminary-20261006/replay.py"),
    ]
    for folder in ("characterization", "performance_models", "native_backend", "metrics"):
        explicit.extend((ROOT / folder).rglob("*.py"))
    explicit.extend((ROOT / "native_backend/cpp/src").glob("*.cpp"))
    return {str(path.resolve()): sha256(path.resolve()) for path in sorted(set(explicit))}


def gate_bindings() -> dict[str, str]:
    paths = (
        ROOT / "results/lqsim-all-family-semantic-check-20261029/run/summary.json",
        ROOT / "results/muqsim-fcfs-semantic-check-20261030/semantic-check.json",
        ROOT / "results/perfsim-cpu-semantic-check-v2-20261030/semantic-check.json",
        ROOT / "results/misim-fifo4-semantic-check-20261030/semantic-check.json",
    )
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing semantic-gate receipts: {missing}")
    lqsim = json.loads(paths[0].read_text(encoding="utf-8"))
    if not lqsim or any(
            metric.get("status") != "equivalent"
            for row in lqsim for metric in row.get("metrics", {}).values()):
        raise ValueError("lqsim semantic gate did not pass")
    for path in paths[1:]:
        if json.loads(path.read_text(encoding="utf-8")).get("passed") is not True:
            raise ValueError(f"semantic gate did not pass: {path}")
    return {str(path.relative_to(ROOT)): sha256(path) for path in paths}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--preregistration", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    data = arguments.data.resolve()
    bundle = arguments.bundle.resolve()
    registration = arguments.preregistration.resolve()
    output = arguments.output.resolve()
    if output.exists():
        raise FileExistsError(f"model freeze output already exists: {output}")

    campaign = read_yaml(bundle / "campaign.yaml")
    if campaign.get("comparison_campaign") != "simulator-comparison-v1":
        raise ValueError("unexpected comparison campaign")
    require_outcome_blind_inputs(data, campaign)
    capacities = {
        family: verify_capacity_receipt(data / family / "frozen-capacity.yaml", data)
        for family in REPRESENTATIVE_FAMILIES
    }

    output.mkdir(parents=True)
    try:
        models, inputs = freeze_common_models(data, bundle, output)
        validate_current_models(output)
        policy_counts = engine_cell_policy(registration / "cells.csv", output)
        receipt = {
            "schema_version": 1,
            "campaign": campaign["comparison_campaign"],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "target_outcomes_used": False,
            "information_condition": "portable_low_load_measurements_only",
            "engines": list(ENGINES),
            "scope": {
                "strict_families": list(STRICT_FAMILIES),
                "representative_families": list(REPRESENTATIVE_FAMILIES),
                "cells": 435,
                "external_multi_replica_policy": "preregistered_abstention",
            },
            "prediction_settings": {
                "replications": 5,
                "seeds": [2610310001, 2610310002, 2610310003, 2610310004, 2610310005],
                "warmup_seconds": 120,
                "measurement_seconds": 600,
                "maximum_drain_seconds": 300,
                "perfsim_time_dilation": 1000,
            },
            "engine_identities": {
                "lqsim": {"binary_sha256": sha256(ROOT / "results/engine-comparison-20261028/binaries/lqsim")},
                "muqsim": {"upstream_commit": "87f4483a644e6dfc2c3e96497b0920e62b1f2b80"},
                "perfsim": {"upstream_commit": "f1f1caf06e9f11cd027b462053ed301b3cb3817e"},
                "misim": {"upstream_commit": "e45d6018bf8fe68c60c104803d430637a18781ce"},
            },
            "models": models,
            "representative_capacity_sha256": {
                family: sha256(data / family / "frozen-capacity.yaml")
                for family in capacities
            },
            "policy": {
                "sha256": sha256(output / "engine-cell-policy.csv"),
                "counts": policy_counts,
            },
            "bindings": {
                "model_inputs": inputs,
                "implementation": implementation_bindings(),
                "semantic_gates": gate_bindings(),
                "preregistration": {
                    name: sha256(registration / name)
                    for name in ("freeze.json", "cells.csv", "decisions.csv")
                },
                "bundle_receipt": sha256(bundle / "bundle-sha256.yaml"),
            },
            "git_head": subprocess.check_output(
                ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip(),
            "notice": (
                "Models and policy were frozen before any target directory existed. "
                "Strict models use declared demands; representative models use only "
                "portable low-load characterization and pilot capacity evidence."
            ),
        }
        write_json(output / "model-freeze.json", receipt)
        print(json.dumps({
            "receipt": str(output / "model-freeze.json"),
            "receipt_sha256": sha256(output / "model-freeze.json"),
            "models": len(models),
            "policy_counts": policy_counts,
        }, indent=2, sort_keys=True))
    except BaseException:
        # Preserve partial output for forensic inspection; never reuse it.
        raise


if __name__ == "__main__":
    main()

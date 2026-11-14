import csv
import hashlib
from pathlib import Path
import sys

import yaml


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.freeze_prospective_simulator_models import (  # noqa: E402
    engine_cell_policy,
    verify_capacity_receipt,
)


def test_policy_never_silently_translates_external_multi_replica_queues(
        tmp_path: Path) -> None:
    cells = tmp_path / "cells.csv"
    with cells.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=(
            "cell_id", "track", "family", "replicas", "load_ratio",
            "arrival_rate_rps", "semantic_status", "expected_repetitions",
        ))
        writer.writeheader()
        writer.writerows((
            {
                "cell_id": "representative/r1", "track": "representative_capability",
                "family": "cpu_fcfs", "replicas": 1, "load_ratio": .3,
                "arrival_rate_rps": "", "semantic_status": "capability",
                "expected_repetitions": 3,
            },
            {
                "cell_id": "representative/r2", "track": "representative_capability",
                "family": "cpu_fcfs", "replicas": 2, "load_ratio": .3,
                "arrival_rate_rps": "", "semantic_status": "capability",
                "expected_repetitions": 3,
            },
            {
                "cell_id": "strict/r1", "track": "processor_sharing_cpu",
                "family": "ps_single", "replicas": 1, "load_ratio": .3,
                "arrival_rate_rps": 30, "semantic_status": "strict",
                "expected_repetitions": 3,
            },
        ))

    output = tmp_path / "output"
    output.mkdir()
    counts = engine_cell_policy(cells, output)
    with (output / "engine-cell-policy.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    by_key = {(row["cell_id"], row["engine"]): row["policy"] for row in rows}

    assert counts == {"predict": 12, "abstain": 4, "ineligible": 2}
    assert by_key[("representative/r2", "kubesizer_simulation")] == "predict"
    assert by_key[("representative/r2", "lqsim")] == "abstain"
    assert by_key[("strict/r1", "muqsim")] == "ineligible"


def test_capacity_receipt_binds_only_campaign_pilot_artifacts(tmp_path: Path) -> None:
    data = tmp_path / "data"
    artifact = data / "cpu_fcfs/pilot/runs/example/measurement.yaml"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("measurement: portable\n", encoding="utf-8")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    receipt_path = data / "cpu_fcfs/frozen-capacity.yaml"
    receipt_path.write_text(yaml.safe_dump({
        "estimated_capacity_rps": {"c1": 100.0},
        "pilot_artifact_sha256": {
            "/home/experiment/experiments/scv1/data/cpu_fcfs/pilot/runs/example/measurement.yaml": digest,
        },
    }), encoding="utf-8")

    receipt = verify_capacity_receipt(receipt_path, data)
    assert receipt["estimated_capacity_rps"] == {"c1": 100.0}

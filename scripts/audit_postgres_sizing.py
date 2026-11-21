"""Check the archived PostgreSQL sizing trial and read its physical evidence."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import gzip
import json
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import run_postgres_pilot as pg
from scripts.run_postgres_sizing import verify_receipt


def load(directory, name):
    return json.loads((directory / name).read_text())


def audit_run(directory, run, freeze_epoch):
    verify_receipt(directory, "receipt.json")
    if run["started_epoch"] <= freeze_epoch:
        raise ValueError(f"Target started before forecast freeze: {run['run_id']}")
    stored_run = load(directory, "summary.json")
    if any(run[key] != value for key, value in stored_run.items()):
        raise ValueError(f"Run summary changed between receipt and treatment index: {run['run_id']}")
    with gzip.open(directory / "requests.jsonl.gz", "rt") as stream:
        rows = [row for line in stream if (row := json.loads(line))["tagged"]]
    by_ordinal = {int(row["id"].rsplit(":", 1)[1]): row for row in rows}
    if len(by_ordinal) != len(rows):
        raise ValueError(f"Duplicate request ID in {run['run_id']}")

    classes = {}
    for key, reported in run["classes"].items():
        cohort = [row for row in rows if row["request_class"] == key]
        successes = [row for row in cohort if row["success"]]
        if len(cohort) != reported["count"] or len(successes) != reported["completed"]:
            raise ValueError(f"The retained raw cohort does not reconcile: {run['run_id']}/{key}")
        mean = statistics.fmean(row["latency_seconds"] for row in successes) if successes else None
        if mean != reported["mean_seconds"]:
            raise ValueError(f"Retained latency does not reconcile: {run['run_id']}/{key}")
        sql = [row["server_response_seconds"] for row in successes]
        classes[key] = {
            "client_mean_seconds": mean,
            "gateway_to_sql_mean_seconds": statistics.fmean(sql) if sql else None,
            "attempts": len(cohort),
            "failures": len(cohort) - len(successes),
            "failure_reasons": dict(Counter(
                row.get("error") or "unspecified" for row in cohort if not row["success"]
            )),
        }

    start = run["warmup_seconds"]
    interval = [
        row for row in load(directory, "snapshots.json")
        if start <= row["elapsed"] < start + run["measurement_seconds"] and "members" in row
    ]
    cpu = {}
    if len(interval) > 1:
        left, right = interval[0], interval[-1]
        elapsed = right["elapsed"] - left["elapsed"]
        cpu = {
            member: {category: seconds / elapsed for category, seconds in values.items()}
            for member, values in pg.cpu_deltas(left, right).items() if member in right["members"]
        }
    return {
        "run_id": run["run_id"], "layout": run["layout"], "pair": run["pair"],
        "classes": classes, "postgres_process_cpu_cores": cpu,
    }, by_ordinal


def audit(directory):
    verify_receipt(directory, "prediction-receipt.json")
    freeze = load(directory, "predictions-frozen.json")
    frozen_epoch = datetime.fromisoformat(freeze["frozen_at"]).timestamp()
    protocol = load(directory, "protocol.json")
    schedule = load(directory, "target-order.json")
    runs = load(directory, "target-results.json")
    if len(schedule) != len(runs) or load(directory, "status.json")["status"] != "COMPLETED":
        raise ValueError("The prospective trial has unfinished targets")

    details, pairs = [], defaultdict(list)
    for treatment, run in zip(schedule, runs, strict=True):
        if any(run[key] != value for key, value in treatment.items()) or (
            run["rate_rps"] != freeze["target_rate_rps"]
        ):
            raise ValueError(f"Target differs from its frozen assignment: {run['run_id']}")
        result, ordinal = audit_run(directory / "runs" / run["run_id"], run, frozen_epoch)
        details.append(result)
        pairs[run["pair"]].append(ordinal)

    paired_evidence = {}
    for pair, (left, right) in pairs.items():
        timed = [
            abs(row["scheduled_seconds"] - right[index]["scheduled_seconds"])
            for index, row in left.items()
            if index in right and "scheduled_seconds" in row and "scheduled_seconds" in right[index]
        ]
        paired_evidence[pair] = {
            "same_request_ordinals": left.keys() == right.keys(),
            "same_request_classes": all(
                row["request_class"] == right[index]["request_class"]
                for index, row in left.items() if index in right
            ),
            "maximum_scheduled_offset_difference_seconds": max(timed, default=None),
        }
    physical = load(directory, "scores.json")
    costs = protocol["costs_in_database_members"]
    feasible = [name for name, row in physical.items() if row["observed"] == "FEASIBLE_IN_PILOT"]
    lowest = min(feasible, key=costs.__getitem__, default=None)
    decisions = {}
    for method, receipt in freeze["forecasts"].items():
        plan = receipt["response"].get("result", {}).get("stateful_planning", {})
        selected = plan.get("selected")
        decisions[method] = {
            "selected": selected,
            "observed": physical[selected]["observed"] if selected else "ABSTAINED",
            "excess_cost_vs_measured_feasible": (
                costs[selected] - costs[lowest] if selected in feasible else None
            ),
        }
    return {
        "target_rate_rps": freeze["target_rate_rps"], "frozen_at": freeze["frozen_at"],
        "lowest_measured_feasible_layout": lowest, "decisions": decisions,
        "paired_schedules": paired_evidence, "runs": details,
        "observed_layouts": physical, "cleanup": load(directory, "cleanup.json"),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.evidence), indent=2, allow_nan=False))

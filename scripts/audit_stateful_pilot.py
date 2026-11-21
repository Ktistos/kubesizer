"""Audit sfp1 demand drift without changing any frozen prediction."""

import argparse
from collections import defaultdict
import gzip
import json
from pathlib import Path
import statistics


def audit(root):
    profile = json.loads((root / "characterization.json").read_text())
    targets = json.loads((root / "target-results.json").read_text())
    values = defaultdict(list)
    for run in targets:
        directory = root / "runs" / run["run_id"]
        with gzip.open(directory / "requests.jsonl.gz", "rt") as stream:
            tagged = {
                row["id"] for line in stream if (row := json.loads(line))["tagged"]
            }
        with gzip.open(directory / "events.json.gz", "rt") as stream:
            members = json.load(stream)
        for member, evidence in members.items():
            for event in evidence["events"]:
                if event["type"] == "local" and event["request_id"] in tagged:
                    values[
                        (member, event["request_class"], run["load_fraction"])
                    ].append(event["local_seconds"])
    rows = []
    for (member, request_class, fraction), samples in sorted(values.items()):
        characterization = statistics.fmean(
            profile["local_samples_seconds"][member][request_class]
        )
        target = statistics.fmean(samples)
        rows.append(
            dict(
                member=member,
                request_class=request_class,
                load_fraction=fraction,
                characterization_mean_seconds=characterization,
                target_mean_seconds=target,
                target_change_percent=100 * (target / characterization - 1),
                sample_count=len(samples),
            )
        )
    lines = [
        "# sfp1 read-error and characterization audit",
        "",
        "Retrospective diagnosis only. No frozen model, original score or target was changed.",
        "",
        "| Member | Class | Offered worker-load fraction | Characterization ms | Target ms | Change % |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['member']} | {row['request_class']} | {row['load_fraction']:.2f} | "
            f"{1000*row['characterization_mean_seconds']:.3f} | "
            f"{1000*row['target_mean_seconds']:.3f} | {row['target_change_percent']:+.1f} |"
        )
    residual = {c: statistics.fmean(v) for c, v in profile["residual_seconds"].items()}
    lines += [
        "",
        "## Interpretation",
        "",
        "Frozen write-worker occupancy exceeds target occupancy at each member. "
        "The model therefore injects more replicated contention than the measured targets. "
        "This is consistent with excessive predicted read waiting near capacity, but "
        "does not by itself quantify the causal contribution of drift or prove that "
        "exclusive-choice routing is incorrect. Independent read paths and Bernoulli "
        "fan-out also have different correlations even when expected visit rates agree.",
        "",
        f"The characterization client-to-entry mean residual is {1000*residual['read']:.2f} ms "
        f"for reads and {1000*residual['write']:.2f} ms for writes. It is shared by both "
        "variants and is a large part of low-load response time. Overall percentage "
        "error should therefore be read alongside internal queue predictions.",
        "",
        "No target-derived demand correction is reported as holdout improvement. "
        "The next database study must retain local demand, lag and occupancy so that "
        "load-dependent work, storage waits and replication can be inspected separately. "
        "The current two physical repetitions and healthy finite cohorts do not "
        "establish reliable tail prediction, steady-state stability or stateful sizing.",
        "",
    ]
    destination = root.parent / "READ_ERROR_AUDIT.md"
    destination.write_text("\n".join(lines))
    destination.with_suffix(".json").write_text(json.dumps(rows, indent=2) + "\n")
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    args = parser.parse_args()
    print(audit(args.data))

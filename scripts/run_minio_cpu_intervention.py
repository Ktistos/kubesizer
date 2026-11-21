#!/usr/bin/env python3
"""Run a balanced 1/2-CPU entry-quota intervention on the MinIO pilot."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import characterize_minio_fragments as fragments
from scripts import characterize_minio_sweep as sweep
from scripts import run_minio_pilot as pilot


QUOTAS_AND_REPS = ((1, 0), (2, 0), (2, 1), (1, 1))
RATE = 10.8
BASE_SEED = 26112092


def restore(output):
    status_path = output / "campaign-status.json"
    if not status_path.exists():
        return
    status = json.loads(status_path.read_text())
    current = status.get("current")
    if current:
        os.environ["KUBESIZER_ENTRY_CPU_LIMIT"] = str(current["entry_cores"])
        fragments.restore(output / current["name"])


def run(output, deps, binary, digest):
    output.mkdir(parents=True, exist_ok=False)
    order = [
        dict(
            name=f"arm-{i:02d}-{limit}cpu",
            entry_cores=limit,
            rep=rep,
            rate=RATE,
            mode="mixed",
            seed=BASE_SEED + rep,
        )
        for i, (limit, rep) in enumerate(QUOTAS_AND_REPS)
    ]
    pilot.write(
        output / "campaign-plan.json",
        dict(
            plan=order,
            endpoint="direct pod to minio1",
            treatment="entry container CPU limit only",
            compare_matched_schedules=True,
            fresh_pods_for_all_arms=True,
            counts_as_prediction_holdout=False,
        ),
    )
    status = dict(status="RUNNING", started_at=pilot.now(), current=None, completed=[])
    pilot.write(output / "campaign-status.json", status)
    try:
        for i, current in enumerate(order):
            status["current"] = current
            pilot.write(output / "campaign-status.json", status)
            os.environ["KUBESIZER_ENTRY_CPU_LIMIT"] = str(current["entry_cores"])
            window = {k: v for k, v in current.items() if k != "name"}
            start = sweep.run(
                output / current["name"],
                deps,
                binary,
                digest,
                plan=[window],
                use_preloaded_bucket=i > 0,
            )
            status["completed"].append(dict(**current, outcome=start["status"]))
            pilot.write(output / "campaign-status.json", status)
            if start["status"] != "CAPTURED":
                status.update(status="STOPPED_AT_GUARD", completed_at=pilot.now())
                break
        else:
            status.update(status="CAPTURED", completed_at=pilot.now())
    except BaseException as error:
        status.update(
            status="FAILED",
            error=f"{type(error).__name__}: {error}",
            failed_at=pilot.now(),
        )
        raise
    finally:
        pilot.write(output / "campaign-status.json", status)
        restore(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deps", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--digest")
    parser.add_argument("--finish", action="store_true")
    args = parser.parse_args()
    if args.finish:
        restore(args.output.resolve())
    else:
        if args.deps is None or args.binary is None or args.digest is None:
            parser.error("--deps, --binary and --digest are required for collection")

        def interrupt(*_):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupt)
        run(
            args.output.resolve(),
            args.deps.resolve(),
            args.binary.resolve(),
            args.digest,
        )

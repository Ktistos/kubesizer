"""Offline analytical replica sizing; no simulator or node placement."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from characterization import (
    build_characterized_system_variants,
    estimate_statistical_stability_floor,
    load_and_prepare_initial_sizing,
)
from optimization import NoFeasibleConfigurationError
from performance_models import QueueingNetworkType, System
from analytical_workflow import optimize_with_analytical_model

CLI_VERSION = "0.5.0"


def _read_yaml(path):
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"Cannot read workload {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError("The workload must contain a YAML mapping.")
    return value


def _load_workflow_system(args):
    path = Path(args.workload).expanduser().resolve()
    if args.measurements is None:
        return (
            System(
                path.stem,
                _read_yaml(path),
                QueueingNetworkType.OPEN,
                workload_base_path=path.parent,
            ),
            None,
        )
    plan = load_and_prepare_initial_sizing(path, args.measurements)
    variants = build_characterized_system_variants(plan.inputs, plan.characterization)
    if len(variants) != 1:
        raise ValueError(
            "Declare call_execution for every request class before optimization."
        )
    _variant, system = variants[0]
    order = tuple(
        service.get_name()
        for service in sorted(
            system.get_services().values(), key=lambda item: item.get_id()
        )
    )
    floor = estimate_statistical_stability_floor(
        plan.inputs,
        plan.characterization,
        analytical_minimum=system.get_minimum_stable_replicas(),
        maximum_configuration=(args.max_replicas,) * len(order),
        service_order=order,
    )
    floor.assert_confidence_bounded_stable()
    return system, floor.configuration


def _progress(event, details):
    if event == "replica_optimization_started":
        print(
            f"Analytical exact search: {details['total_configurations']} bounded candidates",
            file=sys.stderr,
        )
    elif event == "replica_optimization_completed":
        print(
            f"Analytical search completed: {details['completed_configurations']} evaluated",
            file=sys.stderr,
        )


def _command_run(args):
    system, minimum = _load_workflow_system(args)
    result = optimize_with_analytical_model(
        system,
        maximum_replicas=args.max_replicas,
        minimum_configuration=minimum,
        progress_callback=None if args.quiet else _progress,
    )
    services = {
        service.get_name(): {
            "recommended_replicas": result.configuration[service.get_id()]
        }
        for service in sorted(
            system.get_services().values(), key=lambda item: item.get_id()
        )
    }
    return {"services": services}


def build_parser():
    parser = argparse.ArgumentParser(
        prog="kubesizer",
        description="Size replicas using analytical means and exact bounded search.",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {CLI_VERSION}"
    )
    parser.add_argument("--workload", default="workload.yaml", help="Workload YAML.")
    parser.add_argument("--measurements", help="Portable measurement manifest YAML.")
    parser.add_argument(
        "--max-replicas",
        type=int,
        default=10,
        help="Per-service upper bound for exact replica search.",
    )
    parser.add_argument(
        "--replica-evaluation",
        choices=("analytical",),
        default="analytical",
        help="Analytical mean evaluation (the only supported evaluator).",
    )
    parser.add_argument("--json", action="store_true", help="Emit one JSON envelope.")
    parser.add_argument("--out", help="Also persist the JSON envelope to this path.")
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress search progress."
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.max_replicas <= 0:
            raise ValueError("--max-replicas must be a positive integer.")
        result = _command_run(args)
        envelope = {
            "ok": True,
            "command": "kubesizer",
            "cli_version": CLI_VERSION,
            "result": result,
        }
        exit_code = 0
    except (ValueError, TypeError, OSError, NoFeasibleConfigurationError) as error:
        envelope = {
            "ok": False,
            "command": "kubesizer",
            "cli_version": CLI_VERSION,
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        exit_code = 2
    serialized = json.dumps(envelope, indent=2, sort_keys=True, allow_nan=False)
    if args.out:
        path = Path(args.out).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(serialized + "\n", encoding="utf-8")
    if args.json:
        print(serialized)
    elif envelope["ok"]:
        print(yaml.safe_dump(envelope["result"], sort_keys=False).rstrip())
    else:
        print(f"kubesizer: {envelope['error']['message']}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

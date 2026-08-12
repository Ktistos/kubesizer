import argparse
import os

import yaml

from calibration import (
    WarmupCalibrationError,
    calibrate_global_warmup,
    save_warmup_calibration,
    save_warmup_calibration_failure,
    select_warmup_pilot_parameters,
)
from performance_models import QueueingNetworkType, System


DEFAULT_WORKERS = min(8, os.cpu_count() or 1)


def build_argument_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Calibrate and validate one global empty-start warm-up duration for "
            "the bounded replica-sizing search space."
        )
    )
    parser.add_argument("--workload", default="workload.yaml")
    parser.add_argument("--output-directory", default="warmup_calibration_output")
    parser.add_argument(
        "--max-replicas",
        type=int,
        nargs="+",
        default=[10],
        help="One common maximum or one maximum per service.",
    )
    parser.add_argument("--replications", type=int, default=30)
    parser.add_argument("--validation-replications", type=int, default=10)
    parser.add_argument("--calibration-seed-start", type=int, default=100_000)
    parser.add_argument("--validation-seed-start", type=int, default=200_000)
    parser.add_argument(
        "--window-length",
        type=int,
        default=10,
        help=(
            "Analysis-window length in buckets at the initial bucket width; "
            "candidate widths are scaled to preserve this physical duration."
        ),
    )
    parser.add_argument("--max-horizon-doublings", type=int, default=5)
    parser.add_argument("--max-validation-rounds", type=int, default=3)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--no-plots", action="store_true")
    return parser


def main(argv=None):
    args = build_argument_parser().parse_args(argv)
    with open(args.workload, "r", encoding="utf-8") as workload_file:
        workload = yaml.safe_load(workload_file)
    if not isinstance(workload, dict) or not workload:
        raise ValueError("The workload file must contain a non-empty mapping.")

    system = System("warm-up calibration", workload, QueueingNetworkType.OPEN)
    parameters = select_warmup_pilot_parameters(
        system,
        replication_count=args.replications,
        seed_start=args.calibration_seed_start,
    )
    maximum_replicas = (
        args.max_replicas[0]
        if len(args.max_replicas) == 1
        else tuple(args.max_replicas)
    )

    def report_progress(stage, details):
        detail_text = ", ".join(
            f"{name}={value}" for name, value in details.items()
        )
        print(f"{stage}: {detail_text}", flush=True)

    try:
        result = calibrate_global_warmup(
            system,
            maximum_replicas=maximum_replicas,
            parameters=parameters,
            validation_replication_count=args.validation_replications,
            validation_seed_start=args.validation_seed_start,
            max_horizon_doublings=args.max_horizon_doublings,
            max_validation_rounds=args.max_validation_rounds,
            progress_callback=report_progress,
            analysis_options={"window_length": args.window_length},
            workers=args.workers,
        )
    except WarmupCalibrationError as error:
        failure_path = save_warmup_calibration_failure(
            error,
            args.output_directory,
            parameters=parameters,
        )
        print(f"warm-up calibration failed: {error}")
        print(f"failure diagnostics={failure_path}")
        raise SystemExit(2) from error
    artifacts = save_warmup_calibration(
        result,
        args.output_directory,
        create_plots=not args.no_plots,
    )
    print(f"validated global warm-up W={result.global_warmup:g}")
    print(f"bucket width delta={result.bucket_width:g}")
    print(f"pilot horizon H={result.final_parameters.pilot_horizon:g}")
    print(f"diagnostics={artifacts.output_directory}")
    return result


if __name__ == "__main__":
    main()

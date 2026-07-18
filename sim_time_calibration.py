import argparse
import math
import random

import yaml

from performance_models import QueueingNetworkType, System


def find_minimum_simulation_duration(
    system,
    min_duration=1.0,
    max_duration=100_000.0,
    trials=5,
    relative_tolerance=0.05,
    absolute_tolerance=1e-9,
    resolution=1.0,
    base_seed=42,
    progress_callback=None,
):
    """Find the shortest duration whose simulation matches on every trial.

    The result is the minimum on the grid defined by ``resolution``. The search
    assumes that simulation accuracy improves as duration increases.
    """
    if min_duration <= 0:
        raise ValueError("min_duration must be greater than zero.")
    if max_duration < min_duration:
        raise ValueError("max_duration must be at least min_duration.")
    if trials <= 0:
        raise ValueError("trials must be greater than zero.")
    if resolution <= 0:
        raise ValueError("resolution must be greater than zero.")
    if relative_tolerance < 0 or absolute_tolerance < 0:
        raise ValueError("Tolerances must be non-negative.")

    min_step = math.ceil(min_duration / resolution)
    max_step = math.floor(max_duration / resolution)
    if min_step > max_step:
        raise ValueError("No duration exists within the requested resolution.")

    analytical_metrics = system.solve()
    if not analytical_metrics.get_stability():
        raise ValueError(
            "The analytical model is unstable; finite simulation metrics cannot "
            "converge to its infinite mean latency."
        )

    evaluations = {}
    random_state = random.getstate()

    def is_consistent(step):
        if step in evaluations:
            return evaluations[step]

        duration = step * resolution
        passed = True
        completed_trials = 0

        for trial_index in range(trials):
            random.seed(base_seed + trial_index)
            simulation_metrics = system.simulate(duration)
            completed_trials += 1

            if not analytical_metrics.is_nearly_equal(
                simulation_metrics,
                relative_tolerance=relative_tolerance,
                absolute_tolerance=absolute_tolerance,
            ):
                passed = False
                break

        evaluations[step] = passed
        if progress_callback is not None:
            progress_callback(duration, passed, completed_trials, trials)
        return passed

    try:
        last_failed_step = min_step - 1
        candidate_step = min_step
        first_passing_step = None

        while candidate_step <= max_step:
            if is_consistent(candidate_step):
                first_passing_step = candidate_step
                break

            last_failed_step = candidate_step
            if candidate_step == max_step:
                break
            candidate_step = min(max_step, candidate_step * 2)

        if first_passing_step is None:
            raise RuntimeError(
                "No consistently accurate simulation duration was found at or "
                f"below {max_duration:g} seconds."
            )

        lower_step = last_failed_step + 1
        upper_step = first_passing_step

        while lower_step < upper_step:
            middle_step = (lower_step + upper_step) // 2
            if is_consistent(middle_step):
                upper_step = middle_step
            else:
                lower_step = middle_step + 1

        return lower_step * resolution
    finally:
        random.setstate(random_state)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find the minimum simulation duration whose mean metrics consistently "
            "match the analytical model."
        )
    )
    parser.add_argument("--workload", default="workload.yaml")
    parser.add_argument("--min-duration", type=float, default=1.0)
    parser.add_argument("--max-duration", type=float, default=100_000.0)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--relative-tolerance", type=float, default=0.05)
    parser.add_argument("--absolute-tolerance", type=float, default=1e-9)
    parser.add_argument("--resolution", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    with open(args.workload, "r") as workload_file:
        workload = yaml.safe_load(workload_file)

    system = System("calibration", workload, QueueingNetworkType.OPEN)

    def report_progress(duration, passed, completed_trials, total_trials):
        status = "passed" if passed else "failed"
        print(
            f"duration={duration:g}s: {status} "
            f"({completed_trials}/{total_trials} trials evaluated)"
        )

    duration = find_minimum_simulation_duration(
        system,
        min_duration=args.min_duration,
        max_duration=args.max_duration,
        trials=args.trials,
        relative_tolerance=args.relative_tolerance,
        absolute_tolerance=args.absolute_tolerance,
        resolution=args.resolution,
        base_seed=args.seed,
        progress_callback=report_progress,
    )
    print(f"minimum consistent simulation duration: {duration:g} seconds")


if __name__ == "__main__":
    main()

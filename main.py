import os
from pathlib import Path

import yaml

from calibration import (
    CalibrationOptions,
    CalibrationProfileMismatchError,
    load_calibration_profile,
    validate_profile_for_experiment,
)
from optimization import (
    ExhaustiveNodePlacementOptimizer,
    NodePlacementProblem,
)
from performance_models import Cluster, QueueingNetworkType, System
from statistical_workflow import (
    calibrate,
    optimize_with_calibrated_simulation,
    validate_final_configuration,
)


REQUEST_CLASS_SLOS = {
    "read_heavy": 1.0,
    "write_heavy": 1.0,
    "compute_heavy": 1.0,
}
MAX_REPLICAS_PER_SERVICE = 10
SIMULATION_WORKERS = min(8, os.cpu_count() or 1)
CALIBRATION_OUTPUT_DIRECTORY = Path("simulation_calibration_output")
CALIBRATION_PROFILE_PATH = (
    CALIBRATION_OUTPUT_DIRECTORY / "calibration_profile.yaml"
)


def calibrate_before_optimization(system, *, options=None):
    """Run Stage A and persist its profile and complete diagnostic evidence."""
    return calibrate(
        system,
        request_class_slos=REQUEST_CLASS_SLOS,
        maximum_replicas=MAX_REPLICAS_PER_SERVICE,
        options=options or CalibrationOptions(),
        workers=SIMULATION_WORKERS,
        report_directory=CALIBRATION_OUTPUT_DIRECTORY,
        progress_callback=lambda stage, details: print(stage, details),
    )


def load_or_calibrate_profile(system, *, options=None):
    """Load an exact-match profile, or calibrate only when none exists."""
    if CALIBRATION_PROFILE_PATH.exists():
        profile = load_calibration_profile(CALIBRATION_PROFILE_PATH)
        return validate_profile_for_experiment(
            profile,
            system,
            request_class_slos=REQUEST_CLASS_SLOS,
            maximum_configuration=(
                MAX_REPLICAS_PER_SERVICE,
            )
            * len(system.get_services()),
        )
    return calibrate_before_optimization(system, options=options)


def optimize_replicas(system, calibration_profile):
    """Run Stage B with frozen W/M and the calibrated sequential policy."""
    return optimize_with_calibrated_simulation(
        system,
        calibration_profile,
        workers=SIMULATION_WORKERS,
    )


def validate_optimizer_result(system, optimization_result, calibration_profile):
    """Run Stage C on the locked candidate and fresh holdout seeds."""
    return validate_final_configuration(
        system,
        optimization_result.configuration,
        calibration_profile,
        workers=SIMULATION_WORKERS,
        report_directory=CALIBRATION_OUTPUT_DIRECTORY / "final_validation",
    )


def _interval_summary(intervals):
    return {
        request_class: {
            "pooled_p95": interval.p95_estimate,
            "lower_bound": interval.lower_bound,
            "upper_bound": interval.upper_bound,
            "slo": interval.slo,
            "eligible_requests": interval.eligible_request_count,
            "nonempty_replications": interval.nonempty_replication_count,
            "relative_half_width": interval.relative_half_width,
        }
        for request_class, interval in intervals.items()
    }


def report_replica_result(system, result):
    evaluation = result.metrics
    print(
        "Replica configuration:",
        dict(zip(system.get_services(), result.configuration)),
    )
    print("Replicas added from stable anchor:", result.cost)
    print("Classification:", evaluation.classification.value)
    print(
        "Sequential p95 confidence intervals:",
        _interval_summary(evaluation.configuration_evaluation.intervals),
    )
    print("Replications used:", evaluation.configuration_evaluation.replications_used)
    print("Sequential looks:", evaluation.configuration_evaluation.look_count)
    print("Explored states:", result.explored_states)
    print("Search path:", result.path)


def report_final_validation(validation):
    print("Independent final validation:", validation.classification.value)
    print("Final p95 confidence intervals:", _interval_summary(validation.intervals))
    print("Final validation seeds:", validation.seeds)


def main():
    with open("workload.yaml", "r", encoding="utf-8") as workload_file:
        workload = yaml.safe_load(workload_file)
    with open("resources.yaml", "r", encoding="utf-8") as resources_file:
        resources = yaml.safe_load(resources_file)

    system = System("STELAR", workload, QueueingNetworkType.OPEN)
    profile = load_or_calibrate_profile(system)
    print(
        "Calibration profile:",
        {
            "fingerprint": profile.profile_fingerprint,
            "W": profile.warmup_duration,
            "M": profile.measurement_duration,
            "R_min": profile.minimum_optimization_replications,
            "R_max": profile.maximum_optimization_replications,
        },
    )

    replica_result = optimize_replicas(system, profile)
    report_replica_result(system, replica_result)

    final_validation = validate_optimizer_result(system, replica_result, profile)
    report_final_validation(final_validation)
    if not final_validation.passed:
        raise RuntimeError(
            "The optimizer candidate failed independent final validation: "
            f"{final_validation.failure_reason}"
        )

    system.configure_system_replicas(replica_result.configuration)
    cluster = Cluster("okeanos", resources, system)
    placement_result = ExhaustiveNodePlacementOptimizer().optimize(
        NodePlacementProblem(cluster)
    )
    print("Node placement:", placement_result.configuration)


if __name__ == "__main__":
    main()

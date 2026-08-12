import os
import yaml

from calibration import CalibrationOptions
from optimization import (
    NodePlacementProblem,
    OptimizationMode,
    optimize_node_placement,
)
from performance_models import Cluster, QueueingNetworkType, System
from runtime_estimation import (
    calibrate_replica_configuration_limit,
    estimate_workflow_runtime,
    format_replica_configuration_limit_calibration,
    format_runtime_estimate,
)
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
OPTIMIZATION_MODE = OptimizationMode.AUTO
EXACT_REPLICA_SEARCH_TIME_BUDGET_SECONDS = 600.0
RUNTIME_LIMIT_SAFETY_FACTOR = 3.0


def calibrate_before_optimization(system, *, options=None):
    """Run Stage A and keep its profile and diagnostics in memory."""
    return calibrate(
        system,
        request_class_slos=REQUEST_CLASS_SLOS,
        maximum_replicas=MAX_REPLICAS_PER_SERVICE,
        options=options or CalibrationOptions(),
        workers=SIMULATION_WORKERS,
        progress_callback=lambda stage, details: print(stage, details),
    )


def optimize_replicas(
    system,
    calibration_profile,
    *,
    runtime_limit_calibration=None,
):
    """Run Stage B with runtime-adaptive exact/scalable selection."""
    return optimize_with_calibrated_simulation(
        system,
        calibration_profile,
        workers=SIMULATION_WORKERS,
        mode=OPTIMIZATION_MODE,
        runtime_limit_calibration=runtime_limit_calibration,
    )


def validate_optimizer_result(system, optimization_result, calibration_profile):
    """Run Stage C on the locked candidate and fresh holdout seeds."""
    return validate_final_configuration(
        system,
        optimization_result.configuration,
        calibration_profile,
        workers=SIMULATION_WORKERS,
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
    print("Search mode:", result.solution_mode)
    print("Explored states:", result.explored_states)
    print("Search path:", result.path)
    print("Minimum replica cost proven:", result.minimality_proven)
    print("Locally minimal:", result.locally_minimal)
    print("Search budget exhausted:", result.budget_exhausted)
    if result.unresolved_lower_cost_states:
        print(
            "Lower-cost inconclusive configurations:",
            result.unresolved_lower_cost_states,
        )


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
    options = CalibrationOptions()
    runtime_rates = None
    print("Preflight: estimating the conservative workflow runtime")
    try:
        runtime_estimate = estimate_workflow_runtime(
            system,
            resources,
            options=options,
            maximum_replicas=MAX_REPLICAS_PER_SERVICE,
            workers=SIMULATION_WORKERS,
            optimization_mode=OPTIMIZATION_MODE,
        )
    except Exception as error:
        print(f"Runtime speculation unavailable: {error}")
    else:
        print(format_runtime_estimate(runtime_estimate))
        runtime_rates = getattr(runtime_estimate, "benchmark", None)

    print("Stage A: calibrating a fresh statistical profile")
    profile = calibrate_before_optimization(system, options=options)
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

    runtime_limit_calibration = None
    if runtime_rates is not None:
        try:
            runtime_limit_calibration = calibrate_replica_configuration_limit(
                system,
                profile,
                runtime_rates,
                workers=SIMULATION_WORKERS,
                exact_search_time_budget_seconds=(
                    EXACT_REPLICA_SEARCH_TIME_BUDGET_SECONDS
                ),
                safety_factor=RUNTIME_LIMIT_SAFETY_FACTOR,
                mode=OPTIMIZATION_MODE,
            )
        except Exception as error:
            print(
                "Runtime-adaptive replica limit unavailable; using the hard "
                f"configuration cap: {error}"
            )
        else:
            print(
                format_replica_configuration_limit_calibration(
                    runtime_limit_calibration
                )
            )

    print("Stage B: optimizing replica counts with calibrated simulation")
    replica_result = optimize_replicas(
        system,
        profile,
        runtime_limit_calibration=runtime_limit_calibration,
    )
    report_replica_result(system, replica_result)

    print("Stage C: validating the selected replicas with fresh seeds")
    final_validation = validate_optimizer_result(system, replica_result, profile)
    report_final_validation(final_validation)
    if not final_validation.passed:
        raise RuntimeError(
            "The optimizer candidate failed independent final validation: "
            f"{final_validation.failure_reason}"
        )

    print("Stage D: optimizing node placement")
    system.configure_system_replicas(replica_result.configuration)
    cluster = Cluster("okeanos", resources, system)
    placement_result = optimize_node_placement(
        NodePlacementProblem(cluster),
        mode=OPTIMIZATION_MODE,
    )
    print("Node placement:", placement_result.configuration)
    print(
        "Node placement search:",
        {
            "mode": getattr(placement_result, "solution_mode", "EXACT"),
            "complete_placements_evaluated": placement_result.complete_states,
            "complete_placements_pruned": (
                placement_result.pruned_complete_placements
            ),
            "total_placement_space": placement_result.total_configurations,
            "optimality_proven": placement_result.optimality_proven,
            "budget_exhausted": getattr(
                placement_result,
                "budget_exhausted",
                False,
            ),
        },
    )


if __name__ == "__main__":
    main()

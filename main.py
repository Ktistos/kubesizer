import os
import yaml

from calibration import CalibrationOptions
from optimization import (
    NodePlacementProblem,
    OptimizationMode,
    OptimizationModes,
    ReplicaEvaluationMode,
    WorkflowOptimizationMode,
    optimization_workflow_mode,
    optimize_node_placement,
)
from performance_models import Cluster, QueueingNetworkType, ScenarioSystem, System
from runtime_estimation import (
    LiveWorkflowRuntimeEstimator,
    calibrate_replica_configuration_limit,
    estimate_workflow_runtime,
    format_replica_configuration_limit_calibration,
    format_runtime_estimate,
)
from statistical_workflow import (
    calibrate,
    optimize_with_analytical_model,
    optimize_with_calibrated_simulation,
    validate_final_configuration,
)


MAX_REPLICAS_PER_SERVICE = 10
SIMULATION_WORKERS = min(8, os.cpu_count() or 1)
REPLICA_EVALUATION_MODE = ReplicaEvaluationMode.SIMULATION
# AUTO is resolved independently for the two search spaces. An explicit HYBRID
# selection must name which optimizer is EXACT and which is SCALABLE.
OPTIMIZATION_MODES = OptimizationModes(
    mode=WorkflowOptimizationMode.AUTO,
)
EXACT_REPLICA_SEARCH_TIME_BUDGET_SECONDS = 600.0
RUNTIME_LIMIT_SAFETY_FACTOR = 3.0


def enforce_workload_evaluation_mode(system, evaluation_mode):
    """Reject an evaluator that cannot certify the workload's SLO kind."""
    kind_method = getattr(system, "get_request_class_latency_slo_kind", None)
    if kind_method is None:
        return evaluation_mode
    kind = kind_method()
    if kind == "percentile" and evaluation_mode != ReplicaEvaluationMode.SIMULATION:
        raise ValueError(
            "The workload selects percentile latency SLOs, so replica evaluation "
            "must use SIMULATION."
        )
    if kind is None and evaluation_mode != ReplicaEvaluationMode.ANALYTICAL:
        raise ValueError(
            "A workload without latency SLOs can use ANALYTICAL stability-based "
            "replica sizing, but simulation has no percentile threshold to test."
        )
    return evaluation_mode


def calibrate_before_optimization(system, *, options=None, progress_callback=None):
    """Run Stage A and keep its profile and diagnostics in memory."""
    return calibrate(
        system,
        maximum_replicas=MAX_REPLICAS_PER_SERVICE,
        options=options or CalibrationOptions(),
        workers=SIMULATION_WORKERS,
        progress_callback=(
            progress_callback
            if progress_callback is not None
            else lambda stage, details: print(stage, details)
        ),
    )


def optimize_replicas(
    system,
    calibration_profile,
    *,
    runtime_limit_calibration=None,
    progress_callback=None,
):
    """Run Stage B with runtime-adaptive exact/scalable selection."""
    return optimize_with_calibrated_simulation(
        system,
        calibration_profile,
        workers=SIMULATION_WORKERS,
        mode=OPTIMIZATION_MODES.replica,
        runtime_limit_calibration=runtime_limit_calibration,
        progress_callback=progress_callback,
    )


def optimize_replicas_analytically(system):
    """Optimize analytical mean latency without calibration or simulation."""
    return optimize_with_analytical_model(
        system,
        request_class_mean_latency_limits=(
            system.get_request_class_mean_latency_limits()
        ),
        maximum_replicas=MAX_REPLICAS_PER_SERVICE,
        mode=OPTIMIZATION_MODES.replica,
    )


def validate_optimizer_result(
    system,
    optimization_result,
    calibration_profile,
    *,
    progress_callback=None,
):
    """Run Stage C on the locked candidate and fresh holdout seeds."""
    return validate_final_configuration(
        system,
        optimization_result.configuration,
        calibration_profile,
        workers=SIMULATION_WORKERS,
        progress_callback=progress_callback,
    )


def _interval_summary(intervals):
    return {
        request_class: {
            "latency_statistic": interval.latency_statistic,
            "estimate": interval.estimate,
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
    evaluation_basis = getattr(evaluation, "evaluation_basis", None)
    if evaluation_basis in {
        "ANALYTICAL_MEAN_LATENCY",
        "ANALYTICAL_STABILITY",
    }:
        print("Analytical scenario/class mean latencies:", evaluation.scenario_means)
        if evaluation.limits:
            print("Analytical mean-latency limits:", evaluation.limits)
        else:
            print("No latency SLO declared; analytical stability is the constraint.")
        print(
            "Evidence warning: analytical means are not p95 estimates; no "
            "simulation or confidence interval was run."
        )
    else:
        intervals = evaluation.configuration_evaluation.intervals
        statistic = (
            next(iter(intervals.values())).latency_statistic
            if intervals
            else "latency"
        )
        print(
            f"Sequential {statistic} confidence intervals:",
            _interval_summary(intervals),
        )
        print(
            "Replications used:",
            evaluation.configuration_evaluation.replications_used,
        )
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
    intervals = validation.intervals
    statistic = (
        next(iter(intervals.values())).latency_statistic
        if intervals
        else "latency"
    )
    print(
        f"Final {statistic} confidence intervals:",
        _interval_summary(intervals),
    )
    print("Final validation seeds:", validation.seeds)


def main():
    with open("workload.yaml", "r", encoding="utf-8") as workload_file:
        workload = yaml.safe_load(workload_file)
    with open("resources.yaml", "r", encoding="utf-8") as resources_file:
        resources = yaml.safe_load(resources_file)

    legacy_scenario_count = (
        len(workload.get("scenarios", {}))
        if workload.get("schema_version") == 2
        else 0
    )
    system_type = ScenarioSystem if legacy_scenario_count > 1 else System
    system = system_type(
        "STELAR",
        workload,
        QueueingNetworkType.OPEN,
        workload_base_path=".",
    )
    enforce_workload_evaluation_mode(system, REPLICA_EVALUATION_MODE)
    live_runtime = None
    if REPLICA_EVALUATION_MODE == ReplicaEvaluationMode.ANALYTICAL:
        print(
            "Replica sizing: analytical mean-latency evaluation; calibration "
            "and simulation validation are skipped"
        )
        replica_result = optimize_replicas_analytically(system)
        report_replica_result(system, replica_result)
    else:
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
                optimization_modes=OPTIMIZATION_MODES,
            )
        except Exception as error:
            print(f"Runtime speculation unavailable: {error}")
        else:
            print(format_runtime_estimate(runtime_estimate))
            runtime_rates = getattr(runtime_estimate, "benchmark", None)
            try:
                live_runtime = LiveWorkflowRuntimeEstimator(runtime_estimate)
            except (TypeError, ValueError) as error:
                print(f"Live runtime updates unavailable: {error}")

        print("Stage A: calibrating a fresh statistical profile")
        calibration_progress = (
            live_runtime.callback("calibration") if live_runtime else None
        )
        if live_runtime:
            live_runtime.start_stage("calibration")
        try:
            profile = calibrate_before_optimization(
                system,
                options=options,
                progress_callback=calibration_progress,
            )
        except Exception as error:
            if live_runtime:
                live_runtime.fail_stage(detail=str(error))
            raise
        if live_runtime:
            live_runtime.complete_stage(
                detail=(
                    f"W={profile.warmup_duration:g}, "
                    f"M={profile.measurement_duration:g}"
                )
            )
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
                    mode=OPTIMIZATION_MODES.replica,
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
        replica_progress = (
            live_runtime.callback("replica_optimization") if live_runtime else None
        )
        if live_runtime:
            live_runtime.start_stage("replica_optimization")
        try:
            replica_result = optimize_replicas(
                system,
                profile,
                runtime_limit_calibration=runtime_limit_calibration,
                progress_callback=replica_progress,
            )
        except Exception as error:
            if live_runtime:
                live_runtime.fail_stage(detail=str(error))
            raise
        if live_runtime:
            live_runtime.complete_stage(
                detail=f"selected={replica_result.solution_mode}"
            )
        report_replica_result(system, replica_result)

        print("Stage C: validating the selected replicas with fresh seeds")
        validation_progress = (
            live_runtime.callback("final_validation") if live_runtime else None
        )
        if live_runtime:
            live_runtime.start_stage("final_validation")
        try:
            final_validation = validate_optimizer_result(
                system,
                replica_result,
                profile,
                progress_callback=validation_progress,
            )
        except Exception as error:
            if live_runtime:
                live_runtime.fail_stage(detail=str(error))
            raise
        if live_runtime:
            live_runtime.complete_stage(
                detail=f"classification={final_validation.classification.value}"
            )
        report_final_validation(final_validation)
        if not final_validation.passed:
            raise RuntimeError(
                "The optimizer candidate failed independent final validation: "
                f"{final_validation.failure_reason}"
            )

    print("Stage D: optimizing node placement")
    system.configure_system_replicas(replica_result.configuration)
    cluster = Cluster("okeanos", resources, system)
    placement_progress = (
        live_runtime.callback("node_placement")
        if live_runtime
        else None
    )
    if placement_progress is not None:
        live_runtime.start_stage("node_placement")
    try:
        placement_result = optimize_node_placement(
            NodePlacementProblem(cluster),
            mode=OPTIMIZATION_MODES.placement,
            progress_callback=placement_progress,
        )
    except Exception as error:
        if placement_progress is not None:
            live_runtime.fail_stage(detail=str(error))
        raise
    if placement_progress is not None:
        live_runtime.complete_stage(
            detail=f"selected={getattr(placement_result, 'solution_mode', 'EXACT')}"
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
    replica_mode = str(getattr(replica_result, "solution_mode", "EXACT"))
    placement_mode = str(getattr(placement_result, "solution_mode", "EXACT"))
    print(
        "Resolved workflow search mode:",
        optimization_workflow_mode(replica_mode, placement_mode),
    )


if __name__ == "__main__":
    main()

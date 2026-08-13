from dataclasses import dataclass

from calibration import (
    CalibrationOptions,
    MeasurementCalibrationError,
    WarmupCalibrationError,
    build_calibration_profile,
    calibrate_global_warmup,
    calibrate_measurement_duration,
    calibration_diagnostics_summary,
    save_configuration_evaluation,
    save_final_validation,
    save_statistical_calibration,
    save_statistical_calibration_failure,
    select_warmup_pilot_parameters,
    validate_profile_for_experiment,
)
from optimization import (
    DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    CalibratedConfigurationEvaluator,
    CalibratedReplicaSizingProblem,
    OptimizationMode,
    ScalableReplicaOptimizer,
    ScalableReplicaSearchOptions,
    UniformCostReplicaOptimizer,
    replica_configuration_count,
    optimization_mode_decision,
    validate_final_configuration as _validate_final_configuration,
)


@dataclass(frozen=True)
class CalibratedOptimizationResult:
    optimization_result: object
    configuration_evaluations: tuple[tuple[tuple[int, ...], object], ...]
    calibration_profile_fingerprint: str
    runtime_limit_calibration: object | None = None

    @property
    def configuration(self):
        return self.optimization_result.configuration

    @property
    def cost(self):
        return self.optimization_result.cost

    @property
    def metrics(self):
        return self.optimization_result.metrics

    @property
    def explored_states(self):
        return self.optimization_result.explored_states

    @property
    def path(self):
        return self.optimization_result.path

    @property
    def evaluations(self):
        return dict(self.configuration_evaluations)

    @property
    def minimality_proven(self):
        return bool(getattr(self.optimization_result, "minimality_proven", False))

    @property
    def unresolved_lower_cost_states(self):
        return tuple(
            getattr(
                self.optimization_result,
                "unresolved_lower_cost_states",
                (),
            )
        )

    @property
    def solution_mode(self):
        return str(getattr(self.optimization_result, "solution_mode", "EXACT"))

    @property
    def optimality_proven(self):
        return bool(
            getattr(
                self.optimization_result,
                "optimality_proven",
                self.minimality_proven,
            )
        )

    @property
    def budget_exhausted(self):
        return bool(getattr(self.optimization_result, "budget_exhausted", False))

    @property
    def locally_minimal(self):
        return bool(
            getattr(
                self.optimization_result,
                "locally_minimal",
                self.minimality_proven,
            )
        )

    @property
    def method(self):
        return getattr(
            self.optimization_result,
            "method",
            type(self.optimization_result).__name__,
        )

    @property
    def objective_value(self):
        return float(getattr(self.optimization_result, "objective_value", self.cost))

    @property
    def feasibility_status(self):
        return getattr(self.optimization_result, "feasibility_status", None)

    @property
    def termination_reason(self):
        return getattr(self.optimization_result, "termination_reason", None)

    @property
    def evaluation_count(self):
        return int(
            getattr(
                self.optimization_result,
                "evaluation_count",
                self.explored_states,
            )
        )

    @property
    def unique_evaluation_count(self):
        return int(
            getattr(
                self.optimization_result,
                "unique_evaluation_count",
                self.explored_states,
            )
        )

    @property
    def simulation_replications(self):
        return int(getattr(self.optimization_result, "simulation_replications", 0))

    @property
    def runtime_seconds(self):
        return float(getattr(self.optimization_result, "runtime_seconds", 0.0))

    @property
    def restart_count(self):
        return int(getattr(self.optimization_result, "restart_count", 1))

    @property
    def random_seed(self):
        return getattr(self.optimization_result, "random_seed", None)

    @property
    def deletion_local_certified(self):
        return bool(
            getattr(
                self.optimization_result,
                "deletion_local_certified",
                self.locally_minimal,
            )
        )

    @property
    def global_optimal(self):
        return bool(
            getattr(
                self.optimization_result,
                "global_optimal",
                self.optimality_proven,
            )
        )

    @property
    def lower_bound(self):
        return getattr(self.optimization_result, "lower_bound", None)

    @property
    def absolute_bound_gap(self):
        return getattr(self.optimization_result, "absolute_bound_gap", None)

    @property
    def relative_bound_gap(self):
        return getattr(self.optimization_result, "relative_bound_gap", None)

    @property
    def trace(self):
        return tuple(getattr(self.optimization_result, "trace", ()))

    @property
    def selection_reason(self):
        return getattr(self.optimization_result, "selection_reason", None)


def _maximum_configuration(system, maximum_replicas):
    service_count = len(system.get_services())
    if isinstance(maximum_replicas, int):
        maximum = (maximum_replicas,) * service_count
    else:
        maximum = tuple(maximum_replicas)
    if len(maximum) != service_count or any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in maximum
    ):
        raise ValueError("maximum_replicas must define a positive bound per service.")
    minimum = tuple(system.get_minimum_stable_replicas())
    if any(high < low for low, high in zip(minimum, maximum)):
        raise ValueError("Maximum bounds cannot be below the stable minimum.")
    return maximum


def _calibration_seeds(warmup, measurement):
    values = list(warmup.initial_parameters.calibration_seeds)
    for round_result in warmup.validation.rounds:
        for configuration in round_result.configurations:
            values.extend(configuration.seeds)
    values.extend(measurement.seeds)
    return tuple(dict.fromkeys(values))


def calibrate(
    system,
    *,
    request_class_slos,
    maximum_replicas,
    options=None,
    workers=1,
    extra_representative_configurations=(),
    report_directory=None,
    progress_callback=None,
):
    """Calibrate and freeze W, M, replication policy, CIs, and seed policy."""
    options = options or CalibrationOptions()
    maximum = _maximum_configuration(system, maximum_replicas)
    warmup_namespace = options.seed_policy.namespace("warmup_calibration")
    validation_namespace = options.seed_policy.namespace("warmup_validation")
    parameters = select_warmup_pilot_parameters(
        system,
        time_unit=options.time_unit,
        replication_count=options.warmup_replications,
        seed_start=warmup_namespace.start,
    )
    try:
        warmup = calibrate_global_warmup(
            system,
            maximum_replicas=maximum,
            parameters=parameters,
            validation_replication_count=options.warmup_validation_replications,
            validation_seed_start=validation_namespace.start,
            extra_validation_configurations=extra_representative_configurations,
            max_horizon_doublings=options.warmup_max_horizon_doublings,
            horizon_sensitivity_tolerance=options.warmup_sensitivity_tolerance,
            max_validation_rounds=options.warmup_max_validation_rounds,
            max_validation_horizon_extensions=(
                options.warmup_max_validation_horizon_extensions
            ),
            progress_callback=progress_callback,
            analysis_options={
                "window_length": options.warmup_window_length,
                "reference_tail_fraction": options.warmup_reference_tail_fraction,
                "relative_tolerance": options.warmup_relative_tolerance,
                "sensitivity_tolerance": options.warmup_sensitivity_tolerance,
                "required_consecutive_windows": (
                    options.warmup_required_stable_windows
                ),
            },
            workers=workers,
        )
    except WarmupCalibrationError as error:
        if report_directory is not None:
            save_statistical_calibration_failure(
                error,
                report_directory,
                stage="warmup",
                context={"maximum_configuration": maximum},
            )
        raise
    representatives = tuple(warmup.validation.representative_configurations)
    try:
        measurement = calibrate_measurement_duration(
            system,
            warmup_duration=warmup.global_warmup,
            representative_configurations=representatives,
            request_class_slos=request_class_slos,
            options=options,
            workers=workers,
            progress_callback=progress_callback,
        )
    except MeasurementCalibrationError as error:
        if report_directory is not None:
            save_statistical_calibration_failure(
                error,
                report_directory,
                stage="measurement",
                context={
                    "warmup_duration": warmup.global_warmup,
                    "representative_configurations": representatives,
                },
            )
        raise
    diagnostics = calibration_diagnostics_summary(warmup, measurement, options)
    profile = build_calibration_profile(
        system,
        maximum_configuration=maximum,
        request_class_slos=request_class_slos,
        warmup_duration=warmup.global_warmup,
        measurement_duration=measurement.global_measurement_duration,
        measurement_duration_candidates=measurement.candidate_durations,
        warmup_bucket_width=warmup.bucket_width,
        representative_configurations=representatives,
        calibration_diagnostics=diagnostics,
        options=options,
        calibration_seeds=_calibration_seeds(warmup, measurement),
    )
    if report_directory is not None:
        save_statistical_calibration(
            profile,
            warmup,
            measurement,
            report_directory,
            diagnostics=diagnostics,
        )
    return profile


def evaluate_configuration(
    system,
    configuration,
    calibration_profile,
    *,
    workers=1,
    report_directory=None,
):
    """Evaluate one configuration under the frozen sequential-look policy."""
    result = CalibratedConfigurationEvaluator(
        system,
        calibration_profile,
        workers=workers,
    ).evaluate(configuration)
    if report_directory is not None:
        save_configuration_evaluation(result, report_directory)
    return result


def optimize_with_calibrated_simulation(
    system,
    calibration_profile,
    *,
    optimizer=None,
    heuristic=None,
    workers=1,
    mode=OptimizationMode.AUTO,
    exact_configuration_limit=DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    scalable_options=None,
    runtime_limit_calibration=None,
):
    """Run exact or bounded scalable statistically controlled replica sizing.

    AUTO uses the full bounded configuration count, not service count alone.
    Both modes accept only statistically feasible configurations; scalable
    mode changes search coverage, never the confidence-interval procedure.
    """
    validate_profile_for_experiment(
        calibration_profile,
        system,
        request_class_slos=calibration_profile.slos,
        maximum_configuration=calibration_profile.maximum_replica_configuration,
    )
    configuration_count = (
        replica_configuration_count(calibration_profile)
        if optimizer is None or runtime_limit_calibration is not None
        else None
    )
    if runtime_limit_calibration is not None:
        if runtime_limit_calibration.calibration_profile_fingerprint != (
            calibration_profile.profile_fingerprint
        ):
            raise ValueError(
                "Runtime limit calibration belongs to a different "
                "calibration profile."
            )
        if runtime_limit_calibration.configuration_count != configuration_count:
            raise ValueError(
                "Runtime limit calibration has a different replica search space."
            )
        exact_configuration_limit = min(
            exact_configuration_limit,
            runtime_limit_calibration.effective_configuration_limit,
        )

    problem = CalibratedReplicaSizingProblem(
        system,
        calibration_profile,
        heuristic=heuristic,
        simulation_workers=workers,
    )
    if optimizer is None:
        selected_mode, selection_reason = optimization_mode_decision(
            mode,
            configuration_count,
            exact_configuration_limit,
        )
        if selected_mode == OptimizationMode.EXACT:
            optimizer = UniformCostReplicaOptimizer()
            if hasattr(optimizer, "selection_reason"):
                optimizer.selection_reason = selection_reason
        else:
            options = scalable_options or ScalableReplicaSearchOptions()
            optimizer = ScalableReplicaOptimizer(
                maximum_configuration_evaluations=(
                    options.maximum_configuration_evaluations
                ),
                maximum_total_evaluator_calls=(options.maximum_total_evaluator_calls),
                maximum_simulation_replications=(
                    options.maximum_simulation_replications
                ),
                time_budget_seconds=options.time_budget_seconds,
                candidate_width=options.candidate_width,
                restricted_candidate_fraction=(options.restricted_candidate_fraction),
                maximum_restarts=options.maximum_restarts,
                maximum_no_improvement=options.maximum_no_improvement,
                maximum_vns_neighborhood=(options.maximum_vns_neighborhood),
                vns_attempts_per_neighborhood=(options.vns_attempts_per_neighborhood),
                random_seed=options.random_seed,
                removal_order=options.removal_order,
                archive_equal_cost=options.archive_equal_cost,
                collect_trace=options.collect_trace,
                selection_reason=selection_reason,
            )
    raw_result = optimizer.optimize(problem)
    return CalibratedOptimizationResult(
        optimization_result=raw_result,
        configuration_evaluations=tuple(
            sorted(problem.configuration_evaluations.items())
        ),
        calibration_profile_fingerprint=calibration_profile.profile_fingerprint,
        runtime_limit_calibration=runtime_limit_calibration,
    )


def validate_final_configuration(
    system,
    configuration,
    calibration_profile,
    *,
    workers=1,
    report_directory=None,
):
    """Run the fixed fresh-seed holdout for the locked optimizer candidate."""
    result = _validate_final_configuration(
        system,
        configuration,
        calibration_profile,
        workers=workers,
    )
    if report_directory is not None:
        save_final_validation(result, report_directory)
    return result

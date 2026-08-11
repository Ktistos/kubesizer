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
    AStarOptimizer,
    CalibratedConfigurationEvaluator,
    CalibratedReplicaSizingProblem,
    validate_final_configuration as _validate_final_configuration,
)


@dataclass(frozen=True)
class CalibratedOptimizationResult:
    optimization_result: object
    configuration_evaluations: tuple[tuple[tuple[int, ...], object], ...]
    calibration_profile_fingerprint: str

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


def _maximum_configuration(system, maximum_replicas):
    service_count = len(system.get_services())
    if isinstance(maximum_replicas, int):
        maximum = (maximum_replicas,) * service_count
    else:
        maximum = tuple(maximum_replicas)
    if (
        len(maximum) != service_count
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in maximum
        )
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
):
    """Run replica sizing; only statistically feasible states are goals."""
    validate_profile_for_experiment(
        calibration_profile,
        system,
        request_class_slos=calibration_profile.slos,
        maximum_configuration=calibration_profile.maximum_replica_configuration,
    )
    problem = CalibratedReplicaSizingProblem(
        system,
        calibration_profile,
        heuristic=heuristic,
        simulation_workers=workers,
    )
    raw_result = (optimizer or AStarOptimizer()).optimize(problem)
    return CalibratedOptimizationResult(
        optimization_result=raw_result,
        configuration_evaluations=tuple(
            sorted(problem.configuration_evaluations.items())
        ),
        calibration_profile_fingerprint=calibration_profile.profile_fingerprint,
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

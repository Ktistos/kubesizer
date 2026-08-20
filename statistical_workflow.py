from dataclasses import dataclass, replace

from calibration import (
    CalibrationOptions,
    SeedNamespace,
    SeedPolicy,
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
    AnalyticalReplicaSizingProblem,
    CalibratedConfigurationEvaluator,
    CalibratedReplicaSizingProblem,
    OptimizationMode,
    ReplicaEvaluationMode,
    ScalableReplicaOptimizer,
    ScalableReplicaSearchOptions,
    UniformCostReplicaOptimizer,
    replica_configuration_count,
    optimization_mode_decision,
    normalize_replica_evaluation_mode,
    optimize_replica_configuration as _optimize_replica_configuration,
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


def _calibrate_one(
    system,
    *,
    request_class_slos,
    maximum_replicas,
    options=None,
    workers=1,
    extra_representative_configurations=(),
    report_directory=None,
    progress_callback=None,
    minimum_configuration=None,
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
        minimum_configuration=minimum_configuration,
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
    profile_representatives = representatives
    analytical_minimum = tuple(system.get_minimum_stable_replicas())
    if analytical_minimum not in profile_representatives:
        profile_representatives = (
            analytical_minimum,
            *profile_representatives,
        )
    profile = build_calibration_profile(
        system,
        maximum_configuration=maximum,
        request_class_slos=request_class_slos,
        warmup_duration=warmup.global_warmup,
        measurement_duration=measurement.global_measurement_duration,
        measurement_duration_candidates=measurement.candidate_durations,
        warmup_bucket_width=warmup.bucket_width,
        representative_configurations=profile_representatives,
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


def _scenario_calibration_options(options, scenario_index, scenario_count):
    """Give each scenario a disjoint slice of every calibration namespace."""
    namespaces = []
    for namespace in options.seed_policy.namespaces:
        slice_size = namespace.size // scenario_count
        if slice_size == 0:
            raise ValueError(
                f"Seed namespace {namespace.name!r} is too small for "
                f"{scenario_count} scenarios."
            )
        start = namespace.start + scenario_index * slice_size
        stop = (
            namespace.stop_exclusive
            if scenario_index == scenario_count - 1
            else start + slice_size
        )
        namespaces.append(SeedNamespace(namespace.name, start, stop))
    return replace(
        options,
        seed_policy=SeedPolicy(options.seed_policy.version, tuple(namespaces)),
    )


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
    """Calibrate one scenario or a conservative profile for every scenario."""
    options = options or CalibrationOptions()
    if not hasattr(system, "get_scenario_systems"):
        return _calibrate_one(
            system,
            request_class_slos=request_class_slos,
            maximum_replicas=maximum_replicas,
            options=options,
            workers=workers,
            extra_representative_configurations=(
                extra_representative_configurations
            ),
            report_directory=report_directory,
            progress_callback=progress_callback,
        )

    scenario_systems = system.get_scenario_systems()
    scenario_count = len(scenario_systems)
    maximum = _maximum_configuration(system, maximum_replicas)
    robust_minimum = tuple(system.get_minimum_stable_replicas())
    scenario_profiles = []
    for scenario_index, (scenario_name, scenario_system) in enumerate(
        scenario_systems.items()
    ):
        scenario_options = _scenario_calibration_options(
            options,
            scenario_index,
            scenario_count,
        )

        def scenario_progress(event, details, *, _name=scenario_name):
            if progress_callback is not None:
                progress_callback(event, {"scenario": _name, **details})

        scenario_profiles.append(
            (
                scenario_name,
                _calibrate_one(
                    scenario_system,
                    request_class_slos=request_class_slos,
                    maximum_replicas=maximum,
                    options=scenario_options,
                    workers=workers,
                    extra_representative_configurations=(
                        extra_representative_configurations
                    ),
                    report_directory=None,
                    progress_callback=scenario_progress,
                    minimum_configuration=robust_minimum,
                ),
            )
        )

    global_warmup = max(
        profile.warmup_duration for _, profile in scenario_profiles
    )
    global_measurement = max(
        profile.measurement_duration for _, profile in scenario_profiles
    )
    candidates = tuple(
        sorted(
            {
                duration
                for _, profile in scenario_profiles
                for duration in profile.measurement_duration_candidates
            }
        )
    )
    representatives = tuple(
        dict.fromkeys(
            configuration
            for _, profile in scenario_profiles
            for configuration in profile.representative_configurations
            if all(
                value >= required
                for value, required in zip(configuration, robust_minimum)
            )
        )
    )
    calibration_seeds = tuple(
        dict.fromkeys(
            seed
            for _, profile in scenario_profiles
            for seed in profile.calibration_seeds
        )
    )
    diagnostics = {
        "scenario_policy": "all scenarios must pass",
        "global_warmup": global_warmup,
        "global_measurement_duration": global_measurement,
        "scenarios": {
            name: {
                "warmup_duration": profile.warmup_duration,
                "measurement_duration": profile.measurement_duration,
                "warmup_bucket_width": profile.warmup_bucket_width,
                "diagnostics": profile.diagnostics,
            }
            for name, profile in scenario_profiles
        },
    }
    return build_calibration_profile(
        system,
        maximum_configuration=maximum,
        request_class_slos=request_class_slos,
        warmup_duration=global_warmup,
        measurement_duration=global_measurement,
        measurement_duration_candidates=candidates,
        warmup_bucket_width=min(
            profile.warmup_bucket_width for _, profile in scenario_profiles
        ),
        representative_configurations=representatives,
        calibration_diagnostics=diagnostics,
        options=options,
        calibration_seeds=calibration_seeds,
    )


def evaluate_configuration(
    system,
    configuration,
    calibration_profile,
    *,
    workers=1,
    report_directory=None,
    progress_callback=None,
):
    """Evaluate one configuration under the frozen sequential-look policy."""
    evaluator_options = {"workers": workers}
    if progress_callback is not None:
        evaluator_options["progress_callback"] = progress_callback
    result = CalibratedConfigurationEvaluator(
        system,
        calibration_profile,
        **evaluator_options,
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
    replica_costs=None,
    structural_feasibility=None,
    progress_callback=None,
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
        replica_costs=replica_costs,
        structural_feasibility=structural_feasibility,
        progress_callback=progress_callback,
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
        evaluation_limit = (
            configuration_count
            if selected_mode == OptimizationMode.EXACT
            else min(configuration_count, options.maximum_configuration_evaluations)
        )
        if progress_callback is not None:
            progress_callback(
                "replica_optimization_started",
                {
                    "selected_mode": selected_mode.value,
                    "completed_configurations": 0,
                    "total_configurations": evaluation_limit,
                    "search_space_configurations": configuration_count,
                },
            )
    elif progress_callback is not None:
        progress_callback(
            "replica_optimization_started",
            {
                "selected_mode": type(optimizer).__name__,
                "completed_configurations": 0,
                "total_configurations": configuration_count,
                "search_space_configurations": configuration_count,
            },
        )
    raw_result = optimizer.optimize(problem)
    if progress_callback is not None:
        progress_callback(
            "replica_optimization_completed",
            {
                "selected_mode": getattr(raw_result, "solution_mode", "EXACT"),
                "completed_configurations": len(problem.configuration_evaluations),
                "search_space_configurations": configuration_count,
            },
        )
    return CalibratedOptimizationResult(
        optimization_result=raw_result,
        configuration_evaluations=tuple(
            sorted(problem.configuration_evaluations.items())
        ),
        calibration_profile_fingerprint=calibration_profile.profile_fingerprint,
        runtime_limit_calibration=runtime_limit_calibration,
    )


def optimize_with_analytical_model(
    system,
    *,
    request_class_mean_latency_limits,
    maximum_replicas,
    optimizer=None,
    mode=OptimizationMode.AUTO,
    exact_configuration_limit=DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    scalable_options=None,
    replica_costs=None,
    structural_feasibility=None,
):
    """Optimize analytical end-to-end mean latency without simulation.

    EXACT proves minimum weighted replica cost only for the bounded analytical
    model. SCALABLE (also accepted as APPROXIMATE) is a budgeted heuristic.
    Neither mode estimates p95 latency or provides statistical SLO evidence.
    """
    problem = AnalyticalReplicaSizingProblem(
        system,
        request_class_mean_latency_limits,
        maximum_replicas,
        replica_costs=replica_costs,
        structural_feasibility=structural_feasibility,
    )
    if optimizer is not None:
        return optimizer.optimize(problem)
    return _optimize_replica_configuration(
        problem,
        mode=mode,
        exact_configuration_limit=exact_configuration_limit,
        scalable_options=scalable_options,
    )


def optimize_replica_sizing(
    system,
    *,
    evaluation_mode,
    mode=OptimizationMode.AUTO,
    calibration_profile=None,
    request_class_mean_latency_limits=None,
    maximum_replicas=None,
    optimizer=None,
    heuristic=None,
    workers=1,
    exact_configuration_limit=DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    scalable_options=None,
    runtime_limit_calibration=None,
    replica_costs=None,
    structural_feasibility=None,
    progress_callback=None,
):
    """Select evaluation evidence independently from the search algorithm.

    ``evaluation_mode=ANALYTICAL`` uses analytical request-class mean-latency
    constraints and performs no simulation. ``SIMULATION`` requires a frozen
    calibration profile and uses its p95 confidence-bound classification.
    Both modes support AUTO, EXACT, and SCALABLE/APPROXIMATE search.
    """
    evaluation_mode = normalize_replica_evaluation_mode(evaluation_mode)
    if evaluation_mode == ReplicaEvaluationMode.ANALYTICAL:
        if calibration_profile is not None:
            raise ValueError(
                "Analytical optimization does not consume a calibration profile."
            )
        if request_class_mean_latency_limits is None:
            raise ValueError(
                "Analytical optimization requires request-class mean-latency "
                "limits."
            )
        if maximum_replicas is None:
            raise ValueError(
                "Analytical optimization requires maximum replica bounds."
            )
        return optimize_with_analytical_model(
            system,
            request_class_mean_latency_limits=(
                request_class_mean_latency_limits
            ),
            maximum_replicas=maximum_replicas,
            optimizer=optimizer,
            mode=mode,
            exact_configuration_limit=exact_configuration_limit,
            scalable_options=scalable_options,
            replica_costs=replica_costs,
            structural_feasibility=structural_feasibility,
        )

    if calibration_profile is None:
        raise ValueError(
            "Simulation optimization requires a frozen calibration profile."
        )
    if request_class_mean_latency_limits is not None:
        raise ValueError(
            "Simulation optimization uses p95 SLOs from the calibration profile, "
            "not analytical mean-latency limits."
        )
    if maximum_replicas is not None:
        expected = tuple(calibration_profile.maximum_replica_configuration)
        supplied = _maximum_configuration(system, maximum_replicas)
        if supplied != expected:
            raise ValueError(
                "Simulation maximum bounds must match the calibration profile."
            )
    return optimize_with_calibrated_simulation(
        system,
        calibration_profile,
        optimizer=optimizer,
        heuristic=heuristic,
        workers=workers,
        mode=mode,
        exact_configuration_limit=exact_configuration_limit,
        scalable_options=scalable_options,
        runtime_limit_calibration=runtime_limit_calibration,
        replica_costs=replica_costs,
        structural_feasibility=structural_feasibility,
        progress_callback=progress_callback,
    )


def validate_final_configuration(
    system,
    configuration,
    calibration_profile,
    *,
    workers=1,
    report_directory=None,
    progress_callback=None,
):
    """Run the fixed fresh-seed holdout for the locked optimizer candidate."""
    result = _validate_final_configuration(
        system,
        configuration,
        calibration_profile,
        workers=workers,
        progress_callback=progress_callback,
    )
    if report_directory is not None:
        save_final_validation(result, report_directory)
    return result

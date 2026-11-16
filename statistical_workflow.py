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
    validate_failure_rate_replication_policy,
    validate_timeout_failure_rate_constraints,
    validate_profile_for_experiment,
)
from optimization import (
    AnalyticalReplicaSizingProblem,
    CalibratedConfigurationEvaluator,
    CalibratedReplicaSizingProblem,
    ReplicaEvaluationMode,
    UniformCostReplicaOptimizer,
    replica_configuration_count,
    normalize_replica_evaluation_mode,
    optimize_replica_configuration as _optimize_replica_configuration,
    validate_final_configuration as _validate_final_configuration,
)


@dataclass(frozen=True)
class CalibratedOptimizationResult:
    optimization_result: object
    configuration_evaluations: tuple[tuple[tuple[int, ...], object], ...]
    calibration_profile_fingerprint: str
    predictive_validation_status: str = "NOT_PROVIDED"
    predictive_validation_certificate_fingerprint: str | None = None

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
    def optimality_proven(self):
        return bool(
            getattr(
                self.optimization_result,
                "optimality_proven",
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


@dataclass(frozen=True)
class RequestClassRecommendationEvidence:
    request_class: str
    latency_statistic: str
    estimate_seconds: float
    lower_bound_seconds: float
    upper_bound_seconds: float
    slo_seconds: float
    simultaneous_slo_margin_seconds: float
    simultaneous_slo_margin_fraction: float
    predictive_holdout_case_count: int
    maximum_absolute_predictive_error_seconds: float | None
    maximum_absolute_relative_predictive_error: float | None


@dataclass(frozen=True)
class ValidatedRecommendationEvidenceSummary:
    status: str
    request_classes: tuple[RequestClassRecommendationEvidence, ...]
    predictive_validation_status: str
    bounded_model_optimal: bool
    unresolved_lower_cost_configurations: tuple[tuple[int, ...], ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class ValidatedReplicaRecommendation:
    """End-to-end recommendation evidence, including model and final holdouts."""

    service_replicas: tuple[tuple[str, int], ...]
    status: str
    optimization: CalibratedOptimizationResult
    final_validation: object
    predictive_validation: object
    bounded_model_optimal: bool
    guarantee: str

    @property
    def configuration(self):
        return tuple(replicas for _, replicas in self.service_replicas)

    @property
    def recommendable(self):
        return self.status in {
            "VALIDATED_BOUNDED_MODEL_OPTIMUM",
            "VALIDATED_FEASIBLE_NOT_PROVEN",
        }

    @property
    def evidence_summary(self):
        predictive_by_class = {}
        for case in getattr(self.predictive_validation, "cases", ()):
            for diagnostic in getattr(case, "diagnostics", ()):
                predictive_by_class.setdefault(
                    diagnostic.request_class,
                    [],
                ).append(diagnostic)

        request_classes = []
        warnings = []
        for request_class, interval in getattr(
            self.final_validation,
            "request_class_intervals",
            (),
        ):
            slo = float(interval.slo)
            margin = slo - float(interval.upper_bound)
            diagnostics = predictive_by_class.get(request_class, ())
            relative_errors = tuple(
                abs(float(item.relative_error)) for item in diagnostics
            )
            absolute_errors = tuple(
                float(item.absolute_error_seconds) for item in diagnostics
            )
            margin_fraction = margin / slo
            if margin_fraction < 0.05:
                warnings.append(
                    f"{request_class} has less than 5% simultaneous SLO margin"
                )
            request_classes.append(
                RequestClassRecommendationEvidence(
                    request_class=request_class,
                    latency_statistic=str(interval.latency_statistic),
                    estimate_seconds=float(interval.estimate),
                    lower_bound_seconds=float(interval.lower_bound),
                    upper_bound_seconds=float(interval.upper_bound),
                    slo_seconds=slo,
                    simultaneous_slo_margin_seconds=margin,
                    simultaneous_slo_margin_fraction=margin_fraction,
                    predictive_holdout_case_count=len(diagnostics),
                    maximum_absolute_predictive_error_seconds=(
                        max(absolute_errors) if absolute_errors else None
                    ),
                    maximum_absolute_relative_predictive_error=(
                        max(relative_errors) if relative_errors else None
                    ),
                )
            )
        if not self.bounded_model_optimal:
            warnings.append(
                "exact search found a validated configuration, but minimum cost "
                "was not proven"
            )
        unresolved = self.optimization.unresolved_lower_cost_states
        if unresolved:
            warnings.append("lower-cost configurations remain statistically unresolved")
        return ValidatedRecommendationEvidenceSummary(
            status=(
                "BLOCKED"
                if not self.recommendable
                else (
                    "VALIDATED_BOUNDED_MODEL"
                    if self.bounded_model_optimal
                    else "VALIDATED_NOT_PROVEN"
                )
            ),
            request_classes=tuple(request_classes),
            predictive_validation_status=str(
                getattr(
                    getattr(self.predictive_validation, "status", None),
                    "value",
                    "UNKNOWN",
                )
            ),
            bounded_model_optimal=self.bounded_model_optimal,
            unresolved_lower_cost_configurations=unresolved,
            warnings=tuple(warnings),
        )


def _declared_latency_slo_kind(system):
    method = getattr(system, "get_request_class_latency_slo_kind", None)
    return method() if method is not None else None


def _workload_simulation_limits(system):
    kind = _declared_latency_slo_kind(system)
    limits = (
        system.get_request_class_mean_latency_limits()
        if kind == "mean"
        else system.get_request_class_percentile_latency_limits(quantile=0.95)
    )
    if not limits:
        raise ValueError(
            "Simulation calibration requires at least one mean or percentile SLO."
        )
    return limits


def _workload_mean_limits(system):
    kind = _declared_latency_slo_kind(system)
    if kind == "percentile":
        raise ValueError(
            "This workload selects percentile latency SLOs, which require "
            "simulation and cannot be certified by strict analytical evaluation."
        )
    limits = system.get_request_class_mean_latency_limits()
    return limits


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
    maximum = getattr(system, "constrain_maximum_replicas", lambda values: values)(
        maximum
    )
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


def _warmup_bucket_cap(system, options):
    """Convert the global diagnostic-memory budget to a per-series cap."""
    service_count = len(system.get_services())
    derived_series_count = 1 + 4 * service_count
    global_share = options.warmup_max_total_bucket_values // derived_series_count
    return max(
        500,
        min(options.warmup_max_buckets_per_series, global_share),
    )


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
    latency_slo_kind="percentile",
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
            max_buckets_per_series=_warmup_bucket_cap(system, options),
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
            retain_diagnostic_series=report_directory is not None,
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
            latency_slo_kind=latency_slo_kind,
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
    if (
        minimum_configuration is None
        and analytical_minimum not in profile_representatives
    ):
        profile_representatives = (
            analytical_minimum,
            *profile_representatives,
        )
    profile = build_calibration_profile(
        system,
        maximum_configuration=maximum,
        minimum_configuration=minimum_configuration,
        request_class_slos=request_class_slos,
        warmup_duration=warmup.global_warmup,
        measurement_duration=measurement.global_measurement_duration,
        measurement_duration_candidates=measurement.candidate_durations,
        warmup_bucket_width=warmup.bucket_width,
        representative_configurations=profile_representatives,
        calibration_diagnostics=diagnostics,
        options=options,
        calibration_seeds=_calibration_seeds(warmup, measurement),
        latency_slo_kind=latency_slo_kind,
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


def calibrate(
    system,
    *,
    request_class_slos=None,
    maximum_replicas,
    options=None,
    workers=1,
    minimum_configuration=None,
    extra_representative_configurations=(),
    report_directory=None,
    progress_callback=None,
):
    """Calibrate the workload's single traffic model."""
    options = options or CalibrationOptions()
    declared_kind = _declared_latency_slo_kind(system)
    latency_slo_kind = declared_kind or "percentile"
    if request_class_slos is None:
        request_class_slos = _workload_simulation_limits(system)
    elif declared_kind in {"mean", "percentile"}:
        declared_limits = _workload_simulation_limits(system)
        if dict(request_class_slos) != declared_limits:
            raise ValueError(
                "Explicit simulation SLOs must match those declared by the workload."
            )
    if not request_class_slos:
        raise ValueError("Simulation calibration requires at least one latency SLO.")
    validate_timeout_failure_rate_constraints(
        system,
        request_class_slos,
    )
    validate_failure_rate_replication_policy(
        options,
        request_class_slos,
        getattr(system, "get_request_class_failure_rate_slos", lambda: {})(),
    )
    return _calibrate_one(
        system,
        request_class_slos=request_class_slos,
        maximum_replicas=maximum_replicas,
        options=options,
        workers=workers,
        minimum_configuration=minimum_configuration,
        extra_representative_configurations=extra_representative_configurations,
        report_directory=report_directory,
        progress_callback=progress_callback,
        latency_slo_kind=latency_slo_kind,
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
    workers=1,
    replica_costs=None,
    structural_feasibility=None,
    progress_callback=None,
    predictive_validation=None,
    require_predictive_validation=False,
):
    """Run exact, statistically controlled uniform-cost replica sizing."""
    if predictive_validation is None:
        if require_predictive_validation:
            raise ValueError(
                "A held-out predictive-validation certificate is required for "
                "this simulation recommendation."
            )
        predictive_status = "NOT_PROVIDED"
        predictive_fingerprint = None
    else:
        predictive_validation.assert_valid_for(system)
        predictive_status = predictive_validation.status.value
        predictive_fingerprint = predictive_validation.certificate_fingerprint

    validate_profile_for_experiment(
        calibration_profile,
        system,
        request_class_slos=calibration_profile.slos,
        maximum_configuration=calibration_profile.maximum_replica_configuration,
    )
    configuration_count = replica_configuration_count(calibration_profile)

    problem = CalibratedReplicaSizingProblem(
        system,
        calibration_profile,
        simulation_workers=workers,
        replica_costs=replica_costs,
        structural_feasibility=structural_feasibility,
        progress_callback=progress_callback,
    )
    if progress_callback is not None:
        progress_callback(
            "replica_optimization_started",
            {
                "completed_configurations": 0,
                "total_configurations": configuration_count,
                "search_space_configurations": configuration_count,
            },
        )
    optimizer = UniformCostReplicaOptimizer()
    raw_result = optimizer.optimize(problem)
    if progress_callback is not None:
        progress_callback(
            "replica_optimization_completed",
            {
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
        predictive_validation_status=predictive_status,
        predictive_validation_certificate_fingerprint=predictive_fingerprint,
    )


def optimize_with_predictively_validated_simulation(
    system,
    calibration_profile,
    predictive_validation,
    **options,
):
    """Run simulation sizing only after held-out model validation passes."""
    return optimize_with_calibrated_simulation(
        system,
        calibration_profile,
        predictive_validation=predictive_validation,
        require_predictive_validation=True,
        **options,
    )


def optimize_with_analytical_model(
    system,
    *,
    request_class_mean_latency_limits=None,
    maximum_replicas,
    minimum_configuration=None,
    replica_costs=None,
    structural_feasibility=None,
    progress_callback=None,
):
    """Optimize analytical stability and optional mean latency without simulation.

    Uniform-cost search proves minimum weighted replica cost only for the
    bounded analytical model.
    With no declared mean SLO, the stable vector is the capacity target. Neither
    analytical evaluation nor exact search estimates p95 latency or provides
    statistical SLO evidence.
    ``minimum_configuration`` is a hard lower bound and must itself be at or
    above analytical stability.
    """
    declared_kind = _declared_latency_slo_kind(system)
    declared_timeouts = getattr(
        system,
        "get_request_class_end_to_end_timeouts",
        lambda: {},
    )()
    if any(float(value) > 0.0 for value in declared_timeouts.values()):
        raise ValueError(
            "Request-class end-to-end deadlines require simulation; strict "
            "analytical evaluation does not model terminal timeouts."
        )
    analytical_incompatibilities = getattr(
        system,
        "get_strict_analytical_incompatibilities",
        lambda: (),
    )()
    if analytical_incompatibilities:
        raise ValueError(
            "Strict analytical evaluation cannot certify this workload: "
            + "; ".join(analytical_incompatibilities)
            + ". Use simulation replica evaluation."
        )
    if declared_kind == "percentile":
        raise ValueError(
            "This workload selects percentile latency SLOs, which require "
            "simulation and cannot be certified by strict analytical evaluation."
        )
    if request_class_mean_latency_limits is None:
        request_class_mean_latency_limits = _workload_mean_limits(system)
    elif declared_kind == "mean":
        declared_limits = _workload_mean_limits(system)
        if dict(request_class_mean_latency_limits) != declared_limits:
            raise ValueError(
                "Explicit mean-latency limits must match those declared by "
                "the workload."
            )
    problem = AnalyticalReplicaSizingProblem(
        system,
        request_class_mean_latency_limits,
        maximum_replicas,
        minimum_configuration=minimum_configuration,
        replica_costs=replica_costs,
        structural_feasibility=structural_feasibility,
        progress_callback=progress_callback,
    )
    return _optimize_replica_configuration(
        problem,
        progress_callback=progress_callback,
    )


def optimize_replica_sizing(
    system,
    *,
    evaluation_mode,
    calibration_profile=None,
    request_class_mean_latency_limits=None,
    maximum_replicas=None,
    workers=1,
    replica_costs=None,
    structural_feasibility=None,
    progress_callback=None,
    predictive_validation=None,
    require_predictive_validation=False,
):
    """Select evaluation evidence independently from the search algorithm.

    ``evaluation_mode=ANALYTICAL`` uses analytical request-class mean-latency
    constraints and performs no simulation. ``SIMULATION`` requires a frozen
    calibration profile and uses its confidence-bound classification for the
    declared mean or percentile statistic.
    Both evaluation modes use exact uniform-cost replica search.
    """
    evaluation_mode = normalize_replica_evaluation_mode(evaluation_mode)
    if evaluation_mode == ReplicaEvaluationMode.ANALYTICAL:
        if calibration_profile is not None:
            raise ValueError(
                "Analytical optimization does not consume a calibration profile."
            )
        if request_class_mean_latency_limits is None:
            request_class_mean_latency_limits = _workload_mean_limits(system)
        if maximum_replicas is None:
            raise ValueError("Analytical optimization requires maximum replica bounds.")
        return optimize_with_analytical_model(
            system,
            request_class_mean_latency_limits=(request_class_mean_latency_limits),
            maximum_replicas=maximum_replicas,
            replica_costs=replica_costs,
            structural_feasibility=structural_feasibility,
            progress_callback=progress_callback,
        )

    if calibration_profile is None:
        raise ValueError(
            "Simulation optimization requires a frozen calibration profile."
        )
    if request_class_mean_latency_limits is not None:
        raise ValueError(
            "Simulation optimization uses the latency SLOs frozen in the "
            "calibration profile, not separate analytical limits."
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
        workers=workers,
        replica_costs=replica_costs,
        structural_feasibility=structural_feasibility,
        progress_callback=progress_callback,
        predictive_validation=predictive_validation,
        require_predictive_validation=require_predictive_validation,
    )


def validate_final_configuration(
    system,
    configuration,
    calibration_profile,
    *,
    workers=1,
    report_directory=None,
    progress_callback=None,
    predictive_validation=None,
    require_predictive_validation=False,
):
    """Run the fixed fresh-seed holdout for the locked optimizer candidate."""
    if predictive_validation is None:
        if require_predictive_validation:
            raise ValueError(
                "A held-out predictive-validation certificate is required for "
                "this final recommendation."
            )
    else:
        predictive_validation.assert_valid_for(system)
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


def recommend_replicas_with_validated_simulation(
    system,
    calibration_profile,
    predictive_validation,
    *,
    workers=1,
    replica_costs=None,
    structural_feasibility=None,
    progress_callback=None,
):
    """Optimize and validate a plan using a predictively validated simulator.

    ``bounded_model_optimal`` is a proof only inside the declared finite bounds
    and validated simulator domain. It is not a claim about unmodeled live-
    cluster behaviors.
    """
    optimization = optimize_with_predictively_validated_simulation(
        system,
        calibration_profile,
        predictive_validation,
        workers=workers,
        replica_costs=replica_costs,
        structural_feasibility=structural_feasibility,
        progress_callback=progress_callback,
    )
    final_validation = validate_final_configuration(
        system,
        optimization.configuration,
        calibration_profile,
        workers=workers,
        progress_callback=progress_callback,
        predictive_validation=predictive_validation,
        require_predictive_validation=True,
    )
    services = tuple(
        sorted(system.get_services().values(), key=lambda item: item.get_id())
    )
    service_replicas = tuple(
        (service.get_name(), replicas)
        for service, replicas in zip(
            services,
            optimization.configuration,
            strict=True,
        )
    )
    bounded_model_optimal = bool(optimization.optimality_proven)
    if not final_validation.passed:
        status = "FINAL_VALIDATION_FAILED"
        guarantee = (
            "No recommendation: fresh simulation seeds did not certify every "
            "request-class SLO."
        )
    elif bounded_model_optimal:
        status = "VALIDATED_BOUNDED_MODEL_OPTIMUM"
        guarantee = (
            "Minimum weighted replica cost was proven within the declared "
            "bounds for a simulator that passed held-out predictive validation."
        )
    else:
        status = "VALIDATED_FEASIBLE_NOT_PROVEN"
        guarantee = (
            "The plan passed held-out model validation and fresh final SLO "
            "validation; minimum replica cost was not proven because lower-cost "
            "configurations remained statistically unresolved."
        )
    return ValidatedReplicaRecommendation(
        service_replicas=service_replicas,
        status=status,
        optimization=optimization,
        final_validation=final_validation,
        predictive_validation=predictive_validation,
        bounded_model_optimal=bounded_model_optimal,
        guarantee=guarantee,
    )

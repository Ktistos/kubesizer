import math
from dataclasses import dataclass, replace

from .cutoff import (
    WarmupCutoffNotFoundError,
    WarmupCutoffResult,
    detect_ensemble_warmup_cutoff,
)
from .ensemble import (
    WarmupEnsembleSeries,
    average_warmup_replications,
    slice_and_rebucket_warmup_ensemble,
)
from .pilot import (
    rebucket_warmup_replication,
    run_warmup_pilot_replications,
    slice_warmup_replication,
)
from .sensitivity import (
    BucketWidthSensitivityResult,
    HorizonAdequacyResult,
    WarmupDurationComparison,
    check_bucket_width_sensitivity,
    check_horizon_adequacy,
    compare_warmup_durations,
)
from .validation import (
    GlobalWarmupValidationResult,
    WarmupValidationError,
    build_representative_configurations,
    validate_global_warmup,
)


class WarmupCalibrationError(RuntimeError):
    """Raised when the calibration evidence does not support a numerical W."""

    def __init__(self, message, diagnostics=None):
        super().__init__(message)
        self.diagnostics = diagnostics


@dataclass(frozen=True)
class BucketWidthAnalysis:
    bucket_width: float
    ensemble: WarmupEnsembleSeries
    cutoff: WarmupCutoffResult
    horizon_adequacy: HorizonAdequacyResult


@dataclass(frozen=True)
class WarmupHorizonAnalysis:
    pilot_horizon: float
    bucket_width_analyses: tuple[BucketWidthAnalysis, ...]
    bucket_width_sensitivity: BucketWidthSensitivityResult
    selected_bucket_width: float | None
    passed: bool

    def selected_analysis(self):
        if self.selected_bucket_width is None:
            return None
        return next(
            analysis
            for analysis in self.bucket_width_analyses
            if analysis.bucket_width == self.selected_bucket_width
        )


@dataclass(frozen=True)
class WarmupHorizonAttempt:
    pilot_horizon: float
    analysis: WarmupHorizonAnalysis | None
    failure_reason: str | None


@dataclass(frozen=True)
class MinimumConfigurationCalibrationResult:
    parameters: object
    shorter_horizon_parameters: object
    attempts: tuple[WarmupHorizonAttempt, ...]
    final_analysis: WarmupHorizonAnalysis
    shorter_horizon_analysis: WarmupHorizonAnalysis
    horizon_comparison: WarmupDurationComparison

    @property
    def warmup_duration(self):
        return self.final_analysis.selected_analysis().cutoff.warmup_duration

    @property
    def bucket_width(self):
        return self.final_analysis.selected_bucket_width


@dataclass(frozen=True)
class WarmupCalibrationResult:
    initial_parameters: object
    minimum_configuration_calibrations: tuple[
        MinimumConfigurationCalibrationResult, ...
    ]
    validation: GlobalWarmupValidationResult
    validation_seed_starts: tuple[int, ...]
    validation_failure_reasons: tuple[str, ...]
    worker_processes: int
    global_warmup: float

    @property
    def final_parameters(self):
        return self.minimum_configuration_calibrations[-1].parameters

    @property
    def bucket_width(self):
        return self.minimum_configuration_calibrations[-1].bucket_width


def parameters_with_pilot_horizon(parameters, pilot_horizon):
    """Return an immutable parameter record for a longer, bucket-aligned H."""
    if not math.isfinite(pilot_horizon) or pilot_horizon <= 0:
        raise ValueError("pilot_horizon must be finite and greater than zero.")
    largest_width = max(parameters.bucket_width_candidates)
    aligned_horizon = math.ceil(pilot_horizon / largest_width) * largest_width
    bucket_counts = tuple(
        math.floor(aligned_horizon / width + 1e-12)
        for width in parameters.bucket_width_candidates
    )
    return replace(
        parameters,
        raw_pilot_horizon=float(pilot_horizon),
        pilot_horizon=float(aligned_horizon),
        bucket_counts=bucket_counts,
    )


def window_length_for_bucket_width(
    *,
    reference_bucket_width,
    reference_window_length,
    bucket_width,
):
    """Scale a bucket count while preserving the physical window duration."""
    for name, value in (
        ("reference_bucket_width", reference_bucket_width),
        ("bucket_width", bucket_width),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{name} must be finite and greater than zero.")
    if (
        isinstance(reference_window_length, bool)
        or not isinstance(reference_window_length, int)
        or reference_window_length <= 0
    ):
        raise ValueError("reference_window_length must be a positive integer.")

    physical_duration = reference_bucket_width * reference_window_length
    unrounded_length = physical_duration / bucket_width
    scaled_length = round(unrounded_length)
    if scaled_length < 1 or not math.isclose(
        unrounded_length,
        scaled_length,
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        raise ValueError(
            "The physical analysis-window duration must align with every "
            "candidate bucket width."
        )
    return int(scaled_length)


def analyze_warmup_horizon(
    system,
    parameters,
    *,
    window_length=10,
    reference_tail_fraction=0.20,
    relative_tolerance=0.05,
    absolute_tolerances=None,
    sensitivity_tolerance=0.10,
    required_consecutive_windows=3,
    workers=1,
    raw_replications=None,
    native_ensemble=None,
):
    """Run one H and evaluate deltas with one physical analysis-window size.

    ``window_length`` is the number of buckets at the initial bucket width. It
    is scaled for every candidate width so bucket-width sensitivity does not
    also change the amount of simulated time in an analysis window.
    """
    if native_ensemble is not None and raw_replications is not None:
        raise ValueError("Provide either native_ensemble or raw_replications, not both.")
    if native_ensemble is None and raw_replications is None:
        raw_replications = run_warmup_pilot_replications(
            system,
            parameters,
            bucket_width=parameters.initial_bucket_width,
            workers=workers,
        )
    elif native_ensemble is None:
        raw_replications = tuple(raw_replications)
        if not raw_replications:
            raise ValueError("raw_replications cannot be empty.")
        raw_replications = tuple(
            replication
            if (
                math.isclose(
                    replication.pilot_horizon,
                    parameters.pilot_horizon,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
                and math.isclose(
                    replication.bucket_width,
                    parameters.initial_bucket_width,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
            )
            else slice_warmup_replication(
                replication,
                parameters.pilot_horizon,
                bucket_width=parameters.initial_bucket_width,
            )
            for replication in raw_replications
        )
    analyses = []
    for bucket_width in parameters.bucket_width_candidates:
        candidate_window_length = window_length_for_bucket_width(
            reference_bucket_width=parameters.initial_bucket_width,
            reference_window_length=window_length,
            bucket_width=bucket_width,
        )
        if native_ensemble is not None:
            ensemble = slice_and_rebucket_warmup_ensemble(
                native_ensemble,
                parameters.pilot_horizon,
                bucket_width,
            )
        else:
            if bucket_width == parameters.initial_bucket_width:
                replications = raw_replications
            else:
                replications = tuple(
                    rebucket_warmup_replication(replication, bucket_width)
                    for replication in raw_replications
                )
            ensemble = average_warmup_replications(replications)
        cutoff = detect_ensemble_warmup_cutoff(
            ensemble,
            window_length=candidate_window_length,
            reference_tail_fraction=reference_tail_fraction,
            relative_tolerance=relative_tolerance,
            absolute_tolerances=absolute_tolerances,
            required_consecutive_windows=required_consecutive_windows,
        )
        adequacy = check_horizon_adequacy(
            ensemble,
            cutoff,
            relative_tolerance=relative_tolerance,
            absolute_tolerances=absolute_tolerances,
        )
        analyses.append(
            BucketWidthAnalysis(
                bucket_width=bucket_width,
                ensemble=ensemble,
                cutoff=cutoff,
                horizon_adequacy=adequacy,
            )
        )

    sensitivity = check_bucket_width_sensitivity(
        (analysis.cutoff for analysis in analyses),
        pilot_horizon=parameters.pilot_horizon,
        tolerance=sensitivity_tolerance,
    )
    passed = sensitivity.passed and all(
        analysis.horizon_adequacy.passed for analysis in analyses
    )
    return WarmupHorizonAnalysis(
        pilot_horizon=parameters.pilot_horizon,
        bucket_width_analyses=tuple(analyses),
        bucket_width_sensitivity=sensitivity,
        selected_bucket_width=(
            sensitivity.selected_bucket_width if passed else None
        ),
        passed=passed,
    )


def calibrate_minimum_configuration(
    system,
    parameters,
    *,
    starting_horizon=None,
    max_horizon_doublings=5,
    horizon_sensitivity_tolerance=0.10,
    progress_callback=None,
    analysis_options=None,
    workers=1,
):
    """Double H until two adequate estimates agree, reusing paired prefixes."""
    if not isinstance(max_horizon_doublings, int) or max_horizon_doublings < 1:
        raise ValueError("max_horizon_doublings must be at least one.")
    analysis_options = dict(analysis_options or {})
    current_horizon = (
        parameters.pilot_horizon
        if starting_horizon is None
        else float(starting_horizon)
    )
    horizon_parameters = []
    for _ in range(max_horizon_doublings + 1):
        current_parameters = parameters_with_pilot_horizon(
            parameters,
            current_horizon,
        )
        horizon_parameters.append(current_parameters)
        current_horizon = current_parameters.pilot_horizon * 2.0

    previous_success = None
    attempts = []
    current_parameters = horizon_parameters[0]

    for batch_start in range(0, len(horizon_parameters), 2):
        batch = horizon_parameters[batch_start : batch_start + 2]
        simulation_parameters = batch[-1]
        if progress_callback is not None:
            progress_callback(
                "pilot_batch_started",
                {
                    "simulation_horizon": simulation_parameters.pilot_horizon,
                    "analysis_horizons": tuple(
                        item.pilot_horizon for item in batch
                    ),
                    "replications": simulation_parameters.replication_count,
                    "workers": workers,
                },
            )
        batch_ensemble = run_warmup_pilot_replications(
            system,
            simulation_parameters,
            bucket_width=min(simulation_parameters.bucket_width_candidates),
            workers=workers,
            aggregate=True,
        )

        for current_parameters in batch:
            if progress_callback is not None:
                progress_callback(
                    "pilot_horizon_analyzing",
                    {
                        "pilot_horizon": current_parameters.pilot_horizon,
                        "simulation_horizon": simulation_parameters.pilot_horizon,
                        "reused_prefix": (
                            current_parameters.pilot_horizon
                            < simulation_parameters.pilot_horizon
                        ),
                    },
                )
            try:
                analysis = analyze_warmup_horizon(
                    system,
                    current_parameters,
                    workers=workers,
                    native_ensemble=batch_ensemble,
                    **analysis_options,
                )
            except WarmupCutoffNotFoundError as error:
                attempts.append(
                    WarmupHorizonAttempt(
                        pilot_horizon=current_parameters.pilot_horizon,
                        analysis=None,
                        failure_reason=(
                            f"metric {error.metric_name} did not settle"
                        ),
                    )
                )
                previous_success = None
            else:
                failure_reason = None
                if not analysis.passed:
                    failed_widths = [
                        str(item.bucket_width)
                        for item in analysis.bucket_width_analyses
                        if not item.horizon_adequacy.passed
                    ]
                    causes = []
                    if failed_widths:
                        causes.append(
                            "horizon checks failed for delta="
                            + ", ".join(failed_widths)
                        )
                    if not analysis.bucket_width_sensitivity.passed:
                        causes.append("bucket-width sensitivity exceeded tolerance")
                    failure_reason = "; ".join(causes)
                attempts.append(
                    WarmupHorizonAttempt(
                        pilot_horizon=current_parameters.pilot_horizon,
                        analysis=analysis,
                        failure_reason=failure_reason,
                    )
                )

                if analysis.passed:
                    if previous_success is not None:
                        previous_cutoff = (
                            previous_success[1].selected_analysis().cutoff
                        )
                        current_cutoff = analysis.selected_analysis().cutoff
                        comparison = compare_warmup_durations(
                            previous_cutoff.warmup_duration,
                            current_cutoff.warmup_duration,
                            tolerance=horizon_sensitivity_tolerance,
                        )
                        if comparison.passed:
                            if progress_callback is not None:
                                progress_callback(
                                    "minimum_configuration_accepted",
                                    {
                                        "pilot_horizon": (
                                            current_parameters.pilot_horizon
                                        ),
                                        "bucket_width": (
                                            analysis.selected_bucket_width
                                        ),
                                        "warmup_duration": (
                                            current_cutoff.warmup_duration
                                        ),
                                    },
                                )
                            return MinimumConfigurationCalibrationResult(
                                parameters=current_parameters,
                                shorter_horizon_parameters=previous_success[0],
                                attempts=tuple(attempts),
                                final_analysis=analysis,
                                shorter_horizon_analysis=previous_success[1],
                                horizon_comparison=comparison,
                            )
                    previous_success = (current_parameters, analysis)
                else:
                    previous_success = None

    raise WarmupCalibrationError(
        "No two consecutive adequate pilot horizons produced W estimates "
        "within the horizon-sensitivity tolerance.",
        diagnostics={
            "last_parameters": current_parameters,
            "attempts": tuple(attempts),
        },
    )


def _normalize_maximum_configuration(maximum_replicas, service_count):
    if isinstance(maximum_replicas, int):
        configuration = (maximum_replicas,) * service_count
    else:
        configuration = tuple(maximum_replicas)
    if len(configuration) != service_count:
        raise ValueError("maximum_replicas must define every service.")
    if any(not isinstance(value, int) or value <= 0 for value in configuration):
        raise ValueError("maximum_replicas values must be positive integers.")
    return configuration


def calibrate_global_warmup(
    system,
    *,
    maximum_replicas,
    parameters,
    validation_replication_count=100,
    validation_seed_start=200_000,
    extra_validation_configurations=(),
    max_horizon_doublings=5,
    horizon_sensitivity_tolerance=0.10,
    max_validation_rounds=1,
    max_validation_horizon_extensions=2,
    progress_callback=None,
    analysis_options=None,
    workers=1,
):
    """Execute milestones 4–8 and return a validated global W."""
    if (
        not isinstance(max_validation_horizon_extensions, int)
        or max_validation_horizon_extensions < 0
    ):
        raise ValueError("max_validation_horizon_extensions must be non-negative.")
    if not isinstance(validation_replication_count, int) or validation_replication_count <= 0:
        raise ValueError("validation_replication_count must be a positive integer.")
    if not isinstance(max_validation_rounds, int) or max_validation_rounds <= 0:
        raise ValueError("max_validation_rounds must be a positive integer.")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer.")
    maximum_configuration = _normalize_maximum_configuration(
        maximum_replicas,
        len(parameters.service_names),
    )
    analysis_options = dict(analysis_options or {})
    validation_cutoff_options = {
        key: value
        for key, value in analysis_options.items()
        if key
        in {
            "window_length",
            "reference_tail_fraction",
            "relative_tolerance",
            "absolute_tolerances",
            "required_consecutive_windows",
        }
    }
    minimum_calibrations = []
    representatives = build_representative_configurations(
        parameters.minimum_replica_configuration,
        maximum_configuration,
        extra_configurations=extra_validation_configurations,
    )
    validation_seed_stride = (
        len(representatives)
        * validation_replication_count
        * max_validation_rounds
    )
    validation_seed_starts = []
    validation_failure_reasons = []
    starting_horizon = parameters.pilot_horizon
    last_validation_error = None

    for extension_index in range(max_validation_horizon_extensions + 1):
        minimum_calibration = calibrate_minimum_configuration(
            system,
            parameters,
            starting_horizon=starting_horizon,
            max_horizon_doublings=max_horizon_doublings,
            horizon_sensitivity_tolerance=horizon_sensitivity_tolerance,
            progress_callback=progress_callback,
            analysis_options=analysis_options,
            workers=workers,
        )
        minimum_calibrations.append(minimum_calibration)
        validation_parameters = minimum_calibration.shorter_horizon_parameters
        if (
            minimum_calibration.warmup_duration
            > validation_parameters.pilot_horizon / 3.0
            or (
                validation_parameters.pilot_horizon
                - minimum_calibration.warmup_duration
            )
            / minimum_calibration.bucket_width
            < 50
        ):
            validation_parameters = minimum_calibration.parameters
        selected_cutoff = (
            minimum_calibration.final_analysis.selected_analysis().cutoff
        )
        validation_window_length = selected_cutoff.window_length
        if progress_callback is not None:
            progress_callback(
                "validation_started",
                {
                    "pilot_horizon": minimum_calibration.parameters.pilot_horizon,
                    "validation_horizon": validation_parameters.pilot_horizon,
                    "initial_warmup": minimum_calibration.warmup_duration,
                    "analysis_window_duration": (
                        validation_window_length * minimum_calibration.bucket_width
                    ),
                    "workers": workers,
                },
            )
        try:
            current_validation_seed_start = (
                validation_seed_start + extension_index * validation_seed_stride
            )
            validation_seed_starts.append(current_validation_seed_start)
            validation = validate_global_warmup(
                system,
                validation_parameters,
                initial_warmup=minimum_calibration.warmup_duration,
                maximum_configuration=maximum_configuration,
                bucket_width=minimum_calibration.bucket_width,
                window_length=validation_window_length,
                validation_replication_count=validation_replication_count,
                validation_seed_start=current_validation_seed_start,
                extra_configurations=extra_validation_configurations,
                excluded_seeds=parameters.calibration_seeds,
                max_validation_rounds=max_validation_rounds,
                cutoff_options=validation_cutoff_options,
                workers=workers,
            )
        except WarmupValidationError as error:
            last_validation_error = error
            validation_failure_reasons.append(str(error))
            starting_horizon = minimum_calibration.parameters.pilot_horizon * 2.0
            if extension_index == max_validation_horizon_extensions:
                break
            continue

        return WarmupCalibrationResult(
            initial_parameters=parameters,
            minimum_configuration_calibrations=tuple(minimum_calibrations),
            validation=validation,
            validation_seed_starts=tuple(validation_seed_starts),
            validation_failure_reasons=tuple(validation_failure_reasons),
            worker_processes=workers,
            global_warmup=validation.global_warmup,
        )

    raise WarmupCalibrationError(
        "Global validation failed after extending the pilot horizon: "
        f"{last_validation_error}",
        diagnostics={
            "initial_parameters": parameters,
            "minimum_configuration_calibrations": tuple(minimum_calibrations),
            "validation_seed_starts": tuple(validation_seed_starts),
            "validation_failure_reasons": tuple(validation_failure_reasons),
            "worker_processes": workers,
        },
    ) from last_validation_error

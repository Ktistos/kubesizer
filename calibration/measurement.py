import math
from dataclasses import dataclass

from metrics import (
    ClusterBootstrapP95Interval,
    fixed_experiment_tail_alpha,
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
)
from .profile import CalibrationOptions


class _ArrivalCohortLatencyView:
    """Read-only latency view selected by external-arrival time."""

    __slots__ = ("_arrival_times", "_boundary", "_latencies", "_length")

    def __init__(self, arrival_times, latencies, boundary):
        if len(arrival_times) != len(latencies):
            raise ValueError("Arrival and latency arrays must have equal length.")
        self._arrival_times = arrival_times
        self._latencies = latencies
        self._boundary = boundary
        self._length = sum(arrival < boundary for arrival in arrival_times)

    def __len__(self):
        return self._length

    def __iter__(self):
        return (
            latency
            for arrival, latency in zip(
                self._arrival_times,
                self._latencies,
            )
            if arrival < self._boundary
        )


def _latency_clusters_for_duration(
    replications,
    request_class,
    measurement_duration,
):
    clusters = []
    for replication in replications:
        if measurement_duration > replication.measurement_duration:
            raise ValueError(
                "Cannot select a cohort beyond the simulated duration."
            )
        boundary = replication.warmup_duration + measurement_duration
        clusters.append(
            _ArrivalCohortLatencyView(
                replication.request_class_arrival_times[request_class],
                replication.request_class_latencies[request_class],
                boundary,
            )
        )
    return tuple(clusters)


class MeasurementCalibrationError(RuntimeError):
    """Raised when no defensible global measurement duration can be found."""

    def __init__(self, message, diagnostics=None):
        super().__init__(message)
        self.diagnostics = diagnostics


@dataclass(frozen=True)
class MeasurementClassDiagnostic:
    request_class: str
    duration: float
    doubled_duration: float
    interval: ClusterBootstrapP95Interval | None
    doubled_interval: ClusterBootstrapP95Interval | None
    p95_relative_change: float
    ci_width_relative_change: float
    ci_half_width_absolute_change: float
    sample_requirement_passed: bool
    nonempty_replication_requirement_passed: bool
    precision_requirement_passed: bool
    p95_stability_passed: bool
    ci_width_stability_passed: bool

    @property
    def accepted(self):
        return all(
            (
                self.sample_requirement_passed,
                self.nonempty_replication_requirement_passed,
                self.precision_requirement_passed,
                self.p95_stability_passed,
                self.ci_width_stability_passed,
            )
        )


@dataclass(frozen=True)
class MeasurementCandidateDiagnostic:
    duration: float
    doubled_duration: float
    request_classes: tuple[MeasurementClassDiagnostic, ...]

    @property
    def accepted(self):
        return all(item.accepted for item in self.request_classes)


@dataclass(frozen=True)
class ConfigurationMeasurementCalibration:
    replica_configuration: tuple[int, ...]
    seeds: tuple[int, ...]
    candidates: tuple[MeasurementCandidateDiagnostic, ...]
    accepted_duration: float | None


@dataclass(frozen=True)
class MeasurementCalibrationResult:
    request_class_external_arrival_rates: tuple[tuple[str, float], ...]
    initial_duration: float
    candidate_durations: tuple[float, ...]
    representative_configurations: tuple[tuple[int, ...], ...]
    replications_per_configuration: int
    seeds: tuple[int, ...]
    configurations: tuple[ConfigurationMeasurementCalibration, ...]
    global_measurement_duration: float
    latency_slo_kind: str = "percentile"


def request_class_external_arrival_rates(system, request_classes=None):
    services = tuple(system.get_services().values())
    if request_classes is None:
        request_classes = system.get_request_classes()
    request_classes = tuple(request_classes)
    unknown = set(request_classes) - set(system.get_request_classes())
    if not request_classes or unknown:
        raise ValueError(
            "Required request classes must be a nonempty subset of the workload; "
            f"unknown={sorted(unknown)}."
        )
    rates = tuple(
        (
            request_class,
            sum(float(service.get_gamma(request_class)) for service in services),
        )
        for request_class in system.get_request_classes()
    )
    invalid = tuple(
        name for name, rate in rates if not math.isfinite(rate) or rate <= 0
    )
    if invalid:
        raise MeasurementCalibrationError(
            "Every required request class needs a positive stationary external "
            "arrival rate; missing: " + ", ".join(invalid)
        )
    return rates


def initial_measurement_duration(system, options=None, request_classes=None):
    options = options or CalibrationOptions()
    rates = request_class_external_arrival_rates(system, request_classes)
    return options.measurement_duration_safety_factor * max(
        options.target_samples_per_class / rate for _, rate in rates
    )


def measurement_duration_candidates(system, options=None, request_classes=None):
    options = options or CalibrationOptions()
    initial = initial_measurement_duration(system, options, request_classes)
    return tuple(
        initial * (2**doubling)
        for doubling in range(options.measurement_max_doublings + 1)
    )


def _relative_change(left, right):
    denominator = max(abs(left), abs(right))
    return 0.0 if denominator == 0 else abs(right - left) / denominator


def _interval_for(
    clusters,
    request_class,
    slo,
    *,
    options,
    duration,
    configuration,
    tail_alpha,
    latency_slo_kind,
):
    bootstrap_seed = options.seed_policy.deterministic_seed(
        "bootstrap",
        "measurement-calibration",
        configuration,
        request_class,
        float(duration),
    )
    bootstrap = (
        replication_cluster_bootstrap_mean
        if latency_slo_kind == "mean"
        else replication_cluster_bootstrap_p95
    )
    return bootstrap(
        clusters,
        request_class=request_class,
        slo=slo,
        resample_count=options.bootstrap_resample_count,
        seed=bootstrap_seed,
        lower_tail_probability=tail_alpha,
        upper_tail_probability=1.0 - tail_alpha,
        minimum_tail_draws=options.minimum_bootstrap_tail_draws,
    )


def _analyze_measurement_durations(
    replications,
    durations,
    request_classes,
    request_class_slos,
    *,
    options,
    configuration,
    tail_alpha,
    latency_slo_kind="percentile",
):
    intervals = {}
    cohort_counts = {}
    for duration in durations:
        for request_class in request_classes:
            clusters = _latency_clusters_for_duration(
                replications,
                request_class,
                duration,
            )
            cohort_counts[(duration, request_class)] = (
                sum(len(cluster) for cluster in clusters),
                sum(bool(len(cluster)) for cluster in clusters),
            )
            try:
                interval = _interval_for(
                    clusters,
                    request_class,
                    request_class_slos[request_class],
                    options=options,
                    duration=duration,
                    configuration=configuration,
                    tail_alpha=tail_alpha,
                    latency_slo_kind=latency_slo_kind,
                )
            except ValueError as error:
                if "at least one latency" not in str(error):
                    raise MeasurementCalibrationError(
                        "Measurement bootstrap failed for "
                        f"{request_class!r} at M={duration}: {error}"
                    ) from error
                interval = None
            intervals[(duration, request_class)] = interval
    return intervals, cohort_counts


def _measurement_candidate_diagnostic(
    duration,
    doubled_duration,
    request_classes,
    intervals,
    cohort_counts,
    options,
):
    class_results = []
    for request_class in request_classes:
        interval = intervals[(duration, request_class)]
        doubled = intervals[(doubled_duration, request_class)]
        sample_count, nonempty_count = cohort_counts[
            (duration, request_class)
        ]
        intervals_available = interval is not None and doubled is not None
        p95_change = (
            _relative_change(interval.estimate, doubled.estimate)
            if intervals_available
            else math.inf
        )
        width_change = (
            _relative_change(
                2.0 * interval.half_width,
                2.0 * doubled.half_width,
            )
            if intervals_available
            else math.inf
        )
        precision_limit = (
            max(
                options.absolute_ci_half_width_target,
                options.relative_ci_half_width_target
                * interval.estimate,
            )
            if interval is not None
            else -math.inf
        )
        width_stability_limit = (
            max(
                options.absolute_ci_half_width_target,
                options.ci_width_stability_tolerance
                * interval.estimate,
            )
            if interval is not None
            else -math.inf
        )
        half_width_change = (
            abs(doubled.half_width - interval.half_width)
            if intervals_available
            else math.inf
        )
        class_results.append(
            MeasurementClassDiagnostic(
                request_class=request_class,
                duration=duration,
                doubled_duration=doubled_duration,
                interval=interval,
                doubled_interval=doubled,
                p95_relative_change=p95_change,
                ci_width_relative_change=width_change,
                ci_half_width_absolute_change=half_width_change,
                sample_requirement_passed=(
                    sample_count
                    >= options.minimum_eligible_samples_per_class
                ),
                nonempty_replication_requirement_passed=(
                    nonempty_count
                    >= options.minimum_nonempty_replications_per_class
                ),
                precision_requirement_passed=(
                    interval is not None
                    and interval.half_width <= precision_limit
                ),
                p95_stability_passed=(
                    p95_change
                    <= options.measurement_stability_tolerance
                ),
                ci_width_stability_passed=(
                    half_width_change <= width_stability_limit
                ),
            )
        )
    return MeasurementCandidateDiagnostic(
        duration=duration,
        doubled_duration=doubled_duration,
        request_classes=tuple(class_results),
    )


def calibrate_measurement_duration(
    system,
    *,
    warmup_duration,
    representative_configurations,
    request_class_slos,
    options=None,
    workers=1,
    progress_callback=None,
    latency_slo_kind="percentile",
):
    """Select one global M using prefix-reused, drained arrival cohorts."""
    options = options or CalibrationOptions()
    request_classes = tuple(request_class_slos)
    unknown_classes = set(request_classes) - set(system.get_request_classes())
    if not request_classes or unknown_classes:
        raise ValueError(
            "Percentile SLO definitions must be a nonempty subset of request "
            f"classes; unknown={sorted(unknown_classes)}."
        )
    if latency_slo_kind not in {"mean", "percentile"}:
        raise ValueError("latency_slo_kind must be mean or percentile.")
    if not math.isfinite(warmup_duration) or warmup_duration < 0:
        raise ValueError("warmup_duration must be finite and non-negative.")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer.")

    rates = request_class_external_arrival_rates(system, request_classes)
    durations = measurement_duration_candidates(
        system,
        options,
        request_classes,
    )
    configurations = tuple(
        dict.fromkeys(tuple(item) for item in representative_configurations)
    )
    if not configurations:
        raise ValueError("At least one representative configuration is required.")
    service_count = len(system.get_services())
    minimum = tuple(system.get_minimum_stable_replicas())
    for configuration in configurations:
        if (
            len(configuration) != service_count
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
                for value in configuration
            )
        ):
            raise ValueError("Representative configurations are invalid.")
        if any(value < required for value, required in zip(configuration, minimum)):
            raise MeasurementCalibrationError(
                f"Representative configuration {configuration} is analytically unstable."
            )

    seeds = options.seed_policy.seeds(
        "measurement_calibration",
        options.measurement_calibration_replications,
    )
    tail_alpha = fixed_experiment_tail_alpha(
        options.overall_alpha,
        len(request_classes),
    )
    original_configuration = tuple(system.get_system_replicas())
    configuration_results = []
    try:
        for configuration in configurations:
            system.configure_system_replicas(configuration)
            if not system.solve().get_stability():
                raise MeasurementCalibrationError(
                    f"Representative configuration {configuration} is analytically unstable."
                )
            candidate_pairs = tuple(zip(durations, durations[1:]))
            studies = ((candidate_pairs[:1], durations[1]),)
            if len(candidate_pairs) > 1:
                studies += ((candidate_pairs[1:], durations[-1]),)

            candidate_results = []
            accepted_duration = None
            for pairs, simulated_duration in studies:
                if progress_callback is not None:
                    progress_callback(
                        "measurement_trajectories_started",
                        {
                            "configuration": configuration,
                            "simulation_duration": simulated_duration,
                            "candidate_pairs": pairs,
                            "replications": len(seeds),
                            "workers": workers,
                        },
                    )
                replications = system.simulate_measurement_replications(
                    warmup_duration=warmup_duration,
                    measurement_duration=simulated_duration,
                    seeds=seeds,
                    workers=workers,
                    maximum_drain_time=options.maximum_drain_time,
                )
                if progress_callback is not None:
                    progress_callback(
                        "measurement_trajectories_completed",
                        {
                            "configuration": configuration,
                            "simulation_duration": simulated_duration,
                            "replications": len(replications),
                            "workers": workers,
                        },
                    )
                analysis_durations = tuple(
                    dict.fromkeys(
                        value
                        for pair in pairs
                        for value in pair
                    )
                )
                intervals, cohort_counts = _analyze_measurement_durations(
                    replications,
                    analysis_durations,
                    request_classes,
                    request_class_slos,
                    options=options,
                    configuration=configuration,
                    tail_alpha=tail_alpha,
                    latency_slo_kind=latency_slo_kind,
                )
                for duration, doubled_duration in pairs:
                    candidate = _measurement_candidate_diagnostic(
                        duration,
                        doubled_duration,
                        request_classes,
                        intervals,
                        cohort_counts,
                        options,
                    )
                    candidate_results.append(candidate)
                    if progress_callback is not None:
                        progress_callback(
                            "measurement_candidate_analyzed",
                            {
                                "configuration": configuration,
                                "duration": duration,
                                "doubled_duration": doubled_duration,
                                "accepted": candidate.accepted,
                            },
                        )
                    if candidate.accepted:
                        accepted_duration = duration
                        break
                if accepted_duration is not None:
                    break

            configuration_results.append(
                ConfigurationMeasurementCalibration(
                    replica_configuration=configuration,
                    seeds=seeds,
                    candidates=tuple(candidate_results),
                    accepted_duration=accepted_duration,
                )
            )
    finally:
        system.configure_system_replicas(original_configuration)

    failed = tuple(
        item.replica_configuration
        for item in configuration_results
        if item.accepted_duration is None
    )
    if failed:
        diagnostics = MeasurementCalibrationResult(
            request_class_external_arrival_rates=rates,
            initial_duration=durations[0],
            candidate_durations=durations,
            representative_configurations=configurations,
            replications_per_configuration=len(seeds),
            seeds=seeds,
            configurations=tuple(configuration_results),
            global_measurement_duration=math.nan,
            latency_slo_kind=latency_slo_kind,
        )
        raise MeasurementCalibrationError(
            "No acceptable measurement duration was found for configurations: "
            + ", ".join(str(value) for value in failed),
            diagnostics=diagnostics,
        )
    selected = max(item.accepted_duration for item in configuration_results)
    return MeasurementCalibrationResult(
        request_class_external_arrival_rates=rates,
        initial_duration=durations[0],
        candidate_durations=durations,
        representative_configurations=configurations,
        replications_per_configuration=len(seeds),
        seeds=seeds,
        configurations=tuple(configuration_results),
        global_measurement_duration=selected,
        latency_slo_kind=latency_slo_kind,
    )

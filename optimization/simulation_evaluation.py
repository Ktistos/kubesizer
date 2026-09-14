from dataclasses import dataclass
from enum import Enum

from calibration import CalibrationProfile, validate_profile_for_experiment
from metrics import (
    ClusterBootstrapP95Interval,
    IndependentReplicationRateInterval,
    independent_replication_rate_interval,
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
)


class SimulationClassification(str, Enum):
    ANALYTICALLY_UNSTABLE = "ANALYTICALLY_UNSTABLE"
    STRUCTURALLY_INFEASIBLE = "STRUCTURALLY_INFEASIBLE"
    STATISTICALLY_FEASIBLE = "STATISTICALLY_FEASIBLE"
    STATISTICALLY_INFEASIBLE = "STATISTICALLY_INFEASIBLE"
    CONTINUE_SAMPLING = "CONTINUE_SAMPLING"
    INCONCLUSIVE = "INCONCLUSIVE"
    SIMULATION_FAILURE = "SIMULATION_FAILURE"
    CALIBRATION_FAILURE = "CALIBRATION_FAILURE"


@dataclass(frozen=True)
class SequentialLookResult:
    look_index: int
    replication_count: int
    seeds_used: tuple[int, ...]
    request_class_intervals: tuple[tuple[str, ClusterBootstrapP95Interval], ...]
    failure_rate_intervals: tuple[tuple[str, IndependentReplicationRateInterval], ...]
    eligible_request_counts: tuple[tuple[str, int], ...]
    classification: SimulationClassification

    @property
    def intervals(self):
        return dict(self.request_class_intervals)

    @property
    def failure_intervals(self):
        return dict(self.failure_rate_intervals)


@dataclass(frozen=True)
class SimulationUncertaintyDiagnostics:
    """Observed uncertainty sources kept separate from statistical Monte Carlo."""

    simulation_replication_count: int
    input_uncertainty_method: str | None
    sampled_input_block_starts: tuple[int, ...]
    request_class_arrival_multiplier_ranges: tuple[
        tuple[str, float, float], ...
    ]
    service_time_multiplier_ranges: tuple[tuple[int, float, float], ...]
    request_class_statistic_variance_components: tuple[
        tuple[str, float, float, float], ...
    ] = ()
    model_form_assumptions: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class ConfigurationEvaluation:
    configuration: tuple[int, ...]
    classification: SimulationClassification
    replications_used: int
    look_count: int
    seeds_used: tuple[int, ...]
    eligible_request_counts: tuple[tuple[str, int], ...]
    request_class_intervals: tuple[tuple[str, ClusterBootstrapP95Interval], ...]
    failure_rate_intervals: tuple[tuple[str, IndependentReplicationRateInterval], ...]
    warmup_duration: float
    measurement_duration: float
    calibration_profile_fingerprint: str
    sequential_tail_alpha: float
    looks: tuple[SequentialLookResult, ...]
    simulation_diagnostics: tuple[tuple[str, float | int], ...]
    failure_reason: str | None = None
    uncertainty_diagnostics: SimulationUncertaintyDiagnostics | None = None

    @property
    def feasible(self):
        return self.classification == SimulationClassification.STATISTICALLY_FEASIBLE

    @property
    def intervals(self):
        return dict(self.request_class_intervals)

    @property
    def counts(self):
        return dict(self.eligible_request_counts)

    @property
    def failure_intervals(self):
        return dict(self.failure_rate_intervals)


class CalibratedConfigurationEvaluator:
    """Evaluate configurations with frozen W/M and sequential cluster bootstrap."""

    def __init__(
        self,
        system,
        calibration_profile,
        *,
        workers=1,
        progress_callback=None,
    ):
        if not isinstance(calibration_profile, CalibrationProfile):
            raise TypeError("calibration_profile must be a CalibrationProfile.")
        if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
            raise ValueError("workers must be a positive integer.")
        if progress_callback is not None and not callable(progress_callback):
            raise TypeError("progress_callback must be callable or None.")
        self.system = system
        self.profile = calibration_profile
        self.workers = workers
        self.progress_callback = progress_callback

    def _progress(self, event, details):
        if self.progress_callback is not None:
            self.progress_callback(event, details)

    def _failure(self, configuration, classification, reason, *, analytical=False):
        return ConfigurationEvaluation(
            configuration=configuration,
            classification=classification,
            replications_used=0,
            look_count=0,
            seeds_used=(),
            eligible_request_counts=(),
            request_class_intervals=(),
            failure_rate_intervals=(),
            warmup_duration=self.profile.warmup_duration,
            measurement_duration=self.profile.measurement_duration,
            calibration_profile_fingerprint=self.profile.profile_fingerprint,
            sequential_tail_alpha=self._tail_alpha(),
            looks=(),
            simulation_diagnostics=(("analytical_precheck", int(analytical)),),
            failure_reason=reason,
        )

    def _tail_alpha(self):
        constraint_count = len(self.profile.request_class_slos) + len(
            self.profile.request_class_failure_rate_slos
        )
        return self.profile.overall_alpha / (
            2 * self.profile.maximum_sequential_looks * constraint_count
        )

    def _uncertainty_diagnostics(self, replications):
        starts = tuple(
            int(replication.sampled_input_block_start)
            for replication in replications
            if getattr(replication, "sampled_input_block_start", None) is not None
        )
        request_classes = sorted(
            {
                name
                for replication in replications
                for name in getattr(
                    replication,
                    "input_arrival_rate_multipliers",
                    {},
                )
            }
        )
        arrival_ranges = []
        for request_class in request_classes:
            values = tuple(
                replication.input_arrival_rate_multipliers[request_class]
                for replication in replications
            )
            arrival_ranges.append(
                (request_class, min(values), max(values))
            )
        service_count = max(
            (
                len(
                    getattr(
                        replication,
                        "input_service_time_multipliers",
                        (),
                    )
                )
                for replication in replications
            ),
            default=0,
        )
        service_ranges = []
        for service in range(service_count):
            values = tuple(
                replication.input_service_time_multipliers[service]
                for replication in replications
            )
            service_ranges.append((service, min(values), max(values)))
        variance_components = []
        for request_class in self.system.get_request_classes():
            statistics = []
            grouped = {}
            for replication in replications:
                values = tuple(
                    replication.request_class_latencies.get(request_class, ())
                )
                if not values:
                    continue
                if self.profile.latency_slo_kind == "mean":
                    statistic = sum(values) / len(values)
                else:
                    ordered = sorted(values)
                    position = (len(ordered) - 1) * 0.95
                    lower = int(position)
                    upper = min(lower + 1, len(ordered) - 1)
                    weight = position - lower
                    statistic = (
                        ordered[lower] * (1.0 - weight)
                        + ordered[upper] * weight
                    )
                statistics.append(statistic)
                block = getattr(replication, "sampled_input_block_start", None)
                grouped.setdefault(block, []).append(statistic)
            if len(statistics) < 2:
                continue
            grand_mean = sum(statistics) / len(statistics)
            total_sum_squares = sum(
                (value - grand_mean) ** 2 for value in statistics
            )
            within_sum_squares = 0.0
            for values in grouped.values():
                group_mean = sum(values) / len(values)
                within_sum_squares += sum(
                    (value - group_mean) ** 2 for value in values
                )
            divisor = len(statistics) - 1
            total_variance = total_sum_squares / divisor
            within_contribution = within_sum_squares / divisor
            between_input_contribution = max(
                0.0,
                (total_sum_squares - within_sum_squares) / divisor,
            )
            variance_components.append(
                (
                    request_class,
                    total_variance,
                    between_input_contribution,
                    within_contribution,
                )
            )
        return SimulationUncertaintyDiagnostics(
            simulation_replication_count=len(replications),
            input_uncertainty_method=(
                "aligned-moving-block-bootstrap"
                if starts
                else None
            ),
            sampled_input_block_starts=starts,
            request_class_arrival_multiplier_ranges=tuple(arrival_ranges),
            service_time_multiplier_ranges=tuple(service_ranges),
            request_class_statistic_variance_components=tuple(
                variance_components
            ),
            model_form_assumptions=tuple(
                sorted(self.system.get_service_execution_models().items())
            ),
        )

    def _normalize_configuration(self, configuration):
        configuration = tuple(configuration)
        if len(configuration) != len(self.profile.maximum_replica_configuration) or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in configuration
        ):
            raise ValueError(
                "Configuration must contain positive replicas for every service."
            )
        return configuration

    def _look_schedule(self):
        profile = self.profile
        values = [profile.minimum_optimization_replications]
        while values[-1] < profile.maximum_optimization_replications:
            values.append(
                min(
                    profile.maximum_optimization_replications,
                    values[-1] + profile.replication_batch_size,
                )
            )
        if len(values) != profile.maximum_sequential_looks:
            raise ValueError(
                "Calibration profile contains an inconsistent look schedule."
            )
        return tuple(values)

    def _calculate_intervals(
        self,
        configuration,
        replications,
        look_index,
    ):
        intervals = []
        counts = []
        tail_alpha = self._tail_alpha()
        for request_class, slo in self.profile.request_class_slos:
            try:
                clusters = tuple(
                    tuple(replication.request_class_latencies[request_class])
                    for replication in replications
                )
            except (AttributeError, KeyError, TypeError) as error:
                raise ValueError(
                    f"Replication output has no raw cohort for {request_class!r}."
                ) from error
            total = sum(len(cluster) for cluster in clusters)
            nonempty = sum(bool(cluster) for cluster in clusters)
            counts.append((request_class, total))
            if total < self.profile.minimum_eligible_samples_per_request_class:
                continue
            if nonempty < self.profile.minimum_nonempty_replications_per_request_class:
                continue
            bootstrap = (
                replication_cluster_bootstrap_mean
                if self.profile.latency_slo_kind == "mean"
                else replication_cluster_bootstrap_p95
            )
            interval = bootstrap(
                clusters,
                request_class=request_class,
                slo=slo,
                resample_count=self.profile.bootstrap_resample_count,
                seed=self.profile.optimization_seed_policy.deterministic_seed(
                    "bootstrap",
                    "optimization-evaluation",
                    self.profile.profile_fingerprint,
                    configuration,
                    request_class,
                    look_index,
                    len(replications),
                ),
                lower_tail_probability=tail_alpha,
                upper_tail_probability=1.0 - tail_alpha,
                minimum_tail_draws=self.profile.minimum_bootstrap_tail_draws,
            )
            intervals.append((request_class, interval))
        failure_intervals = []
        for request_class, slo in self.profile.request_class_failure_rate_slos:
            try:
                clusters = tuple(
                    tuple(replication.request_class_failures[request_class])
                    for replication in replications
                )
            except (AttributeError, KeyError, TypeError) as error:
                raise ValueError(
                    f"Replication output has no failure cohort for "
                    f"{request_class!r}."
                ) from error
            latency_sizes = tuple(
                len(replication.request_class_latencies[request_class])
                for replication in replications
            )
            if tuple(len(cluster) for cluster in clusters) != latency_sizes:
                raise ValueError(
                    f"Failure and latency cohorts disagree for {request_class!r}."
                )
            if sum(bool(cluster) for cluster in clusters) < (
                self.profile.minimum_nonempty_replications_per_request_class
            ):
                continue
            failure_intervals.append(
                (
                    request_class,
                    independent_replication_rate_interval(
                        clusters,
                        request_class=request_class,
                        slo=slo,
                        per_tail_alpha=tail_alpha,
                    ),
                )
            )
        return tuple(intervals), tuple(failure_intervals), tuple(counts)

    @staticmethod
    def _classification(
        intervals,
        request_class_count,
        failure_intervals,
        failure_constraint_count,
        at_maximum,
    ):
        if (
            len(intervals) != request_class_count
            or len(failure_intervals) != failure_constraint_count
        ):
            return (
                SimulationClassification.SIMULATION_FAILURE
                if at_maximum
                else SimulationClassification.CONTINUE_SAMPLING
            )
        values = tuple(interval for _, interval in intervals)
        failure_values = tuple(interval for _, interval in failure_intervals)
        if all(interval.upper_bound <= interval.slo for interval in values) and all(
            interval.upper_bound <= interval.slo for interval in failure_values
        ):
            return SimulationClassification.STATISTICALLY_FEASIBLE
        if any(interval.lower_bound > interval.slo for interval in values) or any(
            interval.lower_bound > interval.slo for interval in failure_values
        ):
            return SimulationClassification.STATISTICALLY_INFEASIBLE
        return (
            SimulationClassification.INCONCLUSIVE
            if at_maximum
            else SimulationClassification.CONTINUE_SAMPLING
        )

    def evaluate(self, configuration):
        configuration = self._normalize_configuration(configuration)
        self._progress(
            "configuration_evaluation_started",
            {
                "configuration": configuration,
                "maximum_replications": (
                    self.profile.maximum_optimization_replications
                ),
            },
        )
        try:
            validate_profile_for_experiment(
                self.profile,
                self.system,
                request_class_slos=self.profile.slos,
                maximum_configuration=self.profile.maximum_replica_configuration,
            )
        except Exception as error:
            return self._failure(
                configuration,
                SimulationClassification.CALIBRATION_FAILURE,
                str(error),
            )
        if any(
            value > maximum
            for value, maximum in zip(
                configuration,
                self.profile.maximum_replica_configuration,
            )
        ):
            return self._failure(
                configuration,
                SimulationClassification.CALIBRATION_FAILURE,
                "Configuration lies outside the calibrated replica bounds.",
            )

        original_configuration = tuple(self.system.get_system_replicas())
        try:
            self.system.configure_system_replicas(configuration)
            analytical_metrics = self.system.solve()
            if not analytical_metrics.get_stability():
                return self._failure(
                    configuration,
                    SimulationClassification.ANALYTICALLY_UNSTABLE,
                    "At least one analytical utilization is greater than or equal to one.",
                    analytical=True,
                )

            all_seeds = self.profile.optimization_seeds()
            replications = []
            looks = []
            previous_count = 0
            final_intervals = ()
            final_failure_intervals = ()
            final_counts = ()
            final_classification = SimulationClassification.INCONCLUSIVE
            processed_events = 0
            external_arrivals = 0
            completed_requests = 0
            failed_measured_requests = 0
            maximum_stop_time = 0.0
            for look_index, replication_count in enumerate(
                self._look_schedule(), start=1
            ):
                batch_seeds = all_seeds[previous_count:replication_count]
                self._progress(
                    "replication_batch_started",
                    {
                        "configuration": configuration,
                        "look_index": look_index,
                        "batch_replications": len(batch_seeds),
                        "replication_count": replication_count,
                        "maximum_replications": (
                            self.profile.maximum_optimization_replications
                        ),
                    },
                )
                try:
                    batch = tuple(
                        self.system.simulate_measurement_replications(
                            warmup_duration=self.profile.warmup_duration,
                            measurement_duration=self.profile.measurement_duration,
                            seeds=batch_seeds,
                            workers=self.workers,
                            maximum_drain_time=self.profile.maximum_drain_time,
                        )
                    )
                except Exception as error:
                    completed_replications = len(replications)
                    return ConfigurationEvaluation(
                        configuration=configuration,
                        classification=SimulationClassification.SIMULATION_FAILURE,
                        replications_used=completed_replications,
                        look_count=len(looks),
                        seeds_used=all_seeds[:completed_replications],
                        eligible_request_counts=final_counts,
                        request_class_intervals=final_intervals,
                        failure_rate_intervals=final_failure_intervals,
                        warmup_duration=self.profile.warmup_duration,
                        measurement_duration=self.profile.measurement_duration,
                        calibration_profile_fingerprint=self.profile.profile_fingerprint,
                        sequential_tail_alpha=self._tail_alpha(),
                        looks=tuple(looks),
                        simulation_diagnostics=(),
                        failure_reason=str(error),
                    )
                if tuple(item.seed for item in batch) != batch_seeds:
                    return self._failure(
                        configuration,
                        SimulationClassification.SIMULATION_FAILURE,
                        "Simulator returned a different seed sequence than requested.",
                    )
                replications.extend(batch)
                self._progress(
                    "replication_batch_completed",
                    {
                        "configuration": configuration,
                        "look_index": look_index,
                        "batch_replications": len(batch_seeds),
                        "replication_count": replication_count,
                        "maximum_replications": (
                            self.profile.maximum_optimization_replications
                        ),
                    },
                )
                processed_events += sum(
                    int(getattr(item, "processed_event_count", 0)) for item in batch
                )
                external_arrivals += sum(
                    int(getattr(item, "external_arrival_count", 0)) for item in batch
                )
                completed_requests += sum(
                    int(getattr(item, "completed_request_count", 0)) for item in batch
                )
                failed_measured_requests += sum(
                    sum(
                        int(value)
                        for value in getattr(
                            item,
                            "failed_measured_request_counts",
                            {},
                        ).values()
                    )
                    for item in batch
                )
                maximum_stop_time = max(
                    maximum_stop_time,
                    *(float(getattr(item, "stop_time", 0.0)) for item in batch),
                )
                try:
                    (
                        final_intervals,
                        final_failure_intervals,
                        final_counts,
                    ) = self._calculate_intervals(
                        configuration,
                        replications,
                        look_index,
                    )
                except ValueError as error:
                    return ConfigurationEvaluation(
                        configuration=configuration,
                        classification=SimulationClassification.SIMULATION_FAILURE,
                        replications_used=replication_count,
                        look_count=len(looks),
                        seeds_used=all_seeds[:replication_count],
                        eligible_request_counts=final_counts,
                        request_class_intervals=final_intervals,
                        failure_rate_intervals=final_failure_intervals,
                        warmup_duration=self.profile.warmup_duration,
                        measurement_duration=self.profile.measurement_duration,
                        calibration_profile_fingerprint=self.profile.profile_fingerprint,
                        sequential_tail_alpha=self._tail_alpha(),
                        looks=tuple(looks),
                        simulation_diagnostics=(),
                        failure_reason=str(error),
                    )
                final_classification = self._classification(
                    final_intervals,
                    len(self.profile.request_class_slos),
                    final_failure_intervals,
                    len(self.profile.request_class_failure_rate_slos),
                    replication_count == self.profile.maximum_optimization_replications,
                )
                looks.append(
                    SequentialLookResult(
                        look_index=look_index,
                        replication_count=replication_count,
                        seeds_used=all_seeds[:replication_count],
                        request_class_intervals=final_intervals,
                        failure_rate_intervals=final_failure_intervals,
                        eligible_request_counts=final_counts,
                        classification=final_classification,
                    )
                )
                previous_count = replication_count
                if final_classification != SimulationClassification.CONTINUE_SAMPLING:
                    break

            failure_reason = None
            if final_classification == SimulationClassification.SIMULATION_FAILURE:
                failure_reason = (
                    "Minimum eligible-sample or nonempty-replication requirements "
                    "were not met at R_max."
                )
            result = ConfigurationEvaluation(
                configuration=configuration,
                classification=final_classification,
                replications_used=previous_count,
                look_count=len(looks),
                seeds_used=all_seeds[:previous_count],
                eligible_request_counts=final_counts,
                request_class_intervals=final_intervals,
                failure_rate_intervals=final_failure_intervals,
                warmup_duration=self.profile.warmup_duration,
                measurement_duration=self.profile.measurement_duration,
                calibration_profile_fingerprint=self.profile.profile_fingerprint,
                sequential_tail_alpha=self._tail_alpha(),
                looks=tuple(looks),
                simulation_diagnostics=(
                    ("processed_event_count", processed_events),
                    ("external_arrival_count", external_arrivals),
                    ("completed_request_count", completed_requests),
                    ("failed_measured_request_count", failed_measured_requests),
                    ("maximum_stop_time", maximum_stop_time),
                    ("replication_count", previous_count),
                ),
                failure_reason=failure_reason,
                uncertainty_diagnostics=self._uncertainty_diagnostics(
                    replications
                ),
            )
            self._progress(
                "configuration_evaluation_completed",
                {
                    "configuration": configuration,
                    "replication_count": result.replications_used,
                    "look_count": result.look_count,
                    "classification": result.classification.value,
                },
            )
            return result
        finally:
            self.system.configure_system_replicas(original_configuration)

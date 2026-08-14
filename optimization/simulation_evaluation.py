import math
from dataclasses import dataclass
from enum import Enum

from calibration import CalibrationProfile, validate_profile_for_experiment
from metrics import ClusterBootstrapP95Interval, replication_cluster_bootstrap_p95


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
    eligible_request_counts: tuple[tuple[str, int], ...]
    classification: SimulationClassification

    @property
    def intervals(self):
        return dict(self.request_class_intervals)


@dataclass(frozen=True)
class ConfigurationEvaluation:
    configuration: tuple[int, ...]
    classification: SimulationClassification
    replications_used: int
    look_count: int
    seeds_used: tuple[int, ...]
    eligible_request_counts: tuple[tuple[str, int], ...]
    request_class_intervals: tuple[tuple[str, ClusterBootstrapP95Interval], ...]
    warmup_duration: float
    measurement_duration: float
    calibration_profile_fingerprint: str
    sequential_tail_alpha: float
    looks: tuple[SequentialLookResult, ...]
    simulation_diagnostics: tuple[tuple[str, float | int], ...]
    failure_reason: str | None = None

    @property
    def feasible(self):
        return self.classification == SimulationClassification.STATISTICALLY_FEASIBLE

    @property
    def intervals(self):
        return dict(self.request_class_intervals)

    @property
    def counts(self):
        return dict(self.eligible_request_counts)


class CalibratedConfigurationEvaluator:
    """Evaluate configurations with frozen W/M and sequential cluster bootstrap."""

    def __init__(self, system, calibration_profile, *, workers=1):
        if not isinstance(calibration_profile, CalibrationProfile):
            raise TypeError("calibration_profile must be a CalibrationProfile.")
        if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
            raise ValueError("workers must be a positive integer.")
        self.system = system
        self.profile = calibration_profile
        self.workers = workers

    def _failure(self, configuration, classification, reason, *, analytical=False):
        return ConfigurationEvaluation(
            configuration=configuration,
            classification=classification,
            replications_used=0,
            look_count=0,
            seeds_used=(),
            eligible_request_counts=(),
            request_class_intervals=(),
            warmup_duration=self.profile.warmup_duration,
            measurement_duration=self.profile.measurement_duration,
            calibration_profile_fingerprint=self.profile.profile_fingerprint,
            sequential_tail_alpha=self._tail_alpha(),
            looks=(),
            simulation_diagnostics=(("analytical_precheck", int(analytical)),),
            failure_reason=reason,
        )

    def _tail_alpha(self):
        return self.profile.overall_alpha / (
            2
            * self.profile.maximum_sequential_looks
            * len(self.profile.request_class_slos)
            * len(self.profile.required_scenarios)
        )

    def _scenario_systems(self):
        if hasattr(self.system, "get_scenario_systems"):
            return self.system.get_scenario_systems()
        return {self.profile.required_scenarios[0]: self.system}

    def _interval_label(self, scenario, request_class):
        if len(self.profile.required_scenarios) == 1:
            return request_class
        return f"{scenario}/{request_class}"

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
        replications_by_scenario,
        look_index,
    ):
        intervals = []
        counts = []
        tail_alpha = self._tail_alpha()
        for scenario, replications in replications_by_scenario.items():
            for request_class, slo in self.profile.request_class_slos:
                label = self._interval_label(scenario, request_class)
                try:
                    clusters = tuple(
                        tuple(replication.request_class_latencies[request_class])
                        for replication in replications
                    )
                except (AttributeError, KeyError, TypeError) as error:
                    raise ValueError(
                        f"Replication output has no raw cohort for {label!r}."
                    ) from error
                total = sum(len(cluster) for cluster in clusters)
                nonempty = sum(bool(cluster) for cluster in clusters)
                counts.append((label, total))
                if total < self.profile.minimum_eligible_samples_per_request_class:
                    continue
                if nonempty < self.profile.minimum_nonempty_replications_per_request_class:
                    continue
                interval = replication_cluster_bootstrap_p95(
                    clusters,
                    request_class=request_class,
                    slo=slo,
                    resample_count=self.profile.bootstrap_resample_count,
                    seed=self.profile.optimization_seed_policy.deterministic_seed(
                        "bootstrap",
                        "optimization-evaluation",
                        self.profile.profile_fingerprint,
                        configuration,
                        scenario,
                        request_class,
                        look_index,
                        len(replications),
                    ),
                    lower_tail_probability=tail_alpha,
                    upper_tail_probability=1.0 - tail_alpha,
                    minimum_tail_draws=self.profile.minimum_bootstrap_tail_draws,
                )
                intervals.append((label, interval))
        return tuple(intervals), tuple(counts)

    @staticmethod
    def _classification(intervals, request_class_count, at_maximum):
        if len(intervals) != request_class_count:
            return (
                SimulationClassification.SIMULATION_FAILURE
                if at_maximum
                else SimulationClassification.CONTINUE_SAMPLING
            )
        values = tuple(interval for _, interval in intervals)
        if all(interval.upper_bound <= interval.slo for interval in values):
            return SimulationClassification.STATISTICALLY_FEASIBLE
        if any(interval.lower_bound > interval.slo for interval in values):
            return SimulationClassification.STATISTICALLY_INFEASIBLE
        return (
            SimulationClassification.INCONCLUSIVE
            if at_maximum
            else SimulationClassification.CONTINUE_SAMPLING
        )

    def evaluate(self, configuration):
        configuration = self._normalize_configuration(configuration)
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
            scenario_systems = self._scenario_systems()
            replications = {name: [] for name in scenario_systems}
            looks = []
            previous_count = 0
            final_intervals = ()
            final_counts = ()
            final_classification = SimulationClassification.INCONCLUSIVE
            processed_events = 0
            external_arrivals = 0
            completed_requests = 0
            maximum_stop_time = 0.0
            for look_index, replication_count in enumerate(
                self._look_schedule(), start=1
            ):
                batch_seeds = all_seeds[previous_count:replication_count]
                try:
                    batches = {
                        scenario: scenario_system.simulate_measurement_replications(
                            warmup_duration=self.profile.warmup_duration,
                            measurement_duration=self.profile.measurement_duration,
                            seeds=batch_seeds,
                            workers=self.workers,
                            maximum_drain_time=self.profile.maximum_drain_time,
                        )
                        for scenario, scenario_system in scenario_systems.items()
                    }
                except Exception as error:
                    completed_replications = min(
                        (len(items) for items in replications.values()),
                        default=0,
                    )
                    return ConfigurationEvaluation(
                        configuration=configuration,
                        classification=SimulationClassification.SIMULATION_FAILURE,
                        replications_used=completed_replications,
                        look_count=len(looks),
                        seeds_used=all_seeds[:completed_replications],
                        eligible_request_counts=final_counts,
                        request_class_intervals=final_intervals,
                        warmup_duration=self.profile.warmup_duration,
                        measurement_duration=self.profile.measurement_duration,
                        calibration_profile_fingerprint=self.profile.profile_fingerprint,
                        sequential_tail_alpha=self._tail_alpha(),
                        looks=tuple(looks),
                        simulation_diagnostics=(),
                        failure_reason=str(error),
                    )
                if any(
                    tuple(item.seed for item in batch) != batch_seeds
                    for batch in batches.values()
                ):
                    return self._failure(
                        configuration,
                        SimulationClassification.SIMULATION_FAILURE,
                        "Simulator returned a different seed sequence than requested.",
                    )
                for scenario, batch in batches.items():
                    replications[scenario].extend(batch)
                processed_events += sum(
                    int(getattr(item, "processed_event_count", 0))
                    for batch in batches.values()
                    for item in batch
                )
                external_arrivals += sum(
                    int(getattr(item, "external_arrival_count", 0))
                    for batch in batches.values()
                    for item in batch
                )
                completed_requests += sum(
                    int(getattr(item, "completed_request_count", 0))
                    for batch in batches.values()
                    for item in batch
                )
                maximum_stop_time = max(
                    maximum_stop_time,
                    *(
                        float(getattr(item, "stop_time", 0.0))
                        for batch in batches.values()
                        for item in batch
                    ),
                )
                try:
                    final_intervals, final_counts = self._calculate_intervals(
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
                    len(self.profile.request_class_slos)
                    * len(self.profile.required_scenarios),
                    replication_count == self.profile.maximum_optimization_replications,
                )
                looks.append(
                    SequentialLookResult(
                        look_index=look_index,
                        replication_count=replication_count,
                        seeds_used=all_seeds[:replication_count],
                        request_class_intervals=final_intervals,
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
            return ConfigurationEvaluation(
                configuration=configuration,
                classification=final_classification,
                replications_used=previous_count,
                look_count=len(looks),
                seeds_used=all_seeds[:previous_count],
                eligible_request_counts=final_counts,
                request_class_intervals=final_intervals,
                warmup_duration=self.profile.warmup_duration,
                measurement_duration=self.profile.measurement_duration,
                calibration_profile_fingerprint=self.profile.profile_fingerprint,
                sequential_tail_alpha=self._tail_alpha(),
                looks=tuple(looks),
                simulation_diagnostics=(
                    ("processed_event_count", processed_events),
                    ("external_arrival_count", external_arrivals),
                    ("completed_request_count", completed_requests),
                    ("maximum_stop_time", maximum_stop_time),
                    (
                        "scenario_replication_count",
                        previous_count * len(scenario_systems),
                    ),
                ),
                failure_reason=failure_reason,
            )
        finally:
            self.system.configure_system_replicas(original_configuration)

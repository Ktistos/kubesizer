import math
import time
from dataclasses import dataclass

from calibration import (
    CalibrationOptions,
    build_representative_configurations,
    initial_measurement_duration,
    measurement_duration_candidates,
    select_warmup_pilot_parameters,
)
from metrics import (
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
)
RUNTIME_ESTIMATION_SEED_START = 1_200_000_000
DEFAULT_RUNTIME_SAFETY_FACTOR = 3.0


@dataclass(frozen=True)
class RuntimeBenchmarkRates:
    warmup_seconds_per_replication_time_unit: float
    measurement_seconds_per_replication_time_unit: float
    bootstrap_seconds_per_replication_time_unit_per_class: float
    benchmark_wall_time_seconds: float
    benchmark_replications: int
    warmup_benchmark_horizon: float
    measurement_benchmark_duration: float


@dataclass(frozen=True)
class StageRuntimeEstimate:
    stage: str
    ordinary_planning_seconds: float
    configured_ceiling_seconds: float
    ordinary_work: str
    ceiling_work: str


@dataclass(frozen=True)
class WorkflowRuntimeEstimate:
    stages: tuple[StageRuntimeEstimate, ...]
    safety_factor: float
    benchmark: RuntimeBenchmarkRates
    replica_configuration_count: int
    warmup_duration_ceiling: float
    measurement_duration_ceiling: float
    stochastic_warning: str

    @property
    def total_ordinary_planning_seconds(self):
        return sum(stage.ordinary_planning_seconds for stage in self.stages)

    @property
    def total_configured_ceiling_seconds(self):
        return sum(stage.configured_ceiling_seconds for stage in self.stages)


def _bounded_configuration(maximum_replicas, service_count):
    if isinstance(maximum_replicas, int):
        maximum = (maximum_replicas,) * service_count
    else:
        maximum = tuple(maximum_replicas)
    if len(maximum) != service_count or any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in maximum
    ):
        raise ValueError("maximum_replicas must define a positive bound per service.")
    return maximum


def _look_schedule(options):
    values = [options.minimum_optimization_replications]
    while values[-1] < options.maximum_optimization_replications:
        values.append(
            min(
                options.maximum_optimization_replications,
                values[-1] + options.replication_batch_size,
            )
        )
    return tuple(values)


def _parallel_runtime(rate, replication_time_work, workers):
    return rate * replication_time_work / max(1, workers)


def _positive_elapsed(start):
    return max(time.perf_counter() - start, 1e-9)


def benchmark_runtime_rates(
    system,
    *,
    options=None,
    workers=1,
):
    """Measure in-memory simulation and bootstrap rates on this machine."""
    options = options or CalibrationOptions()
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer.")
    parameters = select_warmup_pilot_parameters(
        system,
        replication_count=options.warmup_replications,
        seed_start=options.seed_policy.namespace("warmup_calibration").start,
    )
    benchmark_replications = max(1, min(workers, 8))
    effective_workers = min(workers, benchmark_replications)
    warmup_horizon = min(
        parameters.pilot_horizon,
        max(500.0, 500.0 * parameters.initial_bucket_width),
    )
    measurement_duration = min(
        initial_measurement_duration(system, options),
        max(1_000.0, 500.0 * parameters.initial_bucket_width),
    )
    warmup_seeds = tuple(
        RUNTIME_ESTIMATION_SEED_START + index for index in range(benchmark_replications)
    )
    measurement_seeds = tuple(
        RUNTIME_ESTIMATION_SEED_START + 10_000 + index
        for index in range(benchmark_replications)
    )

    original_configuration = tuple(system.get_system_replicas())
    total_started = time.perf_counter()
    try:
        system.configure_system_replicas(parameters.minimum_replica_configuration)

        started = time.perf_counter()
        system.run_warmup_ensemble(
            seeds=warmup_seeds,
            horizon=warmup_horizon,
            bucket_width=parameters.initial_bucket_width,
            workers=workers,
        )
        warmup_elapsed = _positive_elapsed(started)
        warmup_rate = (
            warmup_elapsed
            * effective_workers
            / (benchmark_replications * warmup_horizon)
        )

        started = time.perf_counter()
        replications = tuple(
            system.simulate_measurement_replications(
                warmup_duration=0.0,
                measurement_duration=measurement_duration,
                seeds=measurement_seeds,
                workers=workers,
                maximum_drain_time=options.maximum_drain_time,
            )
        )
        measurement_elapsed = _positive_elapsed(started)
        measurement_rate = (
            measurement_elapsed
            * effective_workers
            / (benchmark_replications * measurement_duration)
        )

        request_classes = tuple(system.get_request_classes())
        latency_slo_kind = system.get_request_class_latency_slo_kind()
        bootstrap_latency_interval = (
            replication_cluster_bootstrap_mean
            if latency_slo_kind == "mean"
            else replication_cluster_bootstrap_p95
        )
        tail_alpha = options.overall_alpha / (
            2 * options.maximum_sequential_looks * len(request_classes)
        )
        started = time.perf_counter()
        measured_classes = 0
        for class_index, request_class in enumerate(request_classes):
            clusters = tuple(
                replication.request_class_latencies[request_class]
                for replication in replications
            )
            if not any(clusters):
                continue
            bootstrap_latency_interval(
                clusters,
                request_class=request_class,
                slo=1.0,
                resample_count=options.bootstrap_resample_count,
                seed=RUNTIME_ESTIMATION_SEED_START + 20_000 + class_index,
                lower_tail_probability=tail_alpha,
                upper_tail_probability=1.0 - tail_alpha,
                minimum_tail_draws=options.minimum_bootstrap_tail_draws,
            )
            measured_classes += 1
        bootstrap_elapsed = _positive_elapsed(started)
        if measured_classes:
            bootstrap_rate = bootstrap_elapsed / (
                measured_classes * benchmark_replications * measurement_duration
            )
        else:
            bootstrap_rate = 0.0

    finally:
        system.configure_system_replicas(original_configuration)

    return RuntimeBenchmarkRates(
        warmup_seconds_per_replication_time_unit=warmup_rate,
        measurement_seconds_per_replication_time_unit=measurement_rate,
        bootstrap_seconds_per_replication_time_unit_per_class=bootstrap_rate,
        benchmark_wall_time_seconds=_positive_elapsed(total_started),
        benchmark_replications=benchmark_replications,
        warmup_benchmark_horizon=warmup_horizon,
        measurement_benchmark_duration=measurement_duration,
    )


def calculate_workflow_runtime_estimate(
    system,
    rates,
    *,
    options=None,
    maximum_replicas=10,
    workers=1,
    safety_factor=DEFAULT_RUNTIME_SAFETY_FACTOR,
):
    """Project all configured maximum work from measured machine-local rates."""
    options = options or CalibrationOptions()
    if not math.isfinite(safety_factor) or safety_factor < 1.0:
        raise ValueError("safety_factor must be finite and at least one.")
    parameters = select_warmup_pilot_parameters(
        system,
        replication_count=options.warmup_replications,
        seed_start=options.seed_policy.namespace("warmup_calibration").start,
    )
    minimum = parameters.minimum_replica_configuration
    maximum = _bounded_configuration(maximum_replicas, len(minimum))
    if any(high < low for low, high in zip(minimum, maximum)):
        raise ValueError("maximum_replicas cannot be below the stable minimum.")

    representatives = build_representative_configurations(minimum, maximum)
    doubling_count = options.warmup_max_horizon_doublings
    batch_horizon_factor = sum(
        2 ** min(batch_start + 1, doubling_count)
        for batch_start in range(0, doubling_count + 1, 2)
    )
    starting_horizon = parameters.pilot_horizon
    warmup_work = 0.0
    last_accepted_horizon = starting_horizon
    for _ in range(options.warmup_max_validation_horizon_extensions + 1):
        warmup_work += (
            options.warmup_replications * batch_horizon_factor * starting_horizon
        )
        last_accepted_horizon = starting_horizon * (2**doubling_count)
        warmup_work += (
            options.warmup_validation_replications
            * len(representatives)
            * options.warmup_max_validation_rounds
            * last_accepted_horizon
        )
        starting_horizon = last_accepted_horizon * 2.0
    warmup_duration_ceiling = last_accepted_horizon / 3.0

    durations = measurement_duration_candidates(system, options)
    measurement_duration_ceiling = durations[-2]
    measurement_study_durations = [durations[1]]
    measurement_analysis_durations = [durations[0], durations[1]]
    if len(durations) > 2:
        measurement_study_durations.append(durations[-1])
        measurement_analysis_durations.extend(durations[1:])
    measurement_calibration_work = (
        options.measurement_calibration_replications
        * len(representatives)
        * sum(
            warmup_duration_ceiling + duration + options.maximum_drain_time
            for duration in measurement_study_durations
        )
    )
    measurement_calibration_bootstrap_work = (
        options.measurement_calibration_replications
        * len(representatives)
        * len(system.get_request_classes())
        * sum(measurement_analysis_durations)
    )
    calibration_seconds = (
        _parallel_runtime(
            rates.warmup_seconds_per_replication_time_unit,
            warmup_work,
            workers,
        )
        + _parallel_runtime(
            rates.measurement_seconds_per_replication_time_unit,
            measurement_calibration_work,
            workers,
        )
        + rates.bootstrap_seconds_per_replication_time_unit_per_class
        * measurement_calibration_bootstrap_work
    )
    # The ordinary forecast assumes that the first candidate pair may be too
    # short, then that the next paired-prefix batch succeeds.  Further horizon
    # extensions belong to the configured failure-path ceiling, not to the ETA
    # shown as ordinary work.
    ordinary_doubling_count = min(doubling_count, 3)
    ordinary_batch_horizon_factor = sum(
        2 ** min(batch_start + 1, ordinary_doubling_count)
        for batch_start in range(0, ordinary_doubling_count + 1, 2)
    )
    ordinary_validation_horizon = parameters.pilot_horizon * (
        2 ** max(0, ordinary_doubling_count - 1)
    )
    ordinary_warmup_duration = ordinary_validation_horizon / 3.0
    ordinary_warmup_work = (
        options.warmup_replications
        * ordinary_batch_horizon_factor
        * parameters.pilot_horizon
        + options.warmup_validation_replications
        * len(representatives)
        * options.warmup_max_validation_rounds
        * ordinary_validation_horizon
    )
    ordinary_measurement_study_durations = (durations[1],)
    ordinary_measurement_analysis_durations = durations[:2]
    ordinary_measurement_calibration_work = (
        options.measurement_calibration_replications
        * len(representatives)
        * sum(
            ordinary_warmup_duration + duration
            for duration in ordinary_measurement_study_durations
        )
    )
    ordinary_measurement_calibration_bootstrap_work = (
        options.measurement_calibration_replications
        * len(representatives)
        * len(system.get_request_classes())
        * sum(ordinary_measurement_analysis_durations)
    )
    ordinary_calibration_seconds = (
        _parallel_runtime(
            rates.warmup_seconds_per_replication_time_unit,
            ordinary_warmup_work,
            workers,
        )
        + _parallel_runtime(
            rates.measurement_seconds_per_replication_time_unit,
            ordinary_measurement_calibration_work,
            workers,
        )
        + rates.bootstrap_seconds_per_replication_time_unit_per_class
        * ordinary_measurement_calibration_bootstrap_work
    )

    replica_configuration_count = math.prod(
        high - low + 1 for low, high in zip(minimum, maximum)
    )
    per_replication_horizon = (
        warmup_duration_ceiling
        + measurement_duration_ceiling
        + options.maximum_drain_time
    )
    optimization_simulation_work = (
        replica_configuration_count
        * options.maximum_optimization_replications
        * per_replication_horizon
    )
    optimization_bootstrap_work = (
        replica_configuration_count
        * len(system.get_request_classes())
        * measurement_duration_ceiling
        * sum(_look_schedule(options))
    )
    optimization_seconds = (
        _parallel_runtime(
            rates.measurement_seconds_per_replication_time_unit,
            optimization_simulation_work,
            workers,
        )
        + rates.bootstrap_seconds_per_replication_time_unit_per_class
        * optimization_bootstrap_work
    )
    ordinary_replica_configuration_count = min(
        replica_configuration_count,
        1 + sum(high > low for low, high in zip(minimum, maximum)),
    )
    ordinary_measurement_duration = durations[0]
    ordinary_per_replication_horizon = (
        ordinary_warmup_duration + ordinary_measurement_duration
    )
    ordinary_optimization_simulation_work = (
        ordinary_replica_configuration_count
        * options.minimum_optimization_replications
        * ordinary_per_replication_horizon
    )
    ordinary_optimization_bootstrap_work = (
        ordinary_replica_configuration_count
        * len(system.get_request_classes())
        * ordinary_measurement_duration
        * options.minimum_optimization_replications
    )
    ordinary_optimization_seconds = (
        _parallel_runtime(
            rates.measurement_seconds_per_replication_time_unit,
            ordinary_optimization_simulation_work,
            workers,
        )
        + rates.bootstrap_seconds_per_replication_time_unit_per_class
        * ordinary_optimization_bootstrap_work
    )

    final_simulation_work = (
        options.final_validation_replications * per_replication_horizon
    )
    final_bootstrap_work = (
        options.final_validation_replications
        * len(system.get_request_classes())
        * measurement_duration_ceiling
    )
    final_seconds = (
        _parallel_runtime(
            rates.measurement_seconds_per_replication_time_unit,
            final_simulation_work,
            workers,
        )
        + rates.bootstrap_seconds_per_replication_time_unit_per_class
        * final_bootstrap_work
    )
    ordinary_final_simulation_work = (
        options.final_validation_replications * ordinary_per_replication_horizon
    )
    ordinary_final_bootstrap_work = (
        options.final_validation_replications
        * len(system.get_request_classes())
        * ordinary_measurement_duration
    )
    ordinary_final_seconds = (
        _parallel_runtime(
            rates.measurement_seconds_per_replication_time_unit,
            ordinary_final_simulation_work,
            workers,
        )
        + rates.bootstrap_seconds_per_replication_time_unit_per_class
        * ordinary_final_bootstrap_work
    )

    stages = (
        StageRuntimeEstimate(
            stage="Stage A — calibration",
            ordinary_planning_seconds=(ordinary_calibration_seconds * safety_factor),
            configured_ceiling_seconds=calibration_seconds * safety_factor,
            ordinary_work="one warm-up campaign and all measurement candidates",
            ceiling_work=(
                f"up to {options.warmup_max_validation_horizon_extensions + 1} "
                "warm-up horizon campaigns and maximum drain allowance"
            ),
        ),
        StageRuntimeEstimate(
            stage="Stage B — replica optimization",
            ordinary_planning_seconds=(ordinary_optimization_seconds * safety_factor),
            configured_ceiling_seconds=optimization_seconds * safety_factor,
            ordinary_work=(
                f"stable anchor plus {ordinary_replica_configuration_count - 1} "
                f"immediate neighbours at R_min={options.minimum_optimization_replications}"
            ),
            ceiling_work=(
                f"all {replica_configuration_count:,} configurations at up to "
                f"R_max={options.maximum_optimization_replications}"
            ),
        ),
        StageRuntimeEstimate(
            stage="Stage C — final validation",
            ordinary_planning_seconds=ordinary_final_seconds * safety_factor,
            configured_ceiling_seconds=final_seconds * safety_factor,
            ordinary_work=(
                f"one configuration with {options.final_validation_replications} "
                "fresh replications and ordinary draining"
            ),
            ceiling_work=(
                f"the same fixed replications plus the full "
                f"{options.maximum_drain_time:g}-unit drain allowance"
            ),
        ),
    )
    return WorkflowRuntimeEstimate(
        stages=stages,
        safety_factor=safety_factor,
        benchmark=rates,
        replica_configuration_count=replica_configuration_count,
        warmup_duration_ceiling=warmup_duration_ceiling,
        measurement_duration_ceiling=measurement_duration_ceiling,
        stochastic_warning=(
            "This is a conservative planning estimate, not a guaranteed bound: "
            "stochastic workloads may have unbounded or long tails, and host load "
            "can change. Configured retry, replica, replication, and drain-time "
            "limits define the projected work ceiling."
        ),
    )


def estimate_workflow_runtime(
    system,
    *,
    options=None,
    maximum_replicas=10,
    workers=1,
    safety_factor=DEFAULT_RUNTIME_SAFETY_FACTOR,
):
    options = options or CalibrationOptions()
    rates = benchmark_runtime_rates(
        system,
        options=options,
        workers=workers,
    )
    return calculate_workflow_runtime_estimate(
        system,
        rates,
        options=options,
        maximum_replicas=maximum_replicas,
        workers=workers,
        safety_factor=safety_factor,
    )


def format_duration(seconds):
    if not math.isfinite(seconds) or seconds < 0:
        return "unavailable"
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    if seconds < 3_600:
        return f"{seconds / 60:.1f} minutes"
    if seconds < 86_400:
        return f"{seconds / 3_600:.1f} hours"
    return f"{seconds / 86_400:.1f} days"

import math
import time
from dataclasses import dataclass
from itertools import islice

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
from optimization import (
    DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT,
    DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    NodePlacementProblem,
    OptimizationMode,
    OptimizationModes,
    ScalablePlacementSearchOptions,
    ScalableReplicaSearchOptions,
    normalize_optimization_mode,
    optimization_workflow_mode,
    resolve_optimization_mode,
)
from performance_models import Cluster


RUNTIME_ESTIMATION_SEED_START = 1_200_000_000
DEFAULT_RUNTIME_SAFETY_FACTOR = 3.0
DEFAULT_PLACEMENT_BENCHMARK_CANDIDATES = 200
DEFAULT_EXACT_REPLICA_SEARCH_TIME_BUDGET_SECONDS = 600.0


@dataclass(frozen=True)
class RuntimeBenchmarkRates:
    warmup_seconds_per_replication_time_unit: float
    measurement_seconds_per_replication_time_unit: float
    bootstrap_seconds_per_replication_time_unit_per_class: float
    placement_seconds_per_candidate: float
    benchmark_wall_time_seconds: float
    benchmark_replications: int
    warmup_benchmark_horizon: float
    measurement_benchmark_duration: float
    placement_benchmark_candidates: int


@dataclass(frozen=True)
class RuntimeAdaptiveReplicaLimit:
    calibration_profile_fingerprint: str
    configuration_count: int
    hard_configuration_cap: int
    exact_search_time_budget_seconds: float
    safety_factor: float
    worker_count: int
    estimated_simulation_seconds_per_configuration: float
    estimated_bootstrap_seconds_per_configuration: float
    estimated_seconds_per_configuration: float
    runtime_configuration_limit: int
    effective_configuration_limit: int
    projected_exact_search_seconds: float
    selected_mode: str
    selection_reason: str


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
    maximum_placement_count: int
    warmup_duration_ceiling: float
    measurement_duration_ceiling: float
    replica_search_mode: str
    placement_search_mode_at_maximum_bounds: str
    stochastic_warning: str

    @property
    def total_ordinary_planning_seconds(self):
        return sum(stage.ordinary_planning_seconds for stage in self.stages)

    @property
    def total_configured_ceiling_seconds(self):
        return sum(stage.configured_ceiling_seconds for stage in self.stages)

    @property
    def workflow_search_mode(self):
        return optimization_workflow_mode(
            self.replica_search_mode,
            self.placement_search_mode_at_maximum_bounds,
        )


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


def _profile_look_schedule(profile):
    values = [profile.minimum_optimization_replications]
    while values[-1] < profile.maximum_optimization_replications:
        values.append(
            min(
                profile.maximum_optimization_replications,
                values[-1] + profile.replication_batch_size,
            )
        )
    if len(values) != profile.maximum_sequential_looks:
        raise ValueError("Calibration profile contains an inconsistent look schedule.")
    return tuple(values)


def calibrate_replica_configuration_limit(
    system,
    calibration_profile,
    rates,
    *,
    workers=1,
    exact_search_time_budget_seconds=(DEFAULT_EXACT_REPLICA_SEARCH_TIME_BUDGET_SECONDS),
    hard_configuration_cap=DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT,
    safety_factor=DEFAULT_RUNTIME_SAFETY_FACTOR,
    mode=OptimizationMode.AUTO,
):
    """Derive an exact-search limit from calibrated statistical work.

    This is a planning policy rather than a statistical parameter. The native
    benchmark includes its observed cohort-drain overhead; the safety factor
    covers ordinary runtime variation. Stochastic execution remains unbounded,
    so the result is an estimate and never a wall-clock guarantee.
    """
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer.")
    if (
        not math.isfinite(exact_search_time_budget_seconds)
        or exact_search_time_budget_seconds <= 0
    ):
        raise ValueError(
            "exact_search_time_budget_seconds must be finite and positive."
        )
    if not math.isfinite(safety_factor) or safety_factor < 1.0:
        raise ValueError("safety_factor must be finite and at least one.")
    if (
        isinstance(hard_configuration_cap, bool)
        or not isinstance(hard_configuration_cap, int)
        or hard_configuration_cap <= 0
    ):
        raise ValueError("hard_configuration_cap must be a positive integer.")

    rate_values = (
        rates.measurement_seconds_per_replication_time_unit,
        rates.bootstrap_seconds_per_replication_time_unit_per_class,
    )
    if any(not math.isfinite(value) or value < 0 for value in rate_values):
        raise ValueError("Runtime benchmark rates must be finite and nonnegative.")
    if rates.measurement_seconds_per_replication_time_unit <= 0:
        raise ValueError("The measured simulation runtime rate must be positive.")
    if rates.bootstrap_seconds_per_replication_time_unit_per_class <= 0:
        raise ValueError("The measured bootstrap runtime rate must be positive.")

    minimum = tuple(calibration_profile.minimum_replica_configuration)
    maximum = tuple(calibration_profile.maximum_replica_configuration)
    if len(minimum) == 0 or len(minimum) != len(maximum):
        raise ValueError("Calibration profile has invalid replica bounds.")
    if any(high < low for low, high in zip(minimum, maximum)):
        raise ValueError("Calibration profile has invalid replica bounds.")
    configuration_count = math.prod(
        high - low + 1 for low, high in zip(minimum, maximum)
    )

    replication_count = calibration_profile.maximum_optimization_replications
    effective_workers = min(workers, replication_count)
    replication_horizon = (
        calibration_profile.warmup_duration + calibration_profile.measurement_duration
    )
    simulation_seconds = _parallel_runtime(
        rates.measurement_seconds_per_replication_time_unit,
        replication_count * replication_horizon,
        effective_workers,
    )
    profile_request_classes = tuple(
        name for name, _ in calibration_profile.request_class_slos
    )
    system_request_classes = tuple(system.get_request_classes())
    if not profile_request_classes or set(profile_request_classes) != set(
        system_request_classes
    ):
        raise ValueError("Calibration profile request classes do not match the system.")
    request_class_count = len(profile_request_classes)
    bootstrap_seconds = (
        rates.bootstrap_seconds_per_replication_time_unit_per_class
        * request_class_count
        * calibration_profile.measurement_duration
        * sum(_profile_look_schedule(calibration_profile))
    )
    estimated_seconds = safety_factor * (simulation_seconds + bootstrap_seconds)
    if not math.isfinite(estimated_seconds) or estimated_seconds <= 0:
        raise ValueError("Estimated configuration runtime must be positive.")

    runtime_limit = max(
        1,
        math.floor(exact_search_time_budget_seconds / estimated_seconds),
    )
    effective_limit = min(hard_configuration_cap, runtime_limit)
    normalized_mode = normalize_optimization_mode(mode)
    selected_mode = resolve_optimization_mode(
        normalized_mode,
        configuration_count,
        effective_limit,
    )
    projected_seconds = estimated_seconds * configuration_count
    if normalized_mode == OptimizationMode.AUTO:
        if selected_mode == OptimizationMode.EXACT:
            reason = (
                f"{configuration_count:,} configurations fit the runtime-adaptive "
                f"exact limit of {effective_limit:,}."
            )
        else:
            reason = (
                f"{configuration_count:,} configurations exceed the "
                f"runtime-adaptive exact limit of {effective_limit:,}."
            )
    else:
        reason = f"Explicit {selected_mode.value} mode overrides AUTO selection."

    return RuntimeAdaptiveReplicaLimit(
        calibration_profile_fingerprint=(calibration_profile.profile_fingerprint),
        configuration_count=configuration_count,
        hard_configuration_cap=hard_configuration_cap,
        exact_search_time_budget_seconds=float(exact_search_time_budget_seconds),
        safety_factor=float(safety_factor),
        worker_count=workers,
        estimated_simulation_seconds_per_configuration=(
            simulation_seconds * safety_factor
        ),
        estimated_bootstrap_seconds_per_configuration=(
            bootstrap_seconds * safety_factor
        ),
        estimated_seconds_per_configuration=estimated_seconds,
        runtime_configuration_limit=runtime_limit,
        effective_configuration_limit=effective_limit,
        projected_exact_search_seconds=projected_seconds,
        selected_mode=selected_mode.value,
        selection_reason=reason,
    )


def format_replica_configuration_limit_calibration(calibration):
    return "\n".join(
        (
            "Runtime-adaptive replica search selection",
            f"  Replica configuration space: {calibration.configuration_count:,}",
            (
                "  Conservative time per configuration: "
                + format_duration(calibration.estimated_seconds_per_configuration)
            ),
            (
                "  Projected full exact-search time: "
                + format_duration(calibration.projected_exact_search_seconds)
            ),
            (
                "  Exact-search time budget: "
                + format_duration(calibration.exact_search_time_budget_seconds)
            ),
            (
                "  Runtime-derived limit / hard cap / effective limit: "
                f"{calibration.runtime_configuration_limit:,} / "
                f"{calibration.hard_configuration_cap:,} / "
                f"{calibration.effective_configuration_limit:,}"
            ),
            f"  Selected replica mode: {calibration.selected_mode}",
            f"  Reason: {calibration.selection_reason}",
            (
                "  Planning estimate only: stochastic run time is not a "
                "wall-clock guarantee."
            ),
        )
    )


def benchmark_runtime_rates(
    system,
    resources,
    *,
    options=None,
    workers=1,
    placement_sample_size=DEFAULT_PLACEMENT_BENCHMARK_CANDIDATES,
):
    """Measure in-memory kernel, bootstrap, and placement rates on this machine."""
    options = options or CalibrationOptions()
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer.")
    if (
        isinstance(placement_sample_size, bool)
        or not isinstance(placement_sample_size, int)
        or placement_sample_size <= 0
    ):
        raise ValueError("placement_sample_size must be a positive integer.")

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

        placement_count = 0
        placement_rate = 0.0
        if resources is not None:
            cluster = Cluster("runtime-estimation", resources, system)
            placement_problem = NodePlacementProblem(cluster)
            started = time.perf_counter()
            for configuration in islice(
                placement_problem.iter_configurations(),
                placement_sample_size,
            ):
                placement_problem.evaluate(configuration)
                placement_count += 1
            placement_elapsed = _positive_elapsed(started)
            placement_rate = placement_elapsed / max(1, placement_count)
    finally:
        system.configure_system_replicas(original_configuration)

    return RuntimeBenchmarkRates(
        warmup_seconds_per_replication_time_unit=warmup_rate,
        measurement_seconds_per_replication_time_unit=measurement_rate,
        bootstrap_seconds_per_replication_time_unit_per_class=bootstrap_rate,
        placement_seconds_per_candidate=placement_rate,
        benchmark_wall_time_seconds=_positive_elapsed(total_started),
        benchmark_replications=benchmark_replications,
        warmup_benchmark_horizon=warmup_horizon,
        measurement_benchmark_duration=measurement_duration,
        placement_benchmark_candidates=placement_count,
    )


def calculate_workflow_runtime_estimate(
    system,
    resources,
    rates,
    *,
    options=None,
    maximum_replicas=10,
    workers=1,
    safety_factor=DEFAULT_RUNTIME_SAFETY_FACTOR,
    optimization_mode=OptimizationMode.AUTO,
    optimization_modes=None,
    replica_optimization_mode=None,
    placement_optimization_mode=None,
    exact_replica_configuration_limit=(DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT),
    exact_placement_configuration_limit=(DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT),
    scalable_replica_options=None,
    scalable_placement_options=None,
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

    # A simulation workflow with node placement must calibrate the selected
    # fixed placement before fresh final validation because communication
    # latency and node execution multipliers change the simulator semantics.
    # The fixed-placement profile has one representative configuration.
    if resources is None:
        placement_calibration_seconds = 0.0
        ordinary_placement_calibration_seconds = 0.0
    else:
        fixed_warmup_work = 0.0
        fixed_starting_horizon = parameters.pilot_horizon
        for _ in range(options.warmup_max_validation_horizon_extensions + 1):
            fixed_warmup_work += (
                options.warmup_replications
                * batch_horizon_factor
                * fixed_starting_horizon
            )
            fixed_accepted_horizon = fixed_starting_horizon * (2**doubling_count)
            fixed_warmup_work += (
                options.warmup_validation_replications
                * options.warmup_max_validation_rounds
                * fixed_accepted_horizon
            )
            fixed_starting_horizon = fixed_accepted_horizon * 2.0
        fixed_measurement_work = options.measurement_calibration_replications * sum(
            warmup_duration_ceiling + duration + options.maximum_drain_time
            for duration in measurement_study_durations
        )
        fixed_bootstrap_work = (
            options.measurement_calibration_replications
            * len(system.get_request_classes())
            * sum(measurement_analysis_durations)
        )
        placement_calibration_seconds = (
            _parallel_runtime(
                rates.warmup_seconds_per_replication_time_unit,
                fixed_warmup_work,
                workers,
            )
            + _parallel_runtime(
                rates.measurement_seconds_per_replication_time_unit,
                fixed_measurement_work,
                workers,
            )
            + rates.bootstrap_seconds_per_replication_time_unit_per_class
            * fixed_bootstrap_work
        )
        fixed_ordinary_warmup_work = (
            options.warmup_replications
            * ordinary_batch_horizon_factor
            * parameters.pilot_horizon
            + options.warmup_validation_replications
            * options.warmup_max_validation_rounds
            * ordinary_validation_horizon
        )
        fixed_ordinary_measurement_work = (
            options.measurement_calibration_replications
            * sum(
                ordinary_warmup_duration + duration
                for duration in ordinary_measurement_study_durations
            )
        )
        fixed_ordinary_bootstrap_work = (
            options.measurement_calibration_replications
            * len(system.get_request_classes())
            * sum(ordinary_measurement_analysis_durations)
        )
        ordinary_placement_calibration_seconds = (
            _parallel_runtime(
                rates.warmup_seconds_per_replication_time_unit,
                fixed_ordinary_warmup_work,
                workers,
            )
            + _parallel_runtime(
                rates.measurement_seconds_per_replication_time_unit,
                fixed_ordinary_measurement_work,
                workers,
            )
            + rates.bootstrap_seconds_per_replication_time_unit_per_class
            * fixed_ordinary_bootstrap_work
        )

    replica_configuration_count = math.prod(
        high - low + 1 for low, high in zip(minimum, maximum)
    )
    if optimization_modes is not None and (
        replica_optimization_mode is not None or placement_optimization_mode is not None
    ):
        raise ValueError(
            "optimization_modes cannot be combined with per-optimizer modes."
        )
    if optimization_modes is None:
        uniform_mode = normalize_optimization_mode(optimization_mode)
        replica_mode_policy = normalize_optimization_mode(
            uniform_mode
            if replica_optimization_mode is None
            else replica_optimization_mode
        )
        placement_mode_policy = normalize_optimization_mode(
            uniform_mode
            if placement_optimization_mode is None
            else placement_optimization_mode
        )
    else:
        if not isinstance(optimization_modes, OptimizationModes):
            raise TypeError("optimization_modes must be an OptimizationModes value.")
        replica_mode_policy = optimization_modes.replica
        placement_mode_policy = optimization_modes.placement
    selected_replica_mode = resolve_optimization_mode(
        replica_mode_policy,
        replica_configuration_count,
        exact_replica_configuration_limit,
    )
    scalable_replica_options = (
        scalable_replica_options or ScalableReplicaSearchOptions()
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
    if selected_replica_mode == OptimizationMode.SCALABLE:
        one_configuration_seconds = optimization_seconds / max(
            1,
            replica_configuration_count,
        )
        capped_projected_seconds = one_configuration_seconds * (
            scalable_replica_options.maximum_configuration_evaluations
        )
        optimization_seconds = (
            min(
                capped_projected_seconds,
                scalable_replica_options.time_budget_seconds,
            )
            + one_configuration_seconds
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

    scalable_placement_options = (
        scalable_placement_options or ScalablePlacementSearchOptions()
    )
    if resources is None:
        maximum_placement_count = 0
        ordinary_placement_count = 0
        selected_placement_mode = normalize_optimization_mode(placement_mode_policy)
        placement_seconds = 0.0
        ordinary_placement_seconds = 0.0
    else:
        node_count = len(resources["nodes"])
        maximum_placement_count = math.prod(
            math.comb(replica_count + node_count - 1, node_count - 1)
            for replica_count in maximum
        )
        selected_placement_mode = resolve_optimization_mode(
            placement_mode_policy,
            maximum_placement_count,
            exact_placement_configuration_limit,
        )
        placement_seconds = (
            rates.placement_seconds_per_candidate * maximum_placement_count
        )
        if selected_placement_mode == OptimizationMode.SCALABLE:
            scalable_placement_operations = (
                scalable_placement_options.maximum_partial_states
                + scalable_placement_options.maximum_complete_evaluations
            )
            capped_projected_seconds = (
                rates.placement_seconds_per_candidate * scalable_placement_operations
            )
            placement_seconds = (
                min(
                    capped_projected_seconds,
                    scalable_placement_options.time_budget_seconds,
                )
                + rates.placement_seconds_per_candidate
            )
        shell_configurations = [minimum]
        for service_index, (low, high) in enumerate(zip(minimum, maximum)):
            if low >= high:
                continue
            neighbor = list(minimum)
            neighbor[service_index] += 1
            shell_configurations.append(tuple(neighbor))
        ordinary_placement_count = max(
            math.prod(
                math.comb(replica_count + node_count - 1, node_count - 1)
                for replica_count in configuration
            )
            for configuration in shell_configurations
        )
        ordinary_placement_seconds = (
            rates.placement_seconds_per_candidate * ordinary_placement_count
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
                (
                    f"all {replica_configuration_count:,} configurations at up to "
                    f"R_max={options.maximum_optimization_replications}"
                )
                if selected_replica_mode == OptimizationMode.EXACT
                else (
                    f"SCALABLE mode: at most "
                    f"{scalable_replica_options.maximum_configuration_evaluations} "
                    f"configurations or {scalable_replica_options.time_budget_seconds:g}s, "
                    "plus one in-flight evaluation"
                )
            ),
        ),
        StageRuntimeEstimate(
            stage="Stage D — node placement",
            ordinary_planning_seconds=(ordinary_placement_seconds * safety_factor),
            configured_ceiling_seconds=placement_seconds * safety_factor,
            ordinary_work=(
                "placement skipped"
                if resources is None
                else (
                    f"up to {ordinary_placement_count:,} placements near the "
                    "stable anchor"
                )
            ),
            ceiling_work=(
                "placement skipped"
                if resources is None
                else (
                    (
                        f"all {maximum_placement_count:,} maximum-bound placements "
                        "before proof-based pruning"
                    )
                    if selected_placement_mode == OptimizationMode.EXACT
                    else (
                        f"SCALABLE mode: beam {scalable_placement_options.beam_width}, "
                        f"at most {scalable_placement_options.maximum_partial_states:,} "
                        "partial states and "
                        f"{scalable_placement_options.maximum_complete_evaluations:,} "
                        "complete evaluations"
                    )
                )
            ),
        ),
        StageRuntimeEstimate(
            stage="Stage E — placement-aware calibration",
            ordinary_planning_seconds=(
                ordinary_placement_calibration_seconds * safety_factor
            ),
            configured_ceiling_seconds=(
                placement_calibration_seconds * safety_factor
            ),
            ordinary_work=(
                "placement skipped"
                if resources is None
                else "one fixed-placement warm-up and measurement campaign"
            ),
            ceiling_work=(
                "placement skipped"
                if resources is None
                else (
                    "the selected placement with all configured warm-up and "
                    "measurement extensions"
                )
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
        maximum_placement_count=maximum_placement_count,
        warmup_duration_ceiling=warmup_duration_ceiling,
        measurement_duration_ceiling=measurement_duration_ceiling,
        replica_search_mode=selected_replica_mode.value,
        placement_search_mode_at_maximum_bounds=selected_placement_mode.value,
        stochastic_warning=(
            "This is a conservative planning estimate, not a guaranteed bound: "
            "stochastic workloads may have unbounded or long tails, and host load "
            "can change. Configured retry, replica, replication, drain-time, and "
            "placement limits define the projected work ceiling."
        ),
    )


def estimate_workflow_runtime(
    system,
    resources,
    *,
    options=None,
    maximum_replicas=10,
    workers=1,
    safety_factor=DEFAULT_RUNTIME_SAFETY_FACTOR,
    optimization_mode=OptimizationMode.AUTO,
    optimization_modes=None,
    replica_optimization_mode=None,
    placement_optimization_mode=None,
    exact_replica_configuration_limit=(DEFAULT_EXACT_REPLICA_CONFIGURATION_LIMIT),
    exact_placement_configuration_limit=(DEFAULT_EXACT_PLACEMENT_CONFIGURATION_LIMIT),
    scalable_replica_options=None,
    scalable_placement_options=None,
):
    options = options or CalibrationOptions()
    rates = benchmark_runtime_rates(
        system,
        resources,
        options=options,
        workers=workers,
    )
    return calculate_workflow_runtime_estimate(
        system,
        resources,
        rates,
        options=options,
        maximum_replicas=maximum_replicas,
        workers=workers,
        safety_factor=safety_factor,
        optimization_mode=optimization_mode,
        optimization_modes=optimization_modes,
        replica_optimization_mode=replica_optimization_mode,
        placement_optimization_mode=placement_optimization_mode,
        exact_replica_configuration_limit=exact_replica_configuration_limit,
        exact_placement_configuration_limit=exact_placement_configuration_limit,
        scalable_replica_options=scalable_replica_options,
        scalable_placement_options=scalable_placement_options,
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

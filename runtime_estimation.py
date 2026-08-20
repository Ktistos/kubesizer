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
from metrics import replication_cluster_bootstrap_p95
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


@dataclass(frozen=True)
class LiveRuntimeSnapshot:
    stage: str
    event: str
    completed_units: int | None
    total_units: int | None
    unit_name: str | None
    stage_elapsed_seconds: float
    stage_remaining_low_seconds: float | None
    stage_remaining_high_seconds: float | None
    overall_remaining_low_seconds: float | None
    overall_remaining_high_seconds: float | None
    detail: str | None
    completed: bool


class LiveWorkflowRuntimeEstimator:
    """Update preflight planning estimates with observed workflow progress.

    Remaining-time ranges are operational planning estimates. They combine the
    measured completion rate with the preflight ordinary/ceiling projections;
    they are not statistical confidence intervals or wall-clock guarantees.
    """

    _STAGE_NAMES = {
        "calibration": "Stage A — calibration",
        "replica_optimization": "Stage B — replica optimization",
        "final_validation": "Stage C — final validation",
        "node_placement": "Stage D — node placement",
    }

    def __init__(
        self,
        estimate,
        *,
        output=print,
        clock=None,
        minimum_update_interval_seconds=1.0,
    ):
        if not callable(output):
            raise TypeError("output must be callable.")
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable.")
        if (
            not math.isfinite(minimum_update_interval_seconds)
            or minimum_update_interval_seconds < 0
        ):
            raise ValueError(
                "minimum_update_interval_seconds must be finite and non-negative."
            )
        self.estimate = estimate
        self.output = output
        self.clock = clock or time.perf_counter
        self.minimum_update_interval_seconds = float(minimum_update_interval_seconds)
        try:
            stages = tuple(estimate.stages)
        except (AttributeError, TypeError) as error:
            raise TypeError("estimate must expose workflow stage estimates.") from error
        self._stage_estimates = {stage.stage: stage for stage in stages}
        missing = set(self._STAGE_NAMES.values()).difference(self._stage_estimates)
        if missing:
            raise ValueError(
                "Workflow estimate is missing stages: " + ", ".join(sorted(missing))
            )
        self._stage_order = tuple(self._STAGE_NAMES.values())
        self._active_stage = None
        self._stage_started = None
        self._completed_units = None
        self._total_units = None
        self._unit_name = None
        self._last_output_time = -math.inf
        self._completed_stages = set()
        self._last_snapshot = None

    @property
    def last_snapshot(self):
        return self._last_snapshot

    def _stage_name(self, stage):
        name = self._STAGE_NAMES.get(stage, stage)
        if name not in self._stage_estimates:
            raise ValueError(f"Unknown runtime stage: {stage!r}.")
        return name

    def start_stage(self, stage, *, total_units=None, unit_name=None):
        stage = self._stage_name(stage)
        if self._active_stage is not None:
            raise RuntimeError(
                f"Cannot start {stage!r} while {self._active_stage!r} is active."
            )
        self._validate_units(0 if total_units is not None else None, total_units)
        self._active_stage = stage
        self._stage_started = self.clock()
        self._completed_units = 0 if total_units is not None else None
        self._total_units = total_units
        self._unit_name = unit_name
        return self._emit("started", detail=None, force=True, completed=False)

    def update_stage(
        self,
        *,
        completed_units=None,
        total_units=None,
        unit_name=None,
        event="progress",
        detail=None,
        force=False,
    ):
        if self._active_stage is None:
            raise RuntimeError("No runtime stage is active.")
        effective_total = self._total_units if total_units is None else total_units
        effective_completed = (
            self._completed_units if completed_units is None else completed_units
        )
        self._validate_units(effective_completed, effective_total)
        if (
            completed_units is not None
            and self._completed_units is not None
            and completed_units < self._completed_units
        ):
            raise ValueError("completed_units cannot decrease.")
        self._completed_units = effective_completed
        self._total_units = effective_total
        if unit_name is not None:
            self._unit_name = unit_name
        return self._emit(event, detail=detail, force=force, completed=False)

    def complete_stage(self, *, detail=None):
        if self._active_stage is None:
            raise RuntimeError("No runtime stage is active.")
        snapshot = self._emit(
            "completed",
            detail=detail,
            force=True,
            completed=True,
        )
        self._completed_stages.add(self._active_stage)
        self._active_stage = None
        self._stage_started = None
        self._completed_units = None
        self._total_units = None
        self._unit_name = None
        return snapshot

    def fail_stage(self, *, detail=None):
        if self._active_stage is None:
            raise RuntimeError("No runtime stage is active.")
        snapshot = self._emit(
            "failed",
            detail=detail,
            force=True,
            completed=True,
        )
        self._completed_stages.add(self._active_stage)
        self._active_stage = None
        self._stage_started = None
        self._completed_units = None
        self._total_units = None
        self._unit_name = None
        return snapshot

    def callback(self, stage):
        stage_name = self._stage_name(stage)

        def report(event, details):
            if self._active_stage != stage_name:
                return None
            details = dict(details or {})
            completed, total, unit = self._event_units(event, details)
            return self.update_stage(
                completed_units=completed,
                total_units=total,
                unit_name=unit,
                event=event,
                detail=self._event_detail(event, details),
                force=(
                    event.endswith("_started")
                    or event.endswith("_completed")
                    or event == "replica_configuration_evaluated"
                ),
            )

        return report

    @staticmethod
    def _validate_units(completed, total):
        for name, value in (("completed_units", completed), ("total_units", total)):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer or None.")
        if completed is not None and total is not None and completed > total:
            raise ValueError("completed_units cannot exceed total_units.")

    @staticmethod
    def _event_units(event, details):
        if event in {"placement_search_started", "placement_search_progress"} and (
            "total_search_states" in details
        ):
            return (
                details.get("completed_search_states"),
                details.get("total_search_states"),
                "search states",
            )
        mappings = {
            "replica_optimization_started": (
                "completed_configurations",
                "total_configurations",
                "configurations",
            ),
            "replica_configuration_evaluated": (
                "completed_configurations",
                "total_configurations",
                "configurations",
            ),
            "final_validation_started": (
                "completed_replications",
                "total_replications",
                "replications",
            ),
            "final_validation_replications_completed": (
                "completed_replications",
                "total_replications",
                "replications",
            ),
            "placement_search_started": (
                "completed_configurations",
                "total_configurations",
                "placements",
            ),
            "placement_search_progress": (
                "completed_configurations",
                "total_configurations",
                "placements",
            ),
        }
        mapping = mappings.get(event)
        if mapping is None:
            return None, None, None
        completed_key, total_key, unit = mapping
        return details.get(completed_key), details.get(total_key), unit

    @staticmethod
    def _event_detail(event, details):
        labels = {
            "pilot_batch_started": "warm-up pilot batch started",
            "pilot_batch_completed": "warm-up pilot batch completed",
            "pilot_horizon_analyzing": "warm-up horizon analysis",
            "minimum_configuration_accepted": "minimum configuration accepted",
            "validation_started": "global warm-up validation started",
            "validation_completed": "global warm-up validation completed",
            "measurement_trajectories_started": "measurement pilot started",
            "measurement_trajectories_completed": "measurement pilot completed",
            "measurement_candidate_analyzed": "measurement candidate analyzed",
            "configuration_evaluation_started": "configuration simulation started",
            "replication_batch_started": "replication batch started",
            "replication_batch_completed": "replication batch completed",
            "configuration_evaluation_completed": "configuration simulation completed",
            "replica_configuration_evaluated": "replica configuration evaluated",
            "final_validation_started": "final validation started",
            "final_validation_replications_completed": "final replications completed",
            "placement_search_started": "placement search started",
            "placement_search_progress": "placement search progress",
            "placement_search_completed": "placement search completed",
        }
        label = labels.get(event, event.replace("_", " "))
        qualifiers = []
        for key in (
            "scenario",
            "configuration",
            "look_index",
            "replication_count",
            "simulation_horizon",
            "simulation_duration",
            "selected_mode",
        ):
            if key in details:
                qualifiers.append(f"{key}={details[key]}")
        return label + ("; " + ", ".join(qualifiers) if qualifiers else "")

    def _remaining_range(self, elapsed):
        stage = self._stage_estimates[self._active_stage]
        ordinary = stage.ordinary_planning_seconds
        ceiling = stage.configured_ceiling_seconds
        if self._total_units and self._completed_units is not None:
            fraction = min(1.0, self._completed_units / self._total_units)
            ordinary_remaining = ordinary * (1.0 - fraction)
            ceiling_remaining = ceiling * (1.0 - fraction)
            if self._completed_units > 0:
                empirical_remaining = (
                    elapsed
                    / self._completed_units
                    * (self._total_units - self._completed_units)
                )
                return (
                    min(ordinary_remaining, empirical_remaining),
                    max(ceiling_remaining, empirical_remaining),
                )
            return ordinary_remaining, ceiling_remaining
        low = max(0.0, ordinary - elapsed)
        high = max(low, ceiling - elapsed)
        if high == 0.0:
            return None, None
        return low, high

    def _overall_remaining_range(self, stage_low, stage_high, *, completed):
        current_index = self._stage_order.index(self._active_stage)
        future = tuple(
            name
            for name in self._stage_order[current_index + 1 :]
            if name not in self._completed_stages
        )
        future_low = sum(
            self._stage_estimates[name].ordinary_planning_seconds for name in future
        )
        future_high = sum(
            self._stage_estimates[name].configured_ceiling_seconds for name in future
        )
        if completed:
            return future_low, future_high
        return (
            None if stage_low is None else stage_low + future_low,
            None if stage_high is None else stage_high + future_high,
        )

    def _emit(self, event, *, detail, force, completed):
        now = self.clock()
        elapsed = max(0.0, now - self._stage_started)
        stage_low, stage_high = (
            (0.0, 0.0) if completed else self._remaining_range(elapsed)
        )
        overall_low, overall_high = self._overall_remaining_range(
            stage_low,
            stage_high,
            completed=completed,
        )
        snapshot = LiveRuntimeSnapshot(
            stage=self._active_stage,
            event=event,
            completed_units=self._completed_units,
            total_units=self._total_units,
            unit_name=self._unit_name,
            stage_elapsed_seconds=elapsed,
            stage_remaining_low_seconds=stage_low,
            stage_remaining_high_seconds=stage_high,
            overall_remaining_low_seconds=overall_low,
            overall_remaining_high_seconds=overall_high,
            detail=detail,
            completed=completed,
        )
        self._last_snapshot = snapshot
        if (
            force
            or now - self._last_output_time >= self.minimum_update_interval_seconds
        ):
            self.output(format_live_runtime_snapshot(snapshot))
            self._last_output_time = now
        return snapshot


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
            replication_cluster_bootstrap_p95(
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
    ordinary_warmup_duration = min(
        parameters.pilot_horizon,
        parameters.pilot_horizon * (2**doubling_count) / 3.0,
    )
    ordinary_warmup_work = (
        options.warmup_replications * batch_horizon_factor * parameters.pilot_horizon
        + options.warmup_validation_replications
        * len(representatives)
        * options.warmup_max_validation_rounds
        * parameters.pilot_horizon
        * (2**doubling_count)
    )
    ordinary_measurement_calibration_work = (
        options.measurement_calibration_replications
        * len(representatives)
        * sum(
            ordinary_warmup_duration + duration
            for duration in measurement_study_durations
        )
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
        * measurement_calibration_bootstrap_work
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
    ordinary_per_replication_horizon = (
        ordinary_warmup_duration + measurement_duration_ceiling
    )
    ordinary_optimization_simulation_work = (
        ordinary_replica_configuration_count
        * options.minimum_optimization_replications
        * ordinary_per_replication_horizon
    )
    ordinary_optimization_bootstrap_work = (
        ordinary_replica_configuration_count
        * len(system.get_request_classes())
        * measurement_duration_ceiling
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
    ordinary_final_seconds = (
        _parallel_runtime(
            rates.measurement_seconds_per_replication_time_unit,
            ordinary_final_simulation_work,
            workers,
        )
        + rates.bootstrap_seconds_per_replication_time_unit_per_class
        * final_bootstrap_work
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


def _format_duration_range(low, high):
    if low is None or high is None:
        return "unavailable"
    if math.isclose(low, high, rel_tol=0.01, abs_tol=0.05):
        return format_duration(high)
    return f"{format_duration(low)}–{format_duration(high)}"


def format_live_runtime_snapshot(snapshot):
    parts = [f"[{snapshot.stage}] {snapshot.event.replace('_', ' ')}"]
    if snapshot.total_units is not None:
        parts.append(
            f"{snapshot.completed_units}/{snapshot.total_units} "
            f"{snapshot.unit_name or 'units'}"
        )
    parts.append(f"elapsed {format_duration(snapshot.stage_elapsed_seconds)}")
    if not snapshot.completed:
        parts.append(
            "stage remaining "
            + _format_duration_range(
                snapshot.stage_remaining_low_seconds,
                snapshot.stage_remaining_high_seconds,
            )
        )
    parts.append(
        "overall remaining "
        + _format_duration_range(
            snapshot.overall_remaining_low_seconds,
            snapshot.overall_remaining_high_seconds,
        )
    )
    if snapshot.detail:
        parts.append(snapshot.detail)
    return " | ".join(parts)


def format_runtime_estimate(estimate):
    lines = [
        "Conservative workflow runtime speculation",
        (
            "  Local preflight benchmark: "
            f"{format_duration(estimate.benchmark.benchmark_wall_time_seconds)}; "
            f"safety factor: {estimate.safety_factor:g}x"
        ),
        (
            "  Resolved workflow search mode: "
            f"{estimate.workflow_search_mode}; replica "
            f"{estimate.replica_search_mode}; placement-at-maximum-bounds "
            f"{estimate.placement_search_mode_at_maximum_bounds}."
        ),
    ]
    for stage in estimate.stages:
        lines.append(f"  {stage.stage}:")
        lines.append(
            "    Ordinary planning: about "
            f"{format_duration(stage.ordinary_planning_seconds)} "
            f"({stage.ordinary_work})."
        )
        lines.append(
            "    Configured ceiling: about "
            f"{format_duration(stage.configured_ceiling_seconds)} "
            f"({stage.ceiling_work})."
        )
    lines.extend(
        (
            "  Total ordinary planning speculation: about "
            + format_duration(estimate.total_ordinary_planning_seconds),
            "  Total configured-work ceiling speculation: about "
            + format_duration(estimate.total_configured_ceiling_seconds),
            "  " + estimate.stochastic_warning,
            (
                "  Normal runs should be much shorter because calibration usually "
                "accepts early, sequential evaluation stops early, a feasible replica "
                "configuration is normally found near the stable anchor, and placement "
                "branches are pruned."
            ),
        )
    )
    return "\n".join(lines)

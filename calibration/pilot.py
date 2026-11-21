import math
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from metrics import SimulationMetricsCollector, time_weighted_bucket_averages
from metrics.time_weighted_buckets import complete_bucket_count

from .ensemble import warmup_ensemble_from_native
from performance_models import QueueingNetworkType, System
from performance_models.components import simulation_configuration_supported


@dataclass(frozen=True)
class WarmupPilotReplication:
    """State histories and bucket series produced by one empty-start run."""

    replication_id: int
    seed: int
    service_names: tuple[str, ...]
    replica_configuration: tuple[int, ...]
    pilot_horizon: float
    bucket_width: float
    active_history: tuple[tuple[float, int], ...]
    queue_history_by_service: tuple[tuple[tuple[float, int], ...], ...]
    busy_history_by_service: tuple[tuple[tuple[float, int], ...], ...]
    bucketed_active_requests: tuple[float, ...]
    bucketed_queue_length_by_service: tuple[tuple[float, ...], ...]
    bucketed_busy_replicas_by_service: tuple[tuple[float, ...], ...]
    bucketed_number_in_system_by_service: tuple[tuple[float, ...], ...]
    bucketed_utilization_by_service: tuple[tuple[float, ...], ...]

    @property
    def bucket_count(self):
        return len(self.bucketed_active_requests)


@dataclass(frozen=True)
class _WarmupReplicationTask:
    system_name: str
    workload_spec: object
    system_type: str
    parameters: object
    replica_configuration: tuple[int, ...]
    replication_id: int
    seed: int
    bucket_width: float


def _validate_system_matches_parameters(system, parameters):
    services = tuple(
        sorted(system.get_services().values(), key=lambda service: service.get_id())
    )
    names = tuple(service.get_name() for service in services)
    if names != parameters.service_names:
        raise ValueError("Pilot parameters do not match the system service order.")


def _build_replication(
    *,
    replication_id,
    seed,
    service_names,
    replica_configuration,
    pilot_horizon,
    bucket_width,
    collector,
):
    active_history = collector.get_active_history()
    queue_histories = collector.get_queue_history()
    busy_histories = collector.get_busy_history()
    queue_history_by_service = tuple(
        queue_histories[service_id] for service_id in range(len(service_names))
    )
    busy_history_by_service = tuple(
        busy_histories[service_id] for service_id in range(len(service_names))
    )

    active_buckets = tuple(
        time_weighted_bucket_averages(
            active_history,
            bucket_width,
            pilot_horizon,
        )
    )
    queue_buckets = tuple(
        tuple(time_weighted_bucket_averages(history, bucket_width, pilot_horizon))
        for history in queue_history_by_service
    )
    busy_buckets = tuple(
        tuple(time_weighted_bucket_averages(history, bucket_width, pilot_horizon))
        for history in busy_history_by_service
    )
    number_in_system_buckets = tuple(
        tuple(queue + busy for queue, busy in zip(queue_series, busy_series))
        for queue_series, busy_series in zip(queue_buckets, busy_buckets)
    )
    utilization_buckets = tuple(
        tuple(busy / capacity for busy in busy_series)
        for busy_series, capacity in zip(busy_buckets, replica_configuration)
    )

    expected_bucket_count = complete_bucket_count(pilot_horizon, bucket_width)
    all_series = (
        (active_buckets,)
        + queue_buckets
        + busy_buckets
        + number_in_system_buckets
        + utilization_buckets
    )
    if any(len(series) != expected_bucket_count for series in all_series):
        raise RuntimeError("Pilot replication produced inconsistent bucket counts.")

    return WarmupPilotReplication(
        replication_id=replication_id,
        seed=seed,
        service_names=service_names,
        replica_configuration=replica_configuration,
        pilot_horizon=pilot_horizon,
        bucket_width=bucket_width,
        active_history=active_history,
        queue_history_by_service=queue_history_by_service,
        busy_history_by_service=busy_history_by_service,
        bucketed_active_requests=active_buckets,
        bucketed_queue_length_by_service=queue_buckets,
        bucketed_busy_replicas_by_service=busy_buckets,
        bucketed_number_in_system_by_service=number_in_system_buckets,
        bucketed_utilization_by_service=utilization_buckets,
    )


def _run_configured_replication(
    system,
    parameters,
    *,
    replication_id,
    seed,
    bucket_width,
    analytical_metrics,
    replica_configuration,
):
    if not isinstance(replication_id, int) or replication_id < 0:
        raise ValueError("replication_id must be a non-negative integer.")
    if not isinstance(seed, int) or seed < 0:
        raise ValueError("seed must be a non-negative integer.")
    if not math.isfinite(bucket_width) or bucket_width <= 0:
        raise ValueError("bucket_width must be finite and greater than zero.")

    collector = SimulationMetricsCollector(len(parameters.service_names))
    system.simulate(
        parameters.pilot_horizon,
        analytical_metrics=analytical_metrics,
        metrics_collector=collector,
        seed=seed,
    )

    return _build_replication(
        replication_id=replication_id,
        seed=seed,
        service_names=parameters.service_names,
        replica_configuration=replica_configuration,
        pilot_horizon=parameters.pilot_horizon,
        bucket_width=bucket_width,
        collector=collector,
    )


def _run_replication_worker(task):
    """Construct an isolated simulator and execute exactly one replication."""
    system = System(
        task.system_name,
        task.workload_spec,
        QueueingNetworkType(task.system_type),
    )
    system.configure_system_replicas(task.replica_configuration)
    analytical_metrics = system.solve()
    if not simulation_configuration_supported(system, analytical_metrics):
        raise ValueError("Warm-up pilots require a stable replica configuration.")
    return _run_configured_replication(
        system,
        task.parameters,
        replication_id=task.replication_id,
        seed=task.seed,
        bucket_width=task.bucket_width,
        analytical_metrics=analytical_metrics,
        replica_configuration=task.replica_configuration,
    )


def run_warmup_pilot_replication(
    system,
    parameters,
    *,
    replication_id=0,
    seed=None,
    bucket_width=None,
):
    """Run one reproducible empty-start pilot and restore system configuration."""
    _validate_system_matches_parameters(system, parameters)
    if seed is None:
        seed = parameters.calibration_seeds[replication_id]
    if bucket_width is None:
        bucket_width = parameters.initial_bucket_width

    original_configuration = tuple(system.get_system_replicas())
    try:
        system.configure_system_replicas(parameters.minimum_replica_configuration)
        analytical_metrics = system.solve()
        if not simulation_configuration_supported(system, analytical_metrics):
            raise ValueError("Warm-up pilots require a stable replica configuration.")
        return _run_configured_replication(
            system,
            parameters,
            replication_id=replication_id,
            seed=seed,
            bucket_width=bucket_width,
            analytical_metrics=analytical_metrics,
            replica_configuration=parameters.minimum_replica_configuration,
        )
    finally:
        system.configure_system_replicas(original_configuration)


def run_warmup_pilot_replications(
    system,
    parameters,
    *,
    bucket_width=None,
    workers=1,
    aggregate=False,
):
    """Run the configured independent pilot seed set in deterministic order."""
    return run_warmup_replications_for_configuration(
        system,
        parameters,
        parameters.minimum_replica_configuration,
        parameters.calibration_seeds,
        bucket_width=bucket_width,
        workers=workers,
        aggregate=aggregate,
    )


def run_warmup_replications_for_configuration(
    system,
    parameters,
    replica_configuration,
    seeds,
    *,
    bucket_width=None,
    replication_id_start=0,
    workers=1,
    aggregate=False,
):
    """Run independent empty-start pilots for one explicit configuration.

    This is also used for fresh-seed validation. The system's pre-existing
    replica configuration and the caller's global random state are restored.
    """
    _validate_system_matches_parameters(system, parameters)
    replica_configuration = tuple(replica_configuration)
    seeds = tuple(seeds)
    service_count = len(parameters.service_names)
    if len(replica_configuration) != service_count:
        raise ValueError("A replica count is required for every service.")
    if any(not isinstance(count, int) or count <= 0 for count in replica_configuration):
        raise ValueError("Replica counts must be positive integers.")
    if not seeds:
        raise ValueError("At least one independent seed is required.")
    if any(not isinstance(seed, int) or seed < 0 for seed in seeds):
        raise ValueError("Seeds must be non-negative integers.")
    if len(set(seeds)) != len(seeds):
        raise ValueError("Seeds must be unique within a replication batch.")
    if not isinstance(replication_id_start, int) or replication_id_start < 0:
        raise ValueError("replication_id_start must be a non-negative integer.")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer.")
    if bucket_width is None:
        bucket_width = parameters.initial_bucket_width
    if not math.isfinite(bucket_width) or bucket_width <= 0:
        raise ValueError("bucket_width must be finite and greater than zero.")
    if not isinstance(aggregate, bool):
        raise ValueError("aggregate must be boolean.")

    original_configuration = tuple(system.get_system_replicas())
    if aggregate:
        try:
            system.configure_system_replicas(replica_configuration)
            native_result = system.run_warmup_ensemble(
                seeds=seeds,
                horizon=parameters.pilot_horizon,
                bucket_width=bucket_width,
                workers=workers,
            )
            return warmup_ensemble_from_native(native_result)
        finally:
            system.configure_system_replicas(original_configuration)
    if workers > 1:
        try:
            system.configure_system_replicas(replica_configuration)
            if not simulation_configuration_supported(system):
                raise ValueError(
                    "Warm-up pilots require a stable replica configuration."
                )
        finally:
            system.configure_system_replicas(original_configuration)

        workload_spec = system.get_workload_spec()
        tasks = tuple(
            _WarmupReplicationTask(
                system_name=system.get_name(),
                workload_spec=workload_spec,
                system_type=system.get_type().value,
                parameters=parameters,
                replica_configuration=replica_configuration,
                replication_id=replication_id_start + offset,
                seed=seed,
                bucket_width=bucket_width,
            )
            for offset, seed in enumerate(seeds)
        )
        with ProcessPoolExecutor(max_workers=min(workers, len(tasks))) as executor:
            return tuple(executor.map(_run_replication_worker, tasks))

    results = []
    try:
        system.configure_system_replicas(replica_configuration)
        analytical_metrics = system.solve()
        if not simulation_configuration_supported(system, analytical_metrics):
            raise ValueError("Warm-up pilots require a stable replica configuration.")

        for offset, seed in enumerate(seeds):
            results.append(
                _run_configured_replication(
                    system,
                    parameters,
                    replication_id=replication_id_start + offset,
                    seed=seed,
                    bucket_width=bucket_width,
                    analytical_metrics=analytical_metrics,
                    replica_configuration=replica_configuration,
                )
            )
    finally:
        system.configure_system_replicas(original_configuration)

    return tuple(results)


def _history_prefix(history, pilot_horizon):
    """Return state changes through H using the histories' timestamp ordering."""
    lower = 0
    upper = len(history)
    while lower < upper:
        middle = (lower + upper) // 2
        if history[middle][0] <= pilot_horizon:
            lower = middle + 1
        else:
            upper = middle
    return history[:lower]


def slice_warmup_replication(
    replication,
    pilot_horizon,
    *,
    bucket_width=None,
):
    """Derive an exact shorter-horizon replication without rerunning the kernel."""
    if not math.isfinite(pilot_horizon) or pilot_horizon <= 0:
        raise ValueError("pilot_horizon must be finite and greater than zero.")
    if pilot_horizon > replication.pilot_horizon and not math.isclose(
        pilot_horizon,
        replication.pilot_horizon,
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        raise ValueError("pilot_horizon cannot exceed the simulated horizon.")
    pilot_horizon = min(float(pilot_horizon), replication.pilot_horizon)
    if bucket_width is None:
        bucket_width = replication.bucket_width
    if not math.isfinite(bucket_width) or bucket_width <= 0:
        raise ValueError("bucket_width must be finite and greater than zero.")

    active_history = _history_prefix(replication.active_history, pilot_horizon)
    queue_histories = tuple(
        _history_prefix(history, pilot_horizon)
        for history in replication.queue_history_by_service
    )
    busy_histories = tuple(
        _history_prefix(history, pilot_horizon)
        for history in replication.busy_history_by_service
    )

    class CollectorView:
        def get_active_history(self):
            return active_history

        def get_queue_history(self):
            return dict(enumerate(queue_histories))

        def get_busy_history(self):
            return dict(enumerate(busy_histories))

    return _build_replication(
        replication_id=replication.replication_id,
        seed=replication.seed,
        service_names=replication.service_names,
        replica_configuration=replication.replica_configuration,
        pilot_horizon=pilot_horizon,
        bucket_width=bucket_width,
        collector=CollectorView(),
    )


def rebucket_warmup_replication(replication, bucket_width):
    """Recompute buckets from retained event histories without rerunning the kernel."""
    if not math.isfinite(bucket_width) or bucket_width <= 0:
        raise ValueError("bucket_width must be finite and greater than zero.")
    return slice_warmup_replication(
        replication,
        replication.pilot_horizon,
        bucket_width=bucket_width,
    )

from dataclasses import dataclass


@dataclass(frozen=True)
class WarmupEnsembleSeries:
    """Corresponding-bucket averages across independent replications."""

    service_names: tuple[str, ...]
    replica_configuration: tuple[int, ...]
    pilot_horizon: float
    bucket_width: float
    seeds: tuple[int, ...]
    active_requests: tuple[float, ...]
    queue_length_by_service: tuple[tuple[float, ...], ...]
    busy_replicas_by_service: tuple[tuple[float, ...], ...]
    number_in_system_by_service: tuple[tuple[float, ...], ...]
    utilization_by_service: tuple[tuple[float, ...], ...]

    @property
    def bucket_count(self):
        return len(self.active_requests)

    def important_metric_series(self):
        series = {"active_requests": self.active_requests}
        for service_id, service_name in enumerate(self.service_names):
            series[f"queue_length[{service_name}]"] = self.queue_length_by_service[
                service_id
            ]
            series[f"number_in_system[{service_name}]"] = (
                self.number_in_system_by_service[service_id]
            )
            series[f"utilization[{service_name}]"] = self.utilization_by_service[
                service_id
            ]
        return series


def _mean_corresponding_series(series_collection):
    series_collection = tuple(series_collection)
    length = len(series_collection[0])
    if any(len(series) != length for series in series_collection):
        raise ValueError("All replication series must have equal length.")
    count = len(series_collection)
    return tuple(
        sum(series[index] for series in series_collection) / count
        for index in range(length)
    )


def average_warmup_replications(replications):
    """Average equal-length pilot buckets across independent replications."""
    replications = tuple(replications)
    if not replications:
        raise ValueError("At least one pilot replication is required.")

    first = replications[0]
    identity = (
        first.service_names,
        first.replica_configuration,
        first.pilot_horizon,
        first.bucket_width,
        first.bucket_count,
    )
    if any(
        (
            replication.service_names,
            replication.replica_configuration,
            replication.pilot_horizon,
            replication.bucket_width,
            replication.bucket_count,
        )
        != identity
        for replication in replications[1:]
    ):
        raise ValueError("Pilot replications must share configuration, horizon, and buckets.")
    seeds = tuple(replication.seed for replication in replications)
    if len(set(seeds)) != len(seeds):
        raise ValueError("Pilot replication seeds must be unique.")

    def average_service_field(field_name):
        return tuple(
            _mean_corresponding_series(
                getattr(replication, field_name)[service_id]
                for replication in replications
            )
            for service_id in range(len(first.service_names))
        )

    return WarmupEnsembleSeries(
        service_names=first.service_names,
        replica_configuration=first.replica_configuration,
        pilot_horizon=first.pilot_horizon,
        bucket_width=first.bucket_width,
        seeds=seeds,
        active_requests=_mean_corresponding_series(
            replication.bucketed_active_requests for replication in replications
        ),
        queue_length_by_service=average_service_field(
            "bucketed_queue_length_by_service"
        ),
        busy_replicas_by_service=average_service_field(
            "bucketed_busy_replicas_by_service"
        ),
        number_in_system_by_service=average_service_field(
            "bucketed_number_in_system_by_service"
        ),
        utilization_by_service=average_service_field(
            "bucketed_utilization_by_service"
        ),
    )


def centered_moving_average(series, half_window):
    """Return a same-length centered Welch smoothing series."""
    series = tuple(series)
    if not isinstance(half_window, int) or half_window < 0:
        raise ValueError("half_window must be a non-negative integer.")
    if not series:
        return ()

    smoothed = []
    for index in range(len(series)):
        lower = max(0, index - half_window)
        upper = min(len(series), index + half_window + 1)
        smoothed.append(sum(series[lower:upper]) / (upper - lower))
    return tuple(smoothed)


def warmup_ensemble_from_native(native_result):
    """Convert native online bucket means to the calibration data contract."""
    queue = tuple(tuple(row) for row in native_result.queue_length_by_service)
    busy = tuple(tuple(row) for row in native_result.busy_replicas_by_service)
    number_in_system = tuple(
        tuple(queue_value + busy_value for queue_value, busy_value in zip(queue_row, busy_row))
        for queue_row, busy_row in zip(queue, busy)
    )
    utilization = tuple(
        tuple(value / capacity for value in busy_row)
        for busy_row, capacity in zip(busy, native_result.replica_configuration)
    )
    return WarmupEnsembleSeries(
        service_names=tuple(native_result.service_names),
        replica_configuration=tuple(native_result.replica_configuration),
        pilot_horizon=float(native_result.horizon),
        bucket_width=float(native_result.bucket_width),
        seeds=tuple(native_result.seeds),
        active_requests=tuple(native_result.active_requests),
        queue_length_by_service=queue,
        busy_replicas_by_service=busy,
        number_in_system_by_service=number_in_system,
        utilization_by_service=utilization,
    )


def slice_and_rebucket_warmup_ensemble(ensemble, pilot_horizon, bucket_width):
    """Derive an exact aligned prefix/coarsening from finest native means."""
    pilot_horizon = float(pilot_horizon)
    bucket_width = float(bucket_width)
    if pilot_horizon <= 0 or pilot_horizon > ensemble.pilot_horizon:
        raise ValueError("pilot_horizon must be positive and within the native run.")
    ratio = bucket_width / ensemble.bucket_width
    factor = round(ratio)
    if factor < 1 or abs(ratio - factor) > 1e-12 * max(1.0, abs(ratio)):
        raise ValueError("Target bucket width must be an integer multiple of native width.")
    bucket_count = round(pilot_horizon / bucket_width)
    if abs(bucket_count * bucket_width - pilot_horizon) > 1e-12 * max(1.0, pilot_horizon):
        raise ValueError("pilot_horizon must align with target bucket_width.")
    fine_count = bucket_count * factor
    if fine_count > ensemble.bucket_count:
        raise ValueError("Native ensemble does not contain the requested prefix.")

    def coarsen(series):
        return tuple(
            sum(series[index:index + factor]) / factor
            for index in range(0, fine_count, factor)
        )

    queue = tuple(coarsen(row) for row in ensemble.queue_length_by_service)
    busy = tuple(coarsen(row) for row in ensemble.busy_replicas_by_service)
    number_in_system = tuple(
        tuple(queue_value + busy_value for queue_value, busy_value in zip(queue_row, busy_row))
        for queue_row, busy_row in zip(queue, busy)
    )
    utilization = tuple(
        tuple(value / capacity for value in busy_row)
        for busy_row, capacity in zip(busy, ensemble.replica_configuration)
    )
    return WarmupEnsembleSeries(
        service_names=ensemble.service_names,
        replica_configuration=ensemble.replica_configuration,
        pilot_horizon=pilot_horizon,
        bucket_width=bucket_width,
        seeds=ensemble.seeds,
        active_requests=coarsen(ensemble.active_requests),
        queue_length_by_service=queue,
        busy_replicas_by_service=busy,
        number_in_system_by_service=number_in_system,
        utilization_by_service=utilization,
    )

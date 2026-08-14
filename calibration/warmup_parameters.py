import math
from dataclasses import dataclass

import numpy as np

from performance_models import QueueingNetworkType


@dataclass(frozen=True)
class WarmupPilotParameters:
    """Reproducible inputs selected before warm-up pilot execution."""

    time_unit: str
    service_names: tuple[str, ...]
    request_class_names: tuple[str, ...]
    external_arrival_rates_by_class: tuple[tuple[float, ...], ...]
    routing_matrices_by_class: tuple[tuple[tuple[float, ...], ...], ...]
    effective_arrival_rates_by_class: tuple[tuple[float, ...], ...]
    routing_spectral_radii: tuple[float, ...]
    external_arrival_rates: tuple[float, ...]
    effective_arrival_rates: tuple[float, ...]
    service_rates: tuple[float, ...]
    minimum_replica_configuration: tuple[int, ...]
    minimum_configuration_utilizations: tuple[float, ...]
    capacity_slacks: tuple[float, ...]
    slowest_mean_service_time: float
    initial_bucket_width: float
    bucket_width_candidates: tuple[float, float, float]
    slowest_congestion_scale: float
    total_external_arrival_rate: float
    raw_pilot_horizon: float
    pilot_horizon: float
    bucket_counts: tuple[int, int, int]
    replication_count: int
    calibration_seeds: tuple[int, ...]


def _round_up_to_multiple(value, multiple):
    return math.ceil(value / multiple) * multiple


def select_warmup_pilot_parameters(
    system,
    *,
    time_unit="seconds",
    replication_count=30,
    seed_start=100_000,
    minimum_configuration=None,
):
    """Calculate the milestone-three bucket widths and initial pilot horizon.

    The calculation is side-effect free: it derives the minimum stable
    configuration without applying it to ``system``. The pilot horizon is
    rounded up to the largest candidate bucket width, making it an exact number
    of buckets for all three width candidates.
    """
    if system.get_type() != QueueingNetworkType.OPEN:
        raise ValueError("Warm-up pilot parameters require an open queueing network.")
    if not isinstance(time_unit, str) or not time_unit.strip():
        raise ValueError("time_unit must be a non-empty string.")
    if not isinstance(replication_count, int) or replication_count <= 0:
        raise ValueError("replication_count must be a positive integer.")
    if not isinstance(seed_start, int) or seed_start < 0:
        raise ValueError("seed_start must be a non-negative integer.")

    services = tuple(
        sorted(system.get_services().values(), key=lambda service: service.get_id())
    )
    service_names = tuple(service.get_name() for service in services)
    request_class_names = tuple(system.get_request_classes())
    service_rates = tuple(float(service.get_mu()) for service in services)
    if any(not math.isfinite(rate) or rate <= 0 for rate in service_rates):
        raise ValueError("All service rates must be finite and greater than zero.")

    external_arrival_rates_by_class = tuple(
        tuple(float(service.get_gamma(request_class)) for service in services)
        for request_class in request_class_names
    )
    routing_matrices_by_class = tuple(
        tuple(
            tuple(
                sum(
                    edge.get_routing_prob()
                    for edge in service.get_adjacency_list(request_class)
                    if edge.get_target() is not None
                    and edge.get_target().get_id() == target.get_id()
                )
                for target in services
            )
            for service in services
        )
        for request_class in request_class_names
    )
    routing_spectral_radii = tuple(
        float(max(abs(value) for value in np.linalg.eigvals(matrix)))
        for matrix in routing_matrices_by_class
    )
    if any(radius >= 1.0 for radius in routing_spectral_radii):
        raise ValueError(
            "Open-network traffic equations require every request-class "
            "routing matrix to have spectral radius below one."
        )
    external_arrival_rates = tuple(
        sum(rates[service_id] for rates in external_arrival_rates_by_class)
        for service_id in range(len(services))
    )
    if any(
        not math.isfinite(arrival_rate) or arrival_rate < 0
        for arrival_rate in external_arrival_rates
    ):
        raise ValueError("External arrival rates must be finite and non-negative.")

    total_external_arrival_rate = sum(external_arrival_rates)
    if total_external_arrival_rate <= 0:
        raise ValueError("Warm-up calibration requires positive external traffic.")

    try:
        request_class_effective_rates = system.get_request_class_effective_arrival_rates()
        raw_effective_arrival_rates = system.get_effective_arrival_rates()
    except np.linalg.LinAlgError as error:
        raise ValueError(
            "Open-network traffic equations must have a finite unique solution."
        ) from error

    effective_arrival_rates = []
    for arrival_rate in raw_effective_arrival_rates:
        arrival_rate = float(arrival_rate)
        if not math.isfinite(arrival_rate) or arrival_rate < -1e-12:
            raise ValueError(
                "Effective arrival rates must be finite and non-negative."
            )
        effective_arrival_rates.append(max(0.0, arrival_rate))
    effective_arrival_rates = tuple(effective_arrival_rates)
    effective_arrival_rates_by_class = tuple(
        tuple(float(value) for value in request_class_effective_rates[request_class])
        for request_class in request_class_names
    )

    analytical_minimum = tuple(system.get_minimum_stable_replicas())
    minimum_replicas = (
        analytical_minimum
        if minimum_configuration is None
        else tuple(minimum_configuration)
    )
    if (
        len(minimum_replicas) != len(services)
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in minimum_replicas
        )
        or any(
            value < required
            for value, required in zip(minimum_replicas, analytical_minimum)
        )
    ):
        raise ValueError(
            "minimum_configuration must be at or above the analytical "
            "stability vector."
        )
    capacity_slacks = tuple(
        replicas * service_rate - arrival_rate
        for replicas, service_rate, arrival_rate in zip(
            minimum_replicas,
            service_rates,
            effective_arrival_rates,
        )
    )
    if any(not math.isfinite(slack) or slack <= 0 for slack in capacity_slacks):
        raise ValueError(
            "The minimum replica configuration must have positive capacity slack."
        )

    utilizations = tuple(
        arrival_rate / (replicas * service_rate)
        for arrival_rate, replicas, service_rate in zip(
            effective_arrival_rates,
            minimum_replicas,
            service_rates,
        )
    )
    if any(utilization >= 1.0 for utilization in utilizations):
        raise ValueError("The minimum replica configuration must be strictly stable.")

    slowest_mean_service_time = max(1.0 / rate for rate in service_rates)
    initial_bucket_width = 5.0 * slowest_mean_service_time
    bucket_width_candidates = (
        initial_bucket_width / 2.0,
        initial_bucket_width,
        initial_bucket_width * 2.0,
    )

    slowest_congestion_scale = max(1.0 / slack for slack in capacity_slacks)
    raw_pilot_horizon = max(
        100.0 * slowest_congestion_scale,
        1000.0 / total_external_arrival_rate,
        500.0 * initial_bucket_width,
    )
    pilot_horizon = _round_up_to_multiple(
        raw_pilot_horizon,
        max(bucket_width_candidates),
    )
    bucket_counts = tuple(
        int(round(pilot_horizon / bucket_width))
        for bucket_width in bucket_width_candidates
    )

    return WarmupPilotParameters(
        time_unit=time_unit.strip(),
        service_names=service_names,
        request_class_names=request_class_names,
        external_arrival_rates_by_class=external_arrival_rates_by_class,
        routing_matrices_by_class=routing_matrices_by_class,
        effective_arrival_rates_by_class=effective_arrival_rates_by_class,
        routing_spectral_radii=routing_spectral_radii,
        external_arrival_rates=external_arrival_rates,
        effective_arrival_rates=effective_arrival_rates,
        service_rates=service_rates,
        minimum_replica_configuration=minimum_replicas,
        minimum_configuration_utilizations=utilizations,
        capacity_slacks=capacity_slacks,
        slowest_mean_service_time=slowest_mean_service_time,
        initial_bucket_width=initial_bucket_width,
        bucket_width_candidates=bucket_width_candidates,
        slowest_congestion_scale=slowest_congestion_scale,
        total_external_arrival_rate=total_external_arrival_rate,
        raw_pilot_horizon=raw_pilot_horizon,
        pilot_horizon=pilot_horizon,
        bucket_counts=bucket_counts,
        replication_count=replication_count,
        calibration_seeds=tuple(
            range(seed_start, seed_start + replication_count)
        ),
    )

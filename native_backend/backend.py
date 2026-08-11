import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


PROTOCOL_VERSION = 2
SIMULATOR_SEMANTICS_VERSION = "native-event-kernel-cohort-v2"


class NativeBackendError(RuntimeError):
    """Raised when the mandatory native simulator cannot execute safely."""


@dataclass(frozen=True)
class NativeWarmupEnsemble:
    service_names: tuple[str, ...]
    replica_configuration: tuple[int, ...]
    horizon: float
    bucket_width: float
    seeds: tuple[int, ...]
    active_requests: tuple[float, ...]
    queue_length_by_service: tuple[tuple[float, ...], ...]
    busy_replicas_by_service: tuple[tuple[float, ...], ...]
    active_requests_sample_variance: tuple[float, ...]
    queue_length_sample_variance_by_service: tuple[tuple[float, ...], ...]
    busy_replicas_sample_variance_by_service: tuple[tuple[float, ...], ...]
    processed_event_count: int
    external_arrival_count: int
    completed_request_count: int
    wall_time_seconds: float


@dataclass(frozen=True)
class NativeMeasurementReplication:
    seed: int
    replica_configuration: tuple[int, ...]
    warmup_duration: float
    measurement_duration: float
    stop_time: float
    measured_request_counts: dict[str, int]
    completed_measured_request_counts: dict[str, int]
    request_class_p95_latencies: dict[str, float]
    request_class_latencies: dict[str, tuple[float, ...]]
    request_class_arrival_times: dict[str, tuple[float, ...]]
    processed_event_count: int
    external_arrival_count: int
    completed_request_count: int
    active_requests_at_end: int
    queue_lengths_at_end: tuple[int, ...]
    busy_replicas_at_end: tuple[int, ...]
    tagged_requests_in_flight_at_end: int
    maximum_drain_time: float


def _finite_positive(value, label):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be finite and greater than zero.")
    return value



def _normalize_seeds(values, label):
    seeds = tuple(values)
    if (
        not seeds
        or any(
            isinstance(seed, bool)
            or not isinstance(seed, int)
            or not 0 <= seed <= 2**63 - 1
            for seed in seeds
        )
        or len(set(seeds)) != len(seeds)
    ):
        raise ValueError(f"{label} must be unique signed-64-bit non-negative integers.")
    return seeds

def _numeric_tuple(values, label, *, length=None, non_negative=True):
    if not isinstance(values, (list, tuple)):
        raise ValueError(f"{label} must be an array.")
    result = tuple(float(value) for value in values)
    if length is not None and len(result) != length:
        raise ValueError(f"{label} has the wrong length.")
    if any(not math.isfinite(value) for value in result):
        raise ValueError(f"{label} contains a non-finite value.")
    if non_negative and any(value < 0 for value in result):
        raise ValueError(f"{label} contains a negative value.")
    return result


def _numeric_matrix(values, label, *, rows, columns):
    if not isinstance(values, (list, tuple)) or len(values) != rows:
        raise ValueError(f"{label} has the wrong service count.")
    return tuple(
        _numeric_tuple(row, f"{label}[{index}]", length=columns)
        for index, row in enumerate(values)
    )


class CppSimulationBackend:
    """Versioned subprocess adapter for the required native event kernel."""

    def __init__(self, binary_path=None):
        configured_path = binary_path or os.environ.get("KUBESIZER_CPP_BACKEND")
        self.binary_path = Path(configured_path) if configured_path else (
            Path(__file__).resolve().parent
            / "cpp"
            / "build-release"
            / "kubesizer-simulator"
        )

    def ensure_available(self):
        if not self.binary_path.is_file() or not os.access(self.binary_path, os.X_OK):
            build_script = Path(__file__).resolve().parent / "cpp" / "build.sh"
            raise NativeBackendError(
                "The mandatory C++ simulator is not built. Run "
                f"`{build_script}` before using System.simulate(). Expected "
                f"binary: {self.binary_path}"
            )

    @staticmethod
    def serialize_system(system):
        services = tuple(
            sorted(system.get_services().values(), key=lambda item: item.get_id())
        )
        request_classes = []
        for request_class in system.get_request_classes():
            routing = []
            for service in services:
                routes = []
                for edge in service.get_adjacency_list(request_class):
                    target = edge.get_target()
                    routes.append(
                        {
                            "target": -1 if target is None else target.get_id(),
                            "probability": edge.get_routing_prob(),
                        }
                    )
                routing.append(routes)
            request_classes.append({"name": request_class, "routing": routing})

        external_streams = []
        for service in services:
            for class_id, request_class in enumerate(system.get_request_classes()):
                arrival_rate = service.get_gamma(request_class)
                if arrival_rate > 0:
                    external_streams.append(
                        {
                            "request_class": class_id,
                            "entry_service": service.get_id(),
                            "arrival_rate": arrival_rate,
                        }
                    )

        return {
            "name": system.get_name(),
            "services": [
                {
                    "name": service.get_name(),
                    "capacity": service.get_replicas(),
                    "service_rate": service.get_mu(),
                }
                for service in services
            ],
            "request_classes": request_classes,
            "external_streams": external_streams,
        }

    def _invoke(self, operation, system, **arguments):
        self.ensure_available()
        request = {
            "protocol_version": PROTOCOL_VERSION,
            "operation": operation,
            "workload": self.serialize_system(system),
            **arguments,
        }
        try:
            completed = subprocess.run(
                [str(self.binary_path)],
                input=json.dumps(request, separators=(",", ":")),
                text=True,
                capture_output=True,
                check=False,
            )
        except OSError as error:
            raise NativeBackendError(
                f"Could not execute the mandatory native simulator: {error}"
            ) from error
        response = None
        if completed.stdout.strip():
            try:
                response = json.loads(completed.stdout)
            except json.JSONDecodeError as error:
                raise NativeBackendError(
                    "The native simulator returned malformed JSON: "
                    f"{completed.stdout[:300]!r}"
                ) from error
        if completed.returncode != 0 or not isinstance(response, dict):
            detail = completed.stderr.strip() or "no structured error was returned"
            if isinstance(response, dict):
                detail = response.get("error", {}).get("message", detail)
            raise NativeBackendError(f"Native simulation failed: {detail}")
        if response.get("protocol_version") != PROTOCOL_VERSION:
            raise NativeBackendError("Native simulator protocol version mismatch.")
        if response.get("operation") != operation or response.get("ok") is not True:
            detail = response.get("error", {}).get("message", "invalid response")
            raise NativeBackendError(f"Native simulation failed: {detail}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise NativeBackendError(
                f"Native {operation} response has no valid result object."
            )
        return result

    def simulate(self, system, *, seed, horizon, warmup_duration, capture_state):
        horizon = _finite_positive(horizon, "horizon")
        warmup_duration = float(warmup_duration)
        if not math.isfinite(warmup_duration) or not 0 <= warmup_duration <= horizon:
            raise ValueError("warmup_duration must lie within the simulation.")
        result = self._invoke(
            "simulate",
            system,
            seed=int(seed),
            horizon=horizon,
            warmup_duration=warmup_duration,
            capture_state=bool(capture_state),
        )
        service_count = len(system.get_services())
        try:
            metrics = result["metrics"]
            for field in (
                "mean_utilizations",
                "mean_latencies",
                "p95_latencies",
                "p99_latencies",
            ):
                _numeric_tuple(metrics[field], f"metrics.{field}", length=service_count)
            if not isinstance(metrics["request_class_p95_latencies"], dict):
                raise ValueError("request-class p95 values must be an object.")
            expected_classes = set(system.get_request_classes())
            if not set(metrics["request_class_p95_latencies"]).issubset(expected_classes):
                raise ValueError("response contains an unknown request class.")
            if int(result["seed"]) != int(seed):
                raise ValueError("response seed does not match request.")
            if len(result["queue_lengths_at_end"]) != service_count:
                raise ValueError("final queue vector has the wrong length.")
            if len(result["busy_replicas_at_end"]) != service_count:
                raise ValueError("final busy vector has the wrong length.")
            if capture_state and any(
                result[field] is None
                for field in ("active_history", "queue_history", "busy_history")
            ):
                raise ValueError("captured state histories are missing.")
        except (KeyError, TypeError, ValueError) as error:
            raise NativeBackendError(
                f"Native simulate response violates protocol schema: {error}"
            ) from error
        return result

    def warmup_ensemble(self, system, *, seeds, horizon, bucket_width, workers):
        seeds = _normalize_seeds(seeds, "Warm-up seeds")
        horizon = _finite_positive(horizon, "horizon")
        bucket_width = _finite_positive(bucket_width, "bucket_width")
        if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
            raise ValueError("workers must be a positive integer.")
        result = self._invoke(
            "warmup_ensemble",
            system,
            seeds=seeds,
            horizon=horizon,
            bucket_width=bucket_width,
            workers=workers,
        )
        try:
            means = result["means"]
            variances = result["sample_variances"]
            bucket_count = math.floor(horizon / bucket_width + 1e-12)
            service_count = len(system.get_services())
            active = _numeric_tuple(
                means["active_requests"],
                "means.active_requests",
                length=bucket_count,
            )
            queue = _numeric_matrix(
                means["queue_length_by_service"],
                "means.queue_length_by_service",
                rows=service_count,
                columns=bucket_count,
            )
            busy = _numeric_matrix(
                means["busy_replicas_by_service"],
                "means.busy_replicas_by_service",
                rows=service_count,
                columns=bucket_count,
            )
            active_variance = _numeric_tuple(
                variances["active_requests"],
                "sample_variances.active_requests",
                length=bucket_count,
            )
            queue_variance = _numeric_matrix(
                variances["queue_length_by_service"],
                "sample_variances.queue_length_by_service",
                rows=service_count,
                columns=bucket_count,
            )
            busy_variance = _numeric_matrix(
                variances["busy_replicas_by_service"],
                "sample_variances.busy_replicas_by_service",
                rows=service_count,
                columns=bucket_count,
            )
            native_seeds = tuple(int(value) for value in result["seeds"])
            configuration = tuple(int(value) for value in result["replica_configuration"])
            expected_configuration = tuple(system.get_system_replicas())
            if native_seeds != seeds or configuration != expected_configuration:
                raise ValueError("native batch identity does not match request.")
            native_result = NativeWarmupEnsemble(
                service_names=tuple(result["service_names"]),
                replica_configuration=configuration,
                horizon=float(result["horizon"]),
                bucket_width=float(result["bucket_width"]),
                seeds=native_seeds,
                active_requests=active,
                queue_length_by_service=queue,
                busy_replicas_by_service=busy,
                active_requests_sample_variance=active_variance,
                queue_length_sample_variance_by_service=queue_variance,
                busy_replicas_sample_variance_by_service=busy_variance,
                processed_event_count=int(result["processed_event_count"]),
                external_arrival_count=int(result["external_arrival_count"]),
                completed_request_count=int(result["completed_request_count"]),
                wall_time_seconds=float(result["wall_time_seconds"]),
            )
            if not math.isclose(native_result.horizon, horizon) or not math.isclose(
                native_result.bucket_width, bucket_width
            ):
                raise ValueError("native horizon or bucket width does not match request.")
        except (KeyError, TypeError, ValueError) as error:
            raise NativeBackendError(
                f"Native warmup_ensemble response violates protocol schema: {error}"
            ) from error
        return native_result

    def measurement_replications(
        self,
        system,
        *,
        seeds,
        warmup_duration,
        measurement_duration,
        workers,
        maximum_drain_time=10_000.0,
    ):
        seeds = _normalize_seeds(seeds, "Measurement seeds")
        warmup_duration = float(warmup_duration)
        if not math.isfinite(warmup_duration) or warmup_duration < 0:
            raise ValueError("warmup_duration must be finite and non-negative.")
        measurement_duration = _finite_positive(
            measurement_duration, "measurement_duration"
        )
        maximum_drain_time = _finite_positive(
            maximum_drain_time, "maximum_drain_time"
        )
        if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
            raise ValueError("workers must be a positive integer.")
        result = self._invoke(
            "measurement_replications",
            system,
            seeds=seeds,
            warmup_duration=warmup_duration,
            measurement_duration=measurement_duration,
            maximum_drain_time=maximum_drain_time,
            workers=workers,
        )
        try:
            configuration = tuple(int(value) for value in result["replica_configuration"])
            expected_configuration = tuple(system.get_system_replicas())
            if configuration != expected_configuration:
                raise ValueError("native configuration does not match request.")
            raw_replications = result["replications"]
            if len(raw_replications) != len(seeds):
                raise ValueError("native replication count does not match request.")
            expected_classes = set(system.get_request_classes())
            replications = []
            for expected_seed, item in zip(seeds, raw_replications):
                measured = {
                    key: int(value)
                    for key, value in item["measured_request_counts"].items()
                }
                completed_measured = {
                    key: int(value)
                    for key, value in item["completed_measured_request_counts"].items()
                }
                p95 = {
                    key: float(value)
                    for key, value in item["request_class_p95_latencies"].items()
                }
                latencies = {
                    key: _numeric_tuple(
                        value, f"request_class_latencies[{key}]"
                    )
                    for key, value in item["request_class_latencies"].items()
                }
                arrival_times = {
                    key: _numeric_tuple(
                        value, f"request_class_arrival_times[{key}]"
                    )
                    for key, value in item["request_class_arrival_times"].items()
                }
                if (
                    set(measured) != expected_classes
                    or set(latencies) != expected_classes
                    or set(arrival_times) != expected_classes
                    or measured != completed_measured
                ):
                    raise ValueError("tagged request cohort was not drained completely.")
                if any(value < 0 for value in measured.values()):
                    raise ValueError("measured request count is negative.")
                for request_class in expected_classes:
                    if not (
                        measured[request_class]
                        == len(latencies[request_class])
                        == len(arrival_times[request_class])
                    ):
                        raise ValueError("raw latency-cluster size does not match count.")
                    if any(
                        arrival < warmup_duration
                        or arrival >= warmup_duration + measurement_duration
                        for arrival in arrival_times[request_class]
                    ):
                        raise ValueError("measurement arrival lies outside [W, W + M).")
                if set(p95) != {key for key, value in measured.items() if value > 0}:
                    raise ValueError("request-class p95 coverage does not match counts.")
                if any(not math.isfinite(value) or value < 0 for value in p95.values()):
                    raise ValueError("request-class p95 contains an invalid value.")
                for request_class, value in p95.items():
                    if not math.isclose(
                        value,
                        _percentile(latencies[request_class], 0.95),
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    ):
                        raise ValueError("native p95 does not match raw latency cluster.")
                seed_value = int(item["seed"])
                stop_time = float(item["stop_time"])
                if seed_value != expected_seed or not math.isfinite(stop_time):
                    raise ValueError("native replication identity is invalid.")
                if stop_time < warmup_duration + measurement_duration:
                    raise ValueError("native replication stopped before measurement end.")
                queue_lengths = tuple(int(value) for value in item["queue_lengths_at_end"])
                busy_replicas = tuple(int(value) for value in item["busy_replicas_at_end"])
                active_requests = int(item["active_requests_at_end"])
                tagged_in_flight = int(item["tagged_requests_in_flight_at_end"] )
                if (
                    active_requests < 0
                    or len(queue_lengths) != len(configuration)
                    or len(busy_replicas) != len(configuration)
                    or any(value < 0 for value in queue_lengths)
                    or any(
                        value < 0 or value > capacity
                        for value, capacity in zip(busy_replicas, configuration)
                    )
                    or tagged_in_flight != 0
                ):
                    raise ValueError("native final-state diagnostics violate invariants.")
                replications.append(
                    NativeMeasurementReplication(
                        seed=seed_value,
                        replica_configuration=configuration,
                        warmup_duration=warmup_duration,
                        measurement_duration=measurement_duration,
                        stop_time=stop_time,
                        measured_request_counts=measured,
                        completed_measured_request_counts=completed_measured,
                        request_class_p95_latencies=p95,
                        request_class_latencies=latencies,
                        request_class_arrival_times=arrival_times,
                        processed_event_count=int(item["processed_event_count"]),
                        external_arrival_count=int(item["external_arrival_count"]),
                        completed_request_count=int(item["completed_request_count"]),
                        active_requests_at_end=active_requests,
                        queue_lengths_at_end=queue_lengths,
                        busy_replicas_at_end=busy_replicas,
                        tagged_requests_in_flight_at_end=tagged_in_flight,
                        maximum_drain_time=maximum_drain_time,
                    )
                )
        except (KeyError, TypeError, ValueError) as error:
            raise NativeBackendError(
                "Native measurement_replications response violates protocol "
                f"schema: {error}"
            ) from error
        return tuple(replications)


def _percentile(values, probability):
    values = sorted(float(value) for value in values)
    if not values:
        raise ValueError("A percentile requires at least one observation.")
    position = (len(values) - 1) * float(probability)
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def slice_measurement_replication(replication, measurement_duration):
    """Return an arrival-cohort prefix from a longer drained replication."""
    measurement_duration = _finite_positive(
        measurement_duration, "measurement_duration"
    )
    if measurement_duration > replication.measurement_duration:
        raise ValueError("Cannot slice beyond the simulated measurement duration.")
    boundary = replication.warmup_duration + measurement_duration
    latencies = {}
    arrival_times = {}
    for request_class, raw_latencies in replication.request_class_latencies.items():
        selected = tuple(
            (arrival, latency)
            for arrival, latency in zip(
                replication.request_class_arrival_times[request_class],
                raw_latencies,
            )
            if arrival < boundary
        )
        arrival_times[request_class] = tuple(item[0] for item in selected)
        latencies[request_class] = tuple(item[1] for item in selected)
    counts = {name: len(values) for name, values in latencies.items()}
    p95 = {
        name: _percentile(values, 0.95)
        for name, values in latencies.items()
        if values
    }
    return NativeMeasurementReplication(
        seed=replication.seed,
        replica_configuration=replication.replica_configuration,
        warmup_duration=replication.warmup_duration,
        measurement_duration=measurement_duration,
        stop_time=replication.stop_time,
        measured_request_counts=counts,
        completed_measured_request_counts=dict(counts),
        request_class_p95_latencies=p95,
        request_class_latencies=latencies,
        request_class_arrival_times=arrival_times,
        processed_event_count=replication.processed_event_count,
        external_arrival_count=replication.external_arrival_count,
        completed_request_count=replication.completed_request_count,
        active_requests_at_end=replication.active_requests_at_end,
        queue_lengths_at_end=replication.queue_lengths_at_end,
        busy_replicas_at_end=replication.busy_replicas_at_end,
        tagged_requests_in_flight_at_end=replication.tagged_requests_in_flight_at_end,
        maximum_drain_time=replication.maximum_drain_time,
    )


_BACKEND = CppSimulationBackend()


def get_native_backend():
    return _BACKEND

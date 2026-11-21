import hashlib
import json
import math
import os
import subprocess
from dataclasses import dataclass, field, replace
from pathlib import Path

from metrics.time_weighted_buckets import complete_bucket_count

PROTOCOL_VERSION = 18
SIMULATOR_SEMANTICS_VERSION = (
    "native-event-kernel-joint-input-blocks-burst-traces-concurrency-"
    "distributed-overhead-synchronous-calls-dual-resources-deadlines-cpu-"
    "ps-replica-queues-measured-latency-components-rate-preserving-traces-"
    "post-release-delays-source-policies-quorum-background-choice-shared-resource-stages-cpu-bandwidth-components-v21"
)


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
    failed_measured_request_counts: dict[str, int] = field(default_factory=dict)
    request_class_failures: dict[str, tuple[bool, ...]] = field(default_factory=dict)
    sampled_input_block_start: int | None = None
    input_arrival_rate_multipliers: dict[str, float] = field(default_factory=dict)
    input_service_time_multipliers: tuple[float, ...] = ()


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


def _serialize_distribution(specification):
    """Convert a validated workload distribution into protocol JSON."""
    parameters = dict(specification.parameters)
    result = {
        "distribution": specification.distribution,
        "mean": specification.mean,
        "scv": specification.scv,
    }
    for name in ("shape", "sampling", "block_duration"):
        if name in parameters:
            result[name] = parameters[name]
    if specification.distribution == "custom":
        library = Path(parameters["library"])
        digest = hashlib.sha256()
        try:
            with library.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError as error:
            raise NativeBackendError(
                f"Cannot read custom distribution library: {library}"
            ) from error
        if digest.hexdigest() != parameters["library_sha256"]:
            raise NativeBackendError(
                "Custom distribution library changed after workload parsing: "
                f"{library}"
            )
        result.update(
            {
                "library": parameters["library"],
                "library_sha256": parameters["library_sha256"],
                "symbol": parameters["symbol"],
                "abi_version": parameters["abi_version"],
                "parameters": list(parameters["parameters"]),
            }
        )
    if specification.samples:
        result["samples"] = list(specification.samples)
    if specification.sample_clusters:
        result["clusters"] = [
            list(cluster) for cluster in specification.sample_clusters
        ]
    return result


def serialize_stage_workflow(plan, system):
    """Stage nodes reference resource IDs; they do not allocate new queues."""
    indices = {name: i for i, name in enumerate(plan.stages)}
    return {
        "entries": [
            indices[plan.entries[name]] if name in plan.entries else -1
            for name in system.get_services()
        ],
        "stages": [
            {
                "name": name,
                "retry": (
                    {
                        **dict(stage.retry),
                        "backoff": (
                            _serialize_distribution(stage.retry["backoff"])
                            if stage.retry["backoff"] is not None
                            else None
                        ),
                    }
                    if stage.retry is not None
                    else None
                ),
                "cache": (
                    {
                        **dict(stage.cache),
                        "hit": indices.get(stage.cache["hit"], -1),
                        "miss": indices.get(stage.cache["miss"], -1),
                    }
                    if stage.cache is not None
                    else None
                ),
                "resource": (
                    system.get_service(stage.resource).get_id()
                    if stage.resource is not None
                    else -1
                ),
                "service_time": (
                    _serialize_distribution(stage.service_time)
                    if stage.service_time
                    else None
                ),
                "blocking_time": (
                    _serialize_distribution(stage.blocking_time)
                    if stage.blocking_time
                    else None
                ),
                "delay": _serialize_distribution(stage.delay) if stage.delay else None,
                "work_units": stage.work_units,
                "unit": stage.unit,
                "hold_resource": stage.hold_resource,
                "calls": [
                    {"service": indices[target], "mean_visits": count}
                    for target, count in stage.calls.items()
                ],
                "call_policy": stage.policy.to_dict(),
                "next": (
                    indices[stage.next_stage] if stage.next_stage is not None else -1
                ),
            }
            for name, stage in plan.stages.items()
        ],
    }


class CppSimulationBackend:
    """Versioned subprocess adapter for the required native event kernel."""

    def __init__(self, binary_path=None):
        configured_path = binary_path or os.environ.get("KUBESIZER_CPP_BACKEND")
        self.binary_path = (
            Path(configured_path)
            if configured_path
            else (
                Path(__file__).resolve().parent
                / "cpp"
                / "build-release"
                / "kubesizer-simulator"
            )
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
        compatibility_check = getattr(
            system,
            "require_simulation_compatible_workload",
            None,
        )
        if compatibility_check is not None:
            compatibility_check()
        services = tuple(
            sorted(system.get_services().values(), key=lambda item: item.get_id())
        )
        request_classes = []
        concurrency_weights = system.get_request_class_concurrency_weights()
        arrival_specs = system.get_external_arrival_distribution_specs()
        service_specs = system.get_service_time_distribution_specs()
        class_service_specs = system.get_request_class_service_time_distribution_specs()
        class_blocking_specs = getattr(
            system,
            "get_request_class_service_blocking_time_distribution_specs",
            lambda: {},
        )()
        post_release_specs = system.get_service_post_release_delay_distribution_specs()
        workflows = system.get_request_class_workflows()
        call_graphs = system.get_request_class_call_graphs()
        call_execution_modes = system.get_request_class_call_execution_modes()
        call_policies = system.get_request_class_call_policies()
        stage_workflows = system.get_request_class_stage_workflows()
        overhead_specs = system.get_request_class_latency_overhead_distribution_specs()
        latency_components = getattr(
            system,
            "get_request_class_latency_component_distribution_specs",
            lambda: {},
        )()
        latency_boundaries = getattr(
            system,
            "get_request_class_latency_boundaries",
            lambda: {},
        )()
        communication_latency = getattr(
            system,
            "get_communication_latency_distribution_spec",
            lambda: None,
        )()
        timeouts = system.get_request_class_end_to_end_timeouts()
        completion_boundaries = getattr(
            system, "get_request_class_completion_boundaries", lambda: {}
        )()
        concurrency_limits = system.get_service_concurrency_limits_per_replica()
        processing_slots = system.get_service_processing_slots_per_replica()
        execution_models = system.get_service_execution_models()
        cpu_bandwidths = getattr(system, "get_service_cpu_bandwidths", lambda: {})()
        from performance_models.components import serialize_resource_policies

        component_policies = getattr(
            system, "get_service_component_policies", lambda: {}
        )()
        replica_queue_models = getattr(
            system,
            "get_service_replica_queue_models",
            lambda: {service.get_name(): "pooled" for service in services},
        )()
        joint_input_uncertainty = system.get_joint_input_uncertainty()
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
            request_classes.append(
                {
                    "name": request_class,
                    "concurrency_weight": concurrency_weights[request_class],
                    "flow_kind": (
                        "stages"
                        if request_class in stage_workflows
                        else (
                            "workflow"
                            if request_class in workflows
                            else (
                                "calls" if request_class in call_graphs else "routing"
                            )
                        )
                    ),
                    "stage_workflow": (
                        serialize_stage_workflow(stage_workflows[request_class], system)
                        if request_class in stage_workflows
                        else None
                    ),
                    "routing": routing,
                    "workflow": [
                        {
                            "service": system.get_service(visit.service).get_id(),
                            "mean_visits": visit.mean_visits,
                        }
                        for visit in workflows.get(request_class, ())
                    ],
                    "calls": [
                        [
                            {
                                "service": system.get_service(target).get_id(),
                                "mean_visits": mean_calls,
                            }
                            for target, mean_calls in (
                                call_graphs.get(request_class, {})
                                .get(service.get_name(), {})
                                .items()
                            )
                        ]
                        for service in services
                    ],
                    "call_execution": call_execution_modes.get(
                        request_class,
                        "parallel",
                    ),
                    "call_policies": [
                        (
                            call_policies[request_class][service.get_name()].to_dict()
                            if service.get_name()
                            in call_policies.get(request_class, {})
                            else {
                                "execution": call_execution_modes.get(
                                    request_class, "parallel"
                                ),
                                "completion": "all",
                                "required": 0,
                            }
                        )
                        for service in services
                    ],
                    "latency_overhead_distribution": (
                        _serialize_distribution(overhead_specs[request_class])
                        if request_class in overhead_specs
                        and request_class not in latency_components
                        else None
                    ),
                    "latency_boundary": latency_boundaries.get(
                        request_class,
                        "unspecified",
                    ),
                    "latency_components": [
                        {
                            "name": name,
                            "distribution": _serialize_distribution(specification),
                        }
                        for name, specification in latency_components.get(
                            request_class,
                            {},
                        ).items()
                    ],
                    "end_to_end_timeout": timeouts.get(request_class, 0.0),
                    "completion_boundary": completion_boundaries.get(
                        request_class, "client_response"
                    ),
                }
            )

        external_streams = []
        for service in services:
            for class_id, request_class in enumerate(system.get_request_classes()):
                distribution = arrival_specs[request_class].get(service.get_name())
                if distribution is not None:
                    external_streams.append(
                        {
                            "request_class": class_id,
                            "entry_service": service.get_id(),
                            "interarrival_distribution": _serialize_distribution(
                                distribution
                            ),
                        }
                    )

        result = {
            "name": system.get_name(),
            "services": [
                {
                    "name": service.get_name(),
                    "execution_model": execution_models[service.get_name()],
                    "replica_queue_model": replica_queue_models[service.get_name()],
                    "cpu_bandwidth": cpu_bandwidths.get(service.get_name()),
                    "component_policies": serialize_resource_policies(
                        component_policies.get(service.get_name(), {})
                    ),
                    "replicas": service.get_replicas(),
                    "capacity": (
                        service.get_replicas() * processing_slots[service.get_name()]
                    ),
                    **(
                        {}
                        if concurrency_limits[service.get_name()] is None
                        else {
                            "concurrency_capacity": service.get_replicas()
                            * concurrency_limits[service.get_name()]
                        }
                    ),
                    "service_time_distributions": [
                        _serialize_distribution(
                            class_service_specs[request_class].get(
                                service.get_name(),
                                service_specs[service.get_name()],
                            )
                        )
                        for request_class in system.get_request_classes()
                    ],
                    "blocking_time_distributions": [
                        (
                            _serialize_distribution(specification)
                            if (
                                specification := class_blocking_specs.get(
                                    request_class,
                                    {},
                                ).get(service.get_name())
                            )
                            is not None
                            else None
                        )
                        for request_class in system.get_request_classes()
                    ],
                    "post_release_delay": (
                        _serialize_distribution(post_release_specs[service.get_name()])
                        if service.get_name() in post_release_specs
                        else None
                    ),
                }
                for service in services
            ],
            "request_classes": request_classes,
            "external_streams": external_streams,
        }
        if joint_input_uncertainty is not None:
            result["input_uncertainty"] = {
                "sampling": joint_input_uncertainty.sampling,
                "block_length": joint_input_uncertainty.block_length,
                "epochs": [
                    {
                        "arrival_rate_multipliers": [
                            epoch.arrival_rate_multipliers[request_class]
                            for request_class in system.get_request_classes()
                        ],
                        "service_time_multipliers": [
                            epoch.service_time_multipliers[service.get_name()]
                            for service in services
                        ],
                    }
                    for epoch in joint_input_uncertainty.epochs
                ],
            }
        if communication_latency is not None:
            result["communication_latency"] = {
                "same_node_one_way": _serialize_distribution(
                    communication_latency.same_node_one_way
                ),
                "cross_node_one_way": _serialize_distribution(
                    communication_latency.cross_node_one_way
                ),
                "cross_node_probability": (
                    communication_latency.cross_node_probability
                ),
            }
        return result

    def _invoke(
        self,
        operation,
        system,
        *,
        serialized_workload=None,
        **arguments,
    ):
        self.ensure_available()
        request = {
            "protocol_version": PROTOCOL_VERSION,
            "operation": operation,
            "workload": (
                self.serialize_system(system)
                if serialized_workload is None
                else serialized_workload
            ),
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
            for name in (
                "mean_utilizations",
                "mean_latencies",
                "p95_latencies",
                "p99_latencies",
            ):
                _numeric_tuple(metrics[name], f"metrics.{name}", length=service_count)
            if not isinstance(metrics["request_class_p95_latencies"], dict):
                raise ValueError("request-class p95 values must be an object.")
            expected_classes = set(system.get_request_classes())
            if not set(metrics["request_class_p95_latencies"]).issubset(
                expected_classes
            ):
                raise ValueError("response contains an unknown request class.")
            if int(result["seed"]) != int(seed):
                raise ValueError("response seed does not match request.")
            if len(result["queue_lengths_at_end"]) != service_count:
                raise ValueError("final queue vector has the wrong length.")
            if len(result["busy_replicas_at_end"]) != service_count:
                raise ValueError("final busy vector has the wrong length.")
            if capture_state and any(
                result[name] is None
                for name in ("active_history", "queue_history", "busy_history")
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
            bucket_count = complete_bucket_count(horizon, bucket_width)
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
            configuration = tuple(
                int(value) for value in result["replica_configuration"]
            )
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
                raise ValueError(
                    "native horizon or bucket width does not match request."
                )
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
        maximum_drain_time = _finite_positive(maximum_drain_time, "maximum_drain_time")
        if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
            raise ValueError("workers must be a positive integer.")
        result = self._invoke(
            "measurement_replications",
            system,
            seeds=seeds,
            warmup_duration=warmup_duration,
            measurement_duration=measurement_duration,
            maximum_drain_time=maximum_drain_time,
            workers=min(workers, len(seeds)),
        )

        try:
            expected_configuration = tuple(system.get_system_replicas())
            configuration = tuple(
                int(value) for value in result["replica_configuration"]
            )
            raw_replications = result["replications"]
            if configuration != expected_configuration:
                raise ValueError("native configuration does not match request.")
            if tuple(int(item["seed"]) for item in raw_replications) != seeds:
                raise ValueError("native replication batch identity is invalid.")
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
                failed_measured = {
                    key: int(value)
                    for key, value in item["failed_measured_request_counts"].items()
                }
                p95 = {
                    key: float(value)
                    for key, value in item["request_class_p95_latencies"].items()
                }
                latencies = {
                    key: _numeric_tuple(value, f"request_class_latencies[{key}]")
                    for key, value in item["request_class_latencies"].items()
                }
                arrival_times = {
                    key: _numeric_tuple(value, f"request_class_arrival_times[{key}]")
                    for key, value in item["request_class_arrival_times"].items()
                }
                raw_failures = item["request_class_failures"]
                if not isinstance(raw_failures, dict) or any(
                    not isinstance(values, list)
                    or any(value not in (0, 1, False, True) for value in values)
                    for values in raw_failures.values()
                ):
                    raise ValueError("request-class failure flags are invalid.")
                failures = {
                    key: tuple(bool(value) for value in values)
                    for key, values in raw_failures.items()
                }
                effective_capacities = tuple(system.get_effective_service_capacities())
                if (
                    set(measured) != expected_classes
                    or set(completed_measured) != expected_classes
                    or set(failed_measured) != expected_classes
                    or set(latencies) != expected_classes
                    or set(arrival_times) != expected_classes
                    or set(failures) != expected_classes
                    or measured != completed_measured
                ):
                    raise ValueError(
                        "tagged request cohort was not drained completely."
                    )
                if any(value < 0 for value in measured.values()) or any(
                    value < 0 or value > measured[key]
                    for key, value in failed_measured.items()
                ):
                    raise ValueError("measured request outcome count is invalid.")
                for request_class in expected_classes:
                    if not (
                        measured[request_class]
                        == len(latencies[request_class])
                        == len(arrival_times[request_class])
                        == len(failures[request_class])
                    ):
                        raise ValueError(
                            "raw latency-cluster size does not match count."
                        )
                    if sum(failures[request_class]) != failed_measured[request_class]:
                        raise ValueError(
                            "raw failure flags do not match the failure count."
                        )
                    if any(
                        arrival < warmup_duration
                        or arrival >= warmup_duration + measurement_duration
                        for arrival in arrival_times[request_class]
                    ):
                        raise ValueError("measurement arrival lies outside [W, W + M).")
                if set(p95) != {key for key, value in measured.items() if value > 0}:
                    raise ValueError(
                        "request-class p95 coverage does not match counts."
                    )
                if any(not math.isfinite(value) or value < 0 for value in p95.values()):
                    raise ValueError("request-class p95 contains an invalid value.")
                for request_class, value in p95.items():
                    if not math.isclose(
                        value,
                        _percentile(latencies[request_class], 0.95),
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    ):
                        raise ValueError(
                            "native p95 does not match raw latency cluster."
                        )
                seed_value = int(item["seed"])
                stop_time = float(item["stop_time"])
                if seed_value != expected_seed or not math.isfinite(stop_time):
                    raise ValueError("native replication identity is invalid.")
                if stop_time < warmup_duration + measurement_duration:
                    raise ValueError(
                        "native replication stopped before measurement end."
                    )
                queue_lengths = tuple(
                    int(value) for value in item["queue_lengths_at_end"]
                )
                busy_replicas = tuple(
                    int(value) for value in item["busy_replicas_at_end"]
                )
                active_requests = int(item["active_requests_at_end"])
                tagged_in_flight = int(item["tagged_requests_in_flight_at_end"])
                raw_block_start = item["sampled_input_block_start"]
                if raw_block_start is not None and (
                    isinstance(raw_block_start, bool)
                    or not isinstance(raw_block_start, int)
                    or raw_block_start < 0
                ):
                    raise ValueError("sampled input block start is invalid.")
                input_arrival_multipliers_values = _numeric_tuple(
                    item["input_arrival_rate_multipliers"],
                    "input_arrival_rate_multipliers",
                    length=len(expected_classes),
                )
                input_service_multipliers = _numeric_tuple(
                    item["input_service_time_multipliers"],
                    "input_service_time_multipliers",
                    length=len(configuration),
                )
                if any(value <= 0 for value in input_arrival_multipliers_values):
                    raise ValueError("input arrival multipliers must be positive.")
                if any(value <= 0 for value in input_service_multipliers):
                    raise ValueError("input service multipliers must be positive.")
                input_arrival_multipliers = dict(
                    zip(system.get_request_classes(), input_arrival_multipliers_values)
                )
                if (
                    active_requests < 0
                    or len(queue_lengths) != len(configuration)
                    or len(busy_replicas) != len(configuration)
                    or any(value < 0 for value in queue_lengths)
                    or any(
                        value < 0 or value > capacity
                        for value, capacity in zip(
                            busy_replicas,
                            effective_capacities,
                        )
                    )
                    or tagged_in_flight != 0
                ):
                    raise ValueError(
                        "native final-state diagnostics violate invariants."
                    )
                replications.append(
                    NativeMeasurementReplication(
                        seed=seed_value,
                        replica_configuration=configuration,
                        warmup_duration=warmup_duration,
                        measurement_duration=measurement_duration,
                        stop_time=stop_time,
                        measured_request_counts=measured,
                        completed_measured_request_counts=completed_measured,
                        failed_measured_request_counts=failed_measured,
                        request_class_p95_latencies=p95,
                        request_class_latencies=latencies,
                        request_class_arrival_times=arrival_times,
                        request_class_failures=failures,
                        processed_event_count=int(item["processed_event_count"]),
                        external_arrival_count=int(item["external_arrival_count"]),
                        completed_request_count=int(item["completed_request_count"]),
                        active_requests_at_end=active_requests,
                        queue_lengths_at_end=queue_lengths,
                        busy_replicas_at_end=busy_replicas,
                        tagged_requests_in_flight_at_end=tagged_in_flight,
                        maximum_drain_time=maximum_drain_time,
                        sampled_input_block_start=raw_block_start,
                        input_arrival_rate_multipliers=input_arrival_multipliers,
                        input_service_time_multipliers=input_service_multipliers,
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
    failures = {}
    for request_class, raw_latencies in replication.request_class_latencies.items():
        raw_failures = replication.request_class_failures.get(
            request_class,
            (False,) * len(raw_latencies),
        )
        selected = tuple(
            (arrival, latency, failed)
            for arrival, latency, failed in zip(
                replication.request_class_arrival_times[request_class],
                raw_latencies,
                raw_failures,
            )
            if arrival < boundary
        )
        arrival_times[request_class] = tuple(item[0] for item in selected)
        latencies[request_class] = tuple(item[1] for item in selected)
        failures[request_class] = tuple(item[2] for item in selected)
    counts = {name: len(values) for name, values in latencies.items()}
    p95 = {
        name: _percentile(values, 0.95) for name, values in latencies.items() if values
    }
    return replace(
        replication,
        measurement_duration=measurement_duration,
        measured_request_counts=counts,
        completed_measured_request_counts=dict(counts),
        failed_measured_request_counts={
            name: sum(values) for name, values in failures.items()
        },
        request_class_p95_latencies=p95,
        request_class_latencies=latencies,
        request_class_arrival_times=arrival_times,
        request_class_failures=failures,
        input_arrival_rate_multipliers=dict(replication.input_arrival_rate_multipliers),
    )


_BACKEND = CppSimulationBackend()


def get_native_backend():
    return _BACKEND

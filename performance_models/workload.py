import copy
import csv
import hashlib
import math
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType


SINGLE_TRAFFIC_MODEL_NAME = "traffic_model"
CUSTOM_DISTRIBUTION_ABI_VERSION = 1


@dataclass(frozen=True)
class DistributionSpec:
    """Validated moments and parameters for one declared distribution."""

    distribution: str
    mean: float
    scv: float
    parameters: tuple[tuple[str, object], ...]
    samples: tuple[float, ...] = ()
    sample_clusters: tuple[tuple[float, ...], ...] = ()

    @property
    def rate(self):
        return 1.0 / self.mean


@dataclass(frozen=True)
class JointInputEpochSpec:
    arrival_rate_multipliers: Mapping
    service_time_multipliers: Mapping


@dataclass(frozen=True)
class JointInputUncertaintySpec:
    sampling: str
    block_length: int
    epochs: tuple[JointInputEpochSpec, ...]


@dataclass(frozen=True)
class CommunicationLatencySpec:
    """Measured one-way service-call latency used before placement is known."""

    same_node_one_way: DistributionSpec
    cross_node_one_way: DistributionSpec
    unplaced_cross_node_probability: float


@dataclass(frozen=True)
class TrafficModelSpec:
    name: str
    entry_services: Mapping
    service_times: Mapping
    service_times_by_request_class: Mapping
    service_blocking_times: Mapping
    service_blocking_times_by_request_class: Mapping
    service_execution_models: Mapping
    service_replica_queue_models: Mapping
    processing_slots_per_replica: Mapping
    concurrency_limits_per_replica: Mapping
    input_uncertainty: JointInputUncertaintySpec | None
    communication_latency: CommunicationLatencySpec | None


@dataclass(frozen=True)
class WorkflowVisitSpec:
    """One ordered service visit with a possibly fractional expected count."""

    service: str
    mean_visits: float


@dataclass(frozen=True)
class EndToEndLatencySLOSpec:
    """Validated request-class end-to-end latency constraints."""

    mean_seconds: float | None = None
    percentile_quantile: float | None = None
    percentile_maximum_seconds: float | None = None

    @property
    def kind(self):
        return "mean" if self.mean_seconds is not None else "percentile"

    def to_dict(self):
        if self.kind == "mean":
            return {"mean_seconds": self.mean_seconds}
        return {
            "percentile": {
                "quantile": self.percentile_quantile,
                "maximum_seconds": self.percentile_maximum_seconds,
            }
        }


@dataclass(frozen=True)
class ParsedWorkload:
    request_classes: Mapping
    request_class_visit_ratios: Mapping
    request_class_workflows: Mapping
    request_class_call_graphs: Mapping
    request_class_call_execution_modes: Mapping
    request_class_fixed_latency_overheads: Mapping
    request_class_latency_overhead_specs: Mapping
    request_class_latency_boundaries: Mapping
    request_class_latency_component_specs: Mapping
    request_class_end_to_end_timeouts: Mapping
    concurrency_weights: Mapping
    request_class_latency_slos: Mapping
    request_class_failure_rate_slos: Mapping
    traffic_model: TrafficModelSpec
    service_names: tuple[str, ...]


def _freeze_mapping(values):
    return MappingProxyType(dict(values))


def _finite_positive(value, label):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) <= 0.0
    ):
        raise ValueError(f"{label} must be a finite number greater than zero.")
    return float(value)


def _finite_nonnegative(value, label):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0.0
    ):
        raise ValueError(f"{label} must be finite and non-negative.")
    return float(value)


def _finite_probability(value, label):
    value = _finite_nonnegative(value, label)
    if value > 1.0:
        raise ValueError(f"{label} must be no greater than one.")
    return value


def _positive_integer(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer.")
    return value


def _parse_joint_input_uncertainty(
    value,
    *,
    request_classes,
    service_names,
    label,
):
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {
        "sampling",
        "block_length",
        "epochs",
    }:
        raise ValueError(
            f"{label} must contain exactly sampling, block_length, and epochs."
        )
    if value["sampling"] != "moving_block_bootstrap":
        raise ValueError(f"{label}.sampling must be moving_block_bootstrap.")
    block_length = _positive_integer(
        value["block_length"],
        f"{label}.block_length",
    )
    raw_epochs = value["epochs"]
    if not isinstance(raw_epochs, (list, tuple)) or len(raw_epochs) < 2:
        raise ValueError(f"{label}.epochs must contain at least two windows.")
    if block_length > len(raw_epochs):
        raise ValueError(f"{label}.block_length cannot exceed its epoch count.")
    epochs = []
    request_class_names = set(request_classes)
    service_name_set = set(service_names)
    for index, raw_epoch in enumerate(raw_epochs):
        epoch_label = f"{label}.epochs[{index}]"
        if not isinstance(raw_epoch, Mapping) or set(raw_epoch) != {
            "arrival_rate_multipliers",
            "service_time_multipliers",
        }:
            raise ValueError(
                f"{epoch_label} must contain exactly arrival_rate_multipliers "
                "and service_time_multipliers."
            )
        arrivals = raw_epoch["arrival_rate_multipliers"]
        service_times = raw_epoch["service_time_multipliers"]
        if not isinstance(arrivals, Mapping) or set(arrivals) != request_class_names:
            raise ValueError(
                f"{epoch_label}.arrival_rate_multipliers must define every "
                "request class exactly once."
            )
        if (
            not isinstance(service_times, Mapping)
            or set(service_times) != service_name_set
        ):
            raise ValueError(
                f"{epoch_label}.service_time_multipliers must define every "
                "service exactly once."
            )
        epochs.append(
            JointInputEpochSpec(
                arrival_rate_multipliers=_freeze_mapping(
                    {
                        name: _finite_positive(
                            multiplier,
                            f"{epoch_label}.arrival_rate_multipliers.{name}",
                        )
                        for name, multiplier in arrivals.items()
                    }
                ),
                service_time_multipliers=_freeze_mapping(
                    {
                        name: _finite_positive(
                            multiplier,
                            f"{epoch_label}.service_time_multipliers.{name}",
                        )
                        for name, multiplier in service_times.items()
                    }
                ),
            )
        )
    return JointInputUncertaintySpec(
        sampling="moving_block_bootstrap",
        block_length=block_length,
        epochs=tuple(epochs),
    )


def _sample_moments(values, label):
    values = tuple(_finite_positive(value, label) for value in values)
    if not values:
        raise ValueError(f"{label} must contain at least one observation.")
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return mean, variance / (mean * mean)


def _csv_values(source, column, label, base_path):
    if not isinstance(source, str) or not source:
        raise ValueError(f"{label}.source must be a nonempty path string.")
    if not isinstance(column, str) or not column:
        raise ValueError(f"{label}.column must be a nonempty string.")
    path = Path(source)
    if not path.is_absolute():
        if base_path is None:
            raise ValueError(
                f"{label}.source is relative; workload_base_path is required."
            )
        path = Path(base_path) / path
    try:
        with path.open(newline="", encoding="utf-8") as stream:
            rows = tuple(csv.DictReader(stream))
    except OSError as error:
        raise ValueError(f"Cannot read {label}.source at {path}.") from error
    if not rows:
        raise ValueError(f"{label}.source contains no data rows.")
    if column not in rows[0]:
        raise ValueError(f"{label}.source has no column {column!r}.")
    try:
        return tuple(float(row[column]) for row in rows)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{label}.source column {column!r} must contain numeric values."
        ) from error


def _csv_cluster_values(source, column, cluster_column, label, base_path):
    if not isinstance(cluster_column, str) or not cluster_column:
        raise ValueError(f"{label}.cluster_column must be a nonempty string.")
    path = _resolved_data_path(source, label, base_path)
    try:
        with path.open(newline="", encoding="utf-8") as stream:
            rows = tuple(csv.DictReader(stream))
    except OSError as error:
        raise ValueError(f"Cannot read {label}.source at {path}.") from error
    if not rows:
        raise ValueError(f"{label}.source contains no data rows.")
    missing = {column, cluster_column} - set(rows[0])
    if missing:
        raise ValueError(f"{label}.source has no columns {sorted(missing)}.")
    grouped = {}
    try:
        for row in rows:
            cluster = row[cluster_column]
            if not cluster:
                raise ValueError
            grouped.setdefault(cluster, []).append(float(row[column]))
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{label}.source must contain nonempty cluster identities and "
            "numeric sample values."
        ) from error
    if len(grouped) < 2:
        raise ValueError(f"{label}.source must contain at least two clusters.")
    return tuple(tuple(values) for values in grouped.values()), path


def _resolved_data_path(source, label, base_path):
    if not isinstance(source, str) or not source:
        raise ValueError(f"{label}.source must be a nonempty path string.")
    path = Path(source)
    if not path.is_absolute():
        if base_path is None:
            raise ValueError(
                f"{label}.source is relative; workload_base_path is required."
            )
        path = Path(base_path) / path
    return path.resolve()


def _resolved_file(source, label, base_path):
    if not isinstance(source, str) or not source:
        raise ValueError(f"{label} must be a nonempty path string.")
    path = Path(source)
    if not path.is_absolute():
        if base_path is None:
            raise ValueError(f"{label} is relative; workload_base_path is required.")
        path = Path(base_path) / path
    path = path.resolve()
    if not path.is_file():
        raise ValueError(f"{label} does not identify a file: {path}.")
    return path


def _file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parameter_items(specification, *, resolved_source=None):
    values = dict(specification)
    if resolved_source is not None:
        values["source"] = str(resolved_source)
    return tuple(sorted(values.items()))


def _shape_or_scv(specification, label):
    has_shape = "shape" in specification
    has_scv = "scv" in specification
    if has_shape == has_scv:
        raise ValueError(f"{label} must define exactly one of shape or scv.")
    if has_scv:
        return _finite_nonnegative(specification["scv"], f"{label}.scv")
    shape = _finite_positive(specification["shape"], f"{label}.shape")
    return 1.0 / shape


def _weibull_scv(shape):
    first = math.gamma(1.0 + 1.0 / shape)
    second = math.gamma(1.0 + 2.0 / shape)
    return second / (first * first) - 1.0


def _parse_distribution(specification, label, *, quantity, base_path):
    if not isinstance(specification, Mapping):
        raise ValueError(f"{label} must be a mapping.")
    distribution = specification.get("distribution")
    if not isinstance(distribution, str) or not distribution:
        raise ValueError(f"{label}.distribution must be a nonempty string.")

    if quantity == "interarrival":
        scale_field = "rate"
        empirical_name = "empirical_interarrival"
    elif quantity == "service_time":
        scale_field = "mean"
        empirical_name = "empirical"
    else:
        raise ValueError(f"Unknown distribution quantity {quantity!r}.")

    if distribution == "exponential":
        if set(specification) != {"distribution", "rate"}:
            raise ValueError(f"{label} must contain exactly distribution and rate.")
        rate = _finite_positive(specification["rate"], f"{label}.rate")
        return DistributionSpec(
            distribution, 1.0 / rate, 1.0, _parameter_items(specification)
        )

    if distribution == "deterministic":
        expected = {"distribution", scale_field}
        if set(specification) != expected:
            raise ValueError(
                f"{label} must contain exactly distribution and {scale_field}."
            )
        scale = _finite_positive(specification[scale_field], f"{label}.{scale_field}")
        mean = 1.0 / scale if scale_field == "rate" else scale
        return DistributionSpec(
            distribution, mean, 0.0, _parameter_items(specification)
        )

    if distribution == "gamma":
        allowed = {"distribution", scale_field, "shape", "scv"}
        if set(specification) - allowed or scale_field not in specification:
            raise ValueError(
                f"{label} must define distribution, {scale_field}, and exactly "
                "one of shape or scv."
            )
        scv = _shape_or_scv(specification, label)
        if scv <= 0.0:
            raise ValueError(f"{label}.scv must be greater than zero for gamma.")
        scale = _finite_positive(specification[scale_field], f"{label}.{scale_field}")
        mean = 1.0 / scale if scale_field == "rate" else scale
        return DistributionSpec(
            distribution, mean, scv, _parameter_items(specification)
        )

    if distribution == "lognormal":
        expected = {"distribution", scale_field, "scv"}
        if set(specification) != expected:
            raise ValueError(
                f"{label} must contain exactly distribution, {scale_field}, and scv."
            )
        scv = _finite_nonnegative(specification["scv"], f"{label}.scv")
        scale = _finite_positive(specification[scale_field], f"{label}.{scale_field}")
        mean = 1.0 / scale if scale_field == "rate" else scale
        return DistributionSpec(
            distribution, mean, scv, _parameter_items(specification)
        )

    if distribution == "weibull":
        expected = {"distribution", scale_field, "shape"}
        if set(specification) != expected:
            raise ValueError(
                f"{label} must contain exactly distribution, {scale_field}, and shape."
            )
        shape = _finite_positive(specification["shape"], f"{label}.shape")
        scale = _finite_positive(specification[scale_field], f"{label}.{scale_field}")
        mean = 1.0 / scale if scale_field == "rate" else scale
        return DistributionSpec(
            distribution,
            mean,
            _weibull_scv(shape),
            _parameter_items(specification),
        )

    if distribution == empirical_name:
        file_fields = {"distribution", "source", "column", "sampling"}
        inline_fields = {"distribution", "samples", "sampling"}
        cluster_file_fields = {
            "distribution",
            "source",
            "column",
            "cluster_column",
            "sampling",
        }
        cluster_inline_fields = {"distribution", "clusters", "sampling"}
        if set(specification) not in (
            file_fields,
            inline_fields,
            cluster_file_fields,
            cluster_inline_fields,
        ):
            raise ValueError(
                f"{label} must contain distribution and sampling with either "
                "source plus column, inline samples, or clustered samples."
            )
        if specification["sampling"] not in {
            "iid_with_replacement",
            "bayesian_bootstrap",
            "cluster_bootstrap",
        }:
            raise ValueError(
                f"{label}.sampling must be 'iid_with_replacement' or "
                "'bayesian_bootstrap', or 'cluster_bootstrap'."
            )
        clustered = specification["sampling"] == "cluster_bootstrap"
        if clustered != (
            set(specification) in (cluster_file_fields, cluster_inline_fields)
        ):
            raise ValueError(
                f"{label}.sampling='cluster_bootstrap' requires clusters or "
                "source, column, and cluster_column."
            )
        clusters = ()
        if "clusters" in specification:
            raw_clusters = specification["clusters"]
            if (
                not isinstance(raw_clusters, (list, tuple))
                or len(raw_clusters) < 2
                or any(
                    not isinstance(cluster, (list, tuple)) or not cluster
                    for cluster in raw_clusters
                )
            ):
                raise ValueError(
                    f"{label}.clusters must contain at least two nonempty arrays."
                )
            clusters = tuple(tuple(cluster) for cluster in raw_clusters)
            values = tuple(value for cluster in clusters for value in cluster)
            normalized_specification = dict(specification)
            normalized_specification["clusters"] = clusters
            parameters = _parameter_items(normalized_specification)
            sample_label = f"{label}.clusters"
        elif "cluster_column" in specification:
            clusters, resolved = _csv_cluster_values(
                specification["source"],
                specification["column"],
                specification["cluster_column"],
                label,
                base_path,
            )
            values = tuple(value for cluster in clusters for value in cluster)
            parameters = _parameter_items(
                specification,
                resolved_source=resolved,
            )
            sample_label = f"{label}.source values"
        elif "samples" in specification:
            raw_values = specification["samples"]
            if not isinstance(raw_values, (list, tuple)) or not raw_values:
                raise ValueError(f"{label}.samples must be a nonempty array.")
            values = tuple(raw_values)
            normalized_specification = dict(specification)
            normalized_specification["samples"] = values
            parameters = _parameter_items(normalized_specification)
            sample_label = f"{label}.samples"
        else:
            values = _csv_values(
                specification["source"], specification["column"], label, base_path
            )
            source = Path(specification["source"])
            resolved = (
                source if source.is_absolute() else Path(base_path or ".") / source
            )
            parameters = _parameter_items(
                specification,
                resolved_source=resolved.resolve(),
            )
            sample_label = f"{label}.source values"
        mean, scv = _sample_moments(values, sample_label)
        return DistributionSpec(
            distribution,
            mean,
            scv,
            parameters,
            tuple(values),
            tuple(clusters),
        )

    if distribution == "arrival_trace" and quantity == "interarrival":
        file_fields = {
            "distribution",
            "source",
            "timestamp_column",
            "sampling",
            "block_duration",
        }
        inline_fields = {
            "distribution",
            "timestamps",
            "sampling",
            "block_duration",
        }
        if set(specification) not in (file_fields, inline_fields):
            raise ValueError(
                f"{label} must contain sampling and block_duration with either "
                "source plus timestamp_column or inline timestamps."
            )
        if specification["sampling"] != "block_bootstrap":
            raise ValueError(f"{label}.sampling must be 'block_bootstrap'.")
        _finite_positive(specification["block_duration"], f"{label}.block_duration")
        if "timestamps" in specification:
            raw_timestamps = specification["timestamps"]
            if not isinstance(raw_timestamps, (list, tuple)):
                raise ValueError(f"{label}.timestamps must be an array.")
            timestamps = tuple(raw_timestamps)
            normalized = dict(specification)
            normalized["timestamps"] = timestamps
            parameters = _parameter_items(normalized)
            sample_label = f"{label}.timestamps"
        else:
            timestamps = _csv_values(
                specification["source"],
                specification["timestamp_column"],
                label,
                base_path,
            )
            source = Path(specification["source"])
            resolved = (
                source if source.is_absolute() else Path(base_path or ".") / source
            )
            parameters = _parameter_items(
                specification,
                resolved_source=resolved.resolve(),
            )
            sample_label = f"{label}.source timestamps"
        timestamps = tuple(
            _finite_nonnegative(value, sample_label) for value in timestamps
        )
        if len(timestamps) < 2 or any(
            right <= left
            for left, right in zip(timestamps, timestamps[1:])
        ):
            raise ValueError(
                f"{label}.source timestamps must contain at least two strictly "
                "increasing values."
            )
        interarrivals = tuple(
            right - left for left, right in zip(timestamps, timestamps[1:])
        )
        mean, scv = _sample_moments(interarrivals, f"{label}.source intervals")
        return DistributionSpec(
            distribution,
            mean,
            scv,
            parameters,
            tuple(timestamps),
        )

    if distribution == "custom":
        expected = {
            "distribution",
            scale_field,
            "scv",
            "library",
            "symbol",
            "abi_version",
            "parameters",
        }
        if set(specification) != expected:
            raise ValueError(
                f"{label} must contain exactly distribution, {scale_field}, scv, "
                "library, symbol, abi_version, and parameters."
            )
        scale = _finite_positive(specification[scale_field], f"{label}.{scale_field}")
        scv = _finite_nonnegative(specification["scv"], f"{label}.scv")
        if specification["abi_version"] != CUSTOM_DISTRIBUTION_ABI_VERSION:
            raise ValueError(
                f"{label}.abi_version must be " f"{CUSTOM_DISTRIBUTION_ABI_VERSION}."
            )
        symbol = specification["symbol"]
        if not isinstance(symbol, str) or not symbol:
            raise ValueError(f"{label}.symbol must be a nonempty string.")
        raw_parameters = specification["parameters"]
        if not isinstance(raw_parameters, (list, tuple)):
            raise ValueError(f"{label}.parameters must be an array.")
        parameters = []
        for index, value in enumerate(raw_parameters):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise ValueError(
                    f"{label}.parameters[{index}] must be a finite number."
                )
            parameters.append(float(value))
        library = _resolved_file(
            specification["library"],
            f"{label}.library",
            base_path,
        )
        resolved_specification = dict(specification)
        resolved_specification["library"] = str(library)
        resolved_specification["library_sha256"] = _file_sha256(library)
        resolved_specification["parameters"] = tuple(parameters)
        mean = 1.0 / scale if scale_field == "rate" else scale
        return DistributionSpec(
            distribution,
            mean,
            scv,
            _parameter_items(resolved_specification),
        )

    supported = (
        "exponential, deterministic, gamma, lognormal, weibull, "
        f"{empirical_name}"
        + (", arrival_trace" if quantity == "interarrival" else "")
        + ", custom"
    )
    raise ValueError(
        f"{label} uses unsupported {quantity} distribution {distribution!r}; "
        f"supported distributions are: {supported}."
    )


def parse_duration_distribution(specification, label, *, base_path=None):
    """Validate a nonnegative-time distribution used outside service queues."""
    return _parse_distribution(
        specification,
        label,
        quantity="service_time",
        base_path=base_path,
    )


def duration_distribution_to_workload_mapping(specification):
    """Return a self-contained workload mapping for a validated duration."""
    parameters = dict(specification.parameters)
    if specification.distribution == "empirical":
        if specification.sample_clusters:
            return {
                "distribution": "empirical",
                "clusters": [
                    list(cluster) for cluster in specification.sample_clusters
                ],
                "sampling": "cluster_bootstrap",
            }
        return {
            "distribution": "empirical",
            "samples": list(specification.samples),
            "sampling": parameters["sampling"],
        }
    if specification.distribution == "custom":
        parameters.pop("library_sha256", None)
        parameters["parameters"] = list(parameters["parameters"])
    return parameters


def _parse_communication_latency(value, label, *, base_path=None):
    if value is None:
        return None
    expected = {
        "same_node_one_way",
        "cross_node_one_way",
        "unplaced_cross_node_probability",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(
            f"{label} must contain exactly same_node_one_way, "
            "cross_node_one_way, and unplaced_cross_node_probability."
        )
    return CommunicationLatencySpec(
        same_node_one_way=parse_duration_distribution(
            value["same_node_one_way"],
            f"{label}.same_node_one_way",
            base_path=base_path,
        ),
        cross_node_one_way=parse_duration_distribution(
            value["cross_node_one_way"],
            f"{label}.cross_node_one_way",
            base_path=base_path,
        ),
        unplaced_cross_node_probability=_finite_probability(
            value["unplaced_cross_node_probability"],
            f"{label}.unplaced_cross_node_probability",
        ),
    )


def _reachable_services(entry_services, routing):
    reachable = set(entry_services)
    pending = deque(entry_services)
    while pending:
        source = pending.popleft()
        for target in routing.get(source, {}):
            if target not in reachable:
                reachable.add(target)
                pending.append(target)
    return reachable


def _parse_request_classes(
    request_classes,
    service_names,
    *,
    require_concurrency_weights=False,
    base_path=None,
):
    parsed = {}
    visit_ratios = {}
    workflows = {}
    call_graphs = {}
    call_execution_modes = {}
    fixed_latency_overheads = {}
    latency_overhead_specs = {}
    latency_boundaries = {}
    latency_component_specs = {}
    end_to_end_timeouts = {}
    concurrency_weights = {}
    latency_slos = {}
    failure_rate_slos = {}
    weighted_classes = []
    for request_class_name, request_class in request_classes.items():
        if not isinstance(request_class_name, str) or not request_class_name:
            raise ValueError("Request-class names must be nonempty strings.")
        if not isinstance(request_class, Mapping):
            raise ValueError(f"Request class {request_class_name!r} must be a mapping.")
        allowed_fields = {
            "concurrency_weight",
            "end_to_end_latency_slos",
            "end_to_end_timeout_seconds",
            "maximum_terminal_failure_rate",
            "fixed_latency_overhead_seconds",
            "latency_overhead",
            "latency_boundary",
            "latency_components",
            "routing",
            "visits",
            "workflow",
            "calls",
            "call_execution",
        }
        unknown_fields = set(request_class) - allowed_fields
        flow_fields = {"routing", "visits", "workflow", "calls"} & set(request_class)
        if len(flow_fields) != 1 or unknown_fields:
            raise ValueError(
                f"Request class {request_class_name!r} must contain exactly one "
                "of routing, visits, workflow, or calls and may contain "
                "concurrency_weight, "
                "end_to_end_latency_slos, end_to_end_timeout_seconds, "
                "maximum_terminal_failure_rate, and "
                "exactly one of fixed_latency_overhead_seconds, "
                "latency_overhead, or latency_components; "
                f"unknown fields={sorted(unknown_fields)}."
            )
        latency_fields = {
            "fixed_latency_overhead_seconds",
            "latency_overhead",
            "latency_components",
        } & set(request_class)
        if len(latency_fields) > 1:
            raise ValueError(
                f"Request class {request_class_name!r} cannot define both a "
                "legacy latency overhead and latency_components."
            )
        boundary = request_class.get("latency_boundary", "unspecified")
        if boundary not in {"unspecified", "ingress", "external_client"}:
            raise ValueError(
                f"request_classes.{request_class_name}.latency_boundary must be "
                "ingress or external_client."
            )
        if "latency_components" in request_class and boundary == "unspecified":
            raise ValueError(
                f"Request class {request_class_name!r} must declare "
                "latency_boundary with latency_components."
            )
        latency_boundaries[request_class_name] = boundary
        if ("calls" in request_class) != ("call_execution" in request_class):
            raise ValueError(
                f"Request class {request_class_name!r} must define "
                "call_execution exactly when it defines calls."
            )
        weighted = "concurrency_weight" in request_class
        weighted_classes.append(weighted)
        if weighted:
            concurrency_weights[request_class_name] = _finite_positive(
                request_class["concurrency_weight"],
                (f"request_classes.{request_class_name}." "concurrency_weight"),
            )
        has_slos = "end_to_end_latency_slos" in request_class
        if has_slos:
            label = f"request_classes.{request_class_name}.end_to_end_latency_slos"
            raw_slos = request_class["end_to_end_latency_slos"]
            if not isinstance(raw_slos, Mapping) or set(raw_slos) not in (
                {"mean_seconds"},
                {"percentile"},
            ):
                raise ValueError(
                    f"{label} must choose exactly one of mean_seconds or percentile."
                )
            if "mean_seconds" in raw_slos:
                latency_slos[request_class_name] = EndToEndLatencySLOSpec(
                    mean_seconds=_finite_positive(
                        raw_slos["mean_seconds"],
                        f"{label}.mean_seconds",
                    )
                )
            else:
                raw_percentile = raw_slos["percentile"]
                if not isinstance(raw_percentile, Mapping) or set(raw_percentile) != {
                    "quantile",
                    "maximum_seconds",
                }:
                    raise ValueError(
                        f"{label}.percentile must contain exactly quantile and "
                        "maximum_seconds."
                    )
                quantile = raw_percentile["quantile"]
                if (
                    isinstance(quantile, bool)
                    or not isinstance(quantile, (int, float))
                    or not math.isfinite(float(quantile))
                    or not 0.0 < float(quantile) < 1.0
                ):
                    raise ValueError(
                        f"{label}.percentile.quantile must be between 0 and 1."
                    )
                quantile = float(quantile)
                if not math.isclose(quantile, 0.95, rel_tol=0.0, abs_tol=1e-12):
                    raise ValueError(
                        f"{label}.percentile.quantile must be 0.95 because the "
                        "simulation evaluator currently estimates p95."
                    )
                latency_slos[request_class_name] = EndToEndLatencySLOSpec(
                    percentile_quantile=quantile,
                    percentile_maximum_seconds=_finite_positive(
                        raw_percentile["maximum_seconds"],
                        f"{label}.percentile.maximum_seconds",
                    ),
                )
        overhead_label = f"request_classes.{request_class_name}.latency_overhead"
        if "latency_components" in request_class:
            raw_components = request_class["latency_components"]
            allowed_components = {
                "client_to_ingress",
                "client_to_entry",
                "ingress_processing",
                "unmodeled_residual",
            }
            if not isinstance(raw_components, Mapping) or not raw_components:
                raise ValueError(
                    f"request_classes.{request_class_name}.latency_components "
                    "must be a nonempty mapping."
                )
            unknown_components = set(raw_components) - allowed_components
            if unknown_components:
                raise ValueError(
                    f"request_classes.{request_class_name}.latency_components "
                    f"contains unsupported components {sorted(unknown_components)}."
                )
            if (
                boundary == "ingress"
                and ({"client_to_ingress", "client_to_entry"} & set(raw_components))
            ):
                raise ValueError(
                    f"Request class {request_class_name!r} cannot include "
                    "a client path component at the ingress latency boundary."
                )
            if "client_to_entry" in raw_components and (
                {"client_to_ingress", "ingress_processing"} & set(raw_components)
            ):
                raise ValueError(
                    f"Request class {request_class_name!r} cannot combine "
                    "client_to_entry with finer ingress-boundary components."
                )
            components = {
                component_name: parse_duration_distribution(
                    raw_specification,
                    (
                        f"request_classes.{request_class_name}.latency_components."
                        f"{component_name}"
                    ),
                    base_path=base_path,
                )
                for component_name, raw_specification in raw_components.items()
            }
            latency_component_specs[request_class_name] = _freeze_mapping(components)
            fixed_latency_overheads[request_class_name] = sum(
                specification.mean for specification in components.values()
            )
        elif "latency_overhead" in request_class:
            overhead_spec = _parse_distribution(
                request_class["latency_overhead"],
                overhead_label,
                quantity="service_time",
                base_path=base_path,
            )
            latency_overhead_specs[request_class_name] = overhead_spec
            fixed_latency_overheads[request_class_name] = overhead_spec.mean
        else:
            fixed_overhead = _finite_nonnegative(
                request_class.get("fixed_latency_overhead_seconds", 0.0),
                (
                    f"request_classes.{request_class_name}."
                    "fixed_latency_overhead_seconds"
                ),
            )
            fixed_latency_overheads[request_class_name] = fixed_overhead
            if fixed_overhead > 0.0:
                specification = {
                    "distribution": "deterministic",
                    "mean": fixed_overhead,
                }
                latency_overhead_specs[request_class_name] = DistributionSpec(
                    distribution="deterministic",
                    mean=fixed_overhead,
                    scv=0.0,
                    parameters=_parameter_items(specification),
                )
        end_to_end_timeouts[request_class_name] = _finite_nonnegative(
            request_class.get("end_to_end_timeout_seconds", 0.0),
            (f"request_classes.{request_class_name}." "end_to_end_timeout_seconds"),
        )
        if "maximum_terminal_failure_rate" in request_class:
            failure_rate = _finite_nonnegative(
                request_class["maximum_terminal_failure_rate"],
                (
                    f"request_classes.{request_class_name}."
                    "maximum_terminal_failure_rate"
                ),
            )
            if failure_rate >= 1.0:
                raise ValueError(
                    f"request_classes.{request_class_name}."
                    "maximum_terminal_failure_rate must be less than one."
                )
            if end_to_end_timeouts[request_class_name] <= 0.0:
                raise ValueError(
                    f"Request class {request_class_name!r} declares a terminal "
                    "failure-rate SLO but no positive end_to_end_timeout_seconds."
                )
            failure_rate_slos[request_class_name] = failure_rate
        if "workflow" in request_class:
            raw_workflow = request_class["workflow"]
            if not isinstance(raw_workflow, list) or not raw_workflow:
                raise ValueError(
                    f"request_classes.{request_class_name}.workflow must be a "
                    "nonempty list."
                )
            workflow = []
            workflow_visits = {}
            for index, raw_visit in enumerate(raw_workflow):
                label = f"request_classes.{request_class_name}.workflow[{index}]"
                if not isinstance(raw_visit, Mapping) or set(raw_visit) not in (
                    {"service"},
                    {"service", "mean_visits"},
                ):
                    raise ValueError(
                        f"{label} must contain service and optional mean_visits."
                    )
                service_name = raw_visit.get("service")
                if not isinstance(service_name, str) or not service_name:
                    raise ValueError(f"{label}.service must be a nonempty string.")
                if service_name not in service_names:
                    raise ValueError(
                        f"{label} references unknown service {service_name!r}."
                    )
                mean_visits = _finite_positive(
                    raw_visit.get("mean_visits", 1.0),
                    f"{label}.mean_visits",
                )
                workflow.append(WorkflowVisitSpec(service_name, mean_visits))
                workflow_visits[service_name] = (
                    workflow_visits.get(service_name, 0.0) + mean_visits
                )
            workflows[request_class_name] = tuple(workflow)
            visit_ratios[request_class_name] = _freeze_mapping(workflow_visits)
            parsed[request_class_name] = _freeze_mapping({})
            continue

        if "calls" in request_class:
            call_execution = request_class["call_execution"]
            if call_execution not in {"parallel", "sequential"}:
                raise ValueError(
                    f"request_classes.{request_class_name}.call_execution must "
                    "be parallel or sequential."
                )
            raw_calls = request_class["calls"]
            if not isinstance(raw_calls, Mapping):
                raise ValueError(
                    f"request_classes.{request_class_name}.calls must be a mapping."
                )
            unknown_sources = set(raw_calls) - service_names
            if unknown_sources:
                raise ValueError(
                    f"Request class {request_class_name!r} calls from unknown "
                    f"services: {sorted(unknown_sources)}."
                )
            class_calls = {}
            for source, raw_targets in raw_calls.items():
                label = f"request_classes.{request_class_name}.calls.{source}"
                if not isinstance(raw_targets, Mapping):
                    raise ValueError(f"{label} must be a mapping.")
                unknown_targets = set(raw_targets) - service_names
                if unknown_targets:
                    raise ValueError(
                        f"Request class {request_class_name!r} calls unknown "
                        f"services: {sorted(unknown_targets)}."
                    )
                class_calls[source] = _freeze_mapping(
                    {
                        target: _finite_positive(
                            mean_calls,
                            f"{label}.{target}",
                        )
                        for target, mean_calls in raw_targets.items()
                    }
                )
            call_graphs[request_class_name] = _freeze_mapping(class_calls)
            call_execution_modes[request_class_name] = call_execution
            parsed[request_class_name] = _freeze_mapping({})
            continue

        if "visits" in request_class:
            raw_visits = request_class["visits"]
            if not isinstance(raw_visits, Mapping) or not raw_visits:
                raise ValueError(
                    f"request_classes.{request_class_name}.visits must be a "
                    "nonempty mapping."
                )
            unknown_services = set(raw_visits) - service_names
            if unknown_services:
                raise ValueError(
                    f"Request class {request_class_name!r} visits unknown "
                    f"services: {sorted(unknown_services)}."
                )
            visit_ratios[request_class_name] = _freeze_mapping(
                {
                    service_name: _finite_positive(
                        visits,
                        (
                            f"request_classes.{request_class_name}.visits."
                            f"{service_name}"
                        ),
                    )
                    for service_name, visits in raw_visits.items()
                }
            )
            parsed[request_class_name] = _freeze_mapping({})
            continue

        routing = request_class["routing"]
        if not isinstance(routing, Mapping):
            raise ValueError(
                f"request_classes.{request_class_name}.routing must be a mapping."
            )
        unknown_sources = set(routing) - service_names
        if unknown_sources:
            raise ValueError(
                f"Request class {request_class_name!r} routes from unknown "
                f"services: {sorted(unknown_sources)}."
            )
        class_routing = {}
        for source, raw_routes in routing.items():
            if not isinstance(raw_routes, Mapping):
                raise ValueError(
                    f"request_classes.{request_class_name}.routing.{source} "
                    "must be a mapping."
                )
            unknown_targets = set(raw_routes) - service_names
            if unknown_targets:
                raise ValueError(
                    f"Request class {request_class_name!r} routes to unknown "
                    f"services: {sorted(unknown_targets)}."
                )
            routes = {
                target: _finite_nonnegative(
                    probability,
                    f"request_classes.{request_class_name}.routing.{source}.{target}",
                )
                for target, probability in raw_routes.items()
            }
            if sum(routes.values()) > 1.0 + 1e-12:
                raise ValueError(
                    f"Routing probabilities from {source!r} for request class "
                    f"{request_class_name!r} exceed one."
                )
            class_routing[source] = _freeze_mapping(routes)
        parsed[request_class_name] = _freeze_mapping(class_routing)
    if any(weighted_classes) and not all(weighted_classes):
        raise ValueError(
            "Every request class must define concurrency_weight when any "
            "request class defines it."
        )
    if require_concurrency_weights and not all(weighted_classes):
        raise ValueError(
            "Every request class in a single-model workload must define "
            "concurrency_weight."
        )
    slo_kinds = {slo.kind for slo in latency_slos.values()}
    if len(slo_kinds) > 1:
        raise ValueError(
            "All declared request-class end_to_end_latency_slos must use the "
            "same mean or percentile kind."
        )
    if (
        weighted_classes
        and all(weighted_classes)
        and not math.isclose(
            sum(concurrency_weights.values()),
            1.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise ValueError("Request-class concurrency weights must sum to one.")
    return (
        _freeze_mapping(parsed),
        _freeze_mapping(visit_ratios),
        _freeze_mapping(workflows),
        _freeze_mapping(call_graphs),
        _freeze_mapping(call_execution_modes),
        _freeze_mapping(fixed_latency_overheads),
        _freeze_mapping(latency_overhead_specs),
        _freeze_mapping(latency_boundaries),
        _freeze_mapping(latency_component_specs),
        _freeze_mapping(end_to_end_timeouts),
        _freeze_mapping(concurrency_weights),
        _freeze_mapping(latency_slos),
        _freeze_mapping(failure_rate_slos),
    )


def parse_workload_spec(workload, *, base_path=None):
    """Parse and validate the repository's single traffic-model schema."""

    if not isinstance(workload, Mapping):
        raise ValueError("A structured workload must be a mapping.")
    expected_top_level = {"request_classes", "traffic_model"}
    if set(workload) != expected_top_level:
        raise ValueError(
            "A workload must contain exactly request_classes and traffic_model."
        )
    raw_models = {SINGLE_TRAFFIC_MODEL_NAME: workload["traffic_model"]}

    request_classes = workload["request_classes"]
    if not isinstance(request_classes, Mapping) or not request_classes:
        raise ValueError("request_classes must be a nonempty mapping.")
    if not isinstance(raw_models, Mapping) or not raw_models:
        raise ValueError("traffic_model must be a nonempty mapping.")

    first_model = next(iter(raw_models.values()))
    if not isinstance(first_model, Mapping):
        raise ValueError("Every traffic model must be a mapping.")
    raw_first_services = first_model.get("service_times")
    if not isinstance(raw_first_services, Mapping) or not raw_first_services:
        raise ValueError("Every traffic model needs a nonempty service_times mapping.")
    service_names = tuple(raw_first_services)
    service_name_set = set(service_names)
    if any(not isinstance(name, str) or not name for name in service_names):
        raise ValueError("Service names must be nonempty strings.")
    (
        routing,
        visit_ratios,
        workflows,
        call_graphs,
        call_execution_modes,
        fixed_latency_overheads,
        latency_overhead_specs,
        latency_boundaries,
        latency_component_specs,
        end_to_end_timeouts,
        concurrency_weights,
        request_class_latency_slos,
        request_class_failure_rate_slos,
    ) = _parse_request_classes(
        request_classes,
        service_name_set,
        require_concurrency_weights=True,
        base_path=base_path,
    )

    models = {}
    for model_name, model in raw_models.items():
        if not isinstance(model_name, str) or not model_name:
            raise ValueError("Traffic-model names must be nonempty strings.")
        required_model_fields = {"entry_services", "service_times"}
        optional_model_fields = {"input_uncertainty", "communication_latency"}
        if (
            not isinstance(model, Mapping)
            or not required_model_fields <= set(model)
            or set(model) - required_model_fields - optional_model_fields
        ):
            raise ValueError(
                f"Traffic model {model_name!r} must contain entry_services and "
                "service_times, with optional input_uncertainty and "
                "communication_latency."
            )
        raw_service_times = model["service_times"]
        if (
            not isinstance(raw_service_times, Mapping)
            or set(raw_service_times) != service_name_set
        ):
            raise ValueError(
                f"Traffic model {model_name!r}.service_times must define the same "
                f"services as every other traffic model: {list(service_names)}."
            )
        model_path = "traffic_model"
        service_times = {}
        service_blocking_times = {}
        service_execution_models = {}
        service_replica_queue_models = {}
        processing_slots_per_replica = {}
        concurrency_limits_per_replica = {}
        service_times_by_request_class = {
            request_class_name: {} for request_class_name in request_classes
        }
        service_blocking_times_by_request_class = {
            request_class_name: {} for request_class_name in request_classes
        }
        for service_name in service_names:
            raw_service_time = raw_service_times[service_name]
            service_path = f"{model_path}.service_times.{service_name}"
            if not isinstance(raw_service_time, Mapping):
                raise ValueError(f"{service_path} must be a mapping.")
            execution_model = raw_service_time.get("execution_model", "fixed_slots")
            if execution_model not in {"fixed_slots", "processor_sharing"}:
                raise ValueError(
                    f"{service_path}.execution_model must be fixed_slots or "
                    "processor_sharing."
                )
            replica_queue_model = raw_service_time.get(
                "replica_queue_model",
                "independent_round_robin",
            )
            if replica_queue_model not in {
                "pooled",
                "independent_random",
                "independent_round_robin",
            }:
                raise ValueError(
                    f"{service_path}.replica_queue_model must be pooled, "
                    "independent_random, or independent_round_robin."
                )
            processing_slots = _positive_integer(
                raw_service_time.get("processing_slots_per_replica", 1),
                f"{service_path}.processing_slots_per_replica",
            )
            if execution_model == "processor_sharing" and processing_slots != 1:
                raise ValueError(
                    f"{service_path}.processing_slots_per_replica must be one "
                    "for processor_sharing; CPU contention is represented by "
                    "the processor-sharing kernel itself."
                )
            raw_concurrency_limit = raw_service_time.get(
                "concurrency_limit_per_replica"
            )
            concurrency_limit = (
                None
                if raw_concurrency_limit is None
                and execution_model == "processor_sharing"
                else _positive_integer(
                    (
                        processing_slots
                        if raw_concurrency_limit is None
                        else raw_concurrency_limit
                    ),
                    f"{service_path}.concurrency_limit_per_replica",
                )
            )
            if concurrency_limit is not None and concurrency_limit < processing_slots:
                raise ValueError(
                    f"{service_path}.concurrency_limit_per_replica cannot be "
                    "smaller than processing_slots_per_replica."
                )
            processing_slots_per_replica[service_name] = processing_slots
            concurrency_limits_per_replica[service_name] = concurrency_limit
            service_execution_models[service_name] = execution_model
            service_replica_queue_models[service_name] = replica_queue_model
            raw_blocking_time = raw_service_time.get("non_cpu_blocking_time")
            if raw_blocking_time is not None:
                blocking_path = f"{service_path}.non_cpu_blocking_time"
                if not isinstance(raw_blocking_time, Mapping):
                    raise ValueError(f"{blocking_path} must be a mapping.")
                if "distribution" in raw_blocking_time:
                    parsed_blocking_time = parse_duration_distribution(
                        raw_blocking_time,
                        blocking_path,
                        base_path=base_path,
                    )
                    service_blocking_times[service_name] = parsed_blocking_time
                    for request_class_name in request_classes:
                        service_blocking_times_by_request_class[
                            request_class_name
                        ][service_name] = parsed_blocking_time
                else:
                    unknown_classes = set(raw_blocking_time) - set(request_classes)
                    if not raw_blocking_time or unknown_classes:
                        raise ValueError(
                            f"{blocking_path} must be one duration distribution "
                            "or a nonempty request-class mapping; unknown request "
                            f"classes={sorted(unknown_classes)}."
                        )
                    parsed_blocking_times = {
                        request_class_name: parse_duration_distribution(
                            specification,
                            f"{blocking_path}.{request_class_name}",
                            base_path=base_path,
                        )
                        for request_class_name, specification in (
                            raw_blocking_time.items()
                        )
                    }
                    for request_class_name, specification in (
                        parsed_blocking_times.items()
                    ):
                        service_blocking_times_by_request_class[
                            request_class_name
                        ][service_name] = specification
            distribution_specification = {
                key: value
                for key, value in raw_service_time.items()
                if key
                not in {
                    "processing_slots_per_replica",
                    "concurrency_limit_per_replica",
                    "execution_model",
                    "replica_queue_model",
                    "non_cpu_blocking_time",
                }
            }
            if "distribution" in distribution_specification:
                parsed_service_time = _parse_distribution(
                    distribution_specification,
                    service_path,
                    quantity="service_time",
                    base_path=base_path,
                )
                service_times[service_name] = parsed_service_time
                for request_class_name in request_classes:
                    service_times_by_request_class[request_class_name][
                        service_name
                    ] = parsed_service_time
                continue

            unknown_classes = set(distribution_specification) - set(request_classes)
            if not distribution_specification or unknown_classes:
                raise ValueError(
                    f"{service_path} must be one distribution or a nonempty "
                    "request-class mapping; unknown request classes="
                    f"{sorted(unknown_classes)}."
                )
            parsed_by_class = {
                request_class_name: _parse_distribution(
                    specification,
                    f"{service_path}.{request_class_name}",
                    quantity="service_time",
                    base_path=base_path,
                )
                for request_class_name, specification in (
                    distribution_specification.items()
                )
            }
            service_times[service_name] = next(iter(parsed_by_class.values()))
            for request_class_name, specification in parsed_by_class.items():
                service_times_by_request_class[request_class_name][
                    service_name
                ] = specification

        raw_entries = model["entry_services"]
        if not isinstance(raw_entries, Mapping) or set(raw_entries) != set(
            request_classes
        ):
            missing = (
                set(request_classes) - set(raw_entries)
                if isinstance(raw_entries, Mapping)
                else set(request_classes)
            )
            unknown = (
                set(raw_entries) - set(request_classes)
                if isinstance(raw_entries, Mapping)
                else set()
            )
            raise ValueError(
                f"Traffic model {model_name!r}.entry_services must define every "
                f"request class exactly once; missing={sorted(missing)}, "
                f"unknown={sorted(unknown)}."
            )
        entry_services = {}
        for request_class_name, raw_streams in raw_entries.items():
            if not isinstance(raw_streams, Mapping) or not raw_streams:
                raise ValueError(
                    f"Entry services for {request_class_name!r} must be a "
                    "nonempty mapping."
                )
            unknown_services = set(raw_streams) - service_name_set
            if unknown_services:
                raise ValueError(
                    f"Entry services for {request_class_name!r} reference unknown "
                    f"services: {sorted(unknown_services)}."
                )
            if (
                request_class_name in visit_ratios
                and request_class_name not in workflows
            ):
                missing_entry_visits = set(raw_streams) - set(
                    visit_ratios[request_class_name]
                )
                if missing_entry_visits:
                    raise ValueError(
                        f"Request class {request_class_name!r} must include every "
                        "external entry service in visits; missing="
                        f"{sorted(missing_entry_visits)}."
                    )
            entry_services[request_class_name] = _freeze_mapping(
                {
                    service_name: _parse_distribution(
                        arrival_process,
                        (
                            f"{model_path}.entry_services."
                            f"{request_class_name}.{service_name}"
                        ),
                        quantity="interarrival",
                        base_path=base_path,
                    )
                    for service_name, arrival_process in raw_streams.items()
                }
            )
        for request_class_name in request_classes:
            if request_class_name in workflows:
                entry_names = tuple(entry_services[request_class_name])
                first_service = workflows[request_class_name][0].service
                if len(entry_names) != 1 or entry_names[0] != first_service:
                    raise ValueError(
                        f"Request class {request_class_name!r} workflow must start "
                        "at its single external entry service."
                    )
                if workflows[request_class_name][0].mean_visits < 1.0:
                    raise ValueError(
                        f"Request class {request_class_name!r} first workflow visit "
                        "must have mean_visits greater than or equal to one."
                    )
            if request_class_name in call_graphs:
                reachable = _reachable_services(
                    entry_services[request_class_name],
                    call_graphs[request_class_name],
                )
            elif request_class_name in visit_ratios:
                reachable = set(visit_ratios[request_class_name])
            else:
                reachable = _reachable_services(
                    entry_services[request_class_name],
                    routing[request_class_name],
                )
            missing_service_times = reachable - set(
                service_times_by_request_class[request_class_name]
            )
            if missing_service_times:
                raise ValueError(
                    f"Traffic model {model_name!r} has no service-time "
                    f"distribution for request class {request_class_name!r} at "
                    f"services {sorted(missing_service_times)}."
                )
        models[model_name] = TrafficModelSpec(
            name=model_name,
            entry_services=_freeze_mapping(entry_services),
            service_times=_freeze_mapping(service_times),
            service_times_by_request_class=_freeze_mapping(
                {
                    request_class_name: _freeze_mapping(specifications)
                    for request_class_name, specifications in (
                        service_times_by_request_class.items()
                    )
                }
            ),
            service_blocking_times=_freeze_mapping(service_blocking_times),
            service_blocking_times_by_request_class=_freeze_mapping(
                {
                    request_class_name: _freeze_mapping(specifications)
                    for request_class_name, specifications in (
                        service_blocking_times_by_request_class.items()
                    )
                }
            ),
            service_execution_models=_freeze_mapping(service_execution_models),
            service_replica_queue_models=_freeze_mapping(
                service_replica_queue_models
            ),
            processing_slots_per_replica=_freeze_mapping(processing_slots_per_replica),
            concurrency_limits_per_replica=_freeze_mapping(
                concurrency_limits_per_replica
            ),
            input_uncertainty=_parse_joint_input_uncertainty(
                model.get("input_uncertainty"),
                request_classes=request_classes,
                service_names=service_names,
                label=f"Traffic model {model_name!r}.input_uncertainty",
            ),
            communication_latency=_parse_communication_latency(
                model.get("communication_latency"),
                f"Traffic model {model_name!r}.communication_latency",
                base_path=base_path,
            ),
        )

    return ParsedWorkload(
        request_classes=routing,
        request_class_visit_ratios=visit_ratios,
        request_class_workflows=workflows,
        request_class_call_graphs=call_graphs,
        request_class_call_execution_modes=call_execution_modes,
        request_class_fixed_latency_overheads=fixed_latency_overheads,
        request_class_latency_overhead_specs=latency_overhead_specs,
        request_class_latency_boundaries=latency_boundaries,
        request_class_latency_component_specs=latency_component_specs,
        request_class_end_to_end_timeouts=end_to_end_timeouts,
        concurrency_weights=concurrency_weights,
        request_class_latency_slos=request_class_latency_slos,
        request_class_failure_rate_slos=request_class_failure_rate_slos,
        traffic_model=models[SINGLE_TRAFFIC_MODEL_NAME],
        service_names=service_names,
    )


def normalize_workload_spec(workload, *, base_path=None):
    """Return the mean-rate request-class mapping consumed by ``System``."""

    if not isinstance(workload, Mapping) or not ("traffic_model" in workload):
        return copy.deepcopy(workload)

    parsed = parse_workload_spec(workload, base_path=base_path)
    traffic_model = parsed.traffic_model

    entry_rates = {
        request_class_name: {
            service_name: process.rate for service_name, process in streams.items()
        }
        for request_class_name, streams in traffic_model.entry_services.items()
    }
    class_totals = {
        request_class_name: sum(rates.values())
        for request_class_name, rates in entry_rates.items()
    }
    total_rate = sum(class_totals.values())
    concurrency_weights = (
        dict(parsed.concurrency_weights)
        if parsed.concurrency_weights
        else {
            request_class_name: class_rate / total_rate
            for request_class_name, class_rate in class_totals.items()
        }
    )

    normalized = {}
    for request_class_name in parsed.request_classes:
        class_routing = parsed.request_classes[request_class_name]
        class_visits = parsed.request_class_visit_ratios.get(request_class_name)
        class_calls = parsed.request_class_call_graphs.get(request_class_name)
        if class_calls is not None:
            reachable = _reachable_services(
                entry_rates[request_class_name],
                class_calls,
            )
        elif class_visits is not None:
            reachable = set(entry_rates[request_class_name]) | set(class_visits)
        else:
            reachable = _reachable_services(
                entry_rates[request_class_name],
                class_routing,
            )
        normalized_services = {}
        for service_name in parsed.service_names:
            if service_name not in reachable:
                continue
            normalized_services[service_name] = {
                "gamma": entry_rates[request_class_name].get(service_name, 0.0),
                "mu": traffic_model.service_times_by_request_class[request_class_name][
                    service_name
                ].rate,
                "routes": dict(class_routing.get(service_name, {})),
            }
        normalized[request_class_name] = {
            "concurrency_weight": concurrency_weights[request_class_name],
            "services": normalized_services,
        }
    return normalized

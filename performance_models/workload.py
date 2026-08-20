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

    @property
    def rate(self):
        return 1.0 / self.mean


@dataclass(frozen=True)
class TrafficModelSpec:
    name: str
    entry_services: Mapping
    service_times: Mapping


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
    concurrency_weights: Mapping
    request_class_latency_slos: Mapping
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
        scale = _finite_positive(
            specification[scale_field], f"{label}.{scale_field}"
        )
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
        scale = _finite_positive(
            specification[scale_field], f"{label}.{scale_field}"
        )
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
        scale = _finite_positive(
            specification[scale_field], f"{label}.{scale_field}"
        )
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
        scale = _finite_positive(
            specification[scale_field], f"{label}.{scale_field}"
        )
        mean = 1.0 / scale if scale_field == "rate" else scale
        return DistributionSpec(
            distribution,
            mean,
            _weibull_scv(shape),
            _parameter_items(specification),
        )

    if distribution == empirical_name:
        expected = {"distribution", "source", "column", "sampling"}
        if set(specification) != expected:
            raise ValueError(
                f"{label} must contain exactly distribution, source, column, "
                "and sampling."
            )
        if specification["sampling"] != "iid_with_replacement":
            raise ValueError(f"{label}.sampling must be 'iid_with_replacement'.")
        values = _csv_values(
            specification["source"], specification["column"], label, base_path
        )
        mean, scv = _sample_moments(values, f"{label}.source values")
        source = Path(specification["source"])
        resolved = source if source.is_absolute() else Path(base_path or ".") / source
        return DistributionSpec(
            distribution,
            mean,
            scv,
            _parameter_items(specification, resolved_source=resolved.resolve()),
            tuple(values),
        )

    if distribution == "arrival_trace" and quantity == "interarrival":
        expected = {
            "distribution",
            "source",
            "timestamp_column",
            "sampling",
            "block_duration",
        }
        if set(specification) != expected:
            raise ValueError(
                f"{label} must contain exactly distribution, source, "
                "timestamp_column, sampling, and block_duration."
            )
        if specification["sampling"] != "block_bootstrap":
            raise ValueError(f"{label}.sampling must be 'block_bootstrap'.")
        _finite_positive(
            specification["block_duration"], f"{label}.block_duration"
        )
        timestamps = _csv_values(
            specification["source"],
            specification["timestamp_column"],
            label,
            base_path,
        )
        if len(timestamps) < 2 or any(
            right <= left for left, right in zip(timestamps, timestamps[1:])
        ):
            raise ValueError(
                f"{label}.source timestamps must contain at least two strictly "
                "increasing values."
            )
        interarrivals = tuple(
            right - left for left, right in zip(timestamps, timestamps[1:])
        )
        mean, scv = _sample_moments(interarrivals, f"{label}.source intervals")
        source = Path(specification["source"])
        resolved = source if source.is_absolute() else Path(base_path or ".") / source
        return DistributionSpec(
            distribution,
            mean,
            scv,
            _parameter_items(specification, resolved_source=resolved.resolve()),
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
        scale = _finite_positive(
            specification[scale_field], f"{label}.{scale_field}"
        )
        scv = _finite_nonnegative(specification["scv"], f"{label}.scv")
        if specification["abi_version"] != CUSTOM_DISTRIBUTION_ABI_VERSION:
            raise ValueError(
                f"{label}.abi_version must be "
                f"{CUSTOM_DISTRIBUTION_ABI_VERSION}."
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
):
    parsed = {}
    concurrency_weights = {}
    latency_slos = {}
    weighted_classes = []
    for request_class_name, request_class in request_classes.items():
        if not isinstance(request_class_name, str) or not request_class_name:
            raise ValueError("Request-class names must be nonempty strings.")
        if not isinstance(request_class, Mapping):
            raise ValueError(
                f"Request class {request_class_name!r} must be a mapping."
            )
        allowed_fields = {
            "concurrency_weight",
            "end_to_end_latency_slos",
            "routing",
        }
        unknown_fields = set(request_class) - allowed_fields
        if "routing" not in request_class or unknown_fields:
            raise ValueError(
                f"Request class {request_class_name!r} must contain routing and "
                "may contain concurrency_weight and end_to_end_latency_slos; "
                f"unknown fields={sorted(unknown_fields)}."
            )
        weighted = "concurrency_weight" in request_class
        weighted_classes.append(weighted)
        if weighted:
            concurrency_weights[request_class_name] = _finite_positive(
                request_class["concurrency_weight"],
                (
                    f"request_classes.{request_class_name}."
                    "concurrency_weight"
                ),
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
                if (
                    not isinstance(raw_percentile, Mapping)
                    or set(raw_percentile) != {"quantile", "maximum_seconds"}
                ):
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
    if weighted_classes and all(weighted_classes) and not math.isclose(
        sum(concurrency_weights.values()),
        1.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ValueError("Request-class concurrency weights must sum to one.")
    return (
        _freeze_mapping(parsed),
        _freeze_mapping(concurrency_weights),
        _freeze_mapping(latency_slos),
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
    routing, concurrency_weights, request_class_latency_slos = _parse_request_classes(
        request_classes,
        service_name_set,
        require_concurrency_weights=True,
    )

    models = {}
    for model_name, model in raw_models.items():
        if not isinstance(model_name, str) or not model_name:
            raise ValueError("Traffic-model names must be nonempty strings.")
        if not isinstance(model, Mapping) or set(model) != {
            "entry_services",
            "service_times",
        }:
            raise ValueError(
                f"Traffic model {model_name!r} must contain exactly entry_services "
                "and service_times."
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
        service_times = {
            service_name: _parse_distribution(
                raw_service_times[service_name],
                f"{model_path}.service_times.{service_name}",
                quantity="service_time",
                base_path=base_path,
            )
            for service_name in service_names
        }

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
        models[model_name] = TrafficModelSpec(
            name=model_name,
            entry_services=_freeze_mapping(entry_services),
            service_times=_freeze_mapping(service_times),
        )

    return ParsedWorkload(
        request_classes=routing,
        concurrency_weights=concurrency_weights,
        request_class_latency_slos=request_class_latency_slos,
        traffic_model=models[SINGLE_TRAFFIC_MODEL_NAME],
        service_names=service_names,
    )


def normalize_workload_spec(workload, *, base_path=None):
    """Return the mean-rate request-class mapping consumed by ``System``."""

    if not isinstance(workload, Mapping) or not (
        "traffic_model" in workload
    ):
        return copy.deepcopy(workload)

    parsed = parse_workload_spec(workload, base_path=base_path)
    traffic_model = parsed.traffic_model

    entry_rates = {
        request_class_name: {
            service_name: process.rate
            for service_name, process in streams.items()
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
                "mu": traffic_model.service_times[service_name].rate,
                "routes": dict(class_routing.get(service_name, {})),
            }
        normalized[request_class_name] = {
            "concurrency_weight": concurrency_weights[request_class_name],
            "services": normalized_services,
        }
    return normalized

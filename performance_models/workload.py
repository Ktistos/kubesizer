import copy
import csv
import math
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType


WORKLOAD_SCHEMA_VERSION = 2


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
class ScenarioSpec:
    name: str
    entry_services: Mapping
    service_times: Mapping


@dataclass(frozen=True)
class ParsedWorkload:
    schema_version: int
    time_unit: str
    request_classes: Mapping
    concurrency_weights: Mapping
    scenarios: Mapping
    service_names: tuple[str, ...]

    def scenario(self, name):
        try:
            return self.scenarios[name]
        except KeyError as error:
            choices = ", ".join(self.scenarios)
            raise ValueError(
                f"Unknown workload scenario {name!r}; expected one of: {choices}."
            ) from error


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

    supported = (
        "exponential, deterministic, gamma, lognormal, weibull, "
        f"{empirical_name}"
        + (", arrival_trace" if quantity == "interarrival" else "")
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


def _parse_request_classes(request_classes, service_names):
    parsed = {}
    concurrency_weights = {}
    weighted_classes = []
    for request_class_name, request_class in request_classes.items():
        if not isinstance(request_class_name, str) or not request_class_name:
            raise ValueError("Request-class names must be nonempty strings.")
        if not isinstance(request_class, Mapping) or set(request_class) not in (
            {"routing"},
            {"concurrency_weight", "routing"},
        ):
            raise ValueError(
                f"Request class {request_class_name!r} must contain routing and "
                "may contain concurrency_weight."
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
    if weighted_classes and all(weighted_classes) and not math.isclose(
        sum(concurrency_weights.values()),
        1.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ValueError("Request-class concurrency weights must sum to one.")
    return _freeze_mapping(parsed), _freeze_mapping(concurrency_weights)


def parse_workload_spec(workload, *, base_path=None):
    """Parse and validate all scenarios in a versioned workload."""

    if not isinstance(workload, Mapping) or "schema_version" not in workload:
        raise ValueError("parse_workload_spec requires a versioned workload.")
    expected_top_level = {
        "schema_version",
        "time_unit",
        "request_classes",
        "scenarios",
    }
    if set(workload) != expected_top_level:
        raise ValueError(
            "Versioned workload must contain exactly schema_version, time_unit, "
            "request_classes, and scenarios."
        )
    if workload["schema_version"] != WORKLOAD_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported workload schema_version {workload['schema_version']!r}."
        )
    time_unit = workload["time_unit"]
    if not isinstance(time_unit, str) or not time_unit:
        raise ValueError("time_unit must be a nonempty string.")

    request_classes = workload["request_classes"]
    raw_scenarios = workload["scenarios"]
    if not isinstance(request_classes, Mapping) or not request_classes:
        raise ValueError("request_classes must be a nonempty mapping.")
    if not isinstance(raw_scenarios, Mapping) or not raw_scenarios:
        raise ValueError("scenarios must be a nonempty mapping.")

    first_scenario = next(iter(raw_scenarios.values()))
    if not isinstance(first_scenario, Mapping):
        raise ValueError("Every scenario must be a mapping.")
    raw_first_services = first_scenario.get("service_times")
    if not isinstance(raw_first_services, Mapping) or not raw_first_services:
        raise ValueError("Every scenario needs a nonempty service_times mapping.")
    service_names = tuple(raw_first_services)
    service_name_set = set(service_names)
    if any(not isinstance(name, str) or not name for name in service_names):
        raise ValueError("Service names must be nonempty strings.")
    routing, concurrency_weights = _parse_request_classes(
        request_classes,
        service_name_set,
    )

    scenarios = {}
    for scenario_name, scenario in raw_scenarios.items():
        if not isinstance(scenario_name, str) or not scenario_name:
            raise ValueError("Scenario names must be nonempty strings.")
        if not isinstance(scenario, Mapping) or set(scenario) != {
            "entry_services",
            "service_times",
        }:
            raise ValueError(
                f"Scenario {scenario_name!r} must contain exactly entry_services "
                "and service_times."
            )
        raw_service_times = scenario["service_times"]
        if (
            not isinstance(raw_service_times, Mapping)
            or set(raw_service_times) != service_name_set
        ):
            raise ValueError(
                f"Scenario {scenario_name!r}.service_times must define the same "
                f"services as every other scenario: {list(service_names)}."
            )
        service_times = {
            service_name: _parse_distribution(
                raw_service_times[service_name],
                f"scenarios.{scenario_name}.service_times.{service_name}",
                quantity="service_time",
                base_path=base_path,
            )
            for service_name in service_names
        }

        raw_entries = scenario["entry_services"]
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
                f"Scenario {scenario_name!r}.entry_services must define every "
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
                            f"scenarios.{scenario_name}.entry_services."
                            f"{request_class_name}.{service_name}"
                        ),
                        quantity="interarrival",
                        base_path=base_path,
                    )
                    for service_name, arrival_process in raw_streams.items()
                }
            )
        scenarios[scenario_name] = ScenarioSpec(
            name=scenario_name,
            entry_services=_freeze_mapping(entry_services),
            service_times=_freeze_mapping(service_times),
        )

    return ParsedWorkload(
        schema_version=WORKLOAD_SCHEMA_VERSION,
        time_unit=time_unit,
        request_classes=routing,
        concurrency_weights=concurrency_weights,
        scenarios=_freeze_mapping(scenarios),
        service_names=service_names,
    )


def scenario_names(workload):
    if not isinstance(workload, Mapping) or "schema_version" not in workload:
        return ()
    scenarios = workload.get("scenarios")
    if not isinstance(scenarios, Mapping):
        return ()
    return tuple(scenarios)


def select_workload_scenario(workload, scenario_name=None):
    """Return an isolated workload containing exactly one selected scenario."""

    if not isinstance(workload, Mapping) or "schema_version" not in workload:
        if scenario_name is not None:
            raise ValueError("Legacy workloads do not define named scenarios.")
        return copy.deepcopy(workload)
    names = scenario_names(workload)
    if scenario_name is None:
        if len(names) != 1:
            raise ValueError(
                "A scenario_name is required when a workload defines multiple "
                "scenarios."
            )
        scenario_name = names[0]
    if scenario_name not in names:
        choices = ", ".join(names)
        raise ValueError(
            f"Unknown workload scenario {scenario_name!r}; expected one of: {choices}."
        )
    selected = copy.deepcopy(workload)
    selected["scenarios"] = {
        scenario_name: copy.deepcopy(workload["scenarios"][scenario_name])
    }
    return selected


def normalize_workload_spec(workload, *, scenario_name=None, base_path=None):
    """Return the mean-rate request-class mapping consumed by ``System``."""

    if not isinstance(workload, Mapping) or "schema_version" not in workload:
        return copy.deepcopy(workload)

    parsed = parse_workload_spec(workload, base_path=base_path)
    if scenario_name is None:
        if len(parsed.scenarios) != 1:
            raise ValueError(
                "A scenario_name is required when a workload defines multiple "
                "scenarios."
            )
        scenario_name = next(iter(parsed.scenarios))
    scenario = parsed.scenario(scenario_name)

    entry_rates = {
        request_class_name: {
            service_name: process.rate
            for service_name, process in streams.items()
        }
        for request_class_name, streams in scenario.entry_services.items()
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
                "mu": scenario.service_times[service_name].rate,
                "routes": dict(class_routing.get(service_name, {})),
            }
        normalized[request_class_name] = {
            "concurrency_weight": concurrency_weights[request_class_name],
            "services": normalized_services,
        }
    return normalized

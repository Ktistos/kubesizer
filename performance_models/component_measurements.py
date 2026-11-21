"""Offline, explicitly mapped component evidence; no product-specific inference.

Only referenced estimates/sources are consumed. Counter totals are aggregated
before division; per-operation samples are never synthesized from window means.
"""

import copy
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
from collections.abc import Mapping


FIELDS = {
    "metric",
    "component",
    "request_class",
    "window_start",
    "window_end",
    "value",
    "unit",
    "boundary",
    "kind",
}
SELECTOR_FIELDS = {"source", "metric", "component", "request_class", "unit", "boundary"}


def _time(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            raise ValueError()
        return parsed.timestamp()
    except (ValueError, TypeError, AttributeError, OverflowError) as error:
        raise ValueError(
            "Component observation windows must use explicit UTC timestamps."
        ) from error


def _number(value, label):
    if isinstance(value, bool):
        raise ValueError(f"{label} must be finite and nonnegative.")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError(f"{label} must be numeric.") from error
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{label} must be finite and nonnegative.")
    return number


def _observation(row):
    if set(row) != FIELDS or any(
        not isinstance(row[k], str) or not row[k] for k in FIELDS - {"value"}
    ):
        raise ValueError(f"Numeric observations require exactly {sorted(FIELDS)}.")
    if row["kind"] not in {"total", "sample"}:
        raise ValueError("Observation kind must be total or per-operation sample.")
    start, end = _time(row["window_start"]), _time(row["window_end"])
    if not start < end:
        raise ValueError("Observation windows must have positive duration.")
    return {
        **row,
        "value": _number(row["value"], row["metric"]),
        "window": (start, end),
    }


def _prometheus(payload, spec):
    fields = FIELDS - {"value"}
    if (
        set(spec) != fields | {"format", "path", "query", "labels"}
        or spec["kind"] != "total"
    ):
        raise ValueError(
            "Prometheus snapshots require total-window metadata, query and exact label filters."
        )
    if (
        not isinstance(spec["query"], str)
        or not spec["query"]
        or not isinstance(spec["labels"], Mapping)
    ):
        raise ValueError("Prometheus snapshot query and label filters are required.")
    if (
        payload.get("status") != "success"
        or payload.get("data", {}).get("resultType") != "vector"
    ):
        raise ValueError("Expected a successful Prometheus instant-query vector.")
    matched = [
        r
        for r in payload["data"]["result"]
        if all(r.get("metric", {}).get(k) == v for k, v in spec["labels"].items())
    ]
    if not matched:
        raise ValueError("Prometheus snapshot has no matching series.")
    end = _time(spec["window_end"])
    values = []
    for result in matched:
        timestamp, value = result["value"]
        if abs(float(timestamp) - end) > 1:
            raise ValueError("Prometheus query evaluation time must match window_end.")
        values.append(_number(value, spec["metric"]))
    return [_observation({**{k: spec[k] for k in fields}, "value": sum(values)})]


def _otlp(payload, spec):
    fields = FIELDS - {"value"}
    if (
        set(spec) != fields | {"format", "path", "attributes"}
        or spec["kind"] != "sample"
        or spec["unit"] != "seconds"
    ):
        raise ValueError(
            "OTLP spans require sample/seconds metadata and attribute filters; they measure wall time, not CPU."
        )
    if (
        "cpu" in spec["boundary"].lower()
        or not isinstance(spec["attributes"], Mapping)
        or not spec["attributes"]
    ):
        raise ValueError(
            "OTLP wall durations cannot be declared CPU demand; use explicit span attribute filters."
        )
    start, end = _time(spec["window_start"]), _time(spec["window_end"])
    rows = []
    for resource in payload.get("resourceSpans", []):
        for scope in resource.get("scopeSpans", []):
            for span in scope.get("spans", []):
                attrs = {
                    a["key"]: a.get("value", {}).get("stringValue")
                    for a in span.get("attributes", [])
                }
                if not all(attrs.get(k) == v for k, v in spec["attributes"].items()):
                    continue
                a, b = int(span["startTimeUnixNano"]), int(span["endTimeUnixNano"])
                if b < a or not start <= a / 1e9 <= b / 1e9 <= end:
                    raise ValueError(
                        "Selected OTLP spans must fit the declared retained window."
                    )
                rows.append(
                    _observation(
                        {**{k: spec[k] for k in fields}, "value": (b - a) / 1e9}
                    )
                )
    if not rows:
        raise ValueError("No OTLP spans match the declared component/class filters.")
    return rows


def compile_component_measurements(workload, *, base_path=None):
    """Resolve typed {measurement, unit} references and return source provenance."""
    if "component_measurements" not in workload:
        return workload, {}
    result = copy.deepcopy(workload)
    manifest = result.pop("component_measurements")
    if (
        not isinstance(manifest, Mapping)
        or set(manifest) != {"version", "sources", "estimates"}
        or type(manifest["version"]) is not int
        or manifest["version"] != 1
    ):
        raise ValueError(
            "component_measurements requires version: 1, sources and estimates."
        )
    if not isinstance(manifest["sources"], Mapping) or not isinstance(
        manifest["estimates"], Mapping
    ):
        raise ValueError("Component sources and estimates must be mappings.")
    loaded, sources, estimates = {}, {}, {}

    def source(name):
        if name in loaded:
            return loaded[name]
        spec = manifest["sources"].get(name)
        if (
            not isinstance(spec, Mapping)
            or not {"path", "format"} <= set(spec)
            or not isinstance(spec["path"], str)
        ):
            raise ValueError(
                f"Unknown or incomplete component evidence source {name!r}."
            )
        path = (Path(base_path or ".") / spec["path"]).expanduser().resolve()
        try:
            content = path.read_bytes()
            text = content.decode("utf-8")
        except (OSError, UnicodeError) as error:
            raise ValueError(
                f"Cannot read component evidence {path}: {error}"
            ) from error
        if spec["format"] in {"csv", "cache_access_csv"}:
            if set(spec) != {"format", "path"}:
                raise ValueError("CSV sources require only format and path.")
            rows = list(csv.DictReader(io.StringIO(text)))
            if not rows:
                raise ValueError("Component observation source is empty.")
            if spec["format"] == "csv":
                rows = [_observation(r) for r in rows]
        elif spec["format"] in {"prometheus", "otlp_spans"}:
            try:
                payload = json.loads(text)
                rows = (_prometheus if spec["format"] == "prometheus" else _otlp)(
                    payload, spec
                )
            except (KeyError, TypeError, IndexError, json.JSONDecodeError) as error:
                raise ValueError(
                    f"Malformed {spec['format']} snapshot {path}."
                ) from error
        else:
            raise ValueError("Unknown component evidence adapter version 1 format.")
        sources[name] = {
            "adapter_version": 1,
            "mapping": dict(spec),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        loaded[name] = rows
        return rows

    def select(selector, kind):
        if not isinstance(selector, Mapping) or set(selector) != SELECTOR_FIELDS:
            raise ValueError(
                f"A numeric estimate selector needs exactly {sorted(SELECTOR_FIELDS)}."
            )
        rows = [
            r
            for r in source(selector["source"])
            if all(r.get(k) == v for k, v in selector.items() if k != "source")
            and r.get("kind") == kind
        ]
        if not rows:
            raise ValueError(
                "No observations match the requested metric, unit, boundary and class."
            )
        if kind == "total":
            windows = sorted(r["window"] for r in rows)
            if any(a[1] > b[0] for a, b in zip(windows, windows[1:])):
                raise ValueError(
                    "Counter windows must be non-overlapping; aggregate replicas within each window first."
                )
        return rows

    def estimate(name):
        spec = manifest["estimates"].get(name)
        if not isinstance(spec, Mapping) or not {"operation", "unit"} <= set(spec):
            raise ValueError(f"Unknown or incomplete component estimate {name!r}.")
        operation, unit = spec["operation"], spec["unit"]
        if operation in {"total", "rate", "samples", "sample_mean"}:
            if set(spec) != {"operation", "unit", "select"}:
                raise ValueError(f"{operation} requires select and unit.")
            selector = spec["select"]
            rows = select(
                selector,
                "sample" if operation in {"samples", "sample_mean"} else "total",
            )
            values = [r["value"] for r in rows]
            expected = (
                selector["unit"] + "_per_second"
                if operation == "rate"
                else selector["unit"]
            )
            if unit != expected:
                raise ValueError(
                    "Estimate output units do not match the selected observations."
                )
            value = sum(values)
            if operation == "rate":
                value /= sum(b - a for a, b in (r["window"] for r in rows))
            elif operation == "sample_mean":
                value /= len(values)
            elif operation == "samples":
                if unit != "seconds":
                    raise ValueError(
                        "Only per-operation duration samples can form empirical durations."
                    )
                value = {
                    "distribution": "empirical",
                    "samples": values,
                    "sampling": "iid_with_replacement",
                }
        elif operation == "transfer_samples":
            if (
                set(spec)
                != {"operation", "unit", "select", "capacity_bytes_per_second"}
                or unit != "seconds"
            ):
                raise ValueError(
                    "Transfer samples require byte samples, declared capacity_bytes_per_second and seconds output."
                )
            rows = select(spec["select"], "sample")
            capacity = _number(spec["capacity_bytes_per_second"], "link capacity")
            if (
                capacity <= 0
                or spec["select"]["unit"] != "bytes"
                or any(r["value"] <= 0 for r in rows)
            ):
                raise ValueError(
                    "Transfer work requires positive payload bytes and link capacity."
                )
            value = {
                "distribution": "empirical",
                "sampling": "iid_with_replacement",
                "samples": [r["value"] / capacity for r in rows],
            }
        elif operation == "ratio":
            if set(spec) != {"operation", "unit", "numerator", "denominator"}:
                raise ValueError("ratio requires numerator, denominator and unit.")
            a, b = spec["numerator"], spec["denominator"]
            numerator, denominator = select(a, "total"), select(b, "total")
            expected = {
                ("cpu_seconds", "requests"): "seconds",
                ("seconds", "requests"): "seconds",
                ("bytes", "requests"): "bytes",
                ("requests", "requests"): "probability",
            }.get((a["unit"], b["unit"]))
            if (
                unit != expected
                or any(a[k] != b[k] for k in ("component", "request_class"))
                or sorted(r["window"] for r in numerator)
                != sorted(r["window"] for r in denominator)
            ):
                raise ValueError(
                    "Ratio estimates need compatible units and the same component, class and retained windows."
                )
            total = sum(r["value"] for r in denominator)
            if total <= 0:
                raise ValueError(
                    "Cannot estimate demand or hit ratios without completed observations."
                )
            value = sum(r["value"] for r in numerator) / total
            if unit == "probability" and value > 1:
                raise ValueError(
                    "Observed hit/failure counts exceed their declared denominator."
                )
        elif operation in {"key_sizes", "key_accesses"}:
            fields = {"operation", "unit", "source", "component", "request_class"}
            if set(spec) != fields or unit != (
                "key_sizes_bytes" if operation == "key_sizes" else "key_sequence"
            ):
                raise ValueError(
                    "Key estimates need source, component, class and the correct catalog/sequence unit."
                )
            if (
                manifest["sources"].get(spec["source"], {}).get("format")
                != "cache_access_csv"
            ):
                raise ValueError("Key profiles require a cache_access_csv source.")
            rows = [
                r
                for r in source(spec["source"])
                if r.get("component") == spec["component"]
                and (
                    spec["request_class"] == "*"
                    or r.get("request_class") == spec["request_class"]
                )
            ]
            catalog, ordered = {}, []
            for row in rows:
                if set(row) != {
                    "component",
                    "request_class",
                    "window_start",
                    "window_end",
                    "timestamp",
                    "key",
                    "size_bytes",
                }:
                    raise ValueError(
                        "Cache accesses require component/class, UTC window, timestamp, key and size_bytes."
                    )
                start, end, timestamp = (
                    _time(row[k]) for k in ("window_start", "window_end", "timestamp")
                )
                size = _number(row["size_bytes"], "size_bytes")
                if (
                    not start <= timestamp < end
                    or size <= 0
                    or size != int(size)
                    or not row["key"]
                    or catalog.get(row["key"], int(size)) != int(size)
                ):
                    raise ValueError(
                        "Invalid cache key size, changing key size, or out-of-window access."
                    )
                catalog[row["key"]] = int(size)
                ordered.append((timestamp, len(ordered), row["key"]))
            if not rows:
                raise ValueError("No key accesses match this component/class.")
            value = (
                catalog
                if operation == "key_sizes"
                else [k for _t, _i, k in sorted(ordered)]
            )
        else:
            raise ValueError(f"Unsupported component estimate operation {operation!r}.")
        estimates[name] = {"definition": dict(spec), "value": value}
        return value, unit

    def resolve(value, path=()):
        if isinstance(value, Mapping):
            if "measurement" in value:
                if set(value) != {"measurement", "unit"} or not isinstance(
                    value["measurement"], str
                ):
                    raise ValueError(
                        "Measurement references need exactly measurement and unit."
                    )
                derived, unit = estimate(value["measurement"])
                if unit != value["unit"]:
                    raise ValueError(
                        "Measurement reference unit does not match its estimate."
                    )
                field = path[-1] if path else ""
                expected = {
                    "mean": "seconds",
                    "rate": "requests_per_second",
                    "rate_per_second": "requests_per_second",
                    "failure_probability": "probability",
                    "key_sizes_bytes": "key_sizes_bytes",
                    "keys": "key_sequence",
                }.get(field)
                if expected is not None and unit != expected:
                    raise ValueError(
                        f"{'.'.join(path)} requires {expected}, received {unit}."
                    )
                # The ordinary service-time field represents processing work.
                # Explicit byte/I/O stages and delay resources have a different
                # boundary; an inclusive wall span cannot silently become CPU.
                processing_work = False
                resources = result.get("traffic_model", {}).get("service_times", {})
                if path[:2] == ("traffic_model", "service_times") and len(path) >= 3:
                    config = resources.get(path[2], {})
                    field_path = path[3] if len(path) > 3 else "service_time"
                    processing_work = config.get(
                        "execution_model", "fixed_slots"
                    ) != "delay" and field_path not in {
                        "non_cpu_blocking_time",
                        "post_release_delay",
                        "lifecycle",
                        "cache",
                        "admission_queue",
                        "rate_limit",
                        "cpu_bandwidth",
                    }
                elif (
                    len(path) >= 6
                    and path[0] == "request_classes"
                    and path[2:4] == ("stage_workflow", "stages")
                    and path[5] == "service_time"
                ):
                    stage = result["request_classes"][path[1]]["stage_workflow"][
                        "stages"
                    ][path[4]]
                    config = resources.get(stage.get("resource"), {})
                    processing_work = (
                        config.get("execution_model", "fixed_slots") != "delay"
                        and stage.get("unit", "work") == "work"
                    )
                definition = estimates[value["measurement"]]["definition"]
                selector = definition.get("numerator", definition.get("select"))
                if processing_work and unit == "requests_per_second":
                    raise ValueError(
                        "Observed throughput is not processing capacity; use a CPU-attributed mean demand and declare its distribution shape."
                    )
                if processing_work and unit == "seconds" and selector:
                    adapter = manifest["sources"][selector["source"]]["format"]
                    cpu_evidence = selector["unit"] == "cpu_seconds" or selector[
                        "boundary"
                    ] in {"local_cpu", "cpu_demand", "cpu_phase"}
                    if adapter == "otlp_spans" or not cpu_evidence:
                        raise ValueError(
                            "Processing demand requires CPU-attributed evidence; wall response/OTLP spans belong to delay or non-CPU phases."
                        )
                if field in {
                    "concurrency_limit_per_replica",
                    "processing_slots_per_replica",
                    "observed_replicas",
                    "fixed",
                    "minimum",
                    "maximum",
                }:
                    raise ValueError(
                        "Capacity/concurrency bounds must be declared; observed activity does not identify configured limits."
                    )
                return copy.deepcopy(derived)
            return {k: resolve(v, (*path, str(k))) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [resolve(v, path) for v in value]
        return value

    result = resolve(result)
    return result, {"version": 1, "sources": sources, "estimates": estimates}

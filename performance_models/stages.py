"""Finite execution stages sharing physical resource queues.

Stage identities carry continuation/demand; resource identities carry capacity.
The moment projection is for queueing analysis only. The native engine samples
the original stage distributions, never the projected moment-carrier gamma.
"""

import copy
from collections.abc import Mapping
from dataclasses import dataclass
import math
from types import MappingProxyType

from .call_policy import CallPolicySpec, parse_call_policies
from .components import RESOURCE_POLICY_FIELDS


@dataclass(frozen=True)
class StageSpec:
    resource: str | None
    service_time: object | None
    blocking_time: object | None
    delay: object | None
    work_units: float
    unit: str
    hold_resource: bool
    calls: Mapping
    policy: CallPolicySpec
    next_stage: str | None
    retry: Mapping | None = None
    cache: Mapping | None = None


@dataclass(frozen=True)
class StageWorkflowSpec:
    entries: Mapping
    stages: Mapping
    visits_by_entry: Mapping


def parse_stage_workflow(raw, resources, *, base_path, label, request_class=None):
    from .workload import parse_duration_distribution

    def duration(value, suffix):
        return parse_duration_distribution(
            value, f"{label}.{suffix}", base_path=base_path
        )

    if not isinstance(raw, Mapping) or set(raw) != {"entries", "stages"}:
        raise ValueError(f"{label} needs entries and stages.")
    entries, values = raw["entries"], raw["stages"]
    if not isinstance(entries, Mapping) or not entries or set(entries) - set(resources):
        raise ValueError(f"{label}.entries must map known entry resources to stages.")
    if not isinstance(values, Mapping) or not 1 <= len(values) <= 256:
        raise ValueError(f"{label}.stages needs between 1 and 256 named stages.")
    if any(not isinstance(name, str) or not name for name in values):
        raise ValueError(f"{label} stage names must be nonempty strings.")
    stages = {}
    for name, value in values.items():
        path = f"stages.{name}"
        if not isinstance(value, Mapping) or set(value) - {
            "resource",
            "service_time",
            "non_cpu_blocking_time",
            "delay",
            "work_units",
            "unit",
            "hold_resource",
            "calls",
            "call_policy",
            "next",
            "retry",
            "cache",
        }:
            raise ValueError(f"{label}.{path} contains unknown stage fields.")
        resource = value.get("resource")
        if resource is not None and (
            not isinstance(resource, str) or resource not in resources
        ):
            raise ValueError(
                f"{label}.{path}.resource must identify a physical resource."
            )
        units = value.get("work_units", 1.0)
        if (
            isinstance(units, bool)
            or not isinstance(units, (int, float))
            or not math.isfinite(units)
            or units <= 0
        ):
            raise ValueError(f"{label}.{path}.work_units must be finite and positive.")
        unit = value.get("unit", "work")
        if not isinstance(unit, str) or not unit:
            raise ValueError(f"{label}.{path}.unit must be a nonempty label.")
        hold = value.get("hold_resource", False)
        if not isinstance(hold, bool):
            raise ValueError(f"{label}.{path}.hold_resource must be boolean.")
        if resource is None and set(value) & {
            "service_time",
            "non_cpu_blocking_time",
            "work_units",
            "unit",
            "hold_resource",
        }:
            raise ValueError(
                f"{label}.{path}: resource-free control stages cannot own work/capacity."
            )
        if resource is not None and ("service_time" not in value or "delay" in value):
            raise ValueError(
                f"{label}.{path}: resource stages require service_time; use a control stage for delay."
            )
        work = (
            duration(value["service_time"], path + ".service_time")
            if resource is not None
            else None
        )
        if work is not None and (
            not math.isfinite(work.mean * units) or work.mean * units <= 0
        ):
            raise ValueError(
                f"{label}.{path}: scaled service demand must be finite and positive."
            )
        blocking = (
            duration(value["non_cpu_blocking_time"], path + ".non_cpu_blocking_time")
            if "non_cpu_blocking_time" in value
            else None
        )
        delay = duration(value["delay"], path + ".delay") if "delay" in value else None
        cache = None
        calls = value.get("calls", {})
        if "cache" in value:
            cfg = value["cache"]
            resource_config = resources.get(resource, {})
            cache_policy = (
                resource_config.get("cache", {})
                if isinstance(resource_config, Mapping)
                else {}
            )
            if (
                not isinstance(cfg, Mapping)
                or set(cfg) - {"operation", "hit", "miss", "fill_on_miss"}
                or not isinstance(cache_policy, Mapping)
                or not cache_policy
                or set(value) & {"calls", "call_policy", "retry"}
            ):
                raise ValueError(
                    f"{label}.{path}.cache requires a cache resource and no explicit calls/retry policy."
                )
            operation = cfg.get("operation", "lookup")
            if operation not in {"lookup", "put", "invalidate"}:
                raise ValueError(f"{label}.{path}.cache has an invalid operation.")
            if operation == "lookup":
                if (
                    "miss" not in cfg
                    or cfg.get("miss") not in values
                    or (cfg.get("hit") is not None and cfg["hit"] not in values)
                    or cfg.get("hit") == cfg["miss"]
                ):
                    raise ValueError(
                        f"{label}.{path}.cache needs distinct valid hit/miss stages."
                    )
                calls = {
                    target: 1
                    for target in (cfg.get("hit"), cfg["miss"])
                    if target is not None
                }
            elif set(cfg) - {"operation"}:
                raise ValueError(
                    f"{label}.{path}.cache put/invalidate cannot have child branches."
                )
            if not isinstance(cfg.get("fill_on_miss", True), bool):
                raise ValueError(f"{label}.{path}.cache.fill_on_miss must be boolean.")
            mode = cache_policy.get("mode")
            if mode == "fixed_hit_ratio" and (
                operation != "lookup"
                or request_class not in cache_policy.get("hit_ratio_by_class", {})
            ):
                raise ValueError(
                    f"{label}.{path}: fixed hit-ratio lookup needs class-specific evidence."
                )
            if mode == "lru" and request_class not in cache_policy.get(
                "accesses_by_class", {}
            ):
                raise ValueError(
                    f"{label}.{path}: LRU operations need a measured key-access profile."
                )
            cache = MappingProxyType(
                dict(
                    operation=operation,
                    hit=cfg.get("hit"),
                    miss=cfg.get("miss"),
                    fill_on_miss=cfg.get("fill_on_miss", True),
                )
            )
        if not isinstance(calls, Mapping) or set(calls) - set(values):
            raise ValueError(f"{label}.{path}.calls must reference known stage names.")
        for target, count in calls.items():
            if (
                isinstance(count, bool)
                or not isinstance(count, (int, float))
                or not math.isfinite(count)
                or not 0 < count <= 10_000
            ):
                raise ValueError(
                    f"{label}.{path}.calls.{target} must be finite, positive and at most 10000."
                )
        if calls:
            policy = parse_call_policies(
                {name: value.get("call_policy", {})},
                {name: dict(calls)},
                "parallel",
                label=label + "." + path + ".call_policy",
            )[name]
        else:
            if "call_policy" in value:
                raise ValueError(f"{label}.{path}.call_policy requires child calls.")
            policy = CallPolicySpec("parallel")
        next_stage = value.get("next")
        if next_stage is not None and (
            not isinstance(next_stage, str) or next_stage not in values
        ):
            raise ValueError(f"{label}.{path}.next references an unknown stage.")
        retry = None
        if "retry" in value:
            from .workload import _positive_integer, _finite_nonnegative

            retry_raw = value["retry"]
            if (
                resource is not None
                or len(calls) != 1
                or next(iter(calls.values())) != 1
                or policy.execution != "parallel"
                or policy.completion != "all"
                or not isinstance(retry_raw, Mapping)
                or set(retry_raw)
                - {"max_attempts", "backoff", "attempt_timeout_seconds"}
                or "max_attempts" not in retry_raw
            ):
                raise ValueError(
                    f"{label}.{path}.retry requires a resource-free, single-child, all-response stage."
                )
            attempts = _positive_integer(
                retry_raw["max_attempts"], f"{label}.{path}.retry.max_attempts"
            )
            if attempts > 100:
                raise ValueError(
                    f"{label}.{path}.retry.max_attempts cannot exceed 100."
                )
            retry = MappingProxyType(
                dict(
                    max_attempts=attempts,
                    backoff=(
                        duration(retry_raw["backoff"], path + ".retry.backoff")
                        if "backoff" in retry_raw
                        else None
                    ),
                    attempt_timeout_seconds=_finite_nonnegative(
                        retry_raw.get("attempt_timeout_seconds", 0),
                        label + "." + path + ".retry.attempt_timeout_seconds",
                    ),
                )
            )
        stages[name] = StageSpec(
            resource,
            work,
            blocking,
            delay,
            float(units),
            unit,
            hold,
            MappingProxyType(dict(calls)),
            policy,
            next_stage,
            retry,
            cache,
        )
    if any(
        not isinstance(name, str) or name not in stages for name in entries.values()
    ):
        raise ValueError(f"{label}.entries references an unknown stage.")
    visiting, cache, maximum = set(), {}, {}

    def visits(name):
        if name in visiting:
            raise ValueError(
                f"{label} stage graph must be acyclic (physical resource revisits are allowed)."
            )
        if name in cache:
            return cache[name]
        visiting.add(name)
        stage = stages[name]
        totals = {name: 1.0}
        branches = []
        for child, count in stage.calls.items():
            for target, n in visits(child).items():
                totals[target] = totals.get(target, 0) + count * n
            branches.append(math.ceil(count) * maximum[child])
        largest = (
            max(branches, default=0)
            if stage.policy.execution == "choice"
            else sum(branches)
        )
        if stage.retry:
            largest *= stage.retry["max_attempts"]
        if stage.next_stage is not None:
            for target, n in visits(stage.next_stage).items():
                totals[target] = totals.get(target, 0) + n
            largest += maximum[stage.next_stage]
        maximum[name] = 1 + largest
        if maximum[name] > 10_000:
            raise ValueError(f"{label} can exceed the 10000-visit execution guard.")
        visiting.remove(name)
        cache[name] = MappingProxyType(totals)
        return cache[name]

    by_entry = {resource: visits(name) for resource, name in entries.items()}
    if set(cache) != set(stages):
        raise ValueError(
            f"{label} has unreachable stages: {sorted(set(stages)-set(cache))}."
        )
    for entry, counts in by_entry.items():
        if not any(stages[s].resource == entry for s in counts):
            raise ValueError(
                f"{label}: entry resource {entry!r} needs a reachable resource stage."
            )
    return StageWorkflowSpec(
        MappingProxyType(dict(entries)),
        MappingProxyType(stages),
        MappingProxyType(by_entry),
    )


def _moment_distribution(observations):
    try:
        total = math.fsum(weight for weight, _mean, _scv in observations)
        mean = math.fsum(weight * mu for weight, mu, _scv in observations) / total
        second = (
            math.fsum(weight * mu * mu * (1 + scv) for weight, mu, scv in observations)
            / total
        )
    except (OverflowError, ZeroDivisionError) as error:
        raise ValueError(
            "Stage resource moments cannot be represented finitely."
        ) from error
    if not math.isfinite(mean) or not math.isfinite(second) or mean <= 0:
        raise ValueError("Stage resource moments overflowed or have zero demand.")
    scv = max(0.0, second / (mean * mean) - 1)
    return (
        {"distribution": "deterministic", "mean": mean}
        if scv < 1e-14
        else {"distribution": "gamma", "mean": mean, "scv": scv}
    )


def prepare_stage_workflows(workload, *, base_path):
    """Project finite stage work to physical-resource visit/moment equations."""
    from .workload import _parse_distribution

    raw_classes = workload.get("request_classes", {})
    if not isinstance(raw_classes, Mapping) or not any(
        isinstance(c, Mapping) and "stage_workflow" in c for c in raw_classes.values()
    ):
        return workload, {}
    result = copy.deepcopy(workload)
    model = result.get("traffic_model", {})
    if not isinstance(model, Mapping):
        raise ValueError("stage_workflow needs a traffic_model mapping.")
    resources = model.get("service_times", {})
    if not isinstance(resources, Mapping) or not resources:
        raise ValueError(
            "stage_workflow needs physical resources in traffic_model.service_times."
        )
    plans, moments, blocking_moments = {}, {}, {}
    for name, raw_class in result["request_classes"].items():
        if not isinstance(raw_class, Mapping):
            raise ValueError(f"Request class {name!r} must be a mapping.")
        if "stage_workflow" not in raw_class:
            continue
        if set(raw_class) & {
            "routing",
            "visits",
            "workflow",
            "calls",
            "call_execution",
            "call_policies",
        }:
            raise ValueError(
                "stage_workflow cannot be mixed with another flow definition."
            )
        plan = parse_stage_workflow(
            raw_class.pop("stage_workflow"),
            resources,
            base_path=base_path,
            label=f"request_classes.{name}.stage_workflow",
            request_class=name,
        )
        raw_entries = model.get("entry_services", {})
        if not isinstance(raw_entries, Mapping):
            raise ValueError("Stage workflows require an entry_services mapping.")
        streams = raw_entries.get(name)
        if not isinstance(streams, Mapping) or set(streams) != set(plan.entries):
            raise ValueError(
                f"Stage entry resources for {name!r} must match its external streams."
            )
        rates = {
            r: _parse_distribution(
                v,
                f"entry_services.{name}.{r}",
                quantity="interarrival",
                base_path=base_path,
            ).rate
            for r, v in streams.items()
        }
        external = sum(rates.values())
        if not math.isfinite(external) or external <= 0:
            raise ValueError(
                "Staged external arrival rates must have a finite positive total."
            )
        stage_counts = {}
        for entry, counts in plan.visits_by_entry.items():
            for s, n in counts.items():
                stage_counts[s] = stage_counts.get(s, 0) + rates[entry] / external * n
        visits = {}
        for s, count in stage_counts.items():
            spec = plan.stages[s]
            if spec.resource is None:
                continue
            resource = spec.resource
            visits[resource] = visits.get(resource, 0) + count
            moments.setdefault(resource, {}).setdefault(name, []).append(
                (count, spec.service_time.mean * spec.work_units, spec.service_time.scv)
            )
            blocking_moments.setdefault(resource, {}).setdefault(name, []).append(
                (
                    count,
                    spec.blocking_time.mean if spec.blocking_time else 0,
                    spec.blocking_time.scv if spec.blocking_time else 0,
                )
            )
        raw_class["visits"] = visits
        plans[name] = plan
    metadata = RESOURCE_POLICY_FIELDS | {
        "cpu_bandwidth",
        "execution_model",
        "replica_queue_model",
        "processing_slots_per_replica",
        "concurrency_limit_per_replica",
        "non_cpu_blocking_time",
        "post_release_delay",
    }
    for resource, by_class in moments.items():
        raw = resources[resource]
        if not isinstance(raw, Mapping):
            raise ValueError(f"Resource {resource!r} must be a mapping.")
        if "post_release_delay" in raw:
            raise ValueError(
                "Stage workflows require explicit control-stage delays instead of resource post_release_delay."
            )
        config = {k: v for k, v in raw.items() if k in metadata}
        distributions = {k: v for k, v in raw.items() if k not in metadata}
        if "distribution" in distributions:
            # Validate before expansion; never hide misspelled distribution fields.
            _parse_distribution(
                distributions,
                f"service_times.{resource}",
                quantity="service_time",
                base_path=base_path,
            )
            distributions = {
                c: copy.deepcopy(distributions) for c in raw_classes if c not in plans
            }
        else:
            distributions = {c: v for c, v in distributions.items() if c not in plans}
        for c, observations in by_class.items():
            distributions[c] = _moment_distribution(observations)
        raw_block = config.pop("non_cpu_blocking_time", {})
        if not isinstance(raw_block, Mapping):
            raise ValueError(
                f"service_times.{resource}.non_cpu_blocking_time must be a mapping."
            )
        if "distribution" in raw_block:
            _parse_distribution(
                raw_block,
                f"service_times.{resource}.non_cpu_blocking_time",
                quantity="service_time",
                base_path=base_path,
            )
        else:
            if set(raw_block) - set(raw_classes):
                raise ValueError(
                    "Unknown request class in resource blocking-time mapping."
                )
            for c, value in raw_block.items():
                _parse_distribution(
                    value,
                    f"service_times.{resource}.non_cpu_blocking_time.{c}",
                    quantity="service_time",
                    base_path=base_path,
                )
        blocks = (
            {c: copy.deepcopy(raw_block) for c in raw_classes if c not in plans}
            if isinstance(raw_block, Mapping) and "distribution" in raw_block
            else dict(raw_block)
        )
        for c, observations in blocking_moments[resource].items():
            blocks.pop(c, None)
            if any(mu > 0 for _n, mu, _scv in observations):
                blocks[c] = _moment_distribution(observations)
        if blocks:
            config["non_cpu_blocking_time"] = blocks
        model["service_times"][resource] = {**config, **distributions}
    return result, plans

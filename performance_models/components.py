"""Validated generic resource policies; no product-name performance formulas."""

from collections.abc import Mapping


RESOURCE_POLICY_FIELDS = frozenset(
    {
        "admission_queue",
        "rate_limit",
        "failure_probability",
        "lifecycle",
        "cache",
    }
)


def parse_resource_policies(
    raw,
    *,
    label,
    execution_model,
    queue_model,
    concurrency_limit,
    base_path,
    request_classes=(),
):
    from .workload import (
        _finite_nonnegative,
        _finite_positive,
        _positive_integer,
        parse_duration_distribution,
    )

    result = {}
    if "cache" in raw:
        result["cache"] = parse_cache_policy(
            raw["cache"], label + ".cache", request_classes
        )
        if result["cache"]["mode"] == "lru" and queue_model != "independent_key_hash":
            raise ValueError(
                f"{label}: LRU cache requires independent_key_hash queues."
            )
    if (
        queue_model == "independent_key_hash"
        and result.get("cache", {}).get("mode") != "lru"
    ):
        raise ValueError(
            f"{label}: independent_key_hash requires a measured-key LRU cache."
        )
    if "admission_queue" in raw:
        value = raw["admission_queue"]
        if (
            not isinstance(value, Mapping)
            or set(value) - {"capacity_per_replica", "timeout_seconds"}
            or not value
        ):
            raise ValueError(
                f"{label}.admission_queue needs capacity_per_replica and/or timeout_seconds."
            )
        if concurrency_limit is None and "rate_limit" not in raw:
            raise ValueError(f"{label}.admission_queue requires finite concurrency.")
        capacity = value.get("capacity_per_replica")
        if capacity is not None and (
            isinstance(capacity, bool)
            or not isinstance(capacity, int)
            or not 0 <= capacity <= 1_000_000
        ):
            raise ValueError(
                f"{label}.admission_queue capacity must be an integer between 0 and 1000000."
            )
        result["admission_queue"] = dict(
            capacity_per_replica=capacity,
            timeout_seconds=_finite_nonnegative(
                value.get("timeout_seconds", 0),
                label + ".admission_queue.timeout_seconds",
            ),
        )
    if "rate_limit" in raw:
        value = raw["rate_limit"]
        if not isinstance(value, Mapping) or set(value) != {
            "rate_per_second",
            "burst",
            "action",
        }:
            raise ValueError(
                f"{label}.rate_limit needs rate_per_second, burst and action."
            )
        rate = _finite_positive(
            value["rate_per_second"], label + ".rate_limit.rate_per_second"
        )
        if queue_model == "pooled":
            raise ValueError(f"{label}.rate_limit requires independent replica queues.")
        burst = _positive_integer(value["burst"], label + ".rate_limit.burst")
        if burst > 1_000_000 or value["action"] not in {"wait", "reject"}:
            raise ValueError(f"{label}.rate_limit has invalid burst or action.")
        result["rate_limit"] = dict(
            rate_per_second=rate, burst=burst, action=value["action"]
        )
    if "failure_probability" in raw:
        from .workload import _finite_probability

        result["failure_probability"] = _finite_probability(
            raw["failure_probability"], label + ".failure_probability"
        )
    if "lifecycle" in raw:
        if result.get("cache", {}).get("mode") == "fixed_hit_ratio":
            raise ValueError(
                f"{label}: cold cache lifecycle requires a key-state model, not a fixed hit ratio."
            )
        value = raw["lifecycle"]
        if not isinstance(value, Mapping) or set(value) != {
            "startup_delay",
            "idle_timeout_seconds",
            "initially_warm",
        }:
            raise ValueError(
                f"{label}.lifecycle needs startup_delay, idle_timeout_seconds and initially_warm."
            )
        if queue_model == "pooled":
            raise ValueError(f"{label}.lifecycle requires independent replica queues.")
        if not isinstance(value["initially_warm"], bool):
            raise ValueError(f"{label}.lifecycle.initially_warm must be boolean.")
        result["lifecycle"] = dict(
            startup_delay=parse_duration_distribution(
                value["startup_delay"],
                label + ".lifecycle.startup_delay",
                base_path=base_path,
            ),
            idle_timeout_seconds=_finite_positive(
                value["idle_timeout_seconds"], label + ".lifecycle.idle_timeout_seconds"
            ),
            initially_warm=value["initially_warm"],
        )
    return result


def parse_cache_policy(value, label, request_classes):
    from .workload import _positive_integer, _finite_probability, _finite_nonnegative

    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping.")
    mode = value.get("mode")
    if mode == "fixed_hit_ratio":
        fields = {
            "mode",
            "hit_ratio_by_class",
            "capacity_bytes_per_replica",
            "observed_capacity_bytes_per_replica",
            "observed_replicas",
        }
        if set(value) != fields:
            raise ValueError(f"{label}: fixed_hit_ratio requires {sorted(fields)}.")
        capacity = _positive_integer(
            value["capacity_bytes_per_replica"], label + ".capacity_bytes_per_replica"
        )
        observed = _positive_integer(
            value["observed_capacity_bytes_per_replica"],
            label + ".observed_capacity_bytes_per_replica",
        )
        replicas = _positive_integer(
            value["observed_replicas"], label + ".observed_replicas"
        )
        ratios = value["hit_ratio_by_class"]
        if capacity != observed or capacity > 2**60 or replicas > 1_000_000:
            raise ValueError(
                f"{label}: fixed hit ratios cannot extrapolate the measured cache configuration."
            )
        if (
            not isinstance(ratios, Mapping)
            or not ratios
            or set(ratios) - set(request_classes)
        ):
            raise ValueError(
                f"{label}.hit_ratio_by_class must reference known request classes."
            )
        return {
            **dict(value),
            "hit_ratio_by_class": {
                c: _finite_probability(p, label + ".hit_ratio_by_class." + c)
                for c, p in ratios.items()
            },
        }
    if mode != "lru" or set(value) - {
        "mode",
        "capacity_bytes_per_replica",
        "ttl_seconds",
        "key_sizes_bytes",
        "accesses_by_class",
    }:
        raise ValueError(f"{label} needs fixed_hit_ratio or lru with supported fields.")
    required = {"capacity_bytes_per_replica", "key_sizes_bytes", "accesses_by_class"}
    if not required <= set(value):
        raise ValueError(
            f"{label}.lru requires capacity and a measured key/access profile."
        )
    capacity = _positive_integer(
        value["capacity_bytes_per_replica"], label + ".capacity_bytes_per_replica"
    )
    sizes = value["key_sizes_bytes"]
    accesses = value["accesses_by_class"]
    if (
        capacity > 2**60
        or not isinstance(sizes, Mapping)
        or not 1 <= len(sizes) <= 1_000_000
        or any(not isinstance(k, str) or not k for k in sizes)
    ):
        raise ValueError(f"{label} has invalid capacity or key catalog.")
    sizes = {
        k: _positive_integer(sizes[k], label + ".key_sizes_bytes." + k)
        for k in sorted(sizes)
    }
    if (
        any(n > 2**60 for n in sizes.values())
        or not isinstance(accesses, Mapping)
        or not accesses
        or set(accesses) - set(request_classes)
    ):
        raise ValueError(f"{label} has invalid sizes or request classes.")
    profiles = {}
    seen = set()
    for c, profile in accesses.items():
        if (
            not isinstance(profile, Mapping)
            or set(profile) != {"sampling", "keys"}
            or profile["sampling"] not in {"iid", "cyclic_trace"}
        ):
            raise ValueError(f"{label}.accesses_by_class.{c} needs sampling and keys.")
        keys = profile["keys"]
        if (
            not isinstance(keys, (list, tuple))
            or not 1 <= len(keys) <= 1_000_000
            or any(not isinstance(k, str) or k not in sizes for k in keys)
        ):
            raise ValueError(f"{label}.accesses_by_class.{c} has invalid keys.")
        seen.update(keys)
        profiles[c] = {"sampling": profile["sampling"], "keys": list(keys)}
    if seen != set(sizes):
        raise ValueError(f"{label}: every catalog key must occur in an access profile.")
    return dict(
        mode="lru",
        capacity_bytes_per_replica=capacity,
        ttl_seconds=_finite_nonnegative(
            value.get("ttl_seconds", 0), label + ".ttl_seconds"
        ),
        key_sizes_bytes=sizes,
        accesses_by_class=profiles,
    )


def serialize_resource_policies(policies):
    from native_backend.backend import _serialize_distribution

    result = {
        key: dict(value) if isinstance(value, Mapping) else value
        for key, value in policies.items()
    }
    if "lifecycle" in result:
        result["lifecycle"]["startup_delay"] = _serialize_distribution(
            result["lifecycle"]["startup_delay"]
        )
    return result


def simulation_configuration_supported(system, analytical_metrics=None):
    """Permit an empirical stability check without certifying static projections."""
    if not getattr(system, "has_static_stability_model", lambda: True)():
        return True
    return bool((analytical_metrics or system.solve()).get_stability())


def resource_can_fail(policy):
    queue = policy.get("admission_queue", {})
    return (
        policy.get("failure_probability", 0) > 0
        or queue.get("capacity_per_replica") is not None
        or queue.get("timeout_seconds", 0) > 0
        or policy.get("rate_limit", {}).get("action") == "reject"
    )

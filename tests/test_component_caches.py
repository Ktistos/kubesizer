"""Conditional cache work, shared state, configuration scope and invalidation."""

import copy

import pytest

from performance_models import System, QueueingNetworkType


def workload(capacity=2, keys=("a", "b"), **policy):
    return {
        "request_classes": {
            "r": {
                "concurrency_weight": 1,
                "stage_workflow": {
                    "entries": {"cache": "lookup"},
                    "stages": {
                        "lookup": {
                            "resource": "cache",
                            "service_time": {
                                "distribution": "deterministic",
                                "mean": 0.01,
                            },
                            "cache": {"miss": "load"},
                        },
                        "load": {
                            "resource": "backend",
                            "service_time": {
                                "distribution": "deterministic",
                                "mean": 0.1,
                            },
                        },
                    },
                },
            }
        },
        "traffic_model": {
            "entry_services": {
                "r": {"cache": {"distribution": "deterministic", "rate": 1}}
            },
            "service_times": {
                "cache": {
                    "replica_queue_model": "independent_key_hash",
                    "cache": {
                        "mode": "lru",
                        "capacity_bytes_per_replica": capacity,
                        "key_sizes_bytes": {k: 1 for k in set(keys)},
                        "accesses_by_class": {
                            "r": {"sampling": "cyclic_trace", "keys": list(keys)}
                        },
                        **policy,
                    },
                },
                "backend": {},
            },
        },
    }


def simulate(raw, duration=4.001, replicas=None):
    model = System("cache", raw, QueueingNetworkType.OPEN)
    if replicas:
        model.configure_system_replicas(replicas)
    return model.simulate_measurement_replications(
        warmup_duration=0,
        measurement_duration=duration,
        maximum_drain_time=10,
        seeds=(123,),
    )[0]


def test_warm_cache_hits_skip_backend_work():
    assert simulate(workload()).request_class_latencies["r"] == pytest.approx(
        (0.11, 0.11, 0.01, 0.01)
    )


def test_key_routing_gives_distinct_shards_independent_memory():
    assert simulate(workload(capacity=1), replicas=(2, 1)).request_class_latencies[
        "r"
    ] == pytest.approx((0.11, 0.11, 0.01, 0.01))


def test_native_parser_checks_cache_branches_against_the_acyclic_graph():
    from native_backend.backend import CppSimulationBackend, get_native_backend

    model = System("cache", workload(), QueueingNetworkType.OPEN)
    native = CppSimulationBackend.serialize_system(model)
    native["request_classes"][0]["stage_workflow"]["stages"][0]["cache"]["miss"] = 0
    with pytest.raises(RuntimeError, match="branches"):
        get_native_backend()._invoke(
            "simulate",
            model,
            serialized_workload=native,
            seed=1,
            horizon=10,
            warmup_duration=0,
            capture_state=False,
        )


def test_lru_eviction_follows_access_order():
    raw = workload(keys=("a", "b", "a", "c", "b", "c"))
    assert simulate(raw, 6.001).request_class_latencies["r"] == pytest.approx(
        (0.11, 0.11, 0.01, 0.11, 0.11, 0.01)
    )


@pytest.mark.parametrize(
    "changes", [dict(ttl_seconds=0.5), dict(capacity_bytes_per_replica=1)]
)
def test_expired_or_evicted_keys_miss(changes):
    assert simulate(workload(**changes)).request_class_latencies["r"] == pytest.approx(
        (0.11,) * 4
    )


def test_oversized_object_is_not_admitted():
    raw = workload(keys=("a",))
    raw["traffic_model"]["service_times"]["cache"]["cache"]["key_sizes_bytes"]["a"] = 3
    assert simulate(raw).request_class_latencies["r"] == pytest.approx((0.11,) * 4)


def test_failed_miss_does_not_fill_cache():
    raw = workload(keys=("a",))
    raw["traffic_model"]["service_times"]["backend"]["failure_probability"] = 1
    result = simulate(raw)
    assert result.request_class_latencies["r"] == pytest.approx((0.11,) * 4)
    assert result.failed_measured_request_counts["r"] == 4


def test_invalidation_uses_the_requests_key():
    raw = workload(keys=("a",))
    stages = raw["request_classes"]["r"]["stage_workflow"]["stages"]
    stages["lookup"]["next"] = "invalidate"
    stages["invalidate"] = {
        "resource": "cache",
        "service_time": {"distribution": "deterministic", "mean": 0.01},
        "cache": {"operation": "invalidate"},
        "next": "again",
    }
    stages["again"] = copy.deepcopy(stages["lookup"])
    stages["again"].pop("next")
    assert simulate(raw).request_class_latencies["r"] == pytest.approx(
        (0.23, 0.13, 0.13, 0.13)
    )


def test_cold_instance_loses_volatile_cache_state():
    raw = workload(keys=("a",))
    raw["traffic_model"]["service_times"]["cache"]["lifecycle"] = {
        "startup_delay": {"distribution": "deterministic", "mean": 0.05},
        "idle_timeout_seconds": 0.2,
        "initially_warm": True,
    }
    assert simulate(raw).request_class_latencies["r"] == pytest.approx((0.16,) * 4)


def test_miss_return_cannot_repopulate_a_destroyed_instance():
    raw = workload(keys=("a",))
    raw["traffic_model"]["entry_services"]["r"]["cache"]["rate"] = 0.1
    raw["traffic_model"]["service_times"]["cache"]["lifecycle"] = {
        "startup_delay": {"distribution": "deterministic", "mean": 0.05},
        "idle_timeout_seconds": 0.2,
        "initially_warm": True,
    }
    raw["request_classes"]["r"]["stage_workflow"]["stages"]["load"]["service_time"][
        "mean"
    ] = 3
    assert simulate(raw, 20.001).request_class_latencies["r"] == pytest.approx(
        (3.06, 3.06)
    )


@pytest.mark.parametrize("ratio,latency", [(0, 0.11), (1, 0.01)])
def test_fixed_hit_ratio_is_scoped_to_observed_capacity(ratio, latency):
    raw = workload()
    resource = raw["traffic_model"]["service_times"]["cache"]
    resource["replica_queue_model"] = "independent_round_robin"
    resource["cache"] = {
        "mode": "fixed_hit_ratio",
        "hit_ratio_by_class": {"r": ratio},
        "capacity_bytes_per_replica": 2,
        "observed_capacity_bytes_per_replica": 2,
        "observed_replicas": 1,
    }
    assert simulate(raw).request_class_latencies["r"] == pytest.approx((latency,) * 4)
    raw["replica_constraints"] = {"cache": {"minimum": 1, "maximum": 2}}
    with pytest.raises(ValueError, match="fixed|observed"):
        System("bad", raw, QueueingNetworkType.OPEN)


def test_conditional_cache_load_is_not_a_proven_stability_floor():
    raw = workload()
    raw["traffic_model"]["entry_services"]["r"]["cache"]["rate"] = 1000
    model = System("cache", raw, QueueingNetworkType.OPEN)
    assert model.solve().get_stability() is None
    assert model.get_minimum_stable_replicas() == [1, 1]
    assert model.get_strict_analytical_incompatibilities()


@pytest.mark.parametrize(
    "change",
    [
        "unknown_key",
        "no_profile",
        "unknown_branch",
        "unknown_field",
        "no_cache",
        "wrong_queue",
    ],
)
def test_incomplete_cache_evidence_or_semantics_are_rejected(change):
    raw = workload()
    policy = raw["traffic_model"]["service_times"]["cache"]
    if change == "unknown_key":
        policy["cache"]["accesses_by_class"]["r"]["keys"] = ["unknown"]
    elif change == "no_profile":
        policy["cache"]["accesses_by_class"] = {}
    elif change == "unknown_branch":
        raw["request_classes"]["r"]["stage_workflow"]["stages"]["lookup"]["cache"][
            "miss"
        ] = "absent"
    elif change == "unknown_field":
        policy["cache"]["made_up"] = 1
    elif change == "no_cache":
        policy.pop("cache")
    else:
        policy["replica_queue_model"] = "pooled"
    with pytest.raises(ValueError):
        System("bad", raw, QueueingNetworkType.OPEN)

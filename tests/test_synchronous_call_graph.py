import pytest

from calibration.profile import experiment_fingerprints
from performance_models import QueueingNetworkType, System, parse_workload_spec


def _workload(
    *,
    child_processing_slots=1,
    duplicate_child_calls=False,
    call_execution="parallel",
):
    calls = (
        {"api": {"worker": 2.0}}
        if duplicate_child_calls
        else {"api": {"auth": 1.0, "worker": 1.0}}
    )
    service_times = {
        "api": {"distribution": "deterministic", "mean": 0.1},
        "worker": {
            "processing_slots_per_replica": child_processing_slots,
            "concurrency_limit_per_replica": child_processing_slots,
            "distribution": "deterministic",
            "mean": 0.3 if not duplicate_child_calls else 0.2,
        },
    }
    if not duplicate_child_calls:
        service_times["auth"] = {
            "distribution": "deterministic",
            "mean": 0.2,
        }
    return {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "calls": calls,
                "call_execution": call_execution,
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "api": {"distribution": "deterministic", "rate": 1.0}
                }
            },
            "service_times": service_times,
        },
    }


def test_call_graph_models_parallel_children_and_blocking_parent():
    workload = _workload()
    parsed = parse_workload_spec(workload)
    system = System("fork-join", workload, QueueingNetworkType.OPEN)

    replication = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=5.5,
        seeds=(11,),
    )[0]

    assert dict(parsed.request_class_call_graphs["request"]["api"]) == {
        "auth": 1.0,
        "worker": 1.0,
    }
    assert system.get_request_class_effective_arrival_rates()["request"] == (
        pytest.approx([1.0, 1.0, 1.0])
    )
    # Root computes for .1s, children run concurrently for .2s and .3s, and
    # the root keeps its service slot until the slower child completes.
    assert replication.request_class_latencies["request"] == pytest.approx(
        (0.4,) * 5
    )


def test_duplicate_parallel_calls_contend_for_child_capacity():
    serial_child = System(
        "serial-child",
        _workload(duplicate_child_calls=True, child_processing_slots=1),
        QueueingNetworkType.OPEN,
    )
    parallel_child = System(
        "parallel-child",
        _workload(duplicate_child_calls=True, child_processing_slots=2),
        QueueingNetworkType.OPEN,
    )

    serial_latency = serial_child.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=3.5,
        seeds=(19,),
    )[0].request_class_latencies["request"]
    parallel_latency = parallel_child.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=3.5,
        seeds=(19,),
    )[0].request_class_latencies["request"]

    assert serial_latency == pytest.approx((0.5,) * 3)
    assert parallel_latency == pytest.approx((0.3,) * 3)


def test_sequential_call_execution_preserves_declared_child_order():
    system = System(
        "sequential-children",
        _workload(
            duplicate_child_calls=True,
            child_processing_slots=2,
            call_execution="sequential",
        ),
        QueueingNetworkType.OPEN,
    )

    latencies = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=3.5,
        seeds=(23,),
    )[0].request_class_latencies["request"]

    # Two .2-second calls run one after the other even though worker has two
    # processing slots. The root's own .1-second processing makes .5 seconds.
    assert latencies == pytest.approx((0.5,) * 3)


@pytest.mark.parametrize("mode", (None, "overlap"))
def test_call_graph_requires_explicit_supported_execution_semantics(mode):
    workload = _workload()
    if mode is None:
        workload["request_classes"]["request"].pop("call_execution")
    else:
        workload["request_classes"]["request"]["call_execution"] = mode

    with pytest.raises(ValueError, match="call_execution"):
        parse_workload_spec(workload)


def test_call_execution_semantics_invalidate_statistical_calibration():
    parallel = System(
        "parallel",
        _workload(call_execution="parallel"),
        QueueingNetworkType.OPEN,
    )
    sequential = System(
        "sequential",
        _workload(call_execution="sequential"),
        QueueingNetworkType.OPEN,
    )

    assert experiment_fingerprints(parallel)[0] != experiment_fingerprints(
        sequential
    )[0]


def test_blocked_parent_releases_processing_slot_but_keeps_concurrency_slot():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "calls": {"api": {"worker": 1.0}},
                "call_execution": "parallel",
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "api": {"distribution": "deterministic", "rate": 1.5}
                }
            },
            "service_times": {
                "api": {
                    "processing_slots_per_replica": 1,
                    "concurrency_limit_per_replica": 2,
                    "distribution": "deterministic",
                    "mean": 0.1,
                },
                "worker": {
                    "distribution": "deterministic",
                    "mean": 0.6,
                },
            },
        },
    }
    system = System("blocked-parent", workload, QueueingNetworkType.OPEN)

    replication = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.4,
        seeds=(29,),
    )[0]

    # The first API visit waits synchronously for worker until t=1.366..., but
    # its CPU slot is free at t=.766.... The second API visit can therefore
    # start at t=1.333... while the first still occupies a concurrency slot.
    assert replication.request_class_latencies["request"] == pytest.approx(
        (0.7, 0.7)
    )


def test_non_open_call_graph_fails_before_calibration_or_optimization():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "calls": {"api": {"api": 1.0}},
                "call_execution": "parallel",
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "api": {"distribution": "exponential", "rate": 1.0}
                }
            },
            "service_times": {
                "api": {"distribution": "exponential", "rate": 10.0}
            },
        },
    }
    system = System("non-open", workload, QueueingNetworkType.OPEN)

    with pytest.raises(ValueError, match="mean offspring spectral radius"):
        system.get_effective_arrival_rates()

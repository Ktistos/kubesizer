import pytest

from metrics import SimulationMetricsCollector
from native_backend import get_native_backend
from performance_models import QueueingNetworkType, System


def test_state_histories_start_at_zero():
    collector = SimulationMetricsCollector(2)

    assert collector.get_active_requests() == 0
    assert collector.get_active_history() == ((0.0, 0),)
    assert collector.get_queue_history() == {
        0: ((0.0, 0),),
        1: ((0.0, 0),),
    }
    assert collector.get_busy_history() == {
        0: ((0.0, 0),),
        1: ((0.0, 0),),
    }


def test_native_history_adapter_rejects_capacity_and_coalescing_violations():
    base = {
        "active_history": ((0.0, 0),),
        "queue_history": (((0.0, 0),),),
        "busy_history": (((0.0, 0), (1.0, 2)),),
        "active_requests_at_end": 0,
        "queue_lengths_at_end": (0,),
        "busy_replicas_at_end": (2,),
    }
    with pytest.raises(ValueError, match="capacity"):
        SimulationMetricsCollector(1).load_native_state(base, (1,))

    duplicate = dict(base)
    duplicate["busy_history"] = (((0.0, 0), (1.0, 1), (2.0, 1)),)
    duplicate["busy_replicas_at_end"] = (1,)
    with pytest.raises(ValueError, match="changed"):
        SimulationMetricsCollector(1).load_native_state(duplicate, (1,))


def test_internal_routing_does_not_change_active_requests():
    system = System(
        "test",
        {
            "read": {
                "front": {
                    "gamma": 2.0,
                    "mu": 20.0,
                    "routes": {"back": 1.0},
                },
                "back": {"gamma": 0.0, "mu": 20.0},
            }
        },
        QueueingNetworkType.OPEN,
    )
    native_result = get_native_backend().simulate(
        system,
        seed=44,
        horizon=10.0,
        warmup_duration=0.0,
        capture_state=True,
    )
    active_history = native_result["active_history"]
    increments = sum(
        max(0, right[1] - left[1])
        for left, right in zip(active_history, active_history[1:])
    )
    decrements = sum(
        max(0, left[1] - right[1])
        for left, right in zip(active_history, active_history[1:])
    )

    assert increments == native_result["external_arrival_count"]
    assert decrements == native_result["completed_request_count"]
    assert increments > 0

import pytest

from metrics import SimulationMetricsCollector
from native_backend import get_native_backend
from performance_models import QueueingNetworkType, System


class FakeResource:
    def __init__(self, capacity):
        self.capacity = capacity
        self.count = 0
        self.queue = []


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


def test_external_arrival_and_final_exit_update_active_requests_once():
    collector = SimulationMetricsCollector(0)

    request = collector.start_external_request("read", 1.0)

    assert collector.get_active_requests() == 1
    assert collector.get_active_history() == ((0.0, 0), (1.0, 1))

    collector.record_external_request_completion(request, 4.0)

    assert collector.get_active_requests() == 0
    assert collector.get_active_history() == (
        (0.0, 0),
        (1.0, 1),
        (4.0, 0),
    )
    with pytest.raises(AssertionError, match="cannot become negative"):
        collector.record_external_request_completion(request, 5.0)


def test_warmup_gate_tags_external_entry_without_resetting_active_state():
    collector = SimulationMetricsCollector(0, latency_collection_start=5.0)
    pre_warmup = collector.start_external_request("read", 4.0)
    post_warmup = collector.start_external_request("read", 5.0)

    collector.record_external_request_completion(post_warmup, 7.0)
    assert collector.get_active_requests() == 1
    collector.record_external_request_completion(pre_warmup, 14.0)
    metrics = collector.finalize(14.0, (), True)

    assert collector.get_active_history() == (
        (0.0, 0),
        (4.0, 1),
        (5.0, 2),
        (7.0, 1),
        (14.0, 0),
    )
    assert metrics.get_request_class_p95_latencies() == {"read": 2.0}


def test_active_changes_at_same_timestamp_are_coalesced():
    collector = SimulationMetricsCollector(0)

    collector.start_external_request("read", 1.0)
    collector.start_external_request("write", 1.0)

    assert collector.get_active_history() == ((0.0, 0), (1.0, 2))


def test_resource_state_uses_authoritative_resource_and_avoids_duplicates():
    collector = SimulationMetricsCollector(1)
    resource = FakeResource(capacity=2)
    collector.register_service_resource(0, resource)

    resource.count = 1
    collector.record_service_state(0, 1.0)

    assert collector.get_queue_history()[0] == ((0.0, 0),)
    assert collector.get_busy_history()[0] == ((0.0, 0), (1.0, 1))

    collector.record_service_state(0, 1.0)
    assert collector.get_busy_history()[0] == ((0.0, 0), (1.0, 1))

    resource.count = 2
    collector.record_service_state(0, 1.0)
    resource.queue.append(object())
    collector.record_service_state(0, 1.0)

    assert collector.get_busy_history()[0] == ((0.0, 0), (1.0, 2))
    assert collector.get_queue_history()[0] == ((0.0, 0), (1.0, 1))
    assert collector.get_busy_replicas(0) == resource.count == 2
    assert collector.get_queue_length(0) == len(resource.queue) == 1
    assert collector.get_number_in_system(0) == 3
    assert collector.get_utilization(0) == 1.0


def test_waiting_and_release_transitions_are_captured():
    collector = SimulationMetricsCollector(1)
    resource = FakeResource(capacity=1)
    collector.register_service_resource(0, resource)

    first = collector.start_external_request("read", 1.0)
    resource.count = 1
    collector.record_service_state(0, 1.0)

    second = collector.start_external_request("read", 1.25)
    resource.queue.append(second)
    collector.record_service_state(0, 1.25)

    collector.record_external_request_completion(first, 2.0)
    resource.count = 0
    collector.record_service_state(0, 2.0)
    resource.queue.pop(0)
    resource.count = 1
    collector.record_service_state(0, 2.0)

    collector.record_external_request_completion(second, 3.0)
    resource.count = 0
    collector.record_service_state(0, 3.0)

    assert collector.get_queue_history()[0] == (
        (0.0, 0),
        (1.25, 1),
        (2.0, 0),
    )
    assert collector.get_busy_history()[0] == (
        (0.0, 0),
        (1.0, 1),
        (3.0, 0),
    )
    assert collector.get_active_history() == (
        (0.0, 0),
        (1.0, 1),
        (1.25, 2),
        (2.0, 1),
        (3.0, 0),
    )


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

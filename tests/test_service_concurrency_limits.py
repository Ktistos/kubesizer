import pytest

from performance_models import QueueingNetworkType, System, parse_workload_spec


def _workload(
    *,
    arrival_rate=8.0,
    service_rate=5.0,
    processing_slots=2,
    concurrency=4,
):
    return {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "api": {
                        "distribution": "exponential",
                        "rate": arrival_rate,
                    }
                }
            },
            "service_times": {
                "api": {
                    "processing_slots_per_replica": processing_slots,
                    "concurrency_limit_per_replica": concurrency,
                    "distribution": "exponential",
                    "rate": service_rate,
                }
            },
        },
    }


def test_unknown_replica_queue_model_is_rejected():
    workload = _workload()
    workload["traffic_model"]["service_times"]["api"]["replica_queue_model"] = "magical"

    with pytest.raises(ValueError, match="replica_queue_model must be"):
        parse_workload_spec(workload)


def test_processor_sharing_supports_independent_replica_queues():
    workload = _workload(processing_slots=1, concurrency=4)
    service = workload["traffic_model"]["service_times"]["api"]
    service["execution_model"] = "processor_sharing"
    service["replica_queue_model"] = "independent_round_robin"

    parsed = parse_workload_spec(workload)

    assert parsed.traffic_model.service_replica_queue_models == {
        "api": "independent_round_robin"
    }


@pytest.mark.parametrize("value", (0, -1, 1.5, True))
def test_concurrency_limit_must_be_a_positive_integer(value):
    workload = _workload(concurrency=value)

    with pytest.raises(ValueError, match="positive integer"):
        parse_workload_spec(workload)


@pytest.mark.parametrize("value", (0, -1, 1.5, True))
def test_processing_slots_must_be_a_positive_integer(value):
    workload = _workload(processing_slots=value, concurrency=4)

    with pytest.raises(ValueError, match="positive integer"):
        parse_workload_spec(workload)


def test_concurrency_cannot_be_smaller_than_processing_capacity():
    workload = _workload(processing_slots=2, concurrency=1)

    with pytest.raises(ValueError, match="cannot be smaller"):
        parse_workload_spec(workload)


def test_omitted_limits_preserve_one_slot_per_replica():
    workload = _workload(processing_slots=1, concurrency=1)
    del workload["traffic_model"]["service_times"]["api"][
        "processing_slots_per_replica"
    ]
    del workload["traffic_model"]["service_times"]["api"][
        "concurrency_limit_per_replica"
    ]
    system = System("serial-api", workload, QueueingNetworkType.OPEN)

    assert system.get_service_processing_slots_per_replica() == {"api": 1}
    assert system.get_service_concurrency_limits_per_replica() == {"api": 1}
    assert system.get_minimum_stable_replicas() == [2]


def test_processor_sharing_rejects_competing_fixed_slot_semantics():
    workload = _workload(processing_slots=2, concurrency=4)
    workload["traffic_model"]["service_times"]["api"][
        "execution_model"
    ] = "processor_sharing"

    with pytest.raises(ValueError, match="must be one for processor_sharing"):
        parse_workload_spec(workload)

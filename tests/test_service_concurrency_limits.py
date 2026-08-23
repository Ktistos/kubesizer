import pytest

from calibration.profile import experiment_fingerprints
from native_backend import get_native_backend
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


def test_processing_and_concurrency_capacities_are_kept_distinct():
    workload = _workload()
    parsed = parse_workload_spec(workload)
    system = System("parallel-api", workload, QueueingNetworkType.OPEN)
    serialized = get_native_backend().serialize_system(system)

    assert parsed.traffic_model.processing_slots_per_replica == {"api": 2}
    assert parsed.traffic_model.concurrency_limits_per_replica == {"api": 4}
    assert system.get_service_processing_slots_per_replica() == {"api": 2}
    assert system.get_service_concurrency_limits_per_replica() == {"api": 4}
    assert system.get_effective_service_capacities() == [2]
    assert system.get_minimum_stable_replicas() == [1]
    assert system.solve().get_mean_utilizations() == pytest.approx([0.8])
    assert serialized["services"][0]["replicas"] == 1
    assert serialized["services"][0]["capacity"] == 2
    assert serialized["services"][0]["concurrency_capacity"] == 4


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


def test_resource_capacity_semantics_invalidate_statistical_calibration():
    original = System(
        "original",
        _workload(processing_slots=1, concurrency=2),
        QueueingNetworkType.OPEN,
    )
    changed = System(
        "changed",
        _workload(processing_slots=2, concurrency=4),
        QueueingNetworkType.OPEN,
    )

    assert experiment_fingerprints(original)[0] != experiment_fingerprints(
        changed
    )[0]


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

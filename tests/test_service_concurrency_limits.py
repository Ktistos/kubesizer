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
    assert serialized["services"][0]["replica_queue_model"] == (
        "independent_round_robin"
    )


@pytest.mark.parametrize(
    "queue_model",
    ("pooled", "independent_random", "independent_round_robin"),
)
def test_replica_queue_model_is_parsed_and_serialized(queue_model):
    workload = _workload()
    workload["traffic_model"]["service_times"]["api"][
        "replica_queue_model"
    ] = queue_model

    parsed = parse_workload_spec(workload)
    system = System("queue-model", workload, QueueingNetworkType.OPEN)

    assert parsed.traffic_model.service_replica_queue_models == {
        "api": queue_model
    }
    assert system.get_service_replica_queue_models() == {"api": queue_model}
    assert (
        get_native_backend().serialize_system(system)["services"][0][
            "replica_queue_model"
        ]
        == queue_model
    )


def test_unknown_replica_queue_model_is_rejected():
    workload = _workload()
    workload["traffic_model"]["service_times"]["api"][
        "replica_queue_model"
    ] = "magical"

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

    assert experiment_fingerprints(original)[0] != experiment_fingerprints(changed)[0]


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


def test_processor_sharing_has_no_unmeasurable_slot_count():
    workload = _workload(processing_slots=1, concurrency=4)
    service = workload["traffic_model"]["service_times"]["api"]
    service.pop("concurrency_limit_per_replica")
    service["execution_model"] = "processor_sharing"
    parsed = parse_workload_spec(workload)
    system = System("cpu-api", workload, QueueingNetworkType.OPEN)

    assert parsed.traffic_model.service_execution_models == {"api": "processor_sharing"}
    assert system.get_service_execution_models() == {"api": "processor_sharing"}
    assert (
        get_native_backend().serialize_system(system)["services"][0]["execution_model"]
        == "processor_sharing"
    )


def test_processor_sharing_rejects_competing_fixed_slot_semantics():
    workload = _workload(processing_slots=2, concurrency=4)
    workload["traffic_model"]["service_times"]["api"][
        "execution_model"
    ] = "processor_sharing"

    with pytest.raises(ValueError, match="must be one for processor_sharing"):
        parse_workload_spec(workload)


def test_execution_model_changes_calibration_fingerprint():
    fixed = _workload(processing_slots=1, concurrency=1)
    shared = _workload(processing_slots=1, concurrency=1)
    fixed["traffic_model"]["service_times"]["api"].pop("concurrency_limit_per_replica")
    shared["traffic_model"]["service_times"]["api"].pop("concurrency_limit_per_replica")
    shared["traffic_model"]["service_times"]["api"][
        "execution_model"
    ] = "processor_sharing"

    fixed_system = System("api", fixed, QueueingNetworkType.OPEN)
    shared_system = System("api", shared, QueueingNetworkType.OPEN)

    assert (
        experiment_fingerprints(fixed_system)[0]
        != experiment_fingerprints(shared_system)[0]
    )


def test_replica_queue_model_changes_calibration_fingerprint():
    pooled = _workload(processing_slots=1, concurrency=1)
    independent = _workload(processing_slots=1, concurrency=1)
    pooled["traffic_model"]["service_times"]["api"][
        "replica_queue_model"
    ] = "pooled"
    independent["traffic_model"]["service_times"]["api"][
        "replica_queue_model"
    ] = "independent_round_robin"

    pooled_system = System("api", pooled, QueueingNetworkType.OPEN)
    independent_system = System("api", independent, QueueingNetworkType.OPEN)

    assert (
        experiment_fingerprints(pooled_system)[0]
        != experiment_fingerprints(independent_system)[0]
    )


def test_processor_sharing_accepts_a_declared_external_pool_limit():
    workload = _workload(processing_slots=1, concurrency=4)
    workload["traffic_model"]["service_times"]["api"][
        "execution_model"
    ] = "processor_sharing"

    parsed = parse_workload_spec(workload)
    assert parsed.traffic_model.concurrency_limits_per_replica["api"] == 4

    system = System("limited-processor-sharing", workload, QueueingNetworkType.OPEN)
    serialized = get_native_backend().serialize_system(system)
    assert serialized["services"][0]["execution_model"] == "processor_sharing"
    assert serialized["services"][0]["concurrency_capacity"] == 4

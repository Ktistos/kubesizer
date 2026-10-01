import pytest

from performance_models import QueueingNetworkType, System, parse_workload_spec
from analytical_workflow import optimize_with_analytical_model


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
                "request": {"api": {"distribution": "deterministic", "rate": 1.0}}
            },
            "service_times": service_times,
        },
    }


@pytest.mark.parametrize("mode", (None, "overlap"))
def test_call_graph_requires_explicit_supported_execution_semantics(mode):
    workload = _workload()
    if mode is None:
        workload["request_classes"]["request"].pop("call_execution")
    else:
        workload["request_classes"]["request"]["call_execution"] = mode

    with pytest.raises(ValueError, match="call_execution"):
        parse_workload_spec(workload)


def test_strict_analytical_sizing_rejects_held_parent_concurrency():
    system = System(
        "held-parent-analytical",
        _workload(call_execution="sequential"),
        QueueingNetworkType.OPEN,
    )

    assert any(
        "synchronous downstream calls hold finite parent concurrency" in reason
        for reason in system.get_strict_analytical_incompatibilities()
    )
    with pytest.raises(ValueError, match="cannot certify this workload"):
        optimize_with_analytical_model(system, maximum_replicas=4)


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
                "request": {"api": {"distribution": "exponential", "rate": 1.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }
    system = System("non-open", workload, QueueingNetworkType.OPEN)

    with pytest.raises(ValueError, match="mean offspring spectral radius"):
        system.get_effective_arrival_rates()

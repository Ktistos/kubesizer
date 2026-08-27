import copy
from types import SimpleNamespace

import pytest

from calibration.profile import experiment_fingerprints
from optimization.simulation_evaluation import (
    CalibratedConfigurationEvaluator,
)
from performance_models import QueueingNetworkType, System


def _single_service_workload(*, input_uncertainty=None, concurrency_limit=None):
    service = {
        "execution_model": "processor_sharing",
        "distribution": "deterministic",
        "mean": 0.1,
    }
    if concurrency_limit is not None:
        service["concurrency_limit_per_replica"] = concurrency_limit
    traffic_model = {
        "entry_services": {
            "request": {
                "api": {"distribution": "deterministic", "rate": 2.0}
            }
        },
        "service_times": {"api": service},
    }
    if input_uncertainty is not None:
        traffic_model["input_uncertainty"] = input_uncertainty
    return {
        "request_classes": {
            "request": {"concurrency_weight": 1.0, "routing": {}}
        },
        "traffic_model": traffic_model,
    }


def test_aligned_input_epoch_is_sampled_jointly_once_per_replication():
    uncertainty = {
        "sampling": "moving_block_bootstrap",
        "block_length": 1,
        "epochs": [
            {
                "arrival_rate_multipliers": {"request": 0.5},
                "service_time_multipliers": {"api": 0.75},
            },
            {
                "arrival_rate_multipliers": {"request": 2.0},
                "service_time_multipliers": {"api": 1.5},
            },
        ],
    }
    system = System(
        "joint-input",
        _single_service_workload(input_uncertainty=uncertainty),
        QueueingNetworkType.OPEN,
    )

    replications = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=2.0,
        seeds=(17, 18, 19, 20),
        workers=2,
    )

    expected_pairs = {(0.5, 0.75), (2.0, 1.5)}
    assert {
        (
            item.input_arrival_rate_multipliers["request"],
            item.input_service_time_multipliers[0],
        )
        for item in replications
    } <= expected_pairs
    assert all(item.sampled_input_block_start in {0, 1} for item in replications)
    assert replications == system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=2.0,
        seeds=(17, 18, 19, 20),
        workers=2,
    )


def test_joint_input_model_changes_the_calibration_fingerprint():
    first_uncertainty = {
        "sampling": "moving_block_bootstrap",
        "block_length": 1,
        "epochs": [
            {
                "arrival_rate_multipliers": {"request": 0.5},
                "service_time_multipliers": {"api": 0.5},
            },
            {
                "arrival_rate_multipliers": {"request": 1.5},
                "service_time_multipliers": {"api": 1.5},
            },
        ],
    }
    second_uncertainty = copy.deepcopy(first_uncertainty)
    second_uncertainty["epochs"][1]["service_time_multipliers"]["api"] = 2.0
    first = System(
        "first",
        _single_service_workload(input_uncertainty=first_uncertainty),
        QueueingNetworkType.OPEN,
    )
    second = System(
        "second",
        _single_service_workload(input_uncertainty=second_uncertainty),
        QueueingNetworkType.OPEN,
    )

    assert experiment_fingerprints(first)[1] != experiment_fingerprints(second)[1]


def test_arrival_trace_retains_local_burst_order_instead_of_iid_sampling():
    workload = _single_service_workload()
    workload["traffic_model"]["entry_services"]["request"]["api"] = {
        "distribution": "arrival_trace",
        "timestamps": [0.0, 0.1, 0.2, 1.2, 1.3, 1.4, 2.4],
        "sampling": "block_bootstrap",
        "block_duration": 10.0,
    }
    system = System("bursty-trace", workload, QueueingNetworkType.OPEN)

    replication = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.5,
        seeds=(1,),
    )[0]
    arrivals = replication.request_class_arrival_times["request"]
    observed_intervals = tuple(
        round(right - left, 8) for left, right in zip(arrivals, arrivals[1:])
    )

    # The two short gaps remain adjacent to the long gap from the source trace.
    assert observed_intervals[:3] == (0.1, 1.0, 0.1)


def test_processor_sharing_pool_limit_queues_excess_concurrency():
    def two_streams(limit):
        specification = _single_service_workload(concurrency_limit=limit)
        specification["request_classes"] = {
            name: {"concurrency_weight": 0.5, "routing": {}}
            for name in ("left", "right")
        }
        specification["traffic_model"]["entry_services"] = {
            name: {"api": {"distribution": "deterministic", "rate": 0.4}}
            for name in ("left", "right")
        }
        return System("pool", specification, QueueingNetworkType.OPEN)

    unlimited_result = two_streams(None).simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=3.0,
        seeds=(91,),
    )[0]
    limited_result = two_streams(1).simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=3.0,
        seeds=(91,),
    )[0]

    assert sorted(
        latency
        for values in unlimited_result.request_class_latencies.values()
        for latency in values
    ) == pytest.approx([0.2, 0.2])
    assert sorted(
        latency
        for values in limited_result.request_class_latencies.values()
        for latency in values
    ) == pytest.approx([0.1, 0.2])


def test_placement_adds_directional_call_latency_and_node_slowdown():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "calls": {"api": {"db": 1.0}},
                "call_execution": "sequential",
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "api": {"distribution": "deterministic", "rate": 2.0}
                }
            },
            "service_times": {
                service: {"distribution": "deterministic", "mean": 0.1}
                for service in ("api", "db")
            },
        },
    }
    system = System("placed-calls", workload, QueueingNetworkType.OPEN)
    baseline = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(44,),
    )[0]
    system.configure_simulation_placement(
        placement={
            "node-a": {"api": 1, "db": 0},
            "node-b": {"api": 0, "db": 1},
        },
        communication_latency_seconds={
            "node-a": {"node-b": 0.02},
            "node-b": {"node-a": 0.03},
        },
        execution_time_multipliers={"node-a": 2.0, "node-b": 1.0},
    )
    placed = system.simulate_measurement_replications(
        warmup_duration=0.0,
        measurement_duration=1.0,
        seeds=(44,),
    )[0]

    assert baseline.request_class_latencies["request"] == pytest.approx((0.2,))
    assert placed.request_class_latencies["request"] == pytest.approx((0.35,))
    assert placed.placement_model_applied is True


def test_uncertainty_diagnostics_do_not_merge_input_and_monte_carlo_sources():
    evaluator = CalibratedConfigurationEvaluator.__new__(
        CalibratedConfigurationEvaluator
    )
    evaluator.system = SimpleNamespace(
        get_request_classes=lambda: ("request",),
        get_service_execution_models=lambda: {"api": "processor_sharing"},
    )
    evaluator.profile = SimpleNamespace(latency_slo_kind="percentile")
    diagnostics = evaluator._uncertainty_diagnostics(
        (
            type(
                "Replication",
                (),
                {
                    "sampled_input_block_start": 1,
                    "input_arrival_rate_multipliers": {"request": 0.8},
                    "input_service_time_multipliers": (1.2,),
                    "request_class_latencies": {"request": (0.8, 1.0, 1.2)},
                    "placement_model_applied": False,
                },
            )(),
            type(
                "Replication",
                (),
                {
                    "sampled_input_block_start": 2,
                    "input_arrival_rate_multipliers": {"request": 1.3},
                    "input_service_time_multipliers": (0.9,),
                    "request_class_latencies": {"request": (1.1, 1.3, 1.5)},
                    "placement_model_applied": True,
                },
            )(),
        )
    )

    assert diagnostics.simulation_replication_count == 2
    assert diagnostics.input_uncertainty_method == "aligned-moving-block-bootstrap"
    assert diagnostics.sampled_input_block_starts == (1, 2)
    assert diagnostics.request_class_arrival_multiplier_ranges == (
        ("request", 0.8, 1.3),
    )
    assert diagnostics.service_time_multiplier_ranges == ((0, 0.9, 1.2),)
    assert diagnostics.model_form_assumptions == (("api", "processor_sharing"),)
    assert diagnostics.placement_effects_applied is True
    assert diagnostics.request_class_statistic_variance_components[0][0] == (
        "request"
    )

"""Exact stage timing, shared capacity and fail-closed analytical evidence."""

import copy
import math

import pytest

from calibration.profile import experiment_fingerprints
from native_backend import get_native_backend
from native_backend.backend import CppSimulationBackend
from performance_models import QueueingNetworkType, System, parse_workload_spec


def stage(resource, seconds, **extra):
    return dict(
        resource=resource,
        service_time=dict(distribution="deterministic", mean=seconds),
        **extra
    )


def workload(*, completion="all", rate=0.1):
    policy = {"completion": completion}
    if completion == "quorum":
        policy["required"] = 1
    return dict(
        analytical_call_model="response_bounds",
        request_classes={
            "write": dict(
                concurrency_weight=1,
                stage_workflow=dict(
                    entries={"cpu": "start"},
                    stages={
                        "start": stage("cpu", 0.1, next="fanout"),
                        "fanout": dict(
                            calls={"fast": 1, "slow": 1},
                            call_policy=policy,
                            next="finish",
                        ),
                        "fast": stage("disk", 0.2),
                        "slow": stage("disk", 0.4),
                        "finish": stage("cpu", 0.1),
                    },
                ),
                end_to_end_latency_slos={"mean_seconds": 2},
            )
        },
        traffic_model=dict(
            entry_services={
                "write": {"cpu": dict(distribution="deterministic", rate=rate)}
            },
            service_times={
                "cpu": {"replica_queue_model": "pooled"},
                "disk": {"replica_queue_model": "pooled"},
            },
        ),
        replica_constraints={"cpu": {"fixed": 1}, "disk": {"fixed": 1}},
    )


def system(raw):
    return System("staged", raw, QueueingNetworkType.OPEN)


def latencies(raw, *, measurement=35):
    return (
        system(raw)
        .simulate_measurement_replications(
            warmup_duration=0, measurement_duration=measurement, seeds=(17,)
        )[0]
        .request_class_latencies["write"]
    )


@pytest.mark.parametrize(
    "completion,expected", [("all", 0.8), ("quorum", 0.4), ("background", 0.2)]
)
def test_exact_multiphase_response_and_same_resource_revisits(completion, expected):
    assert latencies(workload(completion=completion)) == pytest.approx((expected,) * 3)


def test_resource_moments_and_stability_count_all_late_work():
    raw = workload(completion="background")
    model = system(raw)
    assert tuple(model.get_services()) == ("cpu", "disk")
    assert model.get_effective_arrival_rates() == pytest.approx((0.2, 0.2))
    means = model.get_request_class_service_time_distribution_specs()["write"]
    assert means["cpu"].mean == pytest.approx(0.1)
    assert means["disk"].mean == pytest.approx(0.3)
    raw["traffic_model"]["entry_services"]["write"]["cpu"]["rate"] = 2
    with pytest.raises(ValueError, match="stability"):
        system(raw).get_minimum_stable_replicas()


def test_phases_on_one_disk_share_one_queue_instead_of_independent_capacity():
    raw = workload()
    shared = latencies(raw)
    raw["traffic_model"]["service_times"]["disk2"] = {}
    raw["replica_constraints"]["disk2"] = {"fixed": 1}
    raw["request_classes"]["write"]["stage_workflow"]["stages"]["slow"][
        "resource"
    ] = "disk2"
    independent = latencies(raw)
    assert shared == pytest.approx((0.8,) * 3)
    assert independent == pytest.approx((0.6,) * 3)


def test_control_fork_overlaps_local_work_with_remote_quorum():
    raw = workload(completion="quorum")
    flow = raw["request_classes"]["write"]["stage_workflow"]
    flow["entries"] = {"cpu": "root"}
    flow["stages"]["root"] = {"calls": {"start": 1, "fanout": 1}, "next": "finish"}
    flow["stages"]["start"].pop("next")
    flow["stages"]["fanout"].pop("next")
    # local .1 and first disk completion .2 start together; final CPU .1 follows both.
    assert latencies(raw) == pytest.approx((0.3,) * 3)


def test_background_revisit_can_delay_finalization_on_same_physical_cpu():
    raw = workload(completion="background")
    stages = raw["request_classes"]["write"]["stage_workflow"]["stages"]
    stages["fast"] = stage("cpu", 0.2)
    # The background branch is admitted before the continuation; both need CPU.
    assert latencies(raw) == pytest.approx((0.4,) * 3)


def test_per_stage_demand_and_work_units_are_sampled_instead_of_projected_gamma():
    raw = workload()
    stages = raw["request_classes"]["write"]["stage_workflow"]["stages"]
    stages["start"]["work_units"] = 2
    stages["start"]["unit"] = "KiB"
    assert latencies(raw) == pytest.approx((0.9,) * 3)
    assert system(raw).get_request_class_service_time_distribution_specs()["write"][
        "cpu"
    ].mean == pytest.approx(0.15)


def test_explicit_control_delay_does_not_occupy_physical_resource():
    raw = workload(completion="background")
    stages = raw["request_classes"]["write"]["stage_workflow"]["stages"]
    stages["fanout"]["delay"] = {"distribution": "deterministic", "mean": 0.3}
    assert latencies(raw) == pytest.approx((0.5,) * 3)


def test_no_implicit_communication_delay_for_local_continuation():
    raw = workload(completion="quorum")
    raw["traffic_model"]["communication_latency"] = {
        "same_node_one_way": {"distribution": "deterministic", "mean": 0.01},
        "cross_node_one_way": {"distribution": "deterministic", "mean": 0.01},
        "cross_node_probability": 1,
    }
    assert latencies(raw) == pytest.approx((0.42,) * 3)


@pytest.mark.parametrize("completion", ["quorum", "background"])
def test_late_returns_and_deadlines_preserve_stage_ownership(completion):
    raw = workload(completion=completion, rate=1)
    raw["request_classes"]["write"]["end_to_end_timeout_seconds"] = 0.3
    result = get_native_backend().simulate(
        system(raw), seed=9, horizon=300, warmup_duration=0, capture_state=False
    )
    assert result["completed_request_count"] > 290
    assert result["metrics"]["mean_utilizations"][1] > 0.5


def test_bounds_are_not_published_as_point_predictions():
    raw = workload(completion="quorum")
    model = system(raw)
    low, high = model.get_request_class_latency_bounds()["write"]
    assert low > 0 and high > low
    with pytest.raises(ValueError, match="bounds"):
        model.get_request_class_mean_latencies()
    raw["request_classes"]["write"]["stage_workflow"]["stages"]["start"][
        "hold_resource"
    ] = True
    # No children at start: holding ends before continuation, so this remains valid.
    assert system(raw).get_strict_analytical_incompatibilities() == ()


def test_analytical_holding_and_blocking_abstain():
    raw = workload()
    stages = raw["request_classes"]["write"]["stage_workflow"]["stages"]
    stages["start"]["calls"] = {"fast": 1}
    stages["start"]["hold_resource"] = True
    # fast remains reachable both here and through fanout, sharing its disk.
    with pytest.raises(ValueError, match="hold parent"):
        system(raw).get_request_class_latency_bounds()
    raw = workload()
    stages = raw["request_classes"]["write"]["stage_workflow"]["stages"]
    stages["start"]["non_cpu_blocking_time"] = {
        "distribution": "deterministic",
        "mean": 0.1,
    }
    assert latencies(raw) == pytest.approx((0.9,) * 3)
    with pytest.raises(ValueError, match="blocking"):
        system(raw).get_request_class_latency_bounds()


@pytest.mark.parametrize("change", ["order", "demand", "units", "resource", "policy"])
def test_stage_changes_invalidate_calibration_even_when_resource_counts_match(change):
    original = workload()
    changed = copy.deepcopy(original)
    stages = changed["request_classes"]["write"]["stage_workflow"]["stages"]
    if change == "order":
        stages["fast"]["service_time"], stages["slow"]["service_time"] = (
            stages["slow"]["service_time"],
            stages["fast"]["service_time"],
        )
    elif change == "demand":
        stages["start"]["service_time"]["mean"] += 0.01
    elif change == "units":
        stages["start"]["unit"] = "bytes"
    elif change == "resource":
        stages["fast"]["resource"] = "cpu"
    else:
        stages["fanout"]["call_policy"] = {"completion": "quorum", "required": 1}
    assert experiment_fingerprints(system(original)) != experiment_fingerprints(
        system(changed)
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "cycle",
        "unknown_resource",
        "unknown_next",
        "bad_units",
        "control_work",
        "resource_delay",
        "unknown_field",
        "bad_quorum",
        "unreachable",
        "entry_mismatch",
    ],
)
def test_invalid_stage_plans_fail_before_native_execution(mutation):
    raw = workload()
    flow = raw["request_classes"]["write"]["stage_workflow"]
    stages = flow["stages"]
    if mutation == "cycle":
        stages["finish"]["next"] = "start"
    elif mutation == "unknown_resource":
        stages["start"]["resource"] = "missing"
    elif mutation == "unknown_next":
        stages["start"]["next"] = "missing"
    elif mutation == "bad_units":
        stages["start"]["work_units"] = math.nan
    elif mutation == "control_work":
        stages["fanout"]["service_time"] = {
            "distribution": "deterministic",
            "mean": 0.1,
        }
    elif mutation == "resource_delay":
        stages["start"]["delay"] = {"distribution": "deterministic", "mean": 0.1}
    elif mutation == "unknown_field":
        stages["start"]["mispelled"] = True
    elif mutation == "bad_quorum":
        stages["fanout"]["call_policy"] = {"completion": "quorum", "required": 3}
    elif mutation == "unreachable":
        stages["unused"] = stage("disk", 0.1)
    else:
        flow["entries"] = {"disk": "start"}
    with pytest.raises(ValueError):
        parse_workload_spec(raw)


def test_native_parser_rejects_cycles_when_python_validation_is_bypassed():
    model = system(workload())
    serialized = CppSimulationBackend.serialize_system(model)
    flow = serialized["request_classes"][0]["stage_workflow"]
    flow["stages"][-1]["next"] = 0
    with pytest.raises(RuntimeError, match="acyclic"):
        get_native_backend()._invoke(
            "simulate",
            model,
            serialized_workload=serialized,
            seed=7,
            horizon=30,
            warmup_duration=0,
            capture_state=False,
        )


def test_mixed_classes_keep_one_physical_queue_and_class_specific_stage_work():
    raw = workload(completion="background")
    raw["request_classes"]["write"]["concurrency_weight"] = 0.25
    raw["request_classes"]["read"] = {
        "concurrency_weight": 0.75,
        "calls": {},
        "call_execution": "parallel",
    }
    raw["traffic_model"]["entry_services"]["read"] = {
        "cpu": {"distribution": "deterministic", "rate": 0.3}
    }
    raw["traffic_model"]["service_times"]["cpu"].update(
        distribution="deterministic", mean=0.4
    )
    model = system(raw)
    assert model.get_effective_arrival_rates() == pytest.approx((0.5, 0.2))
    assert model.get_service_time_distribution_specs()["cpu"].mean > 0
    rep = model.simulate_measurement_replications(
        warmup_duration=0, measurement_duration=35, seeds=(2,)
    )[0]
    assert len(rep.request_class_latencies["write"]) == 3
    assert len(rep.request_class_latencies["read"]) == 10
    assert len(model.get_services()) == 2


def test_multiple_entries_project_different_stage_demands_to_one_resource():
    raw = workload()
    raw["traffic_model"]["service_times"]["cpu2"] = {}
    raw["replica_constraints"]["cpu2"] = {"fixed": 1}
    raw["traffic_model"]["entry_services"]["write"]["cpu2"] = {
        "distribution": "deterministic",
        "rate": 0.3,
    }
    flow = raw["request_classes"]["write"]["stage_workflow"]
    flow["entries"]["cpu2"] = "other"
    flow["stages"]["other"] = stage("cpu2", 0.2, next="slow")
    model = system(raw)
    rates = model.get_effective_arrival_rates()
    assert rates == pytest.approx((0.2, 0.5, 0.3))
    means = model.get_request_class_service_time_distribution_specs()["write"]
    assert means["disk"].mean == pytest.approx(0.36)
    # Reweighting only one external entry would invalidate the frozen mixture.
    model.get_service("cpu2").add_gamma("write", 0.2)
    with pytest.raises(ValueError, match="entry shares"):
        model.get_effective_arrival_rates()


def test_stage_choice_selects_one_path_and_sequential_calls_preserve_order():
    raw = workload()
    stages = raw["request_classes"]["write"]["stage_workflow"]["stages"]
    stages["fanout"]["calls"] = {"fast": 0.25, "slow": 0.75}
    stages["fanout"]["call_policy"] = {"execution": "choice"}
    values = latencies(raw, measurement=10010)
    assert all(any(abs(x - v) < 1e-8 for v in (0.4, 0.6)) for x in values)
    raw = workload()
    raw["request_classes"]["write"]["stage_workflow"]["stages"]["fanout"][
        "call_policy"
    ] = {"execution": "sequential"}
    assert latencies(raw) == pytest.approx((0.8,) * 3)


def test_staged_analytical_sizing_keeps_upper_bounds_and_fixed_members():
    from statistical_workflow import optimize_with_analytical_model

    result = optimize_with_analytical_model(system(workload()), maximum_replicas=5)
    assert result.configuration == (1, 1)
    assert result.metrics.evaluation_basis == "ANALYTICAL_MEAN_LATENCY_UPPER_BOUND"
    assert not result.metrics.means


def test_replicas_change_physical_capacity_without_copying_stage_resources():
    raw = workload()
    raw["replica_constraints"]["disk"] = {"minimum": 1, "maximum": 2}
    model = system(raw)
    model.configure_system_replicas((1, 2))
    result = model.simulate_measurement_replications(
        warmup_duration=0, measurement_duration=35, seeds=(8,)
    )[0]
    assert result.request_class_latencies["write"] == pytest.approx((0.6,) * 3)
    assert result.replica_configuration == (1, 2)


def test_empirical_stage_samples_are_preserved_through_work_scaling():
    raw = workload()
    target = raw["request_classes"]["write"]["stage_workflow"]["stages"]["fast"]
    target["service_time"] = {
        "distribution": "empirical",
        "samples": [0.2, 0.6],
        "sampling": "iid_with_replacement",
    }
    target["work_units"] = 2
    values = latencies(raw, measurement=2010)
    assert all(any(abs(x - v) < 1e-8 for v in (1.0, 1.8)) for x in values)
    assert any(abs(x - 1.0) < 1e-8 for x in values)
    assert any(abs(x - 1.8) < 1e-8 for x in values)


def test_joint_input_resource_multipliers_apply_to_stage_work():
    raw = workload()
    epoch = {
        "arrival_rate_multipliers": {"write": 1},
        "service_time_multipliers": {"cpu": 2, "disk": 2},
    }
    raw["traffic_model"]["input_uncertainty"] = {
        "sampling": "moving_block_bootstrap",
        "block_length": 1,
        "epochs": [epoch, copy.deepcopy(epoch)],
    }
    assert latencies(raw) == pytest.approx((1.6,) * 3)


@pytest.mark.parametrize(
    "invalid", ["resource", "next", "entry", "quorum", "work_units"]
)
def test_native_stage_indices_and_policy_are_validated_independently(invalid):
    model = system(workload())
    serialized = CppSimulationBackend.serialize_system(model)
    flow = serialized["request_classes"][0]["stage_workflow"]
    if invalid == "resource":
        flow["stages"][0]["resource"] = 999
    elif invalid == "next":
        flow["stages"][0]["next"] = 999
    elif invalid == "entry":
        flow["entries"][0] = 999
    elif invalid == "quorum":
        flow["stages"][1]["call_policy"] = {
            "execution": "parallel",
            "completion": "quorum",
            "required": 99,
        }
    else:
        flow["stages"][0]["work_units"] = 0
    with pytest.raises(RuntimeError):
        get_native_backend()._invoke(
            "simulate",
            model,
            serialized_workload=serialized,
            seed=7,
            horizon=30,
            warmup_duration=0,
            capture_state=False,
        )

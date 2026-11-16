"""Mechanism checks have exact answers; these are not deployment experiments."""

from pathlib import Path

import pytest
import yaml

from calibration.profile import experiment_fingerprints
from native_backend import get_native_backend
from optimization import AnalyticalReplicaSizingProblem
from performance_models import QueueingNetworkType, System, parse_workload_spec
from statistical_workflow import _maximum_configuration, optimize_with_analytical_model


def workload(policy=None, *, child_times=(0.2, 0.4, 0.8), rate=0.1):
    result = {
        "request_classes": {
            "write": {
                "concurrency_weight": 1.0,
                "calls": {"primary": {f"f{i}": 1.0 for i in range(len(child_times))}},
                "call_execution": "parallel",
            }
        },
        "traffic_model": {
            "entry_services": {
                "write": {"primary": {"distribution": "deterministic", "rate": rate}}
            },
            "service_times": {
                "primary": {"distribution": "deterministic", "mean": 0.1},
                **{
                    f"f{i}": {"distribution": "deterministic", "mean": value}
                    for i, value in enumerate(child_times)
                },
            },
        },
    }
    if policy is not None:
        result["request_classes"]["write"]["call_policies"] = {"primary": policy}
    return result


def system(value):
    return System("stateful-test", value, QueueingNetworkType.OPEN)


def latencies(value, *, measurement=35.0, seeds=(21,)):
    return (
        system(value)
        .simulate_measurement_replications(
            warmup_duration=0.0, measurement_duration=measurement, seeds=seeds
        )[0]
        .request_class_latencies["write"]
    )


@pytest.mark.parametrize(
    "policy,expected",
    [
        (None, 0.9),
        ({"execution": "sequential"}, 1.5),
        ({"completion": "quorum", "required": 1}, 0.3),
        ({"completion": "quorum", "required": 2}, 0.5),
        ({"completion": "quorum", "required": 3}, 0.9),
        ({"completion": "background"}, 0.1),
    ],
)
def test_exact_response_boundary(policy, expected):
    assert latencies(workload(policy)) == pytest.approx((expected,) * 3)


def test_choice_selects_exactly_one_target_instead_of_independent_bernoulli_calls():
    value = workload({"execution": "choice"}, child_times=(0.2, 0.8))
    value["request_classes"]["write"]["calls"]["primary"] = {"f0": 0.25, "f1": 0.75}
    observations = latencies(value, measurement=10010.0)
    assert len(observations) == 1000
    assert all(
        any(abs(x - expected) < 1e-9 for expected in (0.3, 0.9)) for x in observations
    )
    assert sum(abs(x - 0.3) < 1e-9 for x in observations) / len(
        observations
    ) == pytest.approx(0.25, abs=0.05)
    rates = system(value).get_request_class_effective_arrival_rates()["write"]
    assert rates == pytest.approx((0.1, 0.025, 0.075))


def test_mixed_policies_apply_at_their_own_sources():
    value = workload({"execution": "sequential"}, child_times=(0.2, 0.4))
    value["request_classes"]["write"]["calls"]["f0"] = {"disk": 1.0}
    value["traffic_model"]["service_times"]["disk"] = {
        "distribution": "deterministic",
        "mean": 0.05,
    }
    assert latencies(value) == pytest.approx((0.75,) * 3)


@pytest.mark.parametrize("completion", ["background", "quorum"])
def test_unacknowledged_work_keeps_competing_and_survives_request_timeouts(completion):
    policy = {"completion": completion}
    if completion == "quorum":
        policy["required"] = 1
    value = workload(policy, child_times=(0.01, 0.8), rate=1.0)
    value["request_classes"]["write"]["end_to_end_timeout_seconds"] = 0.2
    model = system(value)
    result = get_native_backend().simulate(
        model, seed=22, horizon=30.5, warmup_duration=0.0, capture_state=True
    )
    slow = model.get_service("f1").get_id()
    assert result["metrics"]["mean_utilizations"][slow] > 0.7
    assert result["metrics"]["mean_latencies"][
        model.get_service("primary").get_id()
    ] == pytest.approx(0.1 if completion == "background" else 0.11)
    # Completion/timeout events continue well beyond the first quorum. ASan
    # checks ownership while late work returns to already-responded parents.
    assert result["completed_request_count"] >= 29


def test_nested_background_work_retains_ancestors_until_late_returns():
    value = workload({"completion": "background"}, child_times=(0.1,))
    value["request_classes"]["write"]["calls"]["f0"] = {"disk": 1.0}
    value["traffic_model"]["service_times"]["disk"] = {
        "distribution": "deterministic",
        "mean": 0.9,
    }
    assert latencies(value) == pytest.approx((0.1,) * 3)
    result = get_native_backend().simulate(
        system(value), seed=8, horizon=50.0, warmup_duration=0.0, capture_state=False
    )
    assert result["completed_request_count"] == 4


def test_delayed_quorum_response_does_not_respond_twice_when_children_finish():
    value = workload({"completion": "quorum", "required": 1})
    value["traffic_model"]["service_times"]["primary"]["post_release_delay"] = {
        "distribution": "deterministic",
        "mean": 1.0,
    }
    assert latencies(value) == pytest.approx((1.3,) * 3)


@pytest.mark.parametrize(
    "policy",
    [
        {"completion": "quorum", "required": True},
        {"completion": "quorum", "required": 0},
        {"completion": "quorum", "required": 4},
        {"completion": "quorum", "required": 1, "execution": "sequential"},
        {"completion": "background", "execution": "choice"},
        {"execution": "choice"},
        {"completion": "all", "required": 1},
        {"completion": "any"},
        {"unknown": 1},
    ],
)
def test_invalid_policy_fails_before_simulation(policy):
    with pytest.raises(ValueError, match="call_policies"):
        parse_workload_spec(workload(policy))


def test_quorum_rejects_fractional_or_duplicate_expected_visits():
    value = workload({"completion": "quorum", "required": 1})
    value["request_classes"]["write"]["calls"]["primary"]["f0"] = 0.5
    with pytest.raises(ValueError, match="exactly one call"):
        parse_workload_spec(value)


def test_new_semantics_invalidate_calibration_and_reject_analytical_certification():
    original = system(workload())
    new = system(workload({"completion": "quorum", "required": 1}))
    assert experiment_fingerprints(original) != experiment_fingerprints(new)
    with pytest.raises(ValueError, match="require simulation"):
        optimize_with_analytical_model(new, maximum_replicas=3)


def test_replica_constraints_hold_during_search_and_direct_configuration():
    value = workload()
    value["request_classes"]["write"] = {
        "concurrency_weight": 1.0,
        "visits": {name: 1.0 for name in value["traffic_model"]["service_times"]},
    }
    value["replica_constraints"] = {
        "primary": {"fixed": 1},
        "f0": {"minimum": 2, "maximum": 3},
    }
    model = system(value)
    assert tuple(model.get_system_replicas()) == (1, 2, 1, 1)
    assert model.get_minimum_stable_replicas() == [1, 2, 1, 1]
    assert _maximum_configuration(model, 9) == (1, 3, 9, 9)
    problem = AnalyticalReplicaSizingProblem(model, {}, 9)
    assert problem.maximum_configuration() == (1, 3, 9, 9)
    assert all(
        candidate[0] == 1 for candidate in problem.neighbors(problem.initial_state())
    )
    before = tuple(model.get_system_replicas())
    with pytest.raises(ValueError, match="replica_constraints.primary"):
        model.configure_system_replicas((2, 3, 1, 1))
    assert tuple(model.get_system_replicas()) == before


def test_fixed_component_overload_abstains_instead_of_inventing_a_replica():
    value = workload(child_times=(20.0,))
    value["replica_constraints"] = {"f0": {"fixed": 1}}
    with pytest.raises(ValueError, match="cannot satisfy.*stability"):
        system(value).get_minimum_stable_replicas()


@pytest.mark.parametrize(
    "constraint",
    [
        {"fixed": True},
        {"fixed": 0},
        {"minimum": 2, "maximum": 1},
        {"fixed": 1, "maximum": 2},
    ],
)
def test_invalid_replica_constraints(constraint):
    value = workload()
    value["replica_constraints"] = {"primary": constraint}
    with pytest.raises(ValueError, match="replica_constraints"):
        parse_workload_spec(value)


def test_portable_characterization_preserves_fixed_component_constraints():
    from characterization import (
        build_simulation_workload_variants,
        characterize_deployment,
        parse_portable_sizing_inputs,
    )

    root = Path(__file__).parents[1] / "examples" / "portable_evidence"
    value = yaml.safe_load((root / "workload.yaml").read_text())
    value["replica_constraints"] = {"database": {"fixed": 1}}
    measurement = yaml.safe_load((root / "measurements.yaml").read_text())
    inputs = parse_portable_sizing_inputs(value, measurement, base_path=root)
    variants = build_simulation_workload_variants(
        inputs, characterize_deployment(inputs)
    )
    assert all(
        v.workload["replica_constraints"]["database"] == {"minimum": 1, "maximum": 1}
        for v in variants
    )


def test_async_write_work_contends_with_reads_on_the_same_disk():
    value = {
        "request_classes": {
            "read": {
                "concurrency_weight": 10 / 19,
                "calls": {},
                "call_execution": "parallel",
            },
            "write": {
                "concurrency_weight": 9 / 19,
                "calls": {"primary": {"disk": 1.0}},
                "call_execution": "parallel",
                "call_policies": {"primary": {"completion": "background"}},
            },
        },
        "traffic_model": {
            "entry_services": {
                "read": {"disk": {"distribution": "deterministic", "rate": 1 / 0.9}},
                "write": {"primary": {"distribution": "deterministic", "rate": 1.0}},
            },
            "service_times": {
                "primary": {"write": {"distribution": "deterministic", "mean": 0.1}},
                "disk": {
                    "read": {"distribution": "deterministic", "mean": 0.1},
                    "write": {"distribution": "deterministic", "mean": 0.7},
                },
            },
        },
    }
    model = system(value)
    replication = model.simulate_measurement_replications(
        warmup_duration=0.0, measurement_duration=20.0, seeds=(4,)
    )[0]
    assert max(replication.request_class_latencies["read"]) > 0.5
    assert replication.request_class_latencies["write"] == pytest.approx((0.1,) * 19)


@pytest.mark.parametrize(
    "path",
    sorted(
        path
        for path in (Path(__file__).parents[1] / "examples" / "stateful").glob("*.yaml")
        if path.stem != "candidates"
    ),
    ids=lambda p: p.stem,
)
def test_stateful_examples_are_executable_and_preserve_fixed_members(path):
    value = yaml.safe_load(path.read_text())
    model = system(value)
    model.get_minimum_stable_replicas()
    replication = model.simulate_measurement_replications(
        warmup_duration=0.0, measurement_duration=3.0, seeds=(14,)
    )[0]
    assert all(
        len(observations) > 0
        for observations in replication.request_class_latencies.values()
    )
    assert all(
        count == replication.completed_measured_request_counts[name]
        for name, count in replication.measured_request_counts.items()
    )


def test_replica_bounds_also_invalidate_calibration_fingerprints():
    value = workload()
    before = experiment_fingerprints(system(value))
    value["replica_constraints"] = {"primary": {"fixed": 1}}
    assert experiment_fingerprints(system(value)) != before


def test_late_network_returns_keep_valid_parent_ownership_after_quorum():
    value = workload(
        {"completion": "quorum", "required": 1}, child_times=(0.01, 0.5), rate=0.9
    )
    value["traffic_model"]["communication_latency"] = {
        "same_node_one_way": {"distribution": "deterministic", "mean": 0.2},
        "cross_node_one_way": {"distribution": "deterministic", "mean": 0.2},
        "cross_node_probability": 0.0,
    }
    value["request_classes"]["write"]["end_to_end_timeout_seconds"] = 0.6
    assert latencies(value, measurement=49.5) == pytest.approx((0.51,) * 44)


def test_native_parser_rejects_invalid_quorum_even_if_python_validation_is_bypassed():
    from native_backend import NativeBackendError

    model = system(workload({"completion": "quorum", "required": 1}))
    backend = get_native_backend()
    payload = backend.serialize_system(model)
    payload["request_classes"][0]["call_policies"][0]["required"] = 10
    with pytest.raises(NativeBackendError, match="invalid quorum"):
        backend._invoke(
            "simulate",
            model,
            serialized_workload=payload,
            seed=7,
            horizon=10.0,
            warmup_duration=0.0,
            capture_state=False,
        )

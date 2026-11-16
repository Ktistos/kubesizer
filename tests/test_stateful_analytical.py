"""Conditional analytical bounds, kept separate from point predictions."""

import pytest

from performance_models import QueueingNetworkType, System
from performance_models.call_policy import CallPolicySpec, call_completion_bounds
from statistical_workflow import optimize_with_analytical_model
from optimization import AnalyticalReplicaSizingProblem


@pytest.mark.parametrize(
    "policy,expected",
    [
        (CallPolicySpec("sequential"), (1.4, 1.4)),
        (CallPolicySpec("parallel"), (0.8, 1.4)),
        (CallPolicySpec("parallel", "quorum", 1), (0.0, 0.2)),
        (CallPolicySpec("parallel", "quorum", 2), (0.0, 0.6)),
        (CallPolicySpec("parallel", "quorum", 3), (0.8, 1.4)),
        (CallPolicySpec("parallel", "background"), (0.0, 0.0)),
    ],
)
def test_completion_inequalities(policy, expected):
    assert call_completion_bounds(
        policy, [(1, 0.2, 0.2), (1, 0.4, 0.4), (1, 0.8, 0.8)]
    ) == pytest.approx(expected)


def test_choice_bounds_use_probabilities_not_parallel_fanout():
    assert call_completion_bounds(
        CallPolicySpec("choice"), [(0.25, 0.2, 0.2), (0.75, 0.8, 0.8)]
    ) == pytest.approx((0.65, 0.65))


def workload():
    return {
        "analytical_call_model": "response_bounds",
        "request_classes": {
            "write": {
                "concurrency_weight": 1.0,
                "calls": {"primary": {"f1": 1.0, "f2": 1.0}},
                "call_execution": "parallel",
                "call_policies": {"primary": {"completion": "quorum", "required": 1}},
                "end_to_end_latency_slos": {"mean_seconds": 2.0},
            }
        },
        "traffic_model": {
            "entry_services": {
                "write": {"primary": {"distribution": "exponential", "rate": 0.1}}
            },
            "service_times": {
                "primary": {
                    "execution_model": "processor_sharing",
                    "replica_queue_model": "pooled",
                    "distribution": "exponential",
                    "rate": 10.0,
                },
                "f1": {"distribution": "exponential", "rate": 5.0},
                "f2": {"distribution": "exponential", "rate": 1.25},
            },
        },
        "replica_constraints": {
            "primary": {"fixed": 1},
            "f1": {"fixed": 1},
            "f2": {"fixed": 1},
        },
    }


def test_analytical_quorum_bound_drives_sizing_without_becoming_a_point_prediction():
    model = System("bounds", workload(), QueueingNetworkType.OPEN)
    bounds = model.get_request_class_latency_bounds()["write"]
    assert bounds[0] > 0.1
    assert bounds[1] < 0.4
    with pytest.raises(ValueError, match="require simulation"):
        model.get_request_class_mean_latencies()
    result = optimize_with_analytical_model(model, maximum_replicas=5)
    assert result.configuration == (1, 1, 1)
    assert result.metrics.evaluation_basis == "ANALYTICAL_MEAN_LATENCY_UPPER_BOUND"
    assert result.metrics.means == {}
    assert result.metrics.upper_bounds["write"] == pytest.approx(bounds[1])


def test_acknowledging_fewer_followers_does_not_remove_their_work():
    model = System("bounds", workload(), QueueingNetworkType.OPEN)
    assert model.get_effective_arrival_rates() == pytest.approx((0.1, 0.1, 0.1))
    assert model.get_request_class_latency_bounds()["write"][1] < sum(
        model.solve().get_mean_latencies()
    )


def test_analytical_bounds_still_reject_finite_parent_holding():
    value = workload()
    value["traffic_model"]["service_times"]["primary"] = {
        "distribution": "exponential",
        "rate": 10.0,
    }
    model = System("held", value, QueueingNetworkType.OPEN)
    with pytest.raises(ValueError, match="finite parent concurrency"):
        model.get_request_class_latency_bounds()


def test_background_acknowledgement_accepts_finite_local_capacity_without_child_holding():
    value = workload()
    value["traffic_model"]["service_times"]["primary"] = {
        "distribution": "exponential",
        "rate": 10.0,
    }
    value["request_classes"]["write"]["call_policies"]["primary"] = {
        "completion": "background"
    }
    model = System("async", value, QueueingNetworkType.OPEN)
    assert model.get_strict_analytical_incompatibilities() == ()
    low, high = model.get_request_class_latency_bounds()["write"]
    assert low == high == pytest.approx(model.solve().get_mean_latencies()[0])


@pytest.mark.parametrize(
    "limit,classification", [(0.15, "INCONCLUSIVE"), (0.05, "ANALYTICALLY_INFEASIBLE")]
)
def test_upper_bound_failure_alone_is_not_proof_of_slo_violation(limit, classification):
    value = workload()
    value["request_classes"]["write"]["end_to_end_latency_slos"]["mean_seconds"] = limit
    model = System("bounds-decision", value, QueueingNetworkType.OPEN)
    problem = AnalyticalReplicaSizingProblem(model, {"write": limit}, 3)
    evaluation = problem.evaluate((1, 1, 1))
    assert not evaluation.feasible
    assert evaluation.classification.value == classification
    assert evaluation.lower_bounds["write"] < evaluation.upper_bounds["write"]


def test_unresolved_cheaper_bound_prevents_a_proven_minimum_recommendation():
    value = workload()
    value["request_classes"]["write"]["end_to_end_latency_slos"]["mean_seconds"] = 0.51
    value["traffic_model"]["entry_services"]["write"]["primary"]["rate"] = 8.0
    value["traffic_model"]["service_times"]["f1"]["rate"] = 50.0
    value["traffic_model"]["service_times"]["f2"]["rate"] = 25.0
    value["replica_constraints"]["primary"] = {"minimum": 1, "maximum": 2}
    model = System("bound-minimum", value, QueueingNetworkType.OPEN)
    result = optimize_with_analytical_model(model, maximum_replicas=3)
    assert result.configuration == (2, 1, 1)
    assert not result.minimality_proven
    assert (1, 1, 1) in result.unresolved_lower_cost_states

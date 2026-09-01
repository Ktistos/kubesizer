"""Public, side-effect-free workflow for evidence-based initial sizing."""

from .analysis import characterize_deployment
from .models import PreparedInitialSizing
from .parser import load_portable_sizing_inputs, parse_portable_sizing_inputs
from .readiness import assess_initial_sizing_risk
from .sizing import recommend_initial_replicas, resolve_sizing_policy


def prepare_initial_sizing(
    deployment,
    workload,
    measurement_manifest,
    *,
    base_path,
    policy=None,
    evidence_gate_policy=None,
    require_recommendable=True,
):
    """Parse mappings, derive evidence, and return an initial recommendation."""
    inputs = parse_portable_sizing_inputs(
        deployment,
        workload,
        measurement_manifest,
        base_path=base_path,
    )
    policy = resolve_sizing_policy(inputs, policy)
    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(
        inputs,
        characterization,
        policy=policy,
    )
    risk = assess_initial_sizing_risk(
        inputs,
        characterization,
        recommendation,
        sizing_policy=policy,
        gate_policy=evidence_gate_policy,
    )
    if require_recommendable:
        risk.assert_recommendable()
    return PreparedInitialSizing(inputs, characterization, recommendation, risk)


def load_and_prepare_initial_sizing(
    deployment_path,
    workload_path,
    measurements_path,
    *,
    policy=None,
    evidence_gate_policy=None,
    require_recommendable=True,
):
    """Load the three-file package and return a deterministic sizing result."""
    inputs = load_portable_sizing_inputs(
        deployment_path,
        workload_path,
        measurements_path,
    )
    policy = resolve_sizing_policy(inputs, policy)
    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(
        inputs,
        characterization,
        policy=policy,
    )
    risk = assess_initial_sizing_risk(
        inputs,
        characterization,
        recommendation,
        sizing_policy=policy,
        gate_policy=evidence_gate_policy,
    )
    if require_recommendable:
        risk.assert_recommendable()
    return PreparedInitialSizing(inputs, characterization, recommendation, risk)

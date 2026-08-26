"""Public, side-effect-free workflow for evidence-based initial sizing."""

from .analysis import characterize_deployment
from .models import PreparedInitialSizing
from .parser import load_portable_sizing_inputs, parse_portable_sizing_inputs
from .sizing import recommend_initial_replicas


def prepare_initial_sizing(
    deployment,
    workload,
    measurement_manifest,
    *,
    base_path,
    policy=None,
):
    """Parse mappings, derive evidence, and return an initial recommendation."""
    inputs = parse_portable_sizing_inputs(
        deployment,
        workload,
        measurement_manifest,
        base_path=base_path,
    )
    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(
        inputs,
        characterization,
        policy=policy,
    )
    return PreparedInitialSizing(inputs, characterization, recommendation)


def load_and_prepare_initial_sizing(
    deployment_path,
    workload_path,
    measurements_path,
    *,
    policy=None,
):
    """Load the three-file package and return a deterministic sizing result."""
    inputs = load_portable_sizing_inputs(
        deployment_path,
        workload_path,
        measurements_path,
    )
    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(
        inputs,
        characterization,
        policy=policy,
    )
    return PreparedInitialSizing(inputs, characterization, recommendation)

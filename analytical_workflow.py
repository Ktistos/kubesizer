"""Analytical replica sizing with exact bounded search."""

from optimization import AnalyticalReplicaSizingProblem
from optimization.selection import (
    optimize_replica_configuration as _optimize_replica_configuration,
)


def _declared_latency_slo_kind(system):
    method = getattr(system, "get_request_class_latency_slo_kind", None)
    return method() if method is not None else None


def _workload_mean_limits(system):
    kind = _declared_latency_slo_kind(system)
    if kind == "percentile":
        raise ValueError(
            "This workload selects percentile latency SLOs, which cannot "
            "be certified by analytical mean evaluation."
        )
    limits = system.get_request_class_mean_latency_limits()
    return limits


def optimize_with_analytical_model(
    system,
    *,
    request_class_mean_latency_limits=None,
    maximum_replicas,
    minimum_configuration=None,
    replica_costs=None,
    structural_feasibility=None,
    progress_callback=None,
    analytical_semantics="baseline",
):
    """Optimize analytical stability and optional mean latency without simulation.

    Uniform-cost search proves minimum weighted replica cost only for the
    bounded analytical model.
    With no declared mean SLO, the stable vector is the capacity target. Neither
    analytical evaluation nor exact search estimates p95 latency or provides
    statistical SLO evidence.
    ``minimum_configuration`` is a hard lower bound and must itself be at or
    above analytical stability.
    """
    if analytical_semantics == "experimental-layered":
        from performance_models.layered import LayeredAnalyticalSystem

        system = LayeredAnalyticalSystem(system)
    elif analytical_semantics != "baseline":
        raise ValueError("Unknown analytical semantics.")
    declared_kind = _declared_latency_slo_kind(system)
    declared_timeouts = getattr(
        system,
        "get_request_class_end_to_end_timeouts",
        lambda: {},
    )()
    if any(float(value) > 0.0 for value in declared_timeouts.values()):
        raise ValueError(
            "Strict " "analytical evaluation does not model terminal timeouts."
        )
    analytical_incompatibilities = getattr(
        system,
        "get_strict_analytical_incompatibilities",
        lambda: (),
    )()
    if analytical_incompatibilities:
        raise ValueError(
            "Strict analytical evaluation cannot certify this workload: "
            + "; ".join(analytical_incompatibilities)
            + "."
        )
    if declared_kind == "percentile":
        raise ValueError(
            "This workload selects percentile latency SLOs, which cannot "
            "be certified by analytical mean evaluation."
        )
    if request_class_mean_latency_limits is None:
        request_class_mean_latency_limits = _workload_mean_limits(system)
    elif declared_kind == "mean":
        declared_limits = _workload_mean_limits(system)
        if dict(request_class_mean_latency_limits) != declared_limits:
            raise ValueError(
                "Explicit mean-latency limits must match those declared by "
                "the workload."
            )
    problem = AnalyticalReplicaSizingProblem(
        system,
        request_class_mean_latency_limits,
        maximum_replicas,
        minimum_configuration=minimum_configuration,
        replica_costs=replica_costs,
        structural_feasibility=structural_feasibility,
        progress_callback=progress_callback,
    )
    return _optimize_replica_configuration(
        problem,
        progress_callback=progress_callback,
    )

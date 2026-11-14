"""Portable transformations for latency evidence available to DevOps teams."""

from __future__ import annotations

import math
from dataclasses import dataclass


def _finite_nonnegative(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric.")
    value = float(value)
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"{label} must be finite and non-negative.")
    return value


def _positive_samples(values, label):
    samples = tuple(_finite_nonnegative(value, label) for value in values)
    if len(samples) < 2 or any(value <= 0.0 for value in samples):
        raise ValueError(f"{label} must contain at least two positive samples.")
    return samples


@dataclass(frozen=True)
class MeanLatencyDecomposition:
    """Mean-only decomposition across aligned observation boundaries."""

    client_to_ingress_seconds: float
    ingress_processing_seconds: float
    application_upstream_seconds: float

    def request_class_components(self, latency_boundary="external_client"):
        """Return deterministic components suitable only for a mean SLO model."""
        if latency_boundary not in {"external_client", "ingress"}:
            raise ValueError("latency_boundary must be external_client or ingress.")
        result = {}
        if self.ingress_processing_seconds > 0.0:
            result["ingress_processing"] = {
                "distribution": "deterministic",
                "mean": self.ingress_processing_seconds,
            }
        if (
            latency_boundary == "external_client"
            and self.client_to_ingress_seconds > 0.0
        ):
            result = {
                "client_to_ingress": {
                    "distribution": "deterministic",
                    "mean": self.client_to_ingress_seconds,
                },
                **result,
            }
        return result


def decompose_aligned_latency_means(
    *,
    client_mean_seconds,
    ingress_request_mean_seconds,
    ingress_upstream_mean_seconds,
    difference_tolerance_seconds=0.001,
):
    """Separate aligned client, ingress, and application-entry mean durations.

    All three inputs must describe the same route, request cohort (or completed
    request window), and observation interval. Mean subtraction is valid by
    linearity; independently aggregated percentiles must never be passed here.
    Negative differences indicate incompatible boundaries and are rejected.
    """
    client = _finite_nonnegative(client_mean_seconds, "client_mean_seconds")
    ingress = _finite_nonnegative(
        ingress_request_mean_seconds,
        "ingress_request_mean_seconds",
    )
    upstream = _finite_nonnegative(
        ingress_upstream_mean_seconds,
        "ingress_upstream_mean_seconds",
    )
    tolerance = _finite_nonnegative(
        difference_tolerance_seconds,
        "difference_tolerance_seconds",
    )
    if ingress > client + tolerance:
        raise ValueError(
            "ingress request mean exceeds client mean; observation boundaries "
            "or windows are incompatible."
        )
    if upstream > ingress + tolerance:
        raise ValueError(
            "ingress upstream mean exceeds ingress request mean; observation "
            "boundaries or windows are incompatible."
        )
    return MeanLatencyDecomposition(
        client_to_ingress_seconds=max(0.0, client - ingress),
        ingress_processing_seconds=max(0.0, ingress - upstream),
        application_upstream_seconds=upstream,
    )


def client_to_entry_mean_component(
    *,
    client_mean_seconds,
    application_entry_mean_seconds,
    difference_tolerance_seconds=0.001,
):
    """Return the portable aggregate path component when ingress is unobserved.

    Inputs must cover the same route and time window. The result combines the
    client path, ingress, and the hop to the application entry. It must not be
    combined with finer client-to-ingress or ingress-processing components.
    """
    client = _finite_nonnegative(client_mean_seconds, "client_mean_seconds")
    application = _finite_nonnegative(
        application_entry_mean_seconds,
        "application_entry_mean_seconds",
    )
    tolerance = _finite_nonnegative(
        difference_tolerance_seconds,
        "difference_tolerance_seconds",
    )
    if application > client + tolerance:
        raise ValueError(
            "application-entry mean exceeds client mean; observation boundaries "
            "or windows are incompatible."
        )
    difference = max(0.0, client - application)
    if difference == 0.0:
        return {}
    return {
        "client_to_entry": {
            "distribution": "deterministic",
            "mean": difference,
        }
    }


def communication_latency_from_round_trip_probes(
    *,
    same_node_round_trip_seconds,
    cross_node_round_trip_seconds,
    cross_node_probability,
):
    """Build the simulator input from ordinary sidecar-aware HTTP probes.

    A request and its response traverse the path in opposite directions. In the
    absence of one-way clock synchronization, half of each measured round trip
    is the explicit approximation used for one simulated direction.
    """
    same = _positive_samples(
        same_node_round_trip_seconds,
        "same_node_round_trip_seconds",
    )
    cross = _positive_samples(
        cross_node_round_trip_seconds,
        "cross_node_round_trip_seconds",
    )
    probability = _finite_nonnegative(
        cross_node_probability,
        "cross_node_probability",
    )
    if probability > 1.0:
        raise ValueError("cross_node_probability cannot exceed one.")
    return {
        "same_node_one_way": {
            "distribution": "empirical",
            "samples": [value / 2.0 for value in same],
            "sampling": "iid_with_replacement",
        },
        "cross_node_one_way": {
            "distribution": "empirical",
            "samples": [value / 2.0 for value in cross],
            "sampling": "iid_with_replacement",
        },
        "cross_node_probability": probability,
    }


def estimate_low_load_service_blocking_time(
    *,
    observed_response_mean_seconds,
    cpu_execution_mean_seconds,
    downstream_response_mean_seconds=0.0,
    communication_round_trip_mean_seconds=0.0,
    post_release_delay_mean_seconds=0.0,
    maximum_observed_cpu_utilization,
    maximum_allowed_cpu_utilization=0.20,
    difference_tolerance_seconds=0.001,
):
    """Estimate an explicitly labelled non-CPU wait from a low-load run.

    The response, CPU execution, downstream response, communication, and
    independently measured post-release delay means must describe the same
    service, route, and observation window, with no overlapping components.
    The result is an inferred mean residual, not a directly observed I/O distribution.
    A low-utilization gate prevents ordinary queueing delay from being silently
    relabelled as intrinsic blocking work.
    """
    observed = _finite_nonnegative(
        observed_response_mean_seconds,
        "observed_response_mean_seconds",
    )
    cpu = _finite_nonnegative(
        cpu_execution_mean_seconds,
        "cpu_execution_mean_seconds",
    )
    downstream = _finite_nonnegative(
        downstream_response_mean_seconds,
        "downstream_response_mean_seconds",
    )
    communication = _finite_nonnegative(
        communication_round_trip_mean_seconds,
        "communication_round_trip_mean_seconds",
    )
    post_release = _finite_nonnegative(
        post_release_delay_mean_seconds,
        "post_release_delay_mean_seconds",
    )
    utilization = _finite_nonnegative(
        maximum_observed_cpu_utilization,
        "maximum_observed_cpu_utilization",
    )
    utilization_limit = _finite_nonnegative(
        maximum_allowed_cpu_utilization,
        "maximum_allowed_cpu_utilization",
    )
    tolerance = _finite_nonnegative(
        difference_tolerance_seconds,
        "difference_tolerance_seconds",
    )
    if utilization > 1.0 or utilization_limit <= 0.0 or utilization_limit >= 1.0:
        raise ValueError(
            "CPU utilization and its low-load limit must lie in [0, 1] and "
            "the limit must be strictly between zero and one."
        )
    if utilization > utilization_limit:
        raise ValueError(
            "The characterization run is not low-load enough to separate "
            "non-CPU blocking from queueing delay."
        )
    accounted = cpu + downstream + communication + post_release
    if accounted > observed + tolerance:
        raise ValueError(
            "Modeled CPU, downstream, communication, and post-release means "
            "exceed the "
            "observed service response mean; the boundaries are incompatible."
        )
    residual = max(0.0, observed - accounted)
    if residual == 0.0:
        return None
    return {
        "distribution": "deterministic",
        "mean": residual,
    }


def fit_leaf_blocking_variability(
    *,
    blocking_mean_seconds,
    observed_response_median_seconds,
    accounted_median_seconds,
    maximum_scv=100.0,
):
    """Fit the retained lognormal leaf-blocking model from mean and p50.

    Only the residual median is fitted: observed service p50 minus processing
    and other already-accounted median time. A non-positive or non-right-skewed
    residual cannot identify positive lognormal spread, so the fitted method
    retains the independently estimated blocking mean deterministically.
    """
    blocking_mean = _finite_nonnegative(
        blocking_mean_seconds,
        "blocking_mean_seconds",
    )
    observed_median = _finite_nonnegative(
        observed_response_median_seconds,
        "observed_response_median_seconds",
    )
    accounted_median = _finite_nonnegative(
        accounted_median_seconds,
        "accounted_median_seconds",
    )
    maximum_scv = _finite_nonnegative(maximum_scv, "maximum_scv")
    if maximum_scv <= 0.0:
        raise ValueError("maximum_scv must be greater than zero.")
    if blocking_mean == 0.0:
        return None

    residual_median = max(0.0, observed_median - accounted_median)
    if residual_median <= 0.0 or residual_median >= blocking_mean:
        return {"distribution": "deterministic", "mean": blocking_mean}

    # Work in the log domain and compare with the cap before exponentiating.
    # Both inputs may be finite while their ratio, or exp(sigma_squared), is not.
    sigma_squared = 2.0 * (math.log(blocking_mean) - math.log(residual_median))
    maximum_sigma_squared = math.log1p(maximum_scv)
    scv = (
        maximum_scv
        if sigma_squared >= maximum_sigma_squared
        else math.expm1(sigma_squared)
    )
    if scv <= 1e-9:
        return {"distribution": "deterministic", "mean": blocking_mean}
    return {
        "distribution": "lognormal",
        "mean": blocking_mean,
        "scv": scv,
    }

"""Explicit call semantics; architecture names never imply protocol behavior."""

import math
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class CallPolicySpec:
    execution: str
    completion: str = "all"
    required: int = 0

    def to_dict(self):
        return {
            "execution": self.execution,
            "completion": self.completion,
            "required": self.required,
        }


def parse_call_policies(raw, calls, default_execution, *, label="call_policies"):
    """Validate per-source overrides without changing expected offered work.

    ``choice`` means exactly one target, whereas parallel fractional calls are
    independent Bernoulli calls. Quorums count distinct child responses, so each
    declared target must be called exactly once. Remaining work is never cancelled.
    """
    if not isinstance(raw, Mapping):
        raise ValueError(f"{label} must be a mapping.")
    result = {}
    for source, value in raw.items():
        path = f"{label}.{source}"
        if source not in calls or not calls[source]:
            raise ValueError(f"{path} needs a source with nonempty calls.")
        if not isinstance(value, Mapping) or set(value) - {
            "execution",
            "completion",
            "required",
        }:
            raise ValueError(f"{path} accepts execution, completion, and required.")
        execution = value.get("execution", default_execution)
        completion = value.get("completion", "all")
        if execution not in ("parallel", "sequential", "choice"):
            raise ValueError(
                f"{path}.execution must be parallel, sequential, or choice."
            )
        if completion not in ("all", "quorum", "background"):
            raise ValueError(f"{path}.completion must be all, quorum, or background.")
        required = value.get("required", 0)
        if isinstance(required, bool) or not isinstance(required, int):
            raise ValueError(f"{path}.required must be an integer.")
        if completion == "quorum":
            if execution != "parallel" or not 1 <= required <= len(calls[source]):
                raise ValueError(
                    f"{path}: quorum needs parallel calls and a valid required count."
                )
            if any(count != 1.0 for count in calls[source].values()):
                raise ValueError(
                    f"{path}: quorum needs exactly one call per distinct target."
                )
        elif "required" in value:
            raise ValueError(f"{path}.required is only valid for quorum completion.")
        if completion == "background" and execution != "parallel":
            raise ValueError(f"{path}: background work requires parallel execution.")
        if execution == "choice":
            if completion != "all" or not math.isclose(
                sum(calls[source].values()), 1.0, rel_tol=0.0, abs_tol=1e-12
            ):
                raise ValueError(
                    f"{path}: choice needs all completion and probabilities summing to one."
                )
        result[source] = CallPolicySpec(execution, completion, required)
    return result


def parse_replica_constraints(raw, service_names):
    """Replica bounds constrain a resolved topology; they never create shards."""
    if not isinstance(raw, Mapping):
        raise ValueError("replica_constraints must be a mapping.")
    result = {}
    for service, value in raw.items():
        path = f"replica_constraints.{service}"
        if service not in service_names:
            raise ValueError(f"{path} references an unknown service.")
        if not isinstance(value, Mapping) or set(value) not in (
            {"fixed"},
            {"minimum", "maximum"},
        ):
            raise ValueError(f"{path} needs fixed or both minimum and maximum.")
        low = value.get("fixed", value.get("minimum"))
        high = value.get("fixed", value.get("maximum"))
        if any(
            isinstance(v, bool) or not isinstance(v, int) or v <= 0 for v in (low, high)
        ):
            raise ValueError(f"{path} bounds must be positive integers.")
        if high < low:
            raise ValueError(f"{path}.maximum cannot be below minimum.")
        result[service] = (low, high)
    return result


def call_completion_bounds(policy, children):
    """Conditional mean bounds from nonnegative child response means.

    ``children`` contains (expected calls, lower mean, upper mean). These
    inequalities do not assume independent or exponential child responses.
    Their validity for a deployment still depends on the supplied queue means.
    """
    if not children or policy.completion == "background":
        return 0.0, 0.0
    total_lower = sum(count * low for count, low, _high in children)
    total_upper = sum(count * high for count, _low, high in children)
    if policy.execution in ("sequential", "choice"):
        return total_lower, total_upper
    if policy.completion == "quorum":
        required = policy.required
        upper = min(
            total_upper / (len(children) - required + 1),
            sum(sorted(high for _count, _low, high in children)[:required]),
        )
        lower = (
            max(low for _count, low, _high in children)
            if required == len(children)
            else 0.0
        )
        return lower, upper
    lower = max((low for count, low, _high in children if count >= 1), default=0.0)
    return lower, total_upper

from enum import Enum


class ReplicaEvaluationMode(str, Enum):
    """Choose deterministic analytical means or statistical p95 simulation."""

    ANALYTICAL = "ANALYTICAL"
    SIMULATION = "SIMULATION"


def normalize_replica_evaluation_mode(value):
    if isinstance(value, ReplicaEvaluationMode):
        return value
    try:
        return ReplicaEvaluationMode(str(value).upper())
    except ValueError as error:
        choices = ", ".join(item.value for item in ReplicaEvaluationMode)
        raise ValueError(
            f"replica evaluation mode must be one of: {choices}."
        ) from error

import math
from numbers import Real


def _as_finite_number(value, name):
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite number.")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number.")
    return value


def _complete_bucket_count(horizon, bucket_width):
    ratio = horizon / bucket_width
    nearest_integer = round(ratio)
    if math.isclose(ratio, nearest_integer, rel_tol=1e-12, abs_tol=1e-12):
        return int(nearest_integer)
    return math.floor(ratio)


def time_weighted_bucket_averages(history, bucket_width, horizon):
    """Convert a state-change history into complete time-weighted buckets.

    ``history`` describes a right-continuous, piecewise-constant state with
    ``(timestamp, new_value)`` pairs and must begin at time zero. Buckets are
    half-open intervals ``[j * bucket_width, (j + 1) * bucket_width)``.

    Only complete buckets are returned. If ``horizon`` is not an exact multiple
    of ``bucket_width``, the incomplete final interval is deliberately discarded.
    """
    bucket_width = _as_finite_number(bucket_width, "bucket_width")
    horizon = _as_finite_number(horizon, "horizon")
    if bucket_width <= 0:
        raise ValueError("bucket_width must be greater than zero.")
    if horizon <= 0:
        raise ValueError("horizon must be greater than zero.")

    history = tuple(history)
    if not history:
        raise ValueError("history must contain an initial state at time zero.")

    validated_history = []
    previous_timestamp = None
    for entry in history:
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            raise ValueError("Each history entry must be a (timestamp, value) pair.")

        timestamp = _as_finite_number(entry[0], "history timestamp")
        value = _as_finite_number(entry[1], "history value")
        if timestamp < 0:
            raise ValueError("History timestamps must be non-negative.")
        if value < 0:
            raise ValueError("History values must be non-negative.")
        if previous_timestamp is not None and timestamp <= previous_timestamp:
            raise ValueError("History timestamps must be strictly increasing.")

        validated_history.append((timestamp, value))
        previous_timestamp = timestamp

    if validated_history[0][0] != 0.0:
        raise ValueError("history must begin at time zero.")

    bucket_count = _complete_bucket_count(horizon, bucket_width)
    bucket_averages = []
    event_index = 1
    current_time = 0.0
    current_value = validated_history[0][1]

    for bucket_index in range(bucket_count):
        bucket_end = (bucket_index + 1) * bucket_width
        bucket_area = 0.0

        while (
            event_index < len(validated_history)
            and validated_history[event_index][0] < bucket_end
        ):
            event_time, new_value = validated_history[event_index]
            bucket_area += current_value * (event_time - current_time)
            current_time = event_time
            current_value = new_value
            event_index += 1

        bucket_area += current_value * (bucket_end - current_time)
        bucket_averages.append(bucket_area / bucket_width)
        current_time = bucket_end

    return bucket_averages

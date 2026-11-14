"""Check the ideal PS kernel against an independent M/G/1-PS mean formula."""

import math
import statistics

import pytest

from performance_models import QueueingNetworkType, System


@pytest.mark.parametrize(
    "service",
    [
        {"distribution": "deterministic", "mean": 0.01},
        {"distribution": "exponential", "rate": 100.0},
        {"distribution": "lognormal", "mean": 0.01, "scv": 2.0},
    ],
)
def test_unlimited_ps_mean_is_insensitive_to_service_shape(service):
    # Poisson input, one ideal CPU, unlimited admitted concurrency. This formula
    # does NOT apply to the experiment's finite blocking/admission pools.
    system = System(
        "ps-reference",
        {
            "request_classes": {"request": {"concurrency_weight": 1.0, "routing": {}}},
            "traffic_model": {
                "entry_services": {
                    "request": {"api": {"distribution": "exponential", "rate": 60.0}}
                },
                "service_times": {
                    "api": {**service, "execution_model": "processor_sharing"}
                },
            },
        },
        QueueingNetworkType.OPEN,
    )
    assert system.get_service_concurrency_limits_per_replica()["api"] is None
    records = system.simulate_measurement_replications(
        warmup_duration=50,
        measurement_duration=300,
        seeds=tuple(range(910001, 910021)),
        workers=2,
        maximum_drain_time=30,
    )
    means = [statistics.fmean(r.request_class_latencies["request"]) for r in records]
    expected = 0.01 / (1 - 60 * 0.01)
    se = statistics.stdev(means) / math.sqrt(len(means))
    assert statistics.fmean(means) == pytest.approx(
        expected, abs=max(4 * se, 0.03 * expected)
    )

import copy
import statistics

import pytest

from performance_models import QueueingNetworkType, System, parse_workload_spec


def _workload(overhead, *, timeout=None):
    request_class = {
        "concurrency_weight": 1.0,
        "end_to_end_latency_slos": {
            "percentile": {"quantile": 0.95, "maximum_seconds": 2.0}
        },
        "routing": {},
        "latency_overhead": overhead,
    }
    if timeout is not None:
        request_class["end_to_end_timeout_seconds"] = timeout
        request_class["maximum_terminal_failure_rate"] = 0.5
    return {
        "request_classes": {"request": request_class},
        "traffic_model": {
            "entry_services": {
                "request": {"service": {"distribution": "deterministic", "rate": 2.0}}
            },
            "service_times": {
                "service": {"distribution": "deterministic", "mean": 0.1}
            },
        },
    }


def test_fixed_and_distributional_overhead_are_mutually_exclusive():
    workload = _workload({"distribution": "deterministic", "mean": 0.2})
    workload["request_classes"]["request"]["fixed_latency_overhead_seconds"] = 0.2

    with pytest.raises(ValueError, match="cannot define both"):
        parse_workload_spec(workload)

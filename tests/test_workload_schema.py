import copy

import pytest

from calibration.profile import experiment_fingerprints
from performance_models import QueueingNetworkType, System, parse_workload_spec


def _mean_slo(mean_seconds=1.0):
    return {"mean_seconds": mean_seconds}


def _percentile_slo(*, p95_seconds=2.0, quantile=0.95):
    return {
        "percentile": {
            "quantile": quantile,
            "maximum_seconds": p95_seconds,
        },
    }


def test_single_model_workload_has_no_schema_metadata_or_redundant_name():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "end_to_end_latency_slos": _mean_slo(),
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "exponential", "rate": 2.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }

    parsed = parse_workload_spec(workload)
    system = System("single-traffic-model", workload, QueueingNetworkType.OPEN)

    assert set(workload) == {"request_classes", "traffic_model"}
    assert parsed.traffic_model.name == "traffic_model"
    assert system.get_traffic_model_name() == "traffic_model"
    assert system.get_service("api").get_gamma("request") == pytest.approx(2.0)
    assert parsed.request_class_latency_slos["request"].mean_seconds == 1.0
    assert system.get_request_class_latency_slo_kind() == "mean"
    assert system.get_request_class_mean_latency_limits() == {"request": 1.0}
    with pytest.raises(ValueError, match="mean latency SLOs"):
        system.get_request_class_percentile_latency_limits()


def test_request_class_end_to_end_timeout_is_optional_validated_and_fingerprinted():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "end_to_end_timeout_seconds": 60.0,
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "exponential", "rate": 2.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }
    parsed = parse_workload_spec(workload)
    system = System("timeout", workload, QueueingNetworkType.OPEN)
    _, first_fingerprint = experiment_fingerprints(system)

    assert parsed.request_class_end_to_end_timeouts == {"request": 60.0}
    assert system.get_request_class_end_to_end_timeouts() == {"request": 60.0}

    changed = copy.deepcopy(workload)
    changed["request_classes"]["request"]["end_to_end_timeout_seconds"] = 30.0
    _, second_fingerprint = experiment_fingerprints(
        System("timeout", changed, QueueingNetworkType.OPEN)
    )
    assert second_fingerprint != first_fingerprint

    invalid = copy.deepcopy(workload)
    invalid["request_classes"]["request"]["end_to_end_timeout_seconds"] = -1.0
    with pytest.raises(ValueError, match="end_to_end_timeout_seconds"):
        parse_workload_spec(invalid)


def test_terminal_failure_rate_slo_requires_a_deadline_and_is_fingerprinted():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "end_to_end_latency_slos": _percentile_slo(p95_seconds=1.0),
                "end_to_end_timeout_seconds": 2.0,
                "maximum_terminal_failure_rate": 0.05,
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "exponential", "rate": 2.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }
    parsed = parse_workload_spec(workload)
    system = System("failure-slo", workload, QueueingNetworkType.OPEN)
    _, original = experiment_fingerprints(system)

    assert parsed.request_class_failure_rate_slos == {"request": 0.05}
    assert system.get_request_class_failure_rate_slos() == {"request": 0.05}

    changed = copy.deepcopy(workload)
    changed["request_classes"]["request"]["maximum_terminal_failure_rate"] = 0.10
    _, changed_fingerprint = experiment_fingerprints(
        System("failure-slo", changed, QueueingNetworkType.OPEN)
    )
    assert changed_fingerprint != original

    missing_deadline = copy.deepcopy(workload)
    del missing_deadline["request_classes"]["request"]["end_to_end_timeout_seconds"]
    with pytest.raises(ValueError, match="no positive end_to_end_timeout_seconds"):
        parse_workload_spec(missing_deadline)

    invalid = copy.deepcopy(workload)
    invalid["request_classes"]["request"]["maximum_terminal_failure_rate"] = 1.0
    with pytest.raises(ValueError, match="less than one"):
        parse_workload_spec(invalid)

    zero = copy.deepcopy(workload)
    zero["request_classes"]["request"]["maximum_terminal_failure_rate"] = 0.0
    with pytest.raises(ValueError, match="greater than zero"):
        parse_workload_spec(zero)


def test_single_model_workload_requires_explicit_concurrency_weights():
    workload = {
        "request_classes": {
            "request": {
                "end_to_end_latency_slos": _mean_slo(),
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "exponential", "rate": 2.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }

    with pytest.raises(ValueError, match="single-model workload"):
        System("missing-weight", workload, QueueingNetworkType.OPEN)


def test_single_model_workload_allows_omitting_latency_slos():
    workload = {
        "request_classes": {"request": {"concurrency_weight": 1.0, "routing": {}}},
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "exponential", "rate": 2.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }

    system = System("missing-slos", workload, QueueingNetworkType.OPEN)

    assert system.get_request_class_latency_slo_kind() is None
    assert system.get_request_class_latency_slos() == {}
    assert system.get_request_class_mean_latency_limits() == {}
    assert system.get_request_class_percentile_latency_limits() == {}


@pytest.mark.parametrize(
    ("slos", "message"),
    (
        (_mean_slo(0.0), "mean_seconds"),
        (_percentile_slo(p95_seconds=0.0), "maximum_seconds"),
        (_percentile_slo(quantile=1.0), "between 0 and 1"),
        (_percentile_slo(quantile=0.99), "currently estimates p95"),
        (
            {
                "mean_seconds": 1.0,
                "percentile": {"quantile": 0.95, "maximum_seconds": 2.0},
            },
            "choose exactly one",
        ),
    ),
)
def test_single_model_workload_rejects_invalid_latency_slos(slos, message):
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "end_to_end_latency_slos": slos,
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "exponential", "rate": 2.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }

    with pytest.raises(ValueError, match=message):
        parse_workload_spec(workload)


def test_system_rejects_request_for_undeclared_percentile():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "end_to_end_latency_slos": _percentile_slo(),
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "exponential", "rate": 2.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }
    system = System("percentile-mismatch", workload, QueueingNetworkType.OPEN)

    assert system.get_request_class_latency_slo_kind() == "percentile"
    with pytest.raises(ValueError, match="require simulation"):
        system.get_request_class_mean_latency_limits()
    with pytest.raises(ValueError, match="does not define p99"):
        system.get_request_class_percentile_latency_limits(quantile=0.99)


def test_latency_slo_changes_alter_the_workload_fingerprint():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "end_to_end_latency_slos": _mean_slo(),
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {"api": {"distribution": "exponential", "rate": 2.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }
    changed = copy.deepcopy(workload)
    changed["request_classes"]["request"]["end_to_end_latency_slos"][
        "mean_seconds"
    ] = 0.9

    original_fingerprints = experiment_fingerprints(
        System("original-slos", workload, QueueingNetworkType.OPEN)
    )
    changed_fingerprints = experiment_fingerprints(
        System("changed-slos", changed, QueueingNetworkType.OPEN)
    )

    assert original_fingerprints[0] == changed_fingerprints[0]
    assert original_fingerprints[1] != changed_fingerprints[1]


def test_only_request_classes_with_slos_are_constrained():
    workload = {
        "request_classes": {
            "required": {
                "concurrency_weight": 0.5,
                "end_to_end_latency_slos": _mean_slo(0.5),
                "routing": {},
            },
            "unconstrained": {
                "concurrency_weight": 0.5,
                "routing": {},
            },
        },
        "traffic_model": {
            "entry_services": {
                name: {"api": {"distribution": "exponential", "rate": 1.0}}
                for name in ("required", "unconstrained")
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }

    system = System("partially-constrained", workload, QueueingNetworkType.OPEN)

    assert system.get_request_class_mean_latency_limits() == {"required": 0.5}


def test_workload_rejects_mixed_mean_and_percentile_slo_kinds():
    workload = {
        "request_classes": {
            "mean_class": {
                "concurrency_weight": 0.5,
                "end_to_end_latency_slos": _mean_slo(),
                "routing": {},
            },
            "percentile_class": {
                "concurrency_weight": 0.5,
                "end_to_end_latency_slos": _percentile_slo(),
                "routing": {},
            },
        },
        "traffic_model": {
            "entry_services": {
                name: {"api": {"distribution": "exponential", "rate": 1.0}}
                for name in ("mean_class", "percentile_class")
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }

    with pytest.raises(ValueError, match="same mean or percentile kind"):
        parse_workload_spec(workload)


def test_workload_supports_independent_entry_stream_rates():
    workload = {
        "request_classes": {
            "read": {"concurrency_weight": 0.8, "routing": {}},
            "write": {"concurrency_weight": 0.2, "routing": {}},
        },
        "traffic_model": {
            "entry_services": {
                "read": {
                    "api": {"distribution": "exponential", "rate": 6.0},
                    "worker": {
                        "distribution": "exponential",
                        "rate": 2.0,
                    },
                },
                "write": {"api": {"distribution": "exponential", "rate": 2.0}},
            },
            "service_times": {
                "api": {"distribution": "exponential", "rate": 100.0},
                "worker": {"distribution": "exponential", "rate": 50.0},
            },
        },
    }

    system = System("multiple-entry-streams", workload, QueueingNetworkType.OPEN)

    assert system.get_configured_external_arrival_rates() == {
        "read": (6.0, 2.0),
        "write": (2.0, 0.0),
    }
    assert system.get_request_class_concurrency_weights() == pytest.approx(
        {"read": 0.8, "write": 0.2}
    )


def test_workload_applies_explicit_request_class_concurrency_weights():
    workload = {
        "request_classes": {
            "read": {"concurrency_weight": 0.25, "routing": {}},
            "write": {"concurrency_weight": 0.75, "routing": {}},
        },
        "traffic_model": {
            "entry_services": {
                "read": {"api": {"distribution": "exponential", "rate": 8.0}},
                "write": {"api": {"distribution": "exponential", "rate": 2.0}},
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 100.0}},
        },
    }

    system = System("explicit-class-mix", workload, QueueingNetworkType.OPEN)

    assert system.get_configured_external_arrival_rates() == {
        "read": (8.0,),
        "write": (2.0,),
    }
    assert system.get_request_class_concurrency_weights() == {
        "read": 0.25,
        "write": 0.75,
    }
    assert system.get_service("api").get_gamma("read") == pytest.approx(2.5)
    assert system.get_service("api").get_gamma("write") == pytest.approx(7.5)
    arrival_specs = system.get_external_arrival_distribution_specs()
    assert arrival_specs["read"]["api"].rate == pytest.approx(2.5)
    assert arrival_specs["write"]["api"].rate == pytest.approx(7.5)


@pytest.mark.parametrize(
    ("request_classes", "message"),
    (
        (
            {
                "read": {"concurrency_weight": 0.5, "routing": {}},
                "write": {"routing": {}},
            },
            "Every request class must define concurrency_weight",
        ),
        (
            {
                "read": {"concurrency_weight": 0.6, "routing": {}},
                "write": {"concurrency_weight": 0.3, "routing": {}},
            },
            "weights must sum to one",
        ),
    ),
)
def test_workload_rejects_invalid_concurrency_weights(
    request_classes,
    message,
):
    workload = {
        "request_classes": request_classes,
        "traffic_model": {
            "entry_services": {
                name: {"api": {"distribution": "exponential", "rate": 1.0}}
                for name in request_classes
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 10.0}},
        },
    }

    with pytest.raises(ValueError, match=message):
        System("invalid-class-mix", workload, QueueingNetworkType.OPEN)


def test_workload_requires_every_request_class_entry_stream():
    workload = {
        "request_classes": {
            "read": {"concurrency_weight": 0.5, "routing": {}},
            "write": {"concurrency_weight": 0.5, "routing": {}},
        },
        "traffic_model": {
            "entry_services": {
                "read": {"api": {"distribution": "exponential", "rate": 6.0}}
            },
            "service_times": {"api": {"distribution": "exponential", "rate": 100.0}},
        },
    }

    with pytest.raises(ValueError, match="define every request class"):
        System("missing-stream", workload, QueueingNetworkType.OPEN)

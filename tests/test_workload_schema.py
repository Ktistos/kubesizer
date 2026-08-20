import pytest

from performance_models import QueueingNetworkType, System


def test_versioned_workload_supports_independent_entry_stream_rates():
    workload = {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {
            "read": {"routing": {}},
            "write": {"routing": {}},
        },
        "scenarios": {
            "normal": {
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
            }
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


def test_versioned_workload_applies_explicit_request_class_concurrency_weights():
    workload = {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {
            "read": {"concurrency_weight": 0.25, "routing": {}},
            "write": {"concurrency_weight": 0.75, "routing": {}},
        },
        "scenarios": {
            "normal": {
                "entry_services": {
                    "read": {
                        "api": {"distribution": "exponential", "rate": 8.0}
                    },
                    "write": {
                        "api": {"distribution": "exponential", "rate": 2.0}
                    },
                },
                "service_times": {
                    "api": {"distribution": "exponential", "rate": 100.0}
                },
            }
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
def test_versioned_workload_rejects_invalid_concurrency_weights(
    request_classes,
    message,
):
    workload = {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": request_classes,
        "scenarios": {
            "normal": {
                "entry_services": {
                    name: {
                        "api": {"distribution": "exponential", "rate": 1.0}
                    }
                    for name in request_classes
                },
                "service_times": {
                    "api": {"distribution": "exponential", "rate": 10.0}
                },
            }
        },
    }

    with pytest.raises(ValueError, match=message):
        System("invalid-class-mix", workload, QueueingNetworkType.OPEN)


def test_versioned_workload_requires_every_request_class_entry_stream():
    workload = {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {
            "read": {"routing": {}},
            "write": {"routing": {}},
        },
        "scenarios": {
            "normal": {
                "entry_services": {
                    "read": {"api": {"distribution": "exponential", "rate": 6.0}}
                },
                "service_times": {
                    "api": {"distribution": "exponential", "rate": 100.0}
                },
            }
        },
    }

    with pytest.raises(ValueError, match="define every request class"):
        System("missing-stream", workload, QueueingNetworkType.OPEN)

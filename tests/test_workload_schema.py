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

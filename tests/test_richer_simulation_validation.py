import pytest

from scripts.validate_richer_simulation import (
    MODEL_BASELINE,
    MODEL_ORACLE,
    MODEL_TELEMETRY,
    MODEL_TELEMETRY_BOTTLENECK,
    MODEL_TELEMETRY_VARIABILITY,
    _client_means_by_request_class,
    _leaf_blocking_lognormal,
    _observed_class_rates,
    build_characterized_models,
)


def _fixture():
    workload = {
        "request_classes": {
            "request": {
                "concurrency_weight": 1.0,
                "fixed_latency_overhead_seconds": 0.03,
                "end_to_end_latency_slos": {"mean_seconds": 0.1},
                "routing": {},
            }
        },
        "traffic_model": {
            "entry_services": {
                "request": {
                    "api": {"distribution": "exponential", "rate": 20.0}
                }
            },
            "service_times": {
                "api": {
                    "processing_slots_per_replica": 1,
                    "concurrency_limit_per_replica": 8,
                    "distribution": "deterministic",
                    "mean": 0.02,
                }
            },
        },
    }
    measurement = {
        "services": {
            "api": {
                "replica_count": 1,
                "istio_response_time_ms": {"mean": 30.0, "p50": 15.0},
                "replicas": {
                    "api-1": {
                        "cpu_service_demand_seconds_per_request": 0.005,
                        "cpu_usage_cores": 0.05,
                        "cpu_capacity_cores": 1.0,
                    }
                },
            }
        }
    }
    cohort = {
        "operations": {
            "work": {
                "started_count": 10,
                "latency_ms": {"mean": 50.0},
            }
        }
    }
    config = {
        "load": {
            "measurement_seconds": 10,
            "request_classes": [
                {
                    "name": "request",
                    "operations": [{"name": "work", "weight": 1}],
                }
            ],
        }
    }
    return workload, measurement, cohort, config


def test_characterization_builds_distinct_baseline_and_blocking_models():
    models, diagnostics = build_characterized_models(*_fixture())

    assert set(models) == {
        MODEL_BASELINE,
        MODEL_TELEMETRY,
        MODEL_TELEMETRY_BOTTLENECK,
        MODEL_TELEMETRY_VARIABILITY,
        MODEL_ORACLE,
    }
    assert models[MODEL_BASELINE]["request_classes"]["request"][
        "fixed_latency_overhead_seconds"
    ] == pytest.approx(0.03)
    telemetry = models[MODEL_TELEMETRY]
    boundary = telemetry["request_classes"]["request"]["latency_components"][
        "client_to_entry"
    ]
    assert boundary["distribution"] == "deterministic"
    assert boundary["mean"] == pytest.approx(0.02)
    telemetry_service = telemetry["traffic_model"]["service_times"]["api"]
    assert telemetry_service["mean"] == pytest.approx(0.005)
    assert telemetry_service["non_cpu_blocking_time"]["mean"] == pytest.approx(
        0.025
    )
    assert telemetry_service["concurrency_limit_per_replica"] == 8
    assert models[MODEL_TELEMETRY_BOTTLENECK]["traffic_model"]["service_times"][
        "api"
    ]["concurrency_limit_per_replica"] == 1
    fitted = models[MODEL_TELEMETRY_VARIABILITY]["traffic_model"][
        "service_times"
    ]["api"]["non_cpu_blocking_time"]
    assert fitted["distribution"] == "lognormal"
    assert fitted["mean"] == pytest.approx(0.025)
    assert fitted["scv"] == pytest.approx(5.25)
    assert 0.0 < diagnostics["services"]["api"][
        "oracle_blocking_mean_seconds"
    ] < 0.01


def test_lognormal_blocking_fit_preserves_residual_mean_and_median():
    specification = _leaf_blocking_lognormal(0.025, 0.015, 0.005)

    assert specification["distribution"] == "lognormal"
    assert specification["mean"] == pytest.approx(0.025)
    assert specification["scv"] == pytest.approx(5.25)


def test_characterization_can_use_an_explicit_blocking_pool_capacity():
    models, _diagnostics = build_characterized_models(
        *_fixture(),
        blocking_concurrency_by_service={"api": 4},
    )

    for model_name in (MODEL_TELEMETRY_BOTTLENECK, MODEL_TELEMETRY_VARIABILITY):
        assert models[model_name]["traffic_model"]["service_times"]["api"][
            "concurrency_limit_per_replica"
        ] == 4


def test_multiclass_characterization_uses_class_specific_cohort_evidence():
    config = {
        "load": {
            "measurement_seconds": 10,
            "request_classes": [
                {"name": "read", "operations": [{"name": "work"}]},
                {"name": "write", "operations": [{"name": "work"}]},
            ],
        }
    }
    cohort = {
        "operations": {
            "work": {"started_count": 30, "latency_ms": {"mean": 99.0}}
        },
        "request_classes": {
            "read": {
                "started_count": 20,
                "successful_latency_ms": {"mean": 10.0},
            },
            "write": {
                "started_count": 10,
                "successful_latency_ms": {"mean": 40.0},
            },
        },
    }

    assert _observed_class_rates(config, cohort) == {"read": 2.0, "write": 1.0}
    assert _client_means_by_request_class(config, cohort) == {
        "read": 0.010,
        "write": 0.040,
    }


def test_legacy_multiclass_evidence_is_usable_when_operation_names_are_unique():
    config = {
        "load": {
            "measurement_seconds": 10,
            "request_classes": [
                {"name": "read", "operations": [{"name": "read"}]},
                {"name": "write", "operations": [{"name": "write"}]},
            ],
        }
    }
    cohort = {
        "operations": {
            "read": {"started_count": 20, "latency_ms": {"mean": 10.0}},
            "write": {"started_count": 10, "latency_ms": {"mean": 40.0}},
        }
    }

    assert _observed_class_rates(config, cohort) == {"read": 2.0, "write": 1.0}
    assert _client_means_by_request_class(config, cohort) == {
        "read": 0.010,
        "write": 0.040,
    }


def test_legacy_multiclass_evidence_rejects_ambiguous_operation_names():
    config = {
        "load": {
            "measurement_seconds": 10,
            "request_classes": [
                {"name": "read", "operations": [{"name": "work"}]},
                {"name": "write", "operations": [{"name": "work"}]},
            ],
        }
    }
    cohort = {"operations": {"work": {"started_count": 30, "latency_ms": {"mean": 20}}}}

    with pytest.raises(ValueError, match="class-specific client latency"):
        _client_means_by_request_class(config, cohort)
    with pytest.raises(ValueError, match="class-specific arrival count"):
        _observed_class_rates(config, cohort)

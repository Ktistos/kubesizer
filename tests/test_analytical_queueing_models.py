import pytest

from performance_models import (
    AnalyticalQueueingModel,
    QueueingNetworkType,
    System,
)
from performance_models.analytical import (
    combine_independent_stream_scvs,
    mg1_mean_response_time,
    mmc_mean_response_time,
    thinned_stream_scv,
    variability_adjusted_mean_response_time,
)


def _distribution_spec(distribution, *, arrival):
    if distribution == "exponential":
        return {"distribution": distribution, "rate": 4.0 if arrival else 10.0}
    if distribution == "deterministic":
        return {
            "distribution": distribution,
            "rate" if arrival else "mean": 4.0 if arrival else 0.1,
        }
    if distribution == "gamma":
        return {
            "distribution": distribution,
            "rate" if arrival else "mean": 4.0 if arrival else 0.1,
            "scv": 4.0,
        }
    raise AssertionError(distribution)


def _independent_model_workload():
    arrivals = {
        "mm": "exponential",
        "mg": "exponential",
        "gm": "deterministic",
        "gg": "deterministic",
    }
    services = {
        "mm": "exponential",
        "mg": "gamma",
        "gm": "exponential",
        "gg": "deterministic",
    }
    return {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {"request": {"routing": {}}},
        "scenarios": {
            "models": {
                "entry_services": {
                    "request": {
                        name: _distribution_spec(distribution, arrival=True)
                        for name, distribution in arrivals.items()
                    }
                },
                "service_times": {
                    name: _distribution_spec(distribution, arrival=False)
                    for name, distribution in services.items()
                },
            }
        },
    }


def test_variability_adjustment_reduces_to_exact_mmc():
    exact = mmc_mean_response_time(8.0, 5.0, 2)
    adjusted = variability_adjusted_mean_response_time(
        8.0,
        5.0,
        2,
        arrival_scv=1.0,
        service_scv=1.0,
    )

    assert adjusted == pytest.approx(exact)


def test_single_server_variability_adjustment_matches_kingman_form():
    deterministic = variability_adjusted_mean_response_time(
        4.0,
        10.0,
        1,
        arrival_scv=0.0,
        service_scv=0.0,
    )
    high_service_variability = variability_adjusted_mean_response_time(
        4.0,
        10.0,
        1,
        arrival_scv=1.0,
        service_scv=4.0,
    )

    assert deterministic == pytest.approx(0.1)
    assert high_service_variability > mmc_mean_response_time(4.0, 10.0, 1)


def test_pollaczek_khinchine_matches_the_mg1_variability_form():
    exact = mg1_mean_response_time(4.0, 10.0, service_scv=4.0)
    adjusted = variability_adjusted_mean_response_time(
        4.0,
        10.0,
        1,
        arrival_scv=1.0,
        service_scv=4.0,
    )

    assert exact == pytest.approx(adjusted)


def test_stream_superposition_and_thinning_preserve_poisson_input():
    assert thinned_stream_scv(1.0, 0.25) == pytest.approx(1.0)
    assert combine_independent_stream_scvs(((2.0, 1.0), (3.0, 1.0))) == (
        pytest.approx(1.0)
    )
    assert combine_independent_stream_scvs(((1.0, 0.0), (1.0, 0.0))) == (
        pytest.approx(0.5)
    )


def test_system_automatically_selects_all_four_queueing_models():
    system = System(
        "models",
        _independent_model_workload(),
        QueueingNetworkType.OPEN,
    )

    assert system.get_analytical_queueing_models() == {
        "mm": AnalyticalQueueingModel.MMC,
        "mg": AnalyticalQueueingModel.MG1,
        "gm": AnalyticalQueueingModel.GMC,
        "gg": AnalyticalQueueingModel.GGC,
    }
    details = system.get_analytical_model_details()
    assert details["mm"]["exact"] is True
    assert details["mg"]["exact"] is True
    assert all(details[name]["exact"] is False for name in ("gm", "gg"))
    latencies = system.solve().get_mean_latencies()
    assert latencies[1] > latencies[0] > latencies[3]

    system.configure_system_replicas((1, 2, 1, 1))
    assert system.get_analytical_queueing_models()["mg"] == (
        AnalyticalQueueingModel.MGC
    )


def test_general_service_departure_makes_downstream_arrivals_general():
    workload = {
        "schema_version": 2,
        "time_unit": "seconds",
        "request_classes": {
            "request": {"routing": {"upstream": {"downstream": 1.0}}}
        },
        "scenarios": {
            "routed": {
                "entry_services": {
                    "request": {
                        "upstream": {
                            "distribution": "exponential",
                            "rate": 4.0,
                        }
                    }
                },
                "service_times": {
                    "upstream": {
                        "distribution": "gamma",
                        "mean": 0.1,
                        "scv": 4.0,
                    },
                    "downstream": {
                        "distribution": "exponential",
                        "rate": 10.0,
                    },
                },
            }
        },
    }
    system = System("routed", workload, QueueingNetworkType.OPEN)

    assert system.get_analytical_queueing_models() == {
        "upstream": AnalyticalQueueingModel.MG1,
        "downstream": AnalyticalQueueingModel.GMC,
    }
    assert system.get_effective_arrival_scvs()[0] == pytest.approx(1.0)
    assert system.get_effective_arrival_scvs()[1] > 1.0


def test_generalized_workload_is_supported_by_the_native_simulator():
    system = System(
        "models",
        _independent_model_workload(),
        QueueingNetworkType.OPEN,
    )

    assert system.native_simulation_supports_distributions() is True
    result = system.simulate(10.0, seed=1)
    assert result.get_stability()

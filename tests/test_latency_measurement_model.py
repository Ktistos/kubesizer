import pytest

from characterization import (
    client_to_entry_mean_component,
    communication_latency_from_round_trip_probes,
    decompose_aligned_latency_means,
    estimate_low_load_service_blocking_time,
)


def test_aligned_mean_boundaries_produce_measurable_components():
    result = decompose_aligned_latency_means(
        client_mean_seconds=0.13404,
        ingress_request_mean_seconds=0.085687,
        ingress_upstream_mean_seconds=0.085500,
    )

    assert result.client_to_ingress_seconds == pytest.approx(0.048353)
    assert result.ingress_processing_seconds == pytest.approx(0.000187)
    assert result.application_upstream_seconds == pytest.approx(0.0855)
    assert result.request_class_components("external_client") == {
        "client_to_ingress": {
            "distribution": "deterministic",
            "mean": pytest.approx(0.048353),
        },
        "ingress_processing": {
            "distribution": "deterministic",
            "mean": pytest.approx(0.000187),
        },
    }


@pytest.mark.parametrize(
    "client,ingress,upstream,match",
    (
        (0.08, 0.09, 0.07, "ingress request mean exceeds"),
        (0.10, 0.08, 0.09, "upstream mean exceeds"),
    ),
)
def test_incompatible_latency_boundaries_are_rejected(
    client,
    ingress,
    upstream,
    match,
):
    with pytest.raises(ValueError, match=match):
        decompose_aligned_latency_means(
            client_mean_seconds=client,
            ingress_request_mean_seconds=ingress,
            ingress_upstream_mean_seconds=upstream,
        )


def test_small_ingress_counter_inversion_is_treated_as_measurement_noise():
    result = decompose_aligned_latency_means(
        client_mean_seconds=0.13404,
        ingress_request_mean_seconds=0.085687,
        ingress_upstream_mean_seconds=0.085730,
    )

    assert result.ingress_processing_seconds == 0.0
    assert result.application_upstream_seconds == pytest.approx(0.085730)


def test_client_to_entry_is_available_when_ingress_metrics_are_missing():
    assert client_to_entry_mean_component(
        client_mean_seconds=0.072268,
        application_entry_mean_seconds=0.044910,
    ) == {
        "client_to_entry": {
            "distribution": "deterministic",
            "mean": pytest.approx(0.027358),
        }
    }


def test_http_probe_round_trips_become_one_way_empirical_distributions():
    result = communication_latency_from_round_trip_probes(
        same_node_round_trip_seconds=(0.0020, 0.0024),
        cross_node_round_trip_seconds=(0.0028, 0.0032),
        unplaced_cross_node_probability=0.75,
    )

    assert result == {
        "same_node_one_way": {
            "distribution": "empirical",
            "samples": [0.001, 0.0012],
            "sampling": "iid_with_replacement",
        },
        "cross_node_one_way": {
            "distribution": "empirical",
            "samples": [0.0014, 0.0016],
            "sampling": "iid_with_replacement",
        },
        "unplaced_cross_node_probability": 0.75,
    }


def test_network_probe_model_rejects_insufficient_or_invalid_evidence():
    with pytest.raises(ValueError, match="at least two positive samples"):
        communication_latency_from_round_trip_probes(
            same_node_round_trip_seconds=(0.002,),
            cross_node_round_trip_seconds=(0.003, 0.004),
            unplaced_cross_node_probability=0.5,
        )
    with pytest.raises(ValueError, match="cannot exceed one"):
        communication_latency_from_round_trip_probes(
            same_node_round_trip_seconds=(0.002, 0.003),
            cross_node_round_trip_seconds=(0.003, 0.004),
            unplaced_cross_node_probability=1.1,
        )


def test_low_load_service_residual_becomes_explicit_blocking_time():
    result = estimate_low_load_service_blocking_time(
        observed_response_mean_seconds=0.080,
        cpu_execution_mean_seconds=0.020,
        downstream_response_mean_seconds=0.040,
        communication_round_trip_mean_seconds=0.004,
        maximum_observed_cpu_utilization=0.10,
    )

    assert result == {
        "distribution": "deterministic",
        "mean": pytest.approx(0.016),
    }


def test_service_blocking_estimate_rejects_load_and_boundary_mismatch():
    with pytest.raises(ValueError, match="not low-load enough"):
        estimate_low_load_service_blocking_time(
            observed_response_mean_seconds=0.080,
            cpu_execution_mean_seconds=0.020,
            maximum_observed_cpu_utilization=0.40,
        )
    with pytest.raises(ValueError, match="boundaries are incompatible"):
        estimate_low_load_service_blocking_time(
            observed_response_mean_seconds=0.050,
            cpu_execution_mean_seconds=0.020,
            downstream_response_mean_seconds=0.040,
            maximum_observed_cpu_utilization=0.10,
        )

import copy
from pathlib import Path

import pytest
import yaml

from characterization import (
    SizingPolicy,
    build_analytical_workload_variants,
    build_characterized_system_variants,
    characterize_deployment,
    load_and_prepare_initial_sizing,
    load_portable_sizing_inputs,
    measurement_methodology_contract,
    parse_portable_sizing_inputs,
    recommend_initial_replicas,
)
from performance_models import QueueingNetworkType, System

EXAMPLE = Path(__file__).parents[1] / "examples" / "portable_evidence"


def _loaded_yaml(name):
    with (EXAMPLE / name).open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _inputs():
    return load_portable_sizing_inputs(
        EXAMPLE / "workload.yaml",
        EXAMPLE / "measurements.yaml",
    )


def test_portable_package_derives_rates_demands_edges_and_histograms():
    inputs = _inputs()
    result = characterize_deployment(inputs)

    assert result.rate_source == "measured_workload"
    assert result.observed_external_arrival_rate_rps == pytest.approx(20.0)
    api = result.service_map["api"]
    database = result.service_map["database"]
    assert api.observed_replica_count == 2
    assert database.observed_replica_count == 1
    assert api.observed_arrival_rate_rps == pytest.approx(20.0)
    assert api.workload_arrival_rate_rps == pytest.approx(20.0)
    assert database.observed_arrival_rate_rps == pytest.approx(24.0)
    assert database.workload_arrival_rate_rps == pytest.approx(24.0)
    assert api.flow_balance_relative_error == pytest.approx(0.0)
    assert database.flow_balance_relative_error == pytest.approx(0.0)
    assert api.cpu_demand_window_means_seconds == pytest.approx(
        (0.0080, 0.0082, 0.0084)
    )
    assert dict(api.outgoing_call_rates) == {"database": pytest.approx(24.0)}
    assert dict(api.routing_probabilities) == {"database": pytest.approx(1.0)}
    assert dict(api.visits_per_service_request) == {"database": pytest.approx(1.2)}
    assert api.latency.mean_seconds == pytest.approx(774.0 / 18_000.0)
    external = result.external_request_classes[0]
    assert external.observed_arrival_rate_rps == pytest.approx(20.0)
    assert external.error_rate_5xx == 0.0
    assert external.latency.p95_seconds == pytest.approx(0.1)
    assert api.observed_cpu_usage_cores_per_replica == pytest.approx(
        (0.08, 0.082, 0.084)
    )
    assert api.maximum_memory_bytes_per_replica == 324009984
    assert dict(result.capabilities)["aligned_cpu_demand_windows"] is True
    assert dict(result.capabilities)["explicit_capacity_observations"] is False


def test_measurement_contract_freezes_cross_cluster_counter_semantics():
    contract = measurement_methodology_contract()

    assert contract["version"] == "prometheus-istio-kubernetes-v1"
    assert contract["service_requests"]["reporter"] == "destination"
    assert contract["replica_join"]["keys"] == ["namespace", "pod"]
    assert contract["cpu_capacity"]["source"] == ("kubernetes_container_cpu_requests")
    assert (
        contract["derived_cpu_demand"]["per_request_distribution_identifiable"] is False
    )
    assert contract["optional_capacity_envelope"]["sample_unit"] == (
        "independent_fixed_replica_load_test_run"
    )
    assert contract["statistically_controlled_stability_floor"]["headroom"] == {
        "input": "workload.sizing_policy.maximum_capacity_utilization",
        "default": 0.85,
        "meaning": (
            "maximum confidence-bounded offered-load ratio against measured "
            "healthy capacity"
        ),
    }
    assert (
        "no automatic linear"
        in contract["optional_capacity_envelope"]["replica_scaling"]
    )
    assert contract["initial_sizing_evidence_gate"]["probability_semantics"].startswith(
        "confidence bounds"
    )
    assert contract["closed_loop_quality_experiment"]["sample_unit"] == (
        "independent_complete_load_test_run"
    )
    assert (
        contract["closed_loop_quality_experiment"]["right_censoring"]["feasibility"]
        == "never_certified_from_a_censored_cohort"
    )


def test_capacity_example_produces_detailed_bounded_evidence():
    plan = load_and_prepare_initial_sizing(
        EXAMPLE / "workload.yaml",
        EXAMPLE / "measurements.with-capacity.yaml",
    )

    assert plan.recommendation.confidence == "DETAILED_TELEMETRY"
    assert plan.recommendation.replicas == {"api": 1, "database": 1}
    assert all(
        service.empirical_capacity_evidence_replicas
        for service in plan.recommendation.services
    )


def test_measurement_manifest_requires_the_supported_methodology():
    manifest = _loaded_yaml("measurements.yaml")
    manifest.pop("methodology_version")
    with pytest.raises(ValueError, match="missing.*methodology_version"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )

    manifest["methodology_version"] = "cluster-specific-guess-v1"
    with pytest.raises(ValueError, match="Unsupported measurement methodology"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )


def test_workload_rejects_a_separate_arrival_rate_override():
    workload = _loaded_yaml("workload.yaml")
    workload["total_external_arrival_rate_rps"] = 40.0

    with pytest.raises(ValueError, match="arrival rates come from the measurement"):
        parse_portable_sizing_inputs(
            workload,
            _loaded_yaml("measurements.yaml"),
            base_path=EXAMPLE,
        )


def test_portable_workload_requires_embedded_service_declarations():
    workload = _loaded_yaml("workload.yaml")
    workload.pop("services")

    with pytest.raises(ValueError, match="workload is missing.*services"):
        parse_portable_sizing_inputs(
            workload,
            _loaded_yaml("measurements.yaml"),
            base_path=EXAMPLE,
        )


def test_embedded_service_context_is_part_of_the_input_fingerprint():
    baseline = _inputs()
    workload = _loaded_yaml("workload.yaml")
    workload["services"]["api"]["cpu_request_cores"] = 0.75

    changed = parse_portable_sizing_inputs(
        workload,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )

    assert changed.workload.service("api").cpu_request_cores == pytest.approx(0.75)
    assert changed.fingerprint != baseline.fingerprint


def test_workload_parses_capacity_utilization_policy_and_fingerprints_it():
    default_inputs = _inputs()
    workload = _loaded_yaml("workload.yaml")
    workload["sizing_policy"] = {"maximum_capacity_utilization": 0.70}

    inputs = parse_portable_sizing_inputs(
        workload,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )

    assert default_inputs.workload.maximum_capacity_utilization == pytest.approx(0.85)
    assert inputs.workload.maximum_capacity_utilization == pytest.approx(0.70)
    assert inputs.fingerprint != default_inputs.fingerprint


@pytest.mark.parametrize("value", [0.0, 1.0, -0.1, 1.1, "not-a-number"])
def test_workload_rejects_invalid_capacity_utilization_policy(value):
    workload = _loaded_yaml("workload.yaml")
    workload["sizing_policy"] = {"maximum_capacity_utilization": value}

    with pytest.raises(ValueError, match="maximum_capacity_utilization"):
        parse_portable_sizing_inputs(
            workload,
            _loaded_yaml("measurements.yaml"),
            base_path=EXAMPLE,
        )


def test_workload_capacity_utilization_policy_rejects_unknown_fields():
    workload = _loaded_yaml("workload.yaml")
    workload["sizing_policy"] = {
        "maximum_capacity_utilization": 0.80,
        "certainty": 0.95,
    }

    with pytest.raises(ValueError, match="must contain exactly"):
        parse_portable_sizing_inputs(
            workload,
            _loaded_yaml("measurements.yaml"),
            base_path=EXAMPLE,
        )


def test_manifest_rejects_unobservable_per_request_cpu_samples():
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_demand_samples"] = "service-intervals.csv"

    with pytest.raises(ValueError, match="unsupported fields"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )


def test_measurement_namespace_must_match_the_declared_kubernetes_service():
    workload = _loaded_yaml("workload.yaml")
    workload["services"]["api"]["namespace"] = "another-namespace"
    inputs = parse_portable_sizing_inputs(
        workload,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )

    with pytest.raises(ValueError, match="namespace does not match"):
        characterize_deployment(inputs)


def test_measurement_infers_replicas_and_requires_a_fixed_observation(tmp_path):
    source = (EXAMPLE / "service-intervals.csv").read_text(encoding="utf-8")
    rows = source.splitlines()
    changing_replicas = tmp_path / "changing-replicas.csv"
    changing_replicas.write_text(
        "\n".join(
            row
            for row in rows
            if not (
                row.startswith("2026-08-25T10:10:00Z,") and ",api,example,api-b," in row
            )
        )
        + "\n",
        encoding="utf-8",
    )
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_intervals"] = str(changing_replicas)
    manifest.pop("service_class_intervals")
    inputs = parse_portable_sizing_inputs(
        _loaded_yaml("workload.yaml"),
        manifest,
        base_path=EXAMPLE,
    )

    with pytest.raises(ValueError, match="fixed-replica period"):
        characterize_deployment(inputs)


def test_service_counter_windows_must_be_unique_and_aligned(tmp_path):
    source = (EXAMPLE / "service-intervals.csv").read_text(encoding="utf-8")
    lines = source.splitlines()
    duplicate = tmp_path / "duplicate.csv"
    duplicate.write_text(source + lines[1] + "\n", encoding="utf-8")
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_intervals"] = str(duplicate)
    with pytest.raises(ValueError, match="duplicate rows"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )

    missing_window = tmp_path / "missing-window.csv"
    missing_window.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
    manifest["service_intervals"] = str(missing_window)
    with pytest.raises(ValueError, match="same observation windows"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )


def test_initial_sizing_uses_conservative_measured_cpu_bound():
    inputs = _inputs()
    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(inputs, characterization)

    assert recommendation.confidence == "TELEMETRY_INFORMED"
    assert recommendation.replicas == {"api": 1, "database": 1}
    api_recommendation = {item.service: item for item in recommendation.services}["api"]
    assert api_recommendation.cpu_capacity_cores_per_replica == 0.5
    assert api_recommendation.cpu_capacity_basis == "kubernetes_cpu_request"
    database = {item.service: item for item in recommendation.services}["database"]
    assert database.cpu_based_replicas == 1
    assert database.empirical_capacity_based_replicas is None
    assert database.limiting_factors == (
        "one_replica_floor",
        "cpu",
    )
    assert database.estimated_cpu_utilization < 0.65


def _capacity_manifest(tmp_path, rows):
    capacity = tmp_path / "capacity-observations.csv"
    capacity.write_text(
        "run_id,service,replicas,offered_rate_rps,completed_rate_rps,"
        "error_rate,healthy,cpu_throttling_ratio\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )
    manifest = _loaded_yaml("measurements.yaml")
    manifest["capacity_observations"] = str(capacity)
    return manifest


def test_empirical_capacity_envelope_is_bounded_by_observed_replica_count(
    tmp_path,
):
    manifest = _capacity_manifest(
        tmp_path,
        (
            "api-1-a,api,1,15,15,0,true,0",
            "api-1-b,api,1,15,14.5,0,true,0",
            "api-1-c,api,1,15,14,0,true,0",
            "api-2-a,api,2,32,31,0,true,0",
            "api-2-b,api,2,32,30,0,true,0",
            "api-2-c,api,2,32,29,0,true,0",
        ),
    )
    inputs = parse_portable_sizing_inputs(
        _loaded_yaml("workload.yaml"),
        manifest,
        base_path=EXAMPLE,
    )

    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(inputs, characterization)
    api = {item.service: item for item in recommendation.services}["api"]

    assert tuple(
        point.replicas
        for point in characterization.service_map["api"].capacity_envelope
    ) == (1, 2)
    assert api.empirical_capacity_based_replicas == 2
    assert api.empirical_capacity_evidence_replicas == (1, 2)
    assert api.empirical_capacity_extrapolated is False


def test_empirical_capacity_does_not_extrapolate_linearly(tmp_path):
    manifest = _capacity_manifest(
        tmp_path,
        (
            "api-1-a,api,1,10,10,0,true,0",
            "api-1-b,api,1,10,10,0,true,0",
            "api-1-c,api,1,10,10,0,true,0",
        ),
    )
    inputs = parse_portable_sizing_inputs(
        _loaded_yaml("workload.yaml"),
        manifest,
        base_path=EXAMPLE,
    )

    recommendation = recommend_initial_replicas(
        inputs,
        characterize_deployment(inputs),
    )
    api = {item.service: item for item in recommendation.services}["api"]

    assert api.empirical_capacity_based_replicas is None
    assert api.empirical_capacity_extrapolated is True
    assert any("no linear replica extrapolation" in item for item in api.warnings)


def test_unhealthy_capacity_run_blocks_conflicting_replica_count(tmp_path):
    manifest = _capacity_manifest(
        tmp_path,
        (
            "api-2-a,api,2,32,31,0,true,0",
            "api-2-b,api,2,32,30,0,true,0",
            "api-2-c,api,2,32,29,0,true,0",
            "api-2-failure,api,2,18,17,0.05,false,0",
            "api-3-a,api,3,40,39,0,true,0",
            "api-3-b,api,3,40,38,0,true,0",
            "api-3-c,api,3,40,37,0,true,0",
        ),
    )
    inputs = parse_portable_sizing_inputs(
        _loaded_yaml("workload.yaml"),
        manifest,
        base_path=EXAMPLE,
    )

    recommendation = recommend_initial_replicas(
        inputs,
        characterize_deployment(inputs),
    )
    api = {item.service: item for item in recommendation.services}["api"]

    assert api.empirical_capacity_based_replicas == 3


def test_capacity_run_ids_must_be_unique_per_service_and_replica(tmp_path):
    manifest = _capacity_manifest(
        tmp_path,
        (
            "same,api,1,10,10,0,true,0",
            "same,api,1,11,11,0,true,0",
        ),
    )

    with pytest.raises(ValueError, match="duplicate.*run_id"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )


def test_optional_evidence_can_be_omitted_without_blocking_initial_sizing():
    manifest = {
        "measurement_id": "minimal-observation",
        "methodology_version": "prometheus-istio-kubernetes-v1",
        "service_intervals": "service-intervals.csv",
    }
    inputs = parse_portable_sizing_inputs(
        _loaded_yaml("workload.yaml"),
        manifest,
        base_path=EXAMPLE,
    )

    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(inputs, characterization)

    assert recommendation.confidence == "TELEMETRY_INFORMED"
    assert recommendation.replicas == {"api": 1, "database": 1}
    assert dict(characterization.capabilities)["service_edges"] is False
    assert (
        dict(characterization.capabilities)["explicit_capacity_observations"] is False
    )


def test_sizing_is_idempotent_and_workload_semantics_change_the_fingerprint():
    first = load_and_prepare_initial_sizing(
        EXAMPLE / "workload.yaml",
        EXAMPLE / "measurements.yaml",
    )
    second = load_and_prepare_initial_sizing(
        EXAMPLE / "workload.yaml",
        EXAMPLE / "measurements.yaml",
    )
    assert first == second

    target = _loaded_yaml("workload.yaml")
    target["request_classes"]["browse"]["fixed_latency_overhead_seconds"] = 0.01
    changed = parse_portable_sizing_inputs(
        target,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )
    assert changed.fingerprint != first.inputs.fingerprint
    assert characterize_deployment(changed).rate_source == "measured_workload"


def test_measured_workload_uses_observed_rates_with_inferred_service_visits():
    target = _loaded_yaml("workload.yaml")
    target["request_classes"]["browse"].pop("calls")
    target["request_classes"]["browse"].pop("call_execution")
    inputs = parse_portable_sizing_inputs(
        target,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )

    result = characterize_deployment(inputs)

    assert result.rate_source == "measured_workload"
    assert result.service_map["api"].workload_arrival_rate_rps == pytest.approx(20)
    assert result.service_map["database"].workload_arrival_rate_rps == pytest.approx(24)


def _low_load_leaf_blocking_inputs(
    tmp_path,
    *,
    declare_concurrency,
    call_execution="sequential",
):
    rows = (EXAMPLE / "service-intervals.csv").read_text(encoding="utf-8").splitlines()
    header = rows[0].split(",")
    service_index = header.index("service")
    cpu_index = header.index("cpu_seconds")
    rewritten = [rows[0]]
    for row in rows[1:]:
        fields = row.split(",")
        if fields[service_index] == "database":
            fields[cpu_index] = "10.0"
        rewritten.append(",".join(fields))
    service_intervals = tmp_path / "low-load-service-intervals.csv"
    service_intervals.write_text("\n".join(rewritten) + "\n", encoding="utf-8")

    workload = _loaded_yaml("workload.yaml")
    workload["request_classes"]["browse"]["call_execution"] = call_execution
    if declare_concurrency:
        workload["services"]["database"]["concurrency_limit_per_replica"] = 8
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_intervals"] = str(service_intervals)
    return parse_portable_sizing_inputs(workload, manifest, base_path=EXAMPLE)


def test_analytical_characterization_remains_available_without_fitted_blocking(
    tmp_path,
):
    inputs = _low_load_leaf_blocking_inputs(tmp_path, declare_concurrency=False)
    characterization = characterize_deployment(inputs)

    (variant,) = build_analytical_workload_variants(inputs, characterization)

    database = variant.workload["traffic_model"]["service_times"]["database"]
    assert "non_cpu_blocking_time" not in database
    assert "input_uncertainty" not in variant.workload["traffic_model"]
    assert any(
        "excluded from the analytical model" in assumption
        for assumption in variant.assumptions
    )
    ((_, system),) = build_characterized_system_variants(
        inputs,
        characterization,
    )
    assert system.get_strict_analytical_incompatibilities() == ()


def test_analytical_builder_does_not_require_simulation_blocking_fit():
    inputs = _inputs()
    characterization = characterize_deployment(inputs)

    ((_, system),) = build_characterized_system_variants(inputs, characterization)
    assert system.get_strict_analytical_incompatibilities() == ()


def test_histograms_reject_duplicate_buckets(tmp_path):
    histogram = tmp_path / "duplicate-histogram.csv"
    histogram.write_text(
        "start,end,service,upper_bound_seconds,cumulative_count\n"
        "2026-08-25T10:00:00Z,2026-08-25T10:05:00Z,api,0.1,5000\n"
        "2026-08-25T10:00:00Z,2026-08-25T10:05:00Z,api,0.1,5000\n"
        "2026-08-25T10:00:00Z,2026-08-25T10:05:00Z,api,+Inf,6000\n",
        encoding="utf-8",
    )
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_latency_histograms"] = str(histogram)

    with pytest.raises(ValueError, match="duplicate bucket"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )


def test_histograms_require_consistent_bucket_grids(tmp_path):
    histogram = tmp_path / "inconsistent-histogram.csv"
    histogram.write_text(
        "start,end,service,upper_bound_seconds,cumulative_count\n"
        "2026-08-25T10:00:00Z,2026-08-25T10:05:00Z,api,0.1,5000\n"
        "2026-08-25T10:00:00Z,2026-08-25T10:05:00Z,api,+Inf,6000\n"
        "2026-08-25T10:05:00Z,2026-08-25T10:10:00Z,api,0.2,5000\n"
        "2026-08-25T10:05:00Z,2026-08-25T10:10:00Z,api,+Inf,6000\n",
        encoding="utf-8",
    )
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_latency_histograms"] = str(histogram)

    with pytest.raises(ValueError, match="inconsistent bucket boundaries"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )


def test_histogram_total_must_match_completed_request_count(tmp_path):
    histogram = tmp_path / "wrong-total-histogram.csv"
    histogram.write_text(
        "start,end,service,upper_bound_seconds,cumulative_count\n"
        "2026-08-25T10:00:00Z,2026-08-25T10:05:00Z,api,0.1,5000\n"
        "2026-08-25T10:00:00Z,2026-08-25T10:05:00Z,api,+Inf,5000\n",
        encoding="utf-8",
    )
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_latency_histograms"] = str(histogram)

    with pytest.raises(ValueError, match=r"\+Inf count disagrees"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )


def test_policy_validation_rejects_an_optimistic_invalid_threshold():
    inputs = _inputs()
    characterization = characterize_deployment(inputs)
    with pytest.raises(ValueError, match="target_cpu_utilization"):
        recommend_initial_replicas(
            inputs,
            characterization,
            policy=SizingPolicy(target_cpu_utilization=1.0),
        )

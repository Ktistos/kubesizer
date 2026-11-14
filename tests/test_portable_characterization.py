import copy
from pathlib import Path

import pytest
import yaml

from characterization import (
    SizingPolicy,
    build_analytical_workload_variants,
    build_characterized_system_variants,
    build_simulation_workload_variants,
    characterize_deployment,
    load_and_prepare_initial_sizing,
    load_portable_sizing_inputs,
    measurement_methodology_contract,
    parse_portable_sizing_inputs,
    recommend_initial_replicas,
)
from native_backend import get_native_backend
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
        "healthy\n" + "\n".join(rows) + "\n",
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
            "api-1-a,api,1,15,15,true",
            "api-1-b,api,1,15,14.5,true",
            "api-1-c,api,1,15,14,true",
            "api-2-a,api,2,32,31,true",
            "api-2-b,api,2,32,30,true",
            "api-2-c,api,2,32,29,true",
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
            "api-1-a,api,1,10,10,true",
            "api-1-b,api,1,10,10,true",
            "api-1-c,api,1,10,10,true",
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
            "api-2-a,api,2,32,31,true",
            "api-2-b,api,2,32,30,true",
            "api-2-c,api,2,32,29,true",
            "api-2-failure,api,2,18,17,false",
            "api-3-a,api,3,40,39,true",
            "api-3-b,api,3,40,38,true",
            "api-3-c,api,3,40,37,true",
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
            "same,api,1,10,10,true",
            "same,api,1,11,11,true",
        ),
    )

    with pytest.raises(ValueError, match="duplicate.*run_id"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )


def test_cpu_demand_is_derived_from_time_aligned_cpu_and_request_counters():
    manifest = _loaded_yaml("measurements.yaml")
    inputs = parse_portable_sizing_inputs(
        _loaded_yaml("workload.yaml"),
        manifest,
        base_path=EXAMPLE,
    )

    characterization = characterize_deployment(inputs)

    assert characterization.service_map[
        "api"
    ].cpu_demand_window_means_seconds == pytest.approx((0.0080, 0.0082, 0.0084))
    assert characterization.service_map[
        "database"
    ].cpu_demand_window_means_seconds == pytest.approx(
        (28.1 / 7200, 28.8 / 7200, 30.2 / 7200)
    )
    variants = build_simulation_workload_variants(inputs, characterization)
    assert "aligned arrival-rate and U/X windows" in " ".join(variants[0].assumptions)
    assert variants[0].model_complete is False
    assert variants[0].model_issues


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


def test_portable_workflow_preserves_clustered_latency_overhead_evidence():
    target = _loaded_yaml("workload.yaml")
    target["request_classes"]["browse"]["latency_overhead"] = {
        "distribution": "empirical",
        "clusters": [[0.01, 0.02], [0.10, 0.20]],
        "sampling": "cluster_bootstrap",
    }
    inputs = parse_portable_sizing_inputs(
        target,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)

    (variant,) = build_simulation_workload_variants(inputs, characterization)
    overhead = variant.workload["request_classes"]["browse"]["latency_overhead"]

    assert overhead == {
        "distribution": "empirical",
        "clusters": [[0.01, 0.02], [0.1, 0.2]],
        "sampling": "cluster_bootstrap",
    }


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


def test_simulation_builder_uses_safe_aligned_demand_and_declared_call_semantics():
    inputs = _inputs()
    characterization = characterize_deployment(inputs)
    variants = build_simulation_workload_variants(inputs, characterization)

    assert [variant.name for variant in variants] == ["declared"]
    workload = variants[0].workload
    assert workload["request_classes"]["browse"]["call_execution"] == "sequential"
    assert workload["request_classes"]["browse"]["calls"] == {"api": {"database": 1.2}}
    api_distribution = workload["traffic_model"]["service_times"]["api"]
    assert api_distribution["distribution"] == "deterministic"
    assert api_distribution["mean"] == pytest.approx(0.0082 / 0.5)
    assert (
        workload["traffic_model"]["service_times"]["api"]["execution_model"]
        == "processor_sharing"
    )
    system = System(
        "measured-workload",
        workload,
        QueueingNetworkType.OPEN,
    )
    assert system.get_service_replica_queue_models()["api"] == (
        "independent_round_robin"
    )
    arrival = workload["traffic_model"]["entry_services"]["browse"]["api"]
    assert arrival["distribution"] == "arrival_trace"
    assert arrival["sampling"] == "block_bootstrap"
    assert workload["traffic_model"]["input_uncertainty"]["sampling"] == (
        "moving_block_bootstrap"
    )
    epochs = workload["traffic_model"]["input_uncertainty"]["epochs"]
    assert len(epochs) == len(characterization.aligned_input_windows)
    assert epochs[0]["arrival_rate_multipliers"]["browse"] == pytest.approx(1.0)
    assert epochs[0]["service_time_multipliers"]["api"] == pytest.approx(
        0.0080 / 0.0082
    )


def test_declared_load_forecast_certifies_the_maximum_measured_load():
    target = _loaded_yaml("workload.yaml")
    target["load_forecast"] = {"multipliers": [1.0, 1.25, 1.5]}
    inputs = parse_portable_sizing_inputs(
        target,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)
    (variant,) = build_simulation_workload_variants(inputs, characterization)
    ((_, system),) = build_characterized_system_variants(
        inputs,
        characterization,
        evaluation_mode="analytical",
    )

    assert characterization.rate_source == "measured_workload_forecast_upper_bound"
    assert characterization.service_map["api"].workload_arrival_rate_rps == (
        pytest.approx(30.0)
    )
    assert characterization.service_map["database"].workload_arrival_rate_rps == (
        pytest.approx(36.0)
    )
    assert system.get_external_arrival_distribution_specs()["browse"]["api"].rate == (
        pytest.approx(30.0)
    )
    assert any(
        item.kind == "forecast"
        and item.treatment == "upper_envelope_certification"
        and item.values == (1.0, 1.25, 1.5)
        for item in variant.uncertainty_dimensions
    )


def test_portable_workload_preserves_a_declared_service_pool_limit():
    workload = _loaded_yaml("workload.yaml")
    workload["services"]["api"]["concurrency_limit_per_replica"] = 17
    inputs = parse_portable_sizing_inputs(
        workload,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)
    (variant,) = build_simulation_workload_variants(inputs, characterization)

    assert inputs.workload.services[0].concurrency_limit_per_replica == 17
    assert (
        variant.workload["traffic_model"]["service_times"]["api"][
            "concurrency_limit_per_replica"
        ]
        == 17
    )


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


def test_simulation_builder_fits_leaf_blocking_variability_from_low_load_evidence(
    tmp_path,
):
    inputs = _low_load_leaf_blocking_inputs(tmp_path, declare_concurrency=True)
    characterization = characterize_deployment(inputs)

    (variant,) = build_simulation_workload_variants(inputs, characterization)

    blocking = variant.workload["traffic_model"]["service_times"]["database"][
        "non_cpu_blocking_time"
    ]
    assert blocking["distribution"] == "lognormal"
    assert blocking["mean"] > 0.0
    assert blocking["scv"] > 0.0
    assert any(
        "database blocking uses lognormal_mean_p50_fit" in assumption
        for assumption in variant.assumptions
    )


def test_fitted_blocking_requires_declared_service_concurrency(tmp_path):
    inputs = _low_load_leaf_blocking_inputs(tmp_path, declare_concurrency=False)
    characterization = characterize_deployment(inputs)

    with pytest.raises(
        ValueError,
        match=r"services\.database\.concurrency_limit_per_replica",
    ):
        build_simulation_workload_variants(inputs, characterization)


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
        evaluation_mode="analytical",
    )
    assert system.get_strict_analytical_incompatibilities() == ()


def test_characterized_system_executes_in_mandatory_native_backend():
    workload = _loaded_yaml("workload.yaml")
    workload["service_blocking_times"] = {
        "api": {"distribution": "deterministic", "mean": 0.001},
        "database": {"distribution": "deterministic", "mean": 0.001},
    }
    inputs = parse_portable_sizing_inputs(
        workload,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)
    recommendation = recommend_initial_replicas(inputs, characterization)
    ((variant, system),) = build_characterized_system_variants(
        inputs,
        characterization,
        recommendation=recommendation,
    )

    assert variant.name == "declared"
    payload = get_native_backend().serialize_system(system)
    assert [item["replicas"] for item in payload["services"]] == [1, 1]
    assert all(
        item["execution_model"] == "processor_sharing" for item in payload["services"]
    )
    replication = system.simulate_measurement_replications(
        warmup_duration=0.1,
        measurement_duration=0.2,
        seeds=(101,),
        workers=1,
        maximum_drain_time=2.0,
    )[0]
    assert replication.measured_request_counts["browse"] >= 0
    assert replication.tagged_requests_in_flight_at_end == 0


def test_incomplete_fitted_model_cannot_construct_a_production_system():
    inputs = _inputs()
    characterization = characterize_deployment(inputs)

    with pytest.raises(ValueError, match="incomplete fitted-variability model"):
        build_characterized_system_variants(inputs, characterization)


def test_parallel_downstream_means_do_not_fabricate_parent_blocking(tmp_path):
    inputs = _low_load_leaf_blocking_inputs(
        tmp_path,
        declare_concurrency=True,
        call_execution="parallel",
    )
    characterization = characterize_deployment(inputs)

    (variant,) = build_simulation_workload_variants(inputs, characterization)

    assert any(
        "api blocking cannot be fitted because parallel downstream duration" in issue
        for issue in variant.model_issues
    )


def test_blocking_residual_uses_request_weighted_cpu_demand(tmp_path):
    rows = (EXAMPLE / "service-intervals.csv").read_text(encoding="utf-8").splitlines()
    header = rows[0].split(",")
    service_index = header.index("service")
    request_index = header.index("request_count")
    cpu_index = header.index("cpu_seconds")
    response_index = header.index("response_duration_sum_seconds")
    database_values = iter(
        (
            (7200.0, 0.72),
            (14400.0, 2.88),
            (28800.0, 11.52),
        )
    )
    rewritten = [rows[0]]
    for row in rows[1:]:
        fields = row.split(",")
        if fields[service_index] == "database":
            requests, cpu_seconds = next(database_values)
            fields[request_index] = str(requests)
            fields[cpu_index] = str(cpu_seconds)
            fields[response_index] = str(0.01 * requests)
        rewritten.append(",".join(fields))
    service_intervals = tmp_path / "weighted-service-intervals.csv"
    service_intervals.write_text("\n".join(rewritten) + "\n", encoding="utf-8")

    workload = _loaded_yaml("workload.yaml")
    workload["services"]["api"]["concurrency_limit_per_replica"] = 8
    workload["services"]["database"]["concurrency_limit_per_replica"] = 8
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_intervals"] = str(service_intervals)
    inputs = parse_portable_sizing_inputs(workload, manifest, base_path=EXAMPLE)
    characterization = characterize_deployment(inputs)

    (variant,) = build_simulation_workload_variants(inputs, characterization)

    blocking = variant.workload["traffic_model"]["service_times"]["database"][
        "non_cpu_blocking_time"
    ]
    pooled_cpu_execution = (0.72 + 2.88 + 11.52) / (7200.0 + 14400.0 + 28800.0) / 0.25
    assert blocking["mean"] == pytest.approx(0.01 - pooled_cpu_execution)


def test_inferred_edge_ordering_returns_explicit_sequential_and_parallel_variants():
    target = _loaded_yaml("workload.yaml")
    target["request_classes"]["browse"].pop("calls")
    target["request_classes"]["browse"].pop("call_execution")
    inputs = parse_portable_sizing_inputs(
        target,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)

    variants = build_simulation_workload_variants(inputs, characterization)

    assert [variant.name for variant in variants] == [
        "browse-sequential",
        "browse-parallel",
    ]
    assert {
        variant.workload["request_classes"]["browse"]["call_execution"]
        for variant in variants
    } == {"sequential", "parallel"}


def test_ambiguity_variant_budget_fails_instead_of_sampling_semantics():
    target = _loaded_yaml("workload.yaml")
    target["request_classes"]["browse"]["call_execution"] = "uncertain"
    inputs = parse_portable_sizing_inputs(
        target,
        _loaded_yaml("measurements.yaml"),
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)

    with pytest.raises(ValueError, match="2 ambiguity variants"):
        build_simulation_workload_variants(
            inputs,
            characterization,
            maximum_ambiguity_variants=1,
        )


def test_invalid_histogram_and_missing_simulation_rate_fail_clearly(tmp_path):
    histogram = tmp_path / "hist.csv"
    histogram.write_text(
        "start,end,service,upper_bound_seconds,cumulative_count\n"
        "0,10,api,0.1,10\n"
        "0,10,api,0.2,9\n",
        encoding="utf-8",
    )
    manifest = _loaded_yaml("measurements.yaml")
    manifest["service_latency_histograms"] = str(histogram)
    with pytest.raises(ValueError, match="cumulative counts decrease"):
        parse_portable_sizing_inputs(
            _loaded_yaml("workload.yaml"),
            manifest,
            base_path=EXAMPLE,
        )

    target = copy.deepcopy(_loaded_yaml("workload.yaml"))
    target["request_classes"]["browse"].pop("calls")
    target["request_classes"]["browse"].pop("call_execution")
    manifest = _loaded_yaml("measurements.yaml")
    manifest.pop("service_edges")
    manifest.pop("external_intervals")
    manifest.pop("external_latency_histograms")
    inputs = parse_portable_sizing_inputs(
        target,
        manifest,
        base_path=EXAMPLE,
    )
    characterization = characterize_deployment(inputs)
    with pytest.raises(ValueError, match="positive measured external arrival rate"):
        build_simulation_workload_variants(inputs, characterization)


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

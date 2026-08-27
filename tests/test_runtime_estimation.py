from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from calibration import CalibrationOptions, initial_measurement_duration
from optimization import (
    OptimizationMode,
    OptimizationModes,
    ScalableReplicaSearchOptions,
    WorkflowOptimizationMode,
)
from performance_models import QueueingNetworkType, System
from runtime_estimation import (
    RuntimeBenchmarkRates,
    calculate_workflow_runtime_estimate,
    calibrate_replica_configuration_limit,
    format_duration,
    format_replica_configuration_limit_calibration,
)


ROOT = Path(__file__).resolve().parents[1]


def _system_and_resources():
    workload = yaml.safe_load((ROOT / "workload.yaml").read_text(encoding="utf-8"))
    resources = yaml.safe_load((ROOT / "resources.yaml").read_text(encoding="utf-8"))
    return System("runtime-test", workload, QueueingNetworkType.OPEN), resources


def _rates():
    return RuntimeBenchmarkRates(
        warmup_seconds_per_replication_time_unit=1e-5,
        measurement_seconds_per_replication_time_unit=2e-5,
        bootstrap_seconds_per_replication_time_unit_per_class=1e-7,
        placement_seconds_per_candidate=1e-6,
        benchmark_wall_time_seconds=0.25,
        benchmark_replications=8,
        warmup_benchmark_horizon=500.0,
        measurement_benchmark_duration=500.0,
        placement_benchmark_candidates=200,
    )


def test_estimate_reports_ordinary_and_full_configured_work():
    system, resources = _system_and_resources()
    options = CalibrationOptions()

    result = calculate_workflow_runtime_estimate(
        system,
        resources,
        _rates(),
        options=options,
        maximum_replicas=10,
        workers=8,
        safety_factor=3.0,
    )

    assert result.replica_configuration_count == 60_000
    assert result.maximum_placement_count == 66**5
    assert result.replica_search_mode == "SCALABLE"
    assert result.placement_search_mode_at_maximum_bounds == "SCALABLE"
    assert result.workflow_search_mode == "SCALABLE"
    assert result.measurement_duration_ceiling == pytest.approx(
        4 * initial_measurement_duration(system, options)
    )
    assert all(
        stage.configured_ceiling_seconds >= stage.ordinary_planning_seconds
        for stage in result.stages
    )
    assert result.total_configured_ceiling_seconds > (
        result.total_ordinary_planning_seconds
    )


def test_auto_can_resolve_to_hybrid_workflow():
    system, resources = _system_and_resources()

    result = calculate_workflow_runtime_estimate(
        system,
        resources,
        _rates(),
        options=CalibrationOptions(),
        maximum_replicas=10,
        workers=8,
        exact_replica_configuration_limit=100_000,
        exact_placement_configuration_limit=1,
        optimization_modes=OptimizationModes(),
    )

    assert result.replica_search_mode == "EXACT"
    assert result.placement_search_mode_at_maximum_bounds == "SCALABLE"
    assert result.workflow_search_mode == "HYBRID"


def test_explicit_hybrid_modes_override_each_search_threshold():
    system, resources = _system_and_resources()

    result = calculate_workflow_runtime_estimate(
        system,
        resources,
        _rates(),
        options=CalibrationOptions(),
        maximum_replicas=10,
        workers=8,
        exact_replica_configuration_limit=1,
        exact_placement_configuration_limit=10**15,
        optimization_modes=OptimizationModes(
            mode=WorkflowOptimizationMode.HYBRID,
            replica=OptimizationMode.EXACT,
            placement=OptimizationMode.SCALABLE,
        ),
    )

    assert result.replica_search_mode == "EXACT"
    assert result.placement_search_mode_at_maximum_bounds == "SCALABLE"
    assert result.workflow_search_mode == "HYBRID"


def test_independent_cli_modes_are_estimated_without_hybrid_wrapper():
    system, resources = _system_and_resources()

    result = calculate_workflow_runtime_estimate(
        system,
        resources,
        _rates(),
        options=CalibrationOptions(),
        maximum_replicas=10,
        workers=8,
        replica_optimization_mode=OptimizationMode.EXACT,
        placement_optimization_mode=OptimizationMode.SCALABLE,
    )

    assert result.replica_search_mode == "EXACT"
    assert result.placement_search_mode_at_maximum_bounds == "SCALABLE"
    assert result.workflow_search_mode == "HYBRID"


def test_runtime_estimate_supports_skipped_node_placement():
    system, _ = _system_and_resources()

    result = calculate_workflow_runtime_estimate(
        system,
        None,
        _rates(),
        options=CalibrationOptions(),
        maximum_replicas=10,
        workers=8,
    )

    placement = next(
        stage for stage in result.stages if "node placement" in stage.stage
    )
    assert result.maximum_placement_count == 0
    assert placement.ordinary_planning_seconds == 0.0
    assert placement.configured_ceiling_seconds == 0.0
    assert placement.ordinary_work == "placement skipped"


def test_scalable_replica_mode_caps_projected_configuration_work():
    system, resources = _system_and_resources()
    exact = calculate_workflow_runtime_estimate(
        system,
        resources,
        _rates(),
        options=CalibrationOptions(),
        maximum_replicas=10,
        workers=8,
        exact_replica_configuration_limit=100_000,
    )
    scalable = calculate_workflow_runtime_estimate(
        system,
        resources,
        _rates(),
        options=CalibrationOptions(),
        maximum_replicas=10,
        workers=8,
        exact_replica_configuration_limit=1,
        scalable_replica_options=ScalableReplicaSearchOptions(
            maximum_configuration_evaluations=4,
            time_budget_seconds=30.0,
            candidate_width=2,
        ),
    )

    exact_stage = next(
        stage for stage in exact.stages if "replica optimization" in stage.stage
    )
    scalable_stage = next(
        stage for stage in scalable.stages if "replica optimization" in stage.stage
    )
    assert scalable.replica_search_mode == "SCALABLE"
    assert "at most 4 configurations" in scalable_stage.ceiling_work
    assert scalable_stage.configured_ceiling_seconds < (
        exact_stage.configured_ceiling_seconds
    )


def test_ordinary_forecast_excludes_measurement_extension_failure_paths():
    system, _ = _system_and_resources()
    short = calculate_workflow_runtime_estimate(
        system,
        None,
        _rates(),
        options=CalibrationOptions(measurement_max_doublings=1),
        maximum_replicas=10,
        workers=8,
    )
    extended = calculate_workflow_runtime_estimate(
        system,
        None,
        _rates(),
        options=CalibrationOptions(measurement_max_doublings=4),
        maximum_replicas=10,
        workers=8,
    )

    assert extended.total_ordinary_planning_seconds == pytest.approx(
        short.total_ordinary_planning_seconds
    )
    assert extended.total_configured_ceiling_seconds > (
        short.total_configured_ceiling_seconds
    )


def test_ordinary_forecast_excludes_late_warmup_horizon_extensions():
    system, _ = _system_and_resources()
    ordinary = calculate_workflow_runtime_estimate(
        system,
        None,
        _rates(),
        options=CalibrationOptions(warmup_max_horizon_doublings=3),
        maximum_replicas=10,
        workers=8,
    )
    extended = calculate_workflow_runtime_estimate(
        system,
        None,
        _rates(),
        options=CalibrationOptions(warmup_max_horizon_doublings=6),
        maximum_replicas=10,
        workers=8,
    )

    assert extended.total_ordinary_planning_seconds == pytest.approx(
        ordinary.total_ordinary_planning_seconds
    )
    assert extended.total_configured_ceiling_seconds > (
        ordinary.total_configured_ceiling_seconds
    )


def test_runtime_retry_cap_must_be_nonnegative():
    with pytest.raises(ValueError, match="horizon_extensions"):
        CalibrationOptions(warmup_max_validation_horizon_extensions=-1)


def test_warmup_bucket_memory_budget_must_retain_analysis_resolution():
    with pytest.raises(ValueError, match="warmup_max_buckets_per_series"):
        CalibrationOptions(warmup_max_buckets_per_series=499)
    with pytest.raises(ValueError, match="warmup_max_total_bucket_values"):
        CalibrationOptions(warmup_max_total_bucket_values=4_999)


def test_duration_formatter_uses_readable_units():
    assert format_duration(30) == "30.0 seconds"
    assert format_duration(120) == "2.0 minutes"
    assert format_duration(7_200) == "2.0 hours"
    assert format_duration(172_800) == "2.0 days"


def _runtime_profile(system, *, minimum=(1, 1), maximum=(100, 100)):
    return SimpleNamespace(
        profile_fingerprint="profile-fingerprint",
        minimum_replica_configuration=minimum,
        maximum_replica_configuration=maximum,
        warmup_duration=100.0,
        measurement_duration=200.0,
        minimum_optimization_replications=30,
        maximum_optimization_replications=60,
        replication_batch_size=10,
        maximum_sequential_looks=4,
        request_class_slos=tuple(
            (request_class, 1.0) for request_class in system.get_request_classes()
        ),
    )


def test_runtime_adaptive_limit_uses_frozen_profile_and_machine_rates():
    system, _ = _system_and_resources()
    profile = _runtime_profile(system)

    result = calibrate_replica_configuration_limit(
        system,
        profile,
        _rates(),
        workers=8,
        exact_search_time_budget_seconds=600.0,
        hard_configuration_cap=10_000,
        safety_factor=3.0,
    )

    expected_simulation = 3.0 * 2e-5 * 60 * (100 + 200) / 8
    expected_bootstrap = 3.0 * 1e-7 * 3 * 200 * (30 + 40 + 50 + 60)
    expected_per_configuration = expected_simulation + expected_bootstrap
    assert result.configuration_count == 10_000
    assert result.estimated_simulation_seconds_per_configuration == pytest.approx(
        expected_simulation
    )
    assert result.estimated_bootstrap_seconds_per_configuration == pytest.approx(
        expected_bootstrap
    )
    assert result.estimated_seconds_per_configuration == pytest.approx(
        expected_per_configuration
    )
    assert result.runtime_configuration_limit == int(
        600.0 // expected_per_configuration
    )
    assert result.effective_configuration_limit == (result.runtime_configuration_limit)
    assert result.selected_mode == "SCALABLE"
    assert result.projected_exact_search_seconds == pytest.approx(
        10_000 * expected_per_configuration
    )

    report = format_replica_configuration_limit_calibration(result)
    assert "Replica configuration space: 10,000" in report
    assert "Selected replica mode: SCALABLE" in report
    assert "Planning estimate only" in report


def test_runtime_adaptive_limit_never_exceeds_hard_configuration_cap():
    system, _ = _system_and_resources()
    profile = _runtime_profile(
        system,
        minimum=(1, 1),
        maximum=(200, 100),
    )
    rates = RuntimeBenchmarkRates(
        **{
            **_rates().__dict__,
            "measurement_seconds_per_replication_time_unit": 1e-9,
            "bootstrap_seconds_per_replication_time_unit_per_class": 1e-12,
        }
    )

    result = calibrate_replica_configuration_limit(
        system,
        profile,
        rates,
        workers=8,
        exact_search_time_budget_seconds=600.0,
        hard_configuration_cap=10_000,
    )

    assert result.runtime_configuration_limit > 10_000
    assert result.effective_configuration_limit == 10_000
    assert result.configuration_count == 20_000
    assert result.selected_mode == "SCALABLE"


def test_runtime_adaptive_limit_rejects_mismatched_request_classes():
    system, _ = _system_and_resources()
    profile = _runtime_profile(system)
    profile.request_class_slos = (("unknown", 1.0),)

    with pytest.raises(ValueError, match="request classes"):
        calibrate_replica_configuration_limit(
            system,
            profile,
            _rates(),
        )

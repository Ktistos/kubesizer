from pathlib import Path
import pytest
import yaml

from calibration import CalibrationOptions, initial_measurement_duration
from performance_models import QueueingNetworkType, System
from runtime_estimation import (
    RuntimeBenchmarkRates,
    calculate_workflow_runtime_estimate,
    format_duration,
)


ROOT = Path(__file__).resolve().parents[1]


def _system():
    workload = yaml.safe_load((ROOT / "workload.yaml").read_text(encoding="utf-8"))
    return System("runtime-test", workload, QueueingNetworkType.OPEN)


def _rates():
    return RuntimeBenchmarkRates(
        warmup_seconds_per_replication_time_unit=1e-5,
        measurement_seconds_per_replication_time_unit=2e-5,
        bootstrap_seconds_per_replication_time_unit_per_class=1e-7,
        benchmark_wall_time_seconds=0.25,
        benchmark_replications=8,
        warmup_benchmark_horizon=500.0,
        measurement_benchmark_duration=500.0,
    )


def test_estimate_reports_ordinary_and_full_configured_work():
    system = _system()
    options = CalibrationOptions()

    result = calculate_workflow_runtime_estimate(
        system,
        _rates(),
        options=options,
        maximum_replicas=10,
        workers=8,
        safety_factor=3.0,
    )

    assert result.replica_configuration_count == 60_000
    assert tuple(stage.stage for stage in result.stages) == (
        "Stage A — calibration",
        "Stage B — replica optimization",
        "Stage C — final validation",
    )
    assert result.measurement_duration_ceiling == pytest.approx(
        (2 ** (options.measurement_max_doublings - 1))
        * initial_measurement_duration(system, options)
    )
    assert all(
        stage.configured_ceiling_seconds >= stage.ordinary_planning_seconds
        for stage in result.stages
    )
    assert result.total_configured_ceiling_seconds > (
        result.total_ordinary_planning_seconds
    )
    replica_stage = next(
        stage for stage in result.stages if "replica optimization" in stage.stage
    )
    assert "all 60,000 configurations" in replica_stage.ceiling_work


def test_ordinary_forecast_excludes_measurement_extension_failure_paths():
    system = _system()
    short = calculate_workflow_runtime_estimate(
        system,
        _rates(),
        options=CalibrationOptions(measurement_max_doublings=1),
        maximum_replicas=10,
        workers=8,
    )
    extended = calculate_workflow_runtime_estimate(
        system,
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
    system = _system()
    ordinary = calculate_workflow_runtime_estimate(
        system,
        _rates(),
        options=CalibrationOptions(warmup_max_horizon_doublings=3),
        maximum_replicas=10,
        workers=8,
    )
    extended = calculate_workflow_runtime_estimate(
        system,
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

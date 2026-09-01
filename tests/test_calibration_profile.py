from dataclasses import replace

import numpy as np
import pytest
import yaml

from calibration import (
    CalibrationProfileMismatchError,
    build_calibration_profile,
    default_seed_policy,
    load_calibration_profile,
    save_calibration_profile,
    validate_profile_for_experiment,
)
from performance_models import QueueingNetworkType, System


SLOS = {"read": 1.0, "write": 1.5}


def make_system(*, read_gamma=2.0, route_probability=0.5, api_mu=10.0):
    return System(
        "profile-test",
        {
            "read": {
                "api": {
                    "gamma": read_gamma,
                    "mu": api_mu,
                    "routes": {"db": route_probability},
                },
                "db": {"gamma": 0.0, "mu": 8.0},
            },
            "write": {
                "api": {"gamma": 1.0, "mu": api_mu},
                "db": {"gamma": 0.0, "mu": 8.0},
            },
        },
        QueueingNetworkType.OPEN,
    )


def make_profile(system=None, *, calibration_diagnostics=None):
    system = system or make_system()
    minimum = tuple(system.get_minimum_stable_replicas())
    maximum = tuple(value + 3 for value in minimum)
    return build_calibration_profile(
        system,
        maximum_configuration=maximum,
        request_class_slos=SLOS,
        warmup_duration=20.0,
        measurement_duration=100.0,
        measurement_duration_candidates=(50.0, 100.0, 200.0),
        warmup_bucket_width=0.5,
        representative_configurations=(minimum, maximum),
        calibration_diagnostics=(
            calibration_diagnostics
            if calibration_diagnostics is not None
            else {"determining_metric": "active_requests"}
        ),
        creation_timestamp="2026-08-11T12:00:00+00:00",
    )


def test_seed_namespaces_are_disjoint_and_optimization_uses_common_seeds():
    policy = default_seed_policy()
    ordered = sorted(policy.namespaces, key=lambda item: item.start)

    assert all(
        left.stop_exclusive <= right.start
        for left, right in zip(ordered, ordered[1:])
    )
    # Configuration identity is intentionally absent: these are CRN streams.
    configuration_a = policy.seeds("optimization", 30)
    configuration_b = policy.seeds("optimization", 30)
    assert configuration_a == configuration_b
    assert len(set(configuration_a)) == 30
    assert set(configuration_a).isdisjoint(
        policy.seeds("final_validation", 30)
    )
    assert policy.deterministic_seed("bootstrap", "api", 1) == (
        policy.deterministic_seed("bootstrap", "api", 1)
    )


def test_profile_round_trip_is_stable_and_immutable(tmp_path):
    profile = make_profile()
    path = save_calibration_profile(profile, tmp_path / "profile.yaml")

    loaded = load_calibration_profile(path)

    assert loaded == profile
    assert loaded.profile_fingerprint == profile.profile_fingerprint
    assert loaded.diagnostics == {"determining_metric": "active_requests"}
    assert loaded.confidence_level == pytest.approx(0.95)
    assert loaded.to_dict()["confidence_level"] == pytest.approx(0.95)
    with pytest.raises(Exception):
        loaded.warmup_duration = 99.0


def test_profile_diagnostics_accept_numerical_library_scalars():
    profile = make_profile(
        calibration_diagnostics={
            "numpy_boolean": np.bool_(True),
            "numpy_integer": np.int64(7),
            "numpy_float": np.float64(0.25),
        }
    )

    assert profile.diagnostics == {
        "numpy_boolean": True,
        "numpy_integer": 7,
        "numpy_float": 0.25,
    }


def test_profile_rejects_tampered_persisted_values(tmp_path):
    path = save_calibration_profile(make_profile(), tmp_path / "profile.yaml")
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    value["warmup_duration"] = 999.0
    path.write_text(yaml.safe_dump(value), encoding="utf-8")

    with pytest.raises(CalibrationProfileMismatchError, match="fingerprint"):
        load_calibration_profile(path)


@pytest.mark.parametrize(
    ("changed_system", "reason"),
    (
        (make_system(route_probability=0.4), "topology"),
        (make_system(read_gamma=2.1), "workload"),
        (make_system(api_mu=11.0), "workload"),
    ),
)
def test_profile_invalidates_topology_and_workload_changes(changed_system, reason):
    profile = make_profile()

    with pytest.raises(CalibrationProfileMismatchError, match=reason):
        validate_profile_for_experiment(
            profile,
            changed_system,
            request_class_slos=SLOS,
            maximum_configuration=profile.maximum_replica_configuration,
        )


def test_profile_invalidates_slo_bound_and_semantics_changes():
    system = make_system()
    profile = make_profile(system)

    with pytest.raises(CalibrationProfileMismatchError, match="SLO"):
        validate_profile_for_experiment(
            profile,
            system,
            request_class_slos={"read": 0.9, "write": 1.5},
        )
    with pytest.raises(CalibrationProfileMismatchError, match="bounds"):
        validate_profile_for_experiment(
            profile,
            system,
            maximum_configuration=(99, 99),
        )
    incompatible = replace(
        profile,
        simulator_semantics_version="different-kernel",
    )
    # Re-signing simulates a well-formed profile produced by another kernel.
    value = incompatible.to_dict()
    value["profile_fingerprint"] = profile.profile_fingerprint
    with pytest.raises(CalibrationProfileMismatchError):
        incompatible.validate_integrity()


def test_replica_changes_do_not_change_experiment_fingerprints():
    system = make_system()
    profile = make_profile(system)
    configuration = tuple(
        min(high, low + 1)
        for low, high in zip(
            profile.minimum_replica_configuration,
            profile.maximum_replica_configuration,
        )
    )

    system.configure_system_replicas(configuration)

    assert validate_profile_for_experiment(
        profile,
        system,
        request_class_slos=SLOS,
        maximum_configuration=profile.maximum_replica_configuration,
    ) is profile


def test_attached_placement_timing_invalidates_unplaced_profile():
    system = make_system()
    profile = make_profile(system)
    system.configure_simulation_placement(
        placement={"node-a": {"api": 1, "db": 1}},
        communication_latency_seconds={"node-a": {"node-a": 0.0}},
        execution_time_multipliers={"node-a": 3.0},
    )

    with pytest.raises(CalibrationProfileMismatchError, match="topology"):
        validate_profile_for_experiment(
            profile,
            system,
            request_class_slos=SLOS,
            maximum_configuration=profile.maximum_replica_configuration,
        )


def test_fixed_configuration_profile_can_narrow_both_calibrated_bounds():
    system = make_system()
    fixed = tuple(value + 1 for value in system.get_minimum_stable_replicas())

    profile = build_calibration_profile(
        system,
        minimum_configuration=fixed,
        maximum_configuration=fixed,
        request_class_slos=SLOS,
        warmup_duration=20.0,
        measurement_duration=100.0,
        measurement_duration_candidates=(50.0, 100.0, 200.0),
        warmup_bucket_width=0.5,
        representative_configurations=(fixed,),
        calibration_diagnostics={"fixed": True},
        creation_timestamp="2026-08-11T12:00:00+00:00",
    )

    assert profile.minimum_replica_configuration == fixed
    assert profile.maximum_replica_configuration == fixed
    assert profile.representative_configurations == (fixed,)


def test_profile_rejects_insufficient_bootstrap_tail_resolution():
    profile = make_profile()
    inadequate = replace(profile, bootstrap_resample_count=10)

    with pytest.raises(CalibrationProfileMismatchError, match="fingerprint"):
        inadequate.validate_integrity()
